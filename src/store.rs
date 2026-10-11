use std::{
    fmt,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use tokio::sync::broadcast;

use crate::{
    messaging::{
        AckMessageRequest, AckMessageResponse, Message, ReadInboxRequest, ReadInboxResponse,
        SendMessageRequest,
    },
    sessions::Session,
};

#[derive(Debug)]
pub(crate) enum StoreError {
    Invalid(String),
    Database(rusqlite::Error),
    Unavailable(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::Unavailable(message) => f.write_str(message),
            Self::Database(error) => error.fmt(f),
        }
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

#[derive(Clone)]
pub(crate) struct Store {
    connection: Arc<Mutex<Connection>>,
    changes: broadcast::Sender<String>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS sessions (
                 session_id TEXT PRIMARY KEY NOT NULL,
                 runtime_id TEXT NOT NULL,
                 display_name TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS messages (
                 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                 message_id TEXT UNIQUE NOT NULL,
                 sender_id TEXT NOT NULL REFERENCES sessions(session_id),
                 recipient_id TEXT NOT NULL REFERENCES sessions(session_id),
                 payload TEXT NOT NULL,
                 correlation_id TEXT,
                 acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0, 1))
             );
             CREATE INDEX IF NOT EXISTS pending_inbox
                 ON messages(recipient_id, sequence) WHERE acknowledged = 0;
             COMMIT;",
        )?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            changes: broadcast::channel(256).0,
        })
    }

    // SQLite I/O and lock acquisition run off the async HTTP/MCP executor.
    async fn interact<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = connection
                .lock()
                .map_err(|_| StoreError::Unavailable("database lock poisoned".into()))?;
            operation(&mut connection)
        })
        .await
        .map_err(|error| StoreError::Unavailable(error.to_string()))?
    }

    pub async fn register(&self, session: Session) -> Result<Session, StoreError> {
        self.interact(move |connection| {
            for (field, value) in [
                ("session_id", &session.session_id),
                ("runtime_id", &session.runtime_id),
                ("display_name", &session.display_name),
            ] {
                nonblank(field, value)?;
            }
            connection.execute(
                "INSERT INTO sessions(session_id, runtime_id, display_name) VALUES (?1, ?2, ?3)
                 ON CONFLICT(session_id) DO UPDATE SET
                     runtime_id = excluded.runtime_id, display_name = excluded.display_name",
                params![session.session_id, session.runtime_id, session.display_name],
            )?;
            Ok(session)
        })
        .await
    }

    pub async fn list_sessions(&self) -> Result<Vec<Session>, StoreError> {
        self.interact(|connection| {
            let mut statement = connection.prepare(
                "SELECT session_id, runtime_id, display_name FROM sessions ORDER BY session_id",
            )?;
            Ok(statement
                .query_map([], session_from_row)?
                .collect::<rusqlite::Result<_>>()?)
        })
        .await
    }

    pub async fn lookup_session(&self, session_id: String) -> Result<Option<Session>, StoreError> {
        self.interact(move |connection| {
            Ok(connection
                .query_row(
                    "SELECT session_id, runtime_id, display_name FROM sessions WHERE session_id = ?1",
                    [session_id],
                    session_from_row,
                )
                .optional()?)
        })
        .await
    }

    pub async fn send(&self, request: SendMessageRequest) -> Result<Message, StoreError> {
        let message = self
            .interact(move |connection| {
                for (field, value) in [
                    ("message_id", &request.message_id),
                    ("sender_id", &request.sender_id),
                    ("recipient_id", &request.recipient_id),
                ] {
                    nonblank(field, value)?;
                }
                if let Some(id) = &request.correlation_id {
                    nonblank("correlation_id", id)?;
                }

                // Serialize competing sends, including other connections to this database.
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                registered(&transaction, "sender_id", &request.sender_id)?;
                registered(&transaction, "recipient_id", &request.recipient_id)?;
                if let Some(existing) = lookup_message(&transaction, &request.message_id)? {
                    if existing.sender_id != request.sender_id
                        || existing.recipient_id != request.recipient_id
                        || existing.payload != request.payload
                        || existing.correlation_id != request.correlation_id
                    {
                        return Err(StoreError::Invalid(format!(
                            "message_id '{}' already exists with different data",
                            request.message_id
                        )));
                    }
                    transaction.commit()?;
                    return Ok(existing);
                }

                transaction.execute(
                "INSERT INTO messages(message_id, sender_id, recipient_id, payload, correlation_id)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    request.message_id,
                    request.sender_id,
                    request.recipient_id,
                    request.payload.to_string(),
                    request.correlation_id,
                ],
            )?;
                let message = Message {
                    message_id: request.message_id,
                    sender_id: request.sender_id,
                    recipient_id: request.recipient_id,
                    payload: request.payload,
                    correlation_id: request.correlation_id,
                    sequence: transaction.last_insert_rowid(),
                    acknowledged: false,
                };
                transaction.commit()?;
                Ok(message)
            })
            .await?;
        // A signal is only a hint. Publish after commit; inbox/ACK remains authoritative.
        if !message.acknowledged {
            let _ = self.changes.send(message.recipient_id.clone());
        }
        Ok(message)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.changes.subscribe()
    }

    pub async fn read_inbox(
        &self,
        request: ReadInboxRequest,
    ) -> Result<ReadInboxResponse, StoreError> {
        self.interact(move |connection| {
            nonblank("session_id", &request.session_id)?;
            let cursor = request.cursor.unwrap_or(0);
            let limit = request.limit.unwrap_or(100);
            if cursor < 0 {
                return Err(StoreError::Invalid("cursor must be nonnegative".into()));
            }
            if !(1..=1000).contains(&limit) {
                return Err(StoreError::Invalid(
                    "limit must be between 1 and 1000".into(),
                ));
            }
            registered(connection, "session_id", &request.session_id)?;
            let mut statement = connection.prepare(
                "SELECT message_id, sender_id, recipient_id, payload, correlation_id,
                        sequence, acknowledged
                 FROM messages
                 WHERE recipient_id = ?1 AND acknowledged = 0 AND sequence > ?2
                 ORDER BY sequence LIMIT ?3",
            )?;
            let mut messages: Vec<Message> = statement
                .query_map(
                    params![request.session_id, cursor, limit + 1],
                    message_from_row,
                )?
                .collect::<rusqlite::Result<_>>()?;
            let next_cursor = if messages.len() > limit as usize {
                messages.pop();
                messages.last().map(|message| message.sequence)
            } else {
                None
            };
            Ok(ReadInboxResponse {
                messages,
                next_cursor,
            })
        })
        .await
    }

    pub async fn ack(&self, request: AckMessageRequest) -> Result<AckMessageResponse, StoreError> {
        self.interact(move |connection| {
            nonblank("session_id", &request.session_id)?;
            nonblank("message_id", &request.message_id)?;
            // The recipient predicate makes ACK atomic and rejects other recipients.
            let updated = connection.execute(
                "UPDATE messages SET acknowledged = 1 WHERE message_id = ?1 AND recipient_id = ?2",
                params![request.message_id, request.session_id],
            )?;
            if updated == 0 {
                return Err(StoreError::Invalid(
                    "message does not exist for this recipient".into(),
                ));
            }
            Ok(AckMessageResponse {
                session_id: request.session_id,
                message_id: request.message_id,
                acknowledged: true,
            })
        })
        .await
    }
}

fn nonblank(field: &str, value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() {
        return Err(StoreError::Invalid(format!("{field} must not be blank")));
    }
    Ok(())
}

fn registered(connection: &Connection, field: &str, id: &str) -> Result<(), StoreError> {
    let exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = ?1)",
        [id],
        |row| row.get(0),
    )?;
    if !exists {
        return Err(StoreError::Invalid(format!("unknown {field}: '{id}'")));
    }
    Ok(())
}

fn session_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Session> {
    Ok(Session {
        session_id: row.get(0)?,
        runtime_id: row.get(1)?,
        display_name: row.get(2)?,
    })
}

fn lookup_message(connection: &Connection, id: &str) -> rusqlite::Result<Option<Message>> {
    connection
        .query_row(
            "SELECT message_id, sender_id, recipient_id, payload, correlation_id,
                    sequence, acknowledged FROM messages WHERE message_id = ?1",
            [id],
            message_from_row,
        )
        .optional()
}

fn message_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Message> {
    let payload: String = row.get(3)?;
    let payload = serde_json::from_str(&payload).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(Message {
        message_id: row.get(0)?,
        sender_id: row.get(1)?,
        recipient_id: row.get(2)?,
        payload,
        correlation_id: row.get(4)?,
        sequence: row.get(5)?,
        acknowledged: row.get(6)?,
    })
}
