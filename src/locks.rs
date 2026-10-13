//! Cooperative resource leases: atomic FIFO handoff and durable inbox notifications.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use rmcp::schemars;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::store::{Store, StoreError, nonblank, registered};

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcquireLock {
    pub session_id: String,
    /// Globally unique ticket. Replay identical arguments to inspect status; never renews TTL.
    pub request_id: String,
    /// Shared resource key; use the absolute file path including the checkout.
    pub resource: String,
    /// Lease starts on grant, not while waiting. 1..600 seconds; default 120.
    pub ttl_seconds: Option<u32>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseLock {
    pub session_id: String,
    /// Exact ticket to release or cancel. Cannot release a later owner's ticket.
    pub request_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LockStatus {
    Acquired,
    Waiting,
    Released,
    Expired,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub(crate) struct LockTicket {
    pub request_id: String,
    pub session_id: String,
    pub resource: String,
    pub ttl_seconds: u32,
    pub status: LockStatus,
    pub expires_at_ms: Option<i64>,
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn row_ticket(row: &rusqlite::Row<'_>) -> rusqlite::Result<LockTicket> {
    let status: String = row.get(4)?;
    Ok(LockTicket {
        request_id: row.get(0)?,
        session_id: row.get(1)?,
        resource: row.get(2)?,
        ttl_seconds: row.get(3)?,
        status: match status.as_str() {
            "acquired" => LockStatus::Acquired,
            "waiting" => LockStatus::Waiting,
            "released" => LockStatus::Released,
            "expired" => LockStatus::Expired,
            "cancelled" => LockStatus::Cancelled,
            _ => unreachable!("SQL status constraint"),
        },
        expires_at_ms: row.get(5)?,
    })
}

fn lookup(db: &Connection, id: &str) -> rusqlite::Result<Option<LockTicket>> {
    db.query_row("SELECT request_id,session_id,resource,ttl_seconds,status,expires_at_ms FROM lock_requests WHERE request_id=?1",
                 [id], row_ticket).optional()
}

fn event(
    db: &Connection,
    sender: &str,
    ticket: &LockTicket,
    kind: &str,
    recipients: &mut Vec<String>,
) -> Result<(), StoreError> {
    let body = json!({"type":kind,"request_id":ticket.request_id,"session_id":ticket.session_id,
                      "resource":ticket.resource,"ttl_seconds":ticket.ttl_seconds,"expires_at_ms":ticket.expires_at_ms});
    let message_id = format!("hub-lock:{}", json!([kind, ticket.request_id]));
    db.execute("INSERT INTO messages(message_id,sender_id,recipient_id,payload,correlation_id) VALUES(?1,?2,?3,?4,?5)",
               params![message_id,sender,ticket.session_id,body.to_string(),ticket.request_id])?;
    recipients.push(ticket.session_id.clone());
    Ok(())
}

fn grant_next(
    db: &Connection,
    resource: &str,
    sender: &str,
    now: i64,
    recipients: &mut Vec<String>,
) -> Result<(), StoreError> {
    let next = db.query_row("SELECT request_id FROM lock_requests WHERE resource=?1 AND status='waiting' ORDER BY sequence LIMIT 1",
                            [resource], |row| row.get::<_,String>(0)).optional()?;
    if let Some(id) = next {
        db.execute("UPDATE lock_requests SET status='acquired',expires_at_ms=?2+ttl_seconds*1000 WHERE request_id=?1", params![id,now])?;
        event(
            db,
            sender,
            &lookup(db, &id)?.unwrap(),
            "lock_granted",
            recipients,
        )?;
    }
    Ok(())
}

fn expire(db: &Connection, now: i64, recipients: &mut Vec<String>) -> Result<(), StoreError> {
    let due = {
        let mut query = db.prepare("SELECT request_id FROM lock_requests WHERE status='acquired' AND expires_at_ms<=?1 ORDER BY sequence")?;
        query
            .query_map([now], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    for id in due {
        db.execute(
            "UPDATE lock_requests SET status='expired' WHERE request_id=?1",
            [&id],
        )?;
        let ticket = lookup(db, &id)?.unwrap();
        event(db, &ticket.session_id, &ticket, "lock_expired", recipients)?;
        grant_next(db, &ticket.resource, &ticket.session_id, now, recipients)?;
    }
    Ok(())
}

impl Store {
    fn signal_locks(&self, recipients: Vec<String>) {
        for id in recipients {
            let _ = self.changes.send(id);
        }
        self.lock_changed.notify_one();
    }

    pub(crate) async fn acquire_lock(
        &self,
        request: AcquireLock,
    ) -> Result<LockTicket, StoreError> {
        let clock = self.clock.clone();
        let (ticket, recipients) = self.interact(move |db| {
            nonblank("session_id",&request.session_id)?; nonblank("request_id",&request.request_id)?; nonblank("resource",&request.resource)?;
            let ttl = request.ttl_seconds.unwrap_or(120);
            if !(1..=600).contains(&ttl) { return Err(StoreError::Invalid("ttl_seconds must be between 1 and 600".into())); }
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            registered(&tx,"session_id",&request.session_id)?;
            let now = clock(); let mut recipients = Vec::new(); expire(&tx,now,&mut recipients)?;
            if let Some(ticket) = lookup(&tx,&request.request_id)? {
                if ticket.session_id != request.session_id || ticket.resource != request.resource || ticket.ttl_seconds != ttl {
                    return Err(StoreError::Invalid("request_id already exists with different arguments".into()));
                }
                tx.commit()?; return Ok((ticket,recipients));
            }
            let duplicate: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM lock_requests WHERE resource=?1 AND session_id=?2 AND status IN ('waiting','acquired'))",
                params![request.resource,request.session_id], |row| row.get(0))?;
            if duplicate { return Err(StoreError::Invalid("Session already holds or waits for this resource; reuse its request_id".into())); }
            let busy: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM lock_requests WHERE resource=?1 AND status='acquired')", [&request.resource], |row| row.get(0))?;
            tx.execute("INSERT INTO lock_requests(request_id,session_id,resource,ttl_seconds,status,expires_at_ms) VALUES(?1,?2,?3,?4,?5,?6)",
                params![request.request_id,request.session_id,request.resource,ttl,if busy {"waiting"} else {"acquired"},if busy {None} else {Some(now+i64::from(ttl)*1000)}])?;
            let ticket = lookup(&tx,&request.request_id)?.unwrap(); tx.commit()?; Ok((ticket,recipients))
        }).await?;
        self.signal_locks(recipients);
        Ok(ticket)
    }

    pub(crate) async fn release_lock(
        &self,
        request: ReleaseLock,
    ) -> Result<LockTicket, StoreError> {
        let clock = self.clock.clone();
        let (ticket, recipients) = self
            .interact(move |db| {
                nonblank("session_id", &request.session_id)?;
                nonblank("request_id", &request.request_id)?;
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let ticket = lookup(&tx, &request.request_id)?
                    .ok_or_else(|| StoreError::Invalid("Unknown lock request_id".into()))?;
                if ticket.session_id != request.session_id {
                    return Err(StoreError::Invalid(
                        "Lock request belongs to another session".into(),
                    ));
                }
                let now = clock();
                let mut recipients = Vec::new();
                expire(&tx, now, &mut recipients)?;
                match lookup(&tx, &request.request_id)?.unwrap().status {
                    LockStatus::Acquired => {
                        tx.execute(
                            "UPDATE lock_requests SET status='released' WHERE request_id=?1",
                            [&request.request_id],
                        )?;
                        grant_next(
                            &tx,
                            &ticket.resource,
                            &ticket.session_id,
                            now,
                            &mut recipients,
                        )?;
                    }
                    LockStatus::Waiting => {
                        tx.execute(
                            "UPDATE lock_requests SET status='cancelled' WHERE request_id=?1",
                            [&request.request_id],
                        )?;
                    }
                    _ => {}
                }
                let ticket = lookup(&tx, &request.request_id)?.unwrap();
                tx.commit()?;
                Ok((ticket, recipients))
            })
            .await?;
        self.signal_locks(recipients);
        Ok(ticket)
    }

    pub(crate) async fn lock_ticket(
        &self,
        session_id: String,
        request_id: String,
    ) -> Result<LockTicket, StoreError> {
        self.expire_locks().await?;
        self.interact(move |db| {
            let ticket = lookup(db, &request_id)?
                .ok_or_else(|| StoreError::Invalid("Unknown lock request_id".into()))?;
            if ticket.session_id != session_id {
                return Err(StoreError::Invalid(
                    "Lock request belongs to another session".into(),
                ));
            }
            Ok(ticket)
        })
        .await
    }

    pub(crate) async fn expire_locks(&self) -> Result<(), StoreError> {
        let clock = self.clock.clone();
        let recipients = self
            .interact(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let mut recipients = Vec::new();
                expire(&tx, clock(), &mut recipients)?;
                tx.commit()?;
                Ok(recipients)
            })
            .await?;
        self.signal_locks(recipients);
        Ok(())
    }

    async fn lock_deadline(&self) -> Result<Option<i64>, StoreError> {
        self.interact(|db| {
            Ok(db.query_row(
                "SELECT MIN(expires_at_ms) FROM lock_requests WHERE status='acquired'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
    }
}

pub(crate) fn supervise(store: Store, cancellation: CancellationToken) {
    tokio::spawn(async move {
        if let Err(error) = store.expire_locks().await {
            tracing::error!(%error,"Lock recovery failed");
            return;
        }
        loop {
            let changed = store.lock_changed.notified();
            let deadline = match store.lock_deadline().await {
                Ok(value) => value,
                Err(error) => {
                    tracing::error!(%error,"Lock timer failed");
                    return;
                }
            };
            let delay =
                deadline.map(|at| Duration::from_millis((at - (store.clock)()).max(0) as u64));
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = changed => {},
                _ = async { match delay { Some(value) => tokio::time::sleep(value).await, None => std::future::pending::<()>().await } } => {
                    if let Err(error) = store.expire_locks().await { tracing::error!(%error,"Lock expiry failed"); return; }
                }
            }
        }
    });
}

fn http_error(error: StoreError) -> (StatusCode, Json<Value>) {
    match error {
        StoreError::Invalid(message) => (StatusCode::BAD_REQUEST, Json(json!({"error":message}))),
        error => {
            tracing::error!(%error,"Lock storage failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error":"database operation failed"})),
            )
        }
    }
}
pub(crate) async fn acquire(
    State(store): State<Store>,
    Json(request): Json<AcquireLock>,
) -> Result<Json<LockTicket>, (StatusCode, Json<Value>)> {
    store
        .acquire_lock(request)
        .await
        .map(Json)
        .map_err(http_error)
}
pub(crate) async fn release(
    State(store): State<Store>,
    Json(request): Json<ReleaseLock>,
) -> Result<Json<LockTicket>, (StatusCode, Json<Value>)> {
    store
        .release_lock(request)
        .await
        .map(Json)
        .map_err(http_error)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnerQuery {
    session_id: String,
}
pub(crate) async fn get(
    State(store): State<Store>,
    Path(request_id): Path<String>,
    Query(owner): Query<OwnerQuery>,
) -> Result<Json<LockTicket>, (StatusCode, Json<Value>)> {
    store
        .lock_ticket(owner.session_id, request_id)
        .await
        .map(Json)
        .map_err(http_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{messaging::ReadInboxRequest, sessions::Session};
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };

    async fn setup(path: &str, clock: Arc<AtomicI64>) -> Store {
        let store =
            Store::open_with_clock(path, Arc::new(move || clock.load(Ordering::SeqCst))).unwrap();
        for id in ["a", "b", "c"] {
            store
                .register(Session {
                    session_id: id.into(),
                    runtime_id: id.into(),
                    display_name: id.into(),
                })
                .await
                .unwrap();
        }
        store
    }
    fn acquire(id: &str, ttl: u32) -> AcquireLock {
        AcquireLock {
            session_id: id.into(),
            request_id: id.into(),
            resource: "file".into(),
            ttl_seconds: Some(ttl),
        }
    }
    async fn inbox(store: &Store, id: &str) -> Vec<crate::messaging::Message> {
        store
            .read_inbox(ReadInboxRequest {
                session_id: id.into(),
                cursor: None,
                limit: None,
            })
            .await
            .unwrap()
            .messages
    }

    #[tokio::test]
    async fn expiry_starts_waiters_full_ttl_and_replay_never_renews_or_reacquires() {
        let clock = Arc::new(AtomicI64::new(1000));
        let store = setup(":memory:", clock.clone()).await;
        assert_eq!(
            store
                .acquire_lock(acquire("a", 1))
                .await
                .unwrap()
                .expires_at_ms,
            Some(2000)
        );
        assert_eq!(
            store
                .acquire_lock(acquire("b", 2))
                .await
                .unwrap()
                .expires_at_ms,
            None
        );
        clock.store(1999, Ordering::SeqCst);
        assert_eq!(
            store
                .acquire_lock(acquire("a", 1))
                .await
                .unwrap()
                .expires_at_ms,
            Some(2000)
        );
        clock.store(2000, Ordering::SeqCst);
        store.expire_locks().await.unwrap();
        let b = store.lock_ticket("b".into(), "b".into()).await.unwrap();
        assert_eq!(b.status, LockStatus::Acquired);
        assert_eq!(b.expires_at_ms, Some(4000));
        assert_eq!(
            store.acquire_lock(acquire("a", 1)).await.unwrap().status,
            LockStatus::Expired
        );
        assert_eq!(
            store
                .release_lock(ReleaseLock {
                    session_id: "a".into(),
                    request_id: "a".into()
                })
                .await
                .unwrap()
                .status,
            LockStatus::Expired
        );
        assert_eq!(
            store
                .lock_ticket("b".into(), "b".into())
                .await
                .unwrap()
                .status,
            LockStatus::Acquired
        );
        store.expire_locks().await.unwrap();
        assert_eq!(inbox(&store, "b").await.len(), 1);
        assert_eq!(inbox(&store, "a").await[0].payload["type"], "lock_expired");
    }

    #[tokio::test]
    async fn cancelled_waiter_is_skipped_and_other_sessions_cannot_release_a_ticket() {
        let store = setup(":memory:", Arc::new(AtomicI64::new(1000))).await;
        for id in ["a", "b", "c"] {
            store.acquire_lock(acquire(id, 60)).await.unwrap();
        }
        assert!(
            store
                .release_lock(ReleaseLock {
                    session_id: "b".into(),
                    request_id: "a".into()
                })
                .await
                .is_err()
        );
        assert_eq!(
            store
                .release_lock(ReleaseLock {
                    session_id: "b".into(),
                    request_id: "b".into()
                })
                .await
                .unwrap()
                .status,
            LockStatus::Cancelled
        );
        store
            .release_lock(ReleaseLock {
                session_id: "a".into(),
                request_id: "a".into(),
            })
            .await
            .unwrap();
        assert!(inbox(&store, "b").await.is_empty());
        assert_eq!(inbox(&store, "c").await[0].payload["request_id"], "c");
    }

    #[tokio::test]
    async fn concurrent_acquisition_has_one_owner_and_unrelated_resources_stay_available() {
        let store = setup(":memory:", Arc::new(AtomicI64::new(1000))).await;
        let (a, b, c) = tokio::join!(
            store.acquire_lock(acquire("a", 60)),
            store.acquire_lock(acquire("b", 60)),
            store.acquire_lock(acquire("c", 60))
        );
        let states = [a.unwrap().status, b.unwrap().status, c.unwrap().status];
        assert_eq!(
            states
                .iter()
                .filter(|s| **s == LockStatus::Acquired)
                .count(),
            1
        );
        assert_eq!(
            states.iter().filter(|s| **s == LockStatus::Waiting).count(),
            2
        );
        let mut other = acquire("a", 60);
        other.resource = "other-file".into();
        other.request_id = "other".into();
        assert_eq!(
            store.acquire_lock(other).await.unwrap().status,
            LockStatus::Acquired
        );
    }

    #[tokio::test]
    async fn invalid_ttl_changed_request_and_duplicate_ticket_are_rejected() {
        let store = setup(":memory:", Arc::new(AtomicI64::new(1000))).await;
        for ttl in [0, 601] {
            assert!(store.acquire_lock(acquire("a", ttl)).await.is_err());
        }
        assert!(store.acquire_lock(acquire("missing", 10)).await.is_err());
        let mut blank = acquire("a", 10);
        blank.resource = " ".into();
        assert!(store.acquire_lock(blank).await.is_err());
        store.acquire_lock(acquire("a", 10)).await.unwrap();
        assert!(store.acquire_lock(acquire("a", 20)).await.is_err());
        let mut duplicate = acquire("a", 10);
        duplicate.request_id = "duplicate".into();
        assert!(store.acquire_lock(duplicate).await.is_err());
    }

    #[tokio::test]
    async fn restart_preserves_tickets_and_recovers_expired_owner_without_duplicate_notifications()
    {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("leases.sqlite3");
        let clock = Arc::new(AtomicI64::new(1000));
        let store = setup(path.to_str().unwrap(), clock.clone()).await;
        store.acquire_lock(acquire("a", 1)).await.unwrap();
        store.acquire_lock(acquire("b", 2)).await.unwrap();
        drop(store);
        clock.store(9000, Ordering::SeqCst);
        let store = setup(path.to_str().unwrap(), clock).await;
        store.expire_locks().await.unwrap();
        assert_eq!(
            store
                .lock_ticket("b".into(), "b".into())
                .await
                .unwrap()
                .expires_at_ms,
            Some(11000)
        );
        store.expire_locks().await.unwrap();
        assert_eq!(inbox(&store, "b").await.len(), 1);
    }
}
