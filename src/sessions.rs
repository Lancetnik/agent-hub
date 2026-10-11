use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use axum::{Json, extract::State, http::StatusCode};
use rmcp::schemars;
use serde::{Deserialize, Serialize};

/// An agent session's identity, independent of its MCP connections.
#[derive(Clone, Debug, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Session {
    /// Stable ID supplied by the external integration. Reuse it when registering again.
    pub session_id: String,
    /// Session/thread ID in the external runtime, such as a Codex thread ID.
    pub runtime_id: String,
    /// Human-readable name; it does not need to be unique.
    pub display_name: String,
}

#[derive(Clone, Default)]
pub(crate) struct SessionRegistry {
    sessions: Arc<RwLock<BTreeMap<String, Session>>>,
}

impl SessionRegistry {
    pub fn register(&self, session: Session) -> Result<Session, String> {
        for (field, value) in [
            ("session_id", &session.session_id),
            ("runtime_id", &session.runtime_id),
            ("display_name", &session.display_name),
        ] {
            if value.trim().is_empty() {
                return Err(format!("{field} must not be blank"));
            }
        }

        self.sessions
            .write()
            .expect("session registry lock poisoned")
            .insert(session.session_id.clone(), session.clone());
        Ok(session)
    }

    pub fn list(&self) -> Vec<Session> {
        self.sessions
            .read()
            .expect("session registry lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn lookup(&self, session_id: &str) -> Option<Session> {
        self.sessions
            .read()
            .expect("session registry lock poisoned")
            .get(session_id)
            .cloned()
    }
}

#[derive(Serialize)]
pub(crate) struct RegistrationError {
    error: String,
}

pub(crate) async fn register(
    State(registry): State<SessionRegistry>,
    Json(session): Json<Session>,
) -> Result<Json<Session>, (StatusCode, Json<RegistrationError>)> {
    registry
        .register(session)
        .map(Json)
        .map_err(|error| (StatusCode::BAD_REQUEST, Json(RegistrationError { error })))
}
