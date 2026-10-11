use axum::{Json, extract::State, http::StatusCode};
use rmcp::schemars;
use serde::{Deserialize, Serialize};

use crate::store::{Store, StoreError};

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

#[derive(Serialize)]
pub(crate) struct RegistrationError {
    error: String,
}

pub(crate) async fn register(
    State(store): State<Store>,
    Json(session): Json<Session>,
) -> Result<Json<Session>, (StatusCode, Json<RegistrationError>)> {
    store.register(session).await.map(Json).map_err(|error| {
        let (status, error) = match error {
            StoreError::Invalid(message) => (StatusCode::BAD_REQUEST, message),
            error => {
                tracing::error!(%error, "Session registration failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "database operation failed".into(),
                )
            }
        };
        (status, Json(RegistrationError { error }))
    })
}
