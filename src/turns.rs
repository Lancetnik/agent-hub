use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    messaging::{SendMessageRequest, SendMessageResponse},
    store::{Store, StoreError},
};

/// A terminal runtime status describes a turn, not the success of an assigned task.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum TurnStatus {
    Completed,
    Failed,
    Interrupted,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TurnResult {
    session_id: String,
    runtime_id: String,
    turn_id: String,
    recipient_id: String,
    correlation_id: String,
    status: TurnStatus,
    final_text: Option<String>,
    #[serde(default)]
    artifact_refs: Vec<String>,
    error: Option<Value>,
}

#[derive(Serialize)]
pub(crate) struct TurnResultError {
    error: String,
}

pub(crate) async fn record(
    State(store): State<Store>,
    Json(result): Json<TurnResult>,
) -> Result<Json<SendMessageResponse>, (StatusCode, Json<TurnResultError>)> {
    for (field, value) in [
        ("session_id", &result.session_id),
        ("runtime_id", &result.runtime_id),
        ("turn_id", &result.turn_id),
        ("recipient_id", &result.recipient_id),
        ("correlation_id", &result.correlation_id),
    ] {
        if value.trim().is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(TurnResultError {
                    error: format!("{field} must not be blank"),
                }),
            ));
        }
    }

    // JSON tuple encoding is unambiguous even when IDs contain delimiters or Unicode.
    // A different recipient or correlation cannot create a second result for this turn.
    let message_id = format!(
        "turn-result:{}",
        json!([result.session_id, result.runtime_id, result.turn_id])
    );
    let request = SendMessageRequest {
        message_id,
        sender_id: result.session_id.clone(),
        recipient_id: result.recipient_id.clone(),
        correlation_id: Some(result.correlation_id.clone()),
        payload: json!({
            "type": "turn_completed",
            "runtime_id": result.runtime_id,
            "turn_id": result.turn_id,
            "status": result.status,
            "final_text": result.final_text,
            "artifact_refs": result.artifact_refs,
            "error": result.error,
        }),
    };
    store
        .send(request)
        .await
        .map(|message| Json(SendMessageResponse { message }))
        .map_err(|error| {
            let (status, error) = match error {
                StoreError::Invalid(message) => (StatusCode::BAD_REQUEST, message),
                error => {
                    tracing::error!(%error, "Turn result persistence failed");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "database operation failed".into(),
                    )
                }
            };
            (status, Json(TurnResultError { error }))
        })
}
