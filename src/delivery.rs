use std::{convert::Infallible, time::Duration};

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::{Stream, stream};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::{
    messaging::{AckMessageRequest, AckMessageResponse, ReadInboxRequest, ReadInboxResponse},
    store::{Store, StoreError},
};

type HttpError = (StatusCode, Json<Value>);

fn http_error(error: StoreError) -> HttpError {
    match error {
        StoreError::Invalid(message) => (StatusCode::BAD_REQUEST, Json(json!({"error": message}))),
        error => {
            tracing::error!(%error, "Delivery storage operation failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "database operation failed"})),
            )
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InboxQuery {
    cursor: Option<i64>,
    limit: Option<u32>,
}

pub(crate) async fn inbox(
    State(store): State<Store>,
    Path(session_id): Path<String>,
    Query(query): Query<InboxQuery>,
) -> Result<Json<ReadInboxResponse>, HttpError> {
    store
        .read_inbox(ReadInboxRequest {
            session_id,
            cursor: query.cursor,
            limit: query.limit,
        })
        .await
        .map(Json)
        .map_err(http_error)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AckBody {
    message_id: String,
}

pub(crate) async fn ack(
    State(store): State<Store>,
    Path(session_id): Path<String>,
    Json(body): Json<AckBody>,
) -> Result<Json<AckMessageResponse>, HttpError> {
    store
        .ack(AckMessageRequest {
            session_id,
            message_id: body.message_id,
        })
        .await
        .map(Json)
        .map_err(http_error)
}

pub(crate) async fn events(
    State(store): State<Store>,
    Path(session_id): Path<String>,
    Extension(cancellation): Extension<CancellationToken>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, HttpError> {
    // Subscribe before checking/reading storage so concurrent sends cannot fall in a gap.
    let changes = store.subscribe();
    if store
        .lookup_session(session_id.clone())
        .await
        .map_err(http_error)?
        .is_none()
    {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "unknown session_id"})),
        ));
    }
    let signals = stream::unfold(
        (changes, session_id, cancellation, true),
        |(mut changes, session_id, cancellation, initial)| async move {
            if cancellation.is_cancelled() {
                return None;
            }
            if !initial {
                loop {
                    tokio::select! {
                        _ = cancellation.cancelled() => return None,
                        signal = changes.recv() => match signal {
                            Ok(recipient) if recipient == session_id => break,
                            Ok(_) => continue,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                        }
                    }
                }
            }
            // Always signal on (re)connect. SSE neither carries nor acknowledges messages.
            let event = Event::default().event("inbox_changed").data("{}");
            Some((Ok(event), (changes, session_id, cancellation, false)))
        },
    );
    Ok(Sse::new(signals).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(2))
            .text("poll"),
    ))
}
