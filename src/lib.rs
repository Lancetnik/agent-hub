mod delivery;
mod locks;
mod mcp;
mod messaging;
mod sessions;
mod store;
mod turns;

use std::path::Path;

use axum::{
    Extension, Router,
    routing::{get, post},
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use tokio_util::sync::CancellationToken;

/// HTTP and MCP share a listener. Legacy MCP clients retain session support.
/// Opens a SQLite database, creating its schema if necessary. Use `:memory:` for isolated tests.
pub fn router(
    cancellation: CancellationToken,
    database: impl AsRef<Path>,
) -> rusqlite::Result<Router> {
    let store = store::Store::open(database)?;
    locks::supervise(store.clone(), cancellation.clone());
    let mcp_store = store.clone();
    let service = StreamableHttpService::new(
        move || Ok(mcp::AgentHub::new(mcp_store.clone())),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default()
            .with_json_response(true)
            .with_cancellation_token(cancellation.clone()),
    );

    Ok(Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/sessions", post(sessions::register))
        .route("/turn-results", post(turns::record))
        .route("/locks/acquire", post(locks::acquire))
        .route("/locks/release", post(locks::release))
        .route("/locks/requests/{request_id}", get(locks::get))
        .route("/sessions/{session_id}/inbox", get(delivery::inbox))
        .route("/sessions/{session_id}/ack", post(delivery::ack))
        .route("/sessions/{session_id}/events", get(delivery::events))
        .nest_service("/mcp", service)
        .layer(Extension(cancellation))
        .with_state(store))
}
