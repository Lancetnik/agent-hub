mod mcp;
mod messaging;
mod sessions;
mod store;

use std::path::Path;

use axum::{
    Router,
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
    let mcp_store = store.clone();
    let service = StreamableHttpService::new(
        move || Ok(mcp::AgentHub::new(mcp_store.clone())),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default()
            .with_json_response(true)
            .with_cancellation_token(cancellation),
    );

    Ok(Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/sessions", post(sessions::register))
        .nest_service("/mcp", service)
        .with_state(store))
}
