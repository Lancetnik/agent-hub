mod mcp;
mod sessions;

use axum::{
    Router,
    routing::{get, post},
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use tokio_util::sync::CancellationToken;

/// HTTP and MCP share a listener. Legacy MCP clients retain session support.
pub fn router(cancellation: CancellationToken) -> Router {
    let registry = sessions::SessionRegistry::default();
    let mcp_registry = registry.clone();
    let service = StreamableHttpService::new(
        move || Ok(mcp::AgentHub::new(mcp_registry.clone())),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default()
            .with_json_response(true)
            .with_cancellation_token(cancellation),
    );

    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/sessions", post(sessions::register))
        .nest_service("/mcp", service)
        .with_state(registry)
}
