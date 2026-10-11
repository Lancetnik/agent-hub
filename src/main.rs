use std::net::SocketAddr;

use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "agent_hub=info,rmcp=warn".into()),
        )
        .init();

    let address: SocketAddr = std::env::var("AGENT_HUB_BIND")
        .unwrap_or_else(|_| "127.0.0.1:7331".into())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    let address = listener.local_addr()?;
    let cancellation = CancellationToken::new();
    let app = agent_hub::router(cancellation.clone());

    tracing::info!(%address, "Agent Hub listening; MCP at /mcp, health at /health");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(%error, "Failed to listen for Ctrl-C");
            }
            cancellation.cancel();
        })
        .await?;
    Ok(())
}
