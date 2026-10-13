use std::time::Duration;

use rmcp::{
    ClientLifecycleMode, ClientServiceExt, ServiceExt,
    model::{CallToolRequestParams, ClientConfig, ProtocolVersion},
    transport::StreamableHttpClientTransport,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn http_health_and_mcp_round_trip() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let app = agent_hub::router(cancellation.clone(), ":memory:").unwrap();
        let shutdown = cancellation.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });

        let health = reqwest::get(format!("http://{address}/health"))
            .await
            .unwrap();
        assert!(health.status().is_success());
        assert_eq!(health.text().await.unwrap(), "ok");

        // Verify both initialize (legacy clients) and server/discover (modern clients).
        for modern in [false, true] {
            let transport =
                StreamableHttpClientTransport::from_uri(format!("http://{address}/mcp"));
            let client = if modern {
                ClientConfig::default()
                    .serve_with_lifecycle(
                        transport,
                        ClientLifecycleMode::Discover {
                            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                        },
                    )
                    .await
                    .unwrap()
            } else {
                ClientConfig::default()
                    .with_protocol_version(ProtocolVersion::V_2025_11_25)
                    .serve(transport)
                    .await
                    .unwrap()
            };

            let tools = client.list_all_tools().await.unwrap();
            let mut names: Vec<_> = tools.iter().map(|tool| tool.name.as_ref()).collect();
            names.sort_unstable();
            assert_eq!(
                names,
                [
                    "hub_ack_message",
                    "hub_acquire_lock",
                    "hub_list_sessions",
                    "hub_lookup_session",
                    "hub_ping",
                    "hub_read_inbox",
                    "hub_release_lock",
                    "hub_send_message",
                ]
            );
            assert!(tools.iter().all(|tool| tool.output_schema.is_some()));
            let message = "Hello, Codex! 🦀";
            let result = client
                .call_tool(
                    CallToolRequestParams::new("hub_ping")
                        .with_arguments(json!({ "message": message }).as_object().unwrap().clone()),
                )
                .await
                .unwrap();
            assert_eq!(result.is_error, Some(false));
            assert_eq!(
                result.structured_content,
                Some(json!({
                    "message": message,
                    "server": "agent-hub",
                    "version": env!("CARGO_PKG_VERSION"),
                }))
            );

            let invalid = client
                .call_tool(
                    CallToolRequestParams::new("hub_ping")
                        .with_arguments(json!({ "message": 42 }).as_object().unwrap().clone()),
                )
                .await
                .unwrap();
            assert_eq!(
                invalid.is_error,
                Some(true),
                "Non-string messages must be rejected"
            );
            client.cancel().await.unwrap();
        }

        cancellation.cancel();
        server.await.unwrap();
    })
    .await
    .expect("MCP round trip timed out");
}
