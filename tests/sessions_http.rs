use std::time::Duration;

use rmcp::{
    ClientLifecycleMode, ClientServiceExt, Peer, RoleClient, ServiceExt,
    model::{CallToolRequestParams, ClientConfig, ProtocolVersion},
    transport::StreamableHttpClientTransport,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

async fn call(client: &Peer<RoleClient>, tool: &str, arguments: Value) -> Value {
    let result = client
        .call_tool(
            CallToolRequestParams::new(tool.to_owned())
                .with_arguments(arguments.as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false));
    result.structured_content.unwrap()
}

#[tokio::test]
async fn http_registration_is_shared_by_mcp_clients_and_upserts_by_stable_id() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let app = agent_hub::router(cancellation.clone(), ":memory:").unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        let legacy = ClientConfig::default()
            .with_protocol_version(ProtocolVersion::V_2025_11_25)
            .serve(StreamableHttpClientTransport::from_uri(format!(
                "{base}/mcp"
            )))
            .await
            .unwrap();
        let modern = ClientConfig::default()
            .serve_with_lifecycle(
                StreamableHttpClientTransport::from_uri(format!("{base}/mcp")),
                ClientLifecycleMode::Discover {
                    preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                },
            )
            .await
            .unwrap();
        assert_eq!(
            call(&legacy, "hub_list_sessions", json!({})).await,
            json!({"sessions": []})
        );
        assert_eq!(
            call(
                &modern,
                "hub_lookup_session",
                json!({"session_id": "unknown"})
            )
            .await,
            json!({"session": null}),
        );

        let http = reqwest::Client::new();
        let orchestrator = json!({
            "session_id": "orchestrator",
            "runtime_id": "codex-thread-a",
            "display_name": "Оркестратор 🦀",
        });
        let worker = json!({
            "session_id": "worker",
            "runtime_id": "codex-thread-b",
            "display_name": "Worker",
        });
        // Register in reverse order; list order is stable regardless of arrival order.
        for session in [&worker, &orchestrator] {
            let response = http
                .post(format!("{base}/sessions"))
                .json(session)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            assert_eq!(response.json::<Value>().await.unwrap(), *session);
        }
        let expected = json!({"sessions": [orchestrator, worker]});
        for client in [&legacy, &modern] {
            assert_eq!(call(client, "hub_list_sessions", json!({})).await, expected);
            assert_eq!(
                call(
                    client,
                    "hub_lookup_session",
                    json!({"session_id": "worker"})
                )
                .await,
                json!({"session": worker}),
            );
        }

        let updated = json!({
            "session_id": "worker",
            "runtime_id": "codex-thread-b-resumed",
            // Names may collide; identity comes from session_id.
            "display_name": orchestrator["display_name"],
        });
        for _ in 0..2 {
            let response = http
                .post(format!("{base}/sessions"))
                .json(&updated)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            assert_eq!(response.json::<Value>().await.unwrap(), updated);
        }
        for client in [&legacy, &modern] {
            assert_eq!(
                call(client, "hub_list_sessions", json!({})).await,
                json!({"sessions": [orchestrator, updated]}),
            );
            assert_eq!(
                call(
                    client,
                    "hub_lookup_session",
                    json!({"session_id": "worker"})
                )
                .await,
                json!({"session": updated}),
            );
        }
        let invalid_lookup = modern
            .call_tool(
                CallToolRequestParams::new("hub_lookup_session")
                    .with_arguments(json!({"session_id": 42}).as_object().unwrap().clone()),
            )
            .await
            .unwrap();
        assert_eq!(invalid_lookup.is_error, Some(true));

        // Invalid registrations must not overwrite the existing record.
        for field in ["session_id", "runtime_id", "display_name"] {
            let mut invalid = updated.clone();
            invalid[field] = json!(" \n\t");
            let response = http
                .post(format!("{base}/sessions"))
                .json(&invalid)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
            assert_eq!(
                response.json::<Value>().await.unwrap(),
                json!({"error": format!("{field} must not be blank")})
            );
        }
        for invalid in [
            json!({"session_id": "worker", "runtime_id": 42, "display_name": "Worker"}),
            json!({"session_id": "worker", "display_name": "Worker"}),
        ] {
            let response = http
                .post(format!("{base}/sessions"))
                .json(&invalid)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        }
        assert_eq!(
            call(&modern, "hub_list_sessions", json!({})).await,
            json!({"sessions": [orchestrator, updated]}),
        );

        legacy.cancel().await.unwrap();
        modern.cancel().await.unwrap();
        let reconnected = ClientConfig::default()
            .serve(StreamableHttpClientTransport::from_uri(format!(
                "{base}/mcp"
            )))
            .await
            .unwrap();
        assert_eq!(
            call(&reconnected, "hub_list_sessions", json!({})).await,
            json!({"sessions": [orchestrator, updated]}),
        );
        reconnected.cancel().await.unwrap();
        cancellation.cancel();
        server.await.unwrap();
    })
    .await
    .expect("Session registration demo timed out");
}
