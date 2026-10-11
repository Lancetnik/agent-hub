use std::{path::Path, time::Duration};

use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::StreamableHttpClientTransport,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

async fn start(database: &Path) -> (String, CancellationToken, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let cancellation = CancellationToken::new();
    let app = agent_hub::router(cancellation.clone(), database).unwrap();
    let shutdown = cancellation.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .unwrap();
    });
    (base, cancellation, server)
}

async fn signal(stream: &mut reqwest::Response) {
    let chunk = tokio::time::timeout(Duration::from_secs(3), stream.chunk())
        .await
        .expect("SSE did not signal")
        .unwrap()
        .expect("SSE unexpectedly closed");
    let text = String::from_utf8(chunk.to_vec()).unwrap();
    assert!(text.contains("event: inbox_changed"), "{text}");
    assert!(!text.contains("payload"), "SSE must carry hints only");
}

#[tokio::test]
async fn sse_http_inbox_and_mcp_share_durable_messages_and_ack_after_reconnect() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hub.sqlite3");
        let http = reqwest::Client::new();
        let (base, cancellation, server) = start(&database).await;
        assert_eq!(
            http.get(format!("{base}/sessions/missing/events"))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::NOT_FOUND
        );
        for id in ["sender", "recipient"] {
            http.post(format!("{base}/sessions"))
                .json(&json!({"session_id": id, "runtime_id": id, "display_name": id}))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();
        }
        let client = ClientConfig::default()
            .serve(StreamableHttpClientTransport::from_uri(format!(
                "{base}/mcp"
            )))
            .await
            .unwrap();
        let mut stream = http
            .get(format!("{base}/sessions/recipient/events"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        assert_eq!(stream.headers()["content-type"], "text/event-stream");
        signal(&mut stream).await;
        let mut messages = Vec::new();
        for id in ["one", "two"] {
            let result = client
                .call_tool(
                    CallToolRequestParams::new("hub_send_message").with_arguments(
                        json!({"message_id": id, "sender_id": "sender",
                    "recipient_id": "recipient", "payload": {"text": id}})
                        .as_object()
                        .unwrap()
                        .clone(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(result.is_error, Some(false));
            messages.push(result.structured_content.unwrap()["message"].clone());
            signal(&mut stream).await;
        }
        let first: Value = http
            .get(format!("{base}/sessions/recipient/inbox?limit=1"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(first["messages"], json!([messages[0]]));
        let next = first["next_cursor"].as_i64().unwrap();
        let second: Value = http
            .get(format!(
                "{base}/sessions/recipient/inbox?limit=1&cursor={next}"
            ))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            second,
            json!({"messages": [messages[1]], "next_cursor": null})
        );
        assert_eq!(
            http.post(format!("{base}/sessions/sender/ack"))
                .json(&json!({"message_id": "one"}))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::BAD_REQUEST
        );
        // Neither receiving SSE nor reading either HTTP page acknowledges anything.
        drop(stream);
        client.cancel().await.unwrap();
        cancellation.cancel();
        server.await.unwrap();

        let (base, cancellation, server) = start(&database).await;
        let mut stream = http
            .get(format!("{base}/sessions/recipient/events"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        // Reconnect always signals, including data created before this connection.
        signal(&mut stream).await;
        let restored: Value = http
            .get(format!("{base}/sessions/recipient/inbox"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(restored, json!({"messages": messages, "next_cursor": null}));
        for _ in 0..2 {
            let ack: Value = http
                .post(format!("{base}/sessions/recipient/ack"))
                .json(&json!({"message_id": "one"}))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(
                ack,
                json!({"session_id": "recipient", "message_id": "one", "acknowledged": true})
            );
        }
        let client = ClientConfig::default()
            .serve(StreamableHttpClientTransport::from_uri(format!(
                "{base}/mcp"
            )))
            .await
            .unwrap();
        let inbox = client
            .call_tool(
                CallToolRequestParams::new("hub_read_inbox").with_arguments(
                    json!({"session_id": "recipient"})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
            )
            .await
            .unwrap()
            .structured_content
            .unwrap();
        assert_eq!(
            inbox,
            json!({"messages": [messages[1]], "next_cursor": null})
        );
        client.cancel().await.unwrap();
        cancellation.cancel();
        // Active SSE must not prevent graceful shutdown.
        assert!(stream.chunk().await.unwrap().is_none());
        server.await.unwrap();
    })
    .await
    .expect("delivery check timed out");
}
