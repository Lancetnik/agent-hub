use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

use rmcp::{
    Peer, RoleClient, ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::StreamableHttpClientTransport,
};
use serde_json::{Value, json};

// Use the real executable and abrupt process termination to check committed durability.
struct Hub {
    child: Child,
    base: String,
}

impl Hub {
    async fn start(database: &Path) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let child = Command::new(env!("CARGO_BIN_EXE_agent-hub"))
            .env("AGENT_HUB_BIND", address.to_string())
            .env("AGENT_HUB_DB", database)
            .env("RUST_LOG", "error")
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let mut hub = Self {
            child,
            base: format!("http://{address}"),
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    hub.child.try_wait().unwrap().is_none(),
                    "Hub exited during startup"
                );
                if let Ok(response) = http.get(format!("{}/health", hub.base)).send().await
                    && response.status().is_success()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Hub did not start");
        hub
    }

    async fn client(&self) -> rmcp::service::RunningService<rmcp::RoleClient, ClientConfig> {
        ClientConfig::default()
            .serve(StreamableHttpClientTransport::from_uri(format!(
                "{}/mcp",
                self.base
            )))
            .await
            .unwrap()
    }

    async fn register(&self, id: &str, runtime: &str) -> Value {
        let session = json!({"session_id": id, "runtime_id": runtime, "display_name": id});
        let response = reqwest::Client::new()
            .post(format!("{}/sessions", self.base))
            .json(&session)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.json::<Value>().await.unwrap(), session);
        session
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn call(client: &Peer<RoleClient>, tool: &str, arguments: Value) -> Value {
    let result = client
        .call_tool(
            CallToolRequestParams::new(tool.to_owned())
                .with_arguments(arguments.as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false), "{tool}: {:?}", result.content);
    result.structured_content.unwrap()
}

async fn rejected(client: &Peer<RoleClient>, tool: &str, arguments: Value, expected: &str) {
    let result = client
        .call_tool(
            CallToolRequestParams::new(tool.to_owned())
                .with_arguments(arguments.as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(true), "{tool} accepted invalid input");
    let text = serde_json::to_string(&result.content).unwrap();
    assert!(text.contains(expected), "Expected {expected:?} in {text}");
}

#[tokio::test]
async fn message_ack_and_correlated_reply_survive_two_process_restarts() {
    tokio::time::timeout(Duration::from_secs(40), async {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hub.sqlite3");
        let hub = Hub::start(&database).await;
        let a = hub.register("A", "runtime-a").await;
        hub.register("B", "runtime-b").await;
        let b = hub.register("B", "runtime-b-resumed").await;
        let client = hub.client().await;
        let request = json!({
            "message_id": "request-1", "sender_id": "A", "recipient_id": "B",
            "payload": {"text": "Сделай работу 🦀"}, "correlation_id": "task-1",
        });
        let sent = call(&client, "hub_send_message", request.clone()).await;
        assert_eq!(sent["message"]["acknowledged"], false);
        client.cancel().await.unwrap();
        drop(hub);

        let hub = Hub::start(&database).await;
        let client = hub.client().await;
        assert_eq!(
            call(&client, "hub_list_sessions", json!({})).await,
            json!({"sessions": [a, b]}),
        );
        let inbox = call(&client, "hub_read_inbox", json!({"session_id": "B"})).await;
        assert_eq!(
            inbox,
            json!({"messages": [sent["message"]], "next_cursor": null})
        );
        assert_eq!(
            call(&client, "hub_read_inbox", json!({"session_id": "B"})).await,
            inbox,
            "read must not ACK",
        );
        assert_eq!(
            call(&client, "hub_send_message", request.clone()).await,
            sent
        );

        let ack = json!({"session_id": "B", "message_id": "request-1"});
        let expected_ack =
            json!({"session_id": "B", "message_id": "request-1", "acknowledged": true});
        for _ in 0..2 {
            assert_eq!(
                call(&client, "hub_ack_message", ack.clone()).await,
                expected_ack
            );
        }
        let reply = call(
            &client,
            "hub_send_message",
            json!({
                "message_id": "reply-1", "sender_id": "B", "recipient_id": "A",
                "payload": {"result": "готово"}, "correlation_id": "task-1",
            }),
        )
        .await;
        assert!(
            reply["message"]["sequence"].as_i64().unwrap()
                > sent["message"]["sequence"].as_i64().unwrap()
        );
        client.cancel().await.unwrap();
        drop(hub);

        let hub = Hub::start(&database).await;
        let client = hub.client().await;
        assert_eq!(
            call(&client, "hub_read_inbox", json!({"session_id": "B"})).await,
            json!({"messages": [], "next_cursor": null}),
        );
        assert_eq!(
            call(&client, "hub_read_inbox", json!({"session_id": "A"})).await,
            json!({"messages": [reply["message"]], "next_cursor": null}),
        );
        let mut acknowledged = sent;
        acknowledged["message"]["acknowledged"] = json!(true);
        assert_eq!(
            call(&client, "hub_send_message", request).await,
            acknowledged
        );
        assert_eq!(call(&client, "hub_ack_message", ack).await, expected_ack);
        client.cancel().await.unwrap();
    })
    .await
    .expect("Restart scenario timed out");
}

#[tokio::test]
async fn invalid_sends_and_acks_leave_the_original_message_unchanged() {
    tokio::time::timeout(Duration::from_secs(40), async {
        let directory = tempfile::tempdir().unwrap();
        let hub = Hub::start(&directory.path().join("hub.sqlite3")).await;
        for id in ["A", "B", "C"] {
            hub.register(id, id).await;
        }
        let client = hub.client().await;
        let request = json!({
            "message_id": "one", "sender_id": "A", "recipient_id": "B", "payload": [1, "hello"],
        });
        for (field, value, error) in [
            ("sender_id", "unknown", "unknown sender_id"),
            ("recipient_id", "unknown", "unknown recipient_id"),
            ("message_id", " ", "message_id must not be blank"),
            ("correlation_id", " ", "correlation_id must not be blank"),
        ] {
            let mut invalid = request.clone();
            invalid[field] = json!(value);
            rejected(&client, "hub_send_message", invalid, error).await;
        }
        assert_eq!(
            call(&client, "hub_read_inbox", json!({"session_id": "B"})).await["messages"],
            json!([])
        );
        let sent = call(&client, "hub_send_message", request.clone()).await;
        // Every immutable field participates in the duplicate-ID check.
        for (field, value) in [
            ("sender_id", json!("C")),
            ("recipient_id", json!("C")),
            ("payload", json!("changed")),
            ("correlation_id", json!("changed")),
        ] {
            let mut conflict = request.clone();
            conflict[field] = value;
            rejected(
                &client,
                "hub_send_message",
                conflict,
                "already exists with different data",
            )
            .await;
        }
        for args in [
            json!({"session_id": "A", "message_id": "one"}),
            json!({"session_id": "unknown", "message_id": "one"}),
            json!({"session_id": "B", "message_id": "missing"}),
        ] {
            rejected(
                &client,
                "hub_ack_message",
                args,
                "message does not exist for this recipient",
            )
            .await;
        }
        for (args, error) in [
            (json!({"session_id": "missing"}), "unknown session_id"),
            (
                json!({"session_id": "B", "limit": 0}),
                "limit must be between",
            ),
            (
                json!({"session_id": "B", "limit": 1001}),
                "limit must be between",
            ),
            (
                json!({"session_id": "B", "cursor": -1}),
                "cursor must be nonnegative",
            ),
        ] {
            rejected(&client, "hub_read_inbox", args, error).await;
        }
        assert_eq!(call(&client, "hub_send_message", request).await, sent);
        assert_eq!(
            call(&client, "hub_read_inbox", json!({"session_id": "B"})).await,
            json!({"messages": [sent["message"]], "next_cursor": null}),
        );
        client.cancel().await.unwrap();
    })
    .await
    .expect("Validation scenario timed out");
}

#[tokio::test]
async fn pagination_retries_unacked_messages_and_concurrent_send_is_idempotent() {
    tokio::time::timeout(Duration::from_secs(40), async {
        let directory = tempfile::tempdir().unwrap();
        let hub = Hub::start(&directory.path().join("hub.sqlite3")).await;
        for id in ["A", "B", "C"] {
            hub.register(id, id).await;
        }
        let client = hub.client().await;
        let other = hub.client().await;
        let same = json!({"message_id": "same", "sender_id": "A", "recipient_id": "B", "payload": null});
        let (first, second) = tokio::join!(
            call(&client, "hub_send_message", same.clone()),
            call(&other, "hub_send_message", same),
        );
        assert_eq!(first, second);
        call(&client, "hub_send_message", json!({"message_id": "elsewhere", "sender_id": "A", "recipient_id": "C", "payload": 42})).await;
        let last = call(&other, "hub_send_message", json!({"message_id": "last", "sender_id": "A", "recipient_id": "B", "payload": {"x": true}})).await;
        let page = call(&client, "hub_read_inbox", json!({"session_id": "B", "limit": 1})).await;
        assert_eq!(page["messages"], json!([first["message"]]));
        assert_eq!(page["next_cursor"], first["message"]["sequence"]);
        assert_eq!(
            call(
                &client,
                "hub_read_inbox",
                json!({"session_id": "B", "limit": 1, "cursor": page["next_cursor"]}),
            )
            .await,
            json!({"messages": [last["message"]], "next_cursor": null}),
        );
        call(&other, "hub_ack_message", json!({"session_id": "B", "message_id": "last"})).await;
        assert_eq!(
            call(&client, "hub_read_inbox", json!({"session_id": "B", "limit": 1, "cursor": page["next_cursor"]})).await,
            json!({"messages": [], "next_cursor": null}),
        );
        assert_eq!(
            call(&other, "hub_read_inbox", json!({"session_id": "B"})).await,
            json!({"messages": [first["message"]], "next_cursor": null}),
            "new poll must retry the earlier unacknowledged message",
        );
        assert!(last["message"]["sequence"].as_i64().unwrap() > first["message"]["sequence"].as_i64().unwrap() + 1);
        client.cancel().await.unwrap();
        other.cancel().await.unwrap();
    }).await.expect("Pagination scenario timed out");
}
