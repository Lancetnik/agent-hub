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

async fn post_turn(hub: &Hub, event: &Value) -> (reqwest::StatusCode, Value) {
    let response = reqwest::Client::new()
        .post(format!("{}/turn-results", hub.base))
        .json(event)
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.json().await.unwrap())
}

fn turn_event(turn_id: &str) -> Value {
    json!({
        "session_id": "worker", "runtime_id": "codex-thread", "turn_id": turn_id,
        "recipient_id": "orchestrator", "correlation_id": "assignment-1",
        "status": "completed", "final_text": "4", "artifact_refs": [], "error": null,
    })
}

#[tokio::test]
async fn runtime_result_survives_restarts_retries_and_ack_without_a_second_message() {
    tokio::time::timeout(Duration::from_secs(40), async {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("hub.sqlite3");
        let hub = Hub::start(&database).await;
        hub.register("orchestrator", "orchestrator-thread").await;
        hub.register("worker", "codex-thread").await;
        let event = turn_event("turn-1");
        let (status, result) = post_turn(&hub, &event).await;
        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(result["message"]["sender_id"], "worker");
        assert_eq!(result["message"]["correlation_id"], "assignment-1");
        assert_eq!(
            result["message"]["payload"],
            json!({
                "type": "turn_completed", "runtime_id": "codex-thread", "turn_id": "turn-1",
                "status": "completed", "final_text": "4", "artifact_refs": [], "error": null,
            })
        );
        drop(hub);

        let hub = Hub::start(&database).await;
        let client = hub.client().await;
        let expected = json!({"messages": [result["message"]], "next_cursor": null});
        assert_eq!(
            call(
                &client,
                "hub_read_inbox",
                json!({"session_id": "orchestrator"})
            )
            .await,
            expected
        );
        let ((a_status, a), (b_status, b)) =
            tokio::join!(post_turn(&hub, &event), post_turn(&hub, &event));
        assert_eq!(a_status, reqwest::StatusCode::OK);
        assert_eq!(b_status, reqwest::StatusCode::OK);
        assert_eq!(a, result);
        assert_eq!(b, result);
        assert_eq!(
            call(
                &client,
                "hub_read_inbox",
                json!({"session_id": "orchestrator"})
            )
            .await,
            expected
        );
        call(
            &client,
            "hub_ack_message",
            json!({
                "session_id": "orchestrator", "message_id": result["message"]["message_id"],
            }),
        )
        .await;
        client.cancel().await.unwrap();
        drop(hub);

        let hub = Hub::start(&database).await;
        // An old committed event can be retried after the session's runtime has changed.
        hub.register("worker", "resumed-elsewhere").await;
        let (_, replay) = post_turn(&hub, &event).await;
        assert_eq!(replay["message"]["sequence"], result["message"]["sequence"]);
        assert_eq!(replay["message"]["acknowledged"], true);
        let client = hub.client().await;
        assert_eq!(
            call(
                &client,
                "hub_read_inbox",
                json!({"session_id": "orchestrator"})
            )
            .await["messages"],
            json!([])
        );
        client.cancel().await.unwrap();
    })
    .await
    .expect("Runtime result durability scenario timed out");
}

#[tokio::test]
async fn runtime_result_preserves_terminal_status_null_text_errors_and_artifact_refs() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let hub = Hub::start(&directory.path().join("hub.sqlite3")).await;
        hub.register("orchestrator", "orchestrator-thread").await;
        hub.register("worker", "codex-thread").await;
        for status in ["completed", "failed", "interrupted"] {
            let mut event = turn_event(status);
            event["status"] = json!(status);
            event["final_text"] = Value::Null;
            event["error"] = if status == "failed" {
                json!({"message": "runtime error", "codexErrorInfo": "other"})
            } else {
                Value::Null
            };
            event["artifact_refs"] = json!(["/tmp/report.md"]);
            let (response_status, result) = post_turn(&hub, &event).await;
            assert_eq!(response_status, reqwest::StatusCode::OK);
            for field in ["status", "final_text", "error", "artifact_refs"] {
                assert_eq!(result["message"]["payload"][field], event[field]);
            }
            assert!(result["message"]["payload"].get("task_success").is_none());
        }
        let client = hub.client().await;
        assert_eq!(
            call(
                &client,
                "hub_read_inbox",
                json!({"session_id": "orchestrator"})
            )
            .await["messages"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        client.cancel().await.unwrap();
    })
    .await
    .expect("Runtime statuses scenario timed out");
}

#[tokio::test]
async fn invalid_or_changed_runtime_results_do_not_create_messages() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let hub = Hub::start(&directory.path().join("hub.sqlite3")).await;
        hub.register("orchestrator", "orchestrator-thread").await;
        hub.register("worker", "codex-thread").await;
        hub.register("other", "other-thread").await;
        for field in [
            "session_id",
            "runtime_id",
            "turn_id",
            "recipient_id",
            "correlation_id",
        ] {
            let mut invalid = turn_event("turn-1");
            invalid[field] = json!(" \t ");
            let (status, body) = post_turn(&hub, &invalid).await;
            assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
            assert_eq!(body["error"], format!("{field} must not be blank"));
        }
        for (field, value) in [
            ("status", json!("inProgress")),
            ("status", json!("success")),
            ("unknown", json!(true)),
            ("final_text", json!(123)),
            ("artifact_refs", json!("path")),
        ] {
            let mut invalid = turn_event("turn-1");
            invalid[field] = value;
            let response = reqwest::Client::new()
                .post(format!("{}/turn-results", hub.base))
                .json(&invalid)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        }
        for field in ["session_id", "recipient_id"] {
            let mut invalid = turn_event("turn-1");
            invalid[field] = json!("unregistered");
            assert_eq!(
                post_turn(&hub, &invalid).await.0,
                reqwest::StatusCode::BAD_REQUEST
            );
        }
        let client = hub.client().await;
        assert_eq!(
            call(
                &client,
                "hub_read_inbox",
                json!({"session_id": "orchestrator"})
            )
            .await["messages"],
            json!([])
        );
        let event = turn_event("turn-1");
        let (_, sent) = post_turn(&hub, &event).await;
        for (field, value) in [
            ("recipient_id", json!("other")),
            ("correlation_id", json!("changed")),
            ("status", json!("failed")),
            ("final_text", json!("changed")),
            ("artifact_refs", json!(["new-artifact"])),
            ("error", json!({"message": "changed"})),
        ] {
            let mut changed = event.clone();
            changed[field] = value;
            let (status, body) = post_turn(&hub, &changed).await;
            assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
            assert!(
                body["error"]
                    .as_str()
                    .unwrap()
                    .contains("already exists with different data")
            );
        }
        assert_eq!(
            call(
                &client,
                "hub_read_inbox",
                json!({"session_id": "orchestrator"})
            )
            .await,
            json!({"messages": [sent["message"]], "next_cursor": null})
        );
        client.cancel().await.unwrap();
    })
    .await
    .expect("Runtime result validation scenario timed out");
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
