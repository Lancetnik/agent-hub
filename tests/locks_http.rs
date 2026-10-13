use std::time::Duration;

use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::StreamableHttpClientTransport,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn expiry_alone_wakes_a_waiter_over_sse_without_a_release_or_status_poll() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let cancellation = CancellationToken::new();
        let app = agent_hub::router(cancellation.clone(), ":memory:").unwrap();
        let shutdown = cancellation.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        let http = reqwest::Client::new();
        for id in ["a", "b"] {
            http.post(format!("{base}/sessions"))
                .json(&json!({"session_id":id,"runtime_id":id,"display_name":id}))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();
        }
        let mut events = http
            .get(format!("{base}/sessions/b/events"))
            .send()
            .await
            .unwrap();
        events.chunk().await.unwrap().unwrap(); // initial reconnect hint
        for (id, ttl, status) in [("a", 1, "acquired"), ("b", 60, "waiting")] {
            let reply: Value = http
                .post(format!("{base}/locks/acquire"))
                .json(&json!({"session_id":id,"request_id":id,"resource":"file","ttl_seconds":ttl}))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(reply["status"], status);
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut text = String::new();
            while !text.contains("event: inbox_changed") {
                text += std::str::from_utf8(&events.chunk().await.unwrap().unwrap()).unwrap();
            }
        })
        .await
        .expect("Expiry did not independently signal the waiter");
        // Reading inbox does not run expiry: the timer must already have committed handoff.
        let inbox: Value = http
            .get(format!("{base}/sessions/b/inbox"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(inbox["messages"][0]["payload"]["type"], "lock_granted");
        assert_eq!(inbox["messages"][0]["payload"]["ttl_seconds"], 60);
        drop(events);
        cancellation.cancel();
        server.await.unwrap();
    })
    .await
    .expect("Expiry integration test timed out");
}

#[tokio::test]
async fn mcp_lock_handoff_is_fifo_durable_and_stale_release_cannot_unlock_the_next_owner() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let cancellation = CancellationToken::new();
        let app = agent_hub::router(cancellation.clone(), ":memory:").unwrap();
        let shutdown = cancellation.clone();
        let server = tokio::spawn(async move { axum::serve(listener, app).with_graceful_shutdown(shutdown.cancelled_owned()).await.unwrap(); });
        let http = reqwest::Client::new();
        for id in ["a", "b", "c"] {
            http.post(format!("{base}/sessions")).json(&json!({"session_id":id,"runtime_id":id,"display_name":id})).send().await.unwrap().error_for_status().unwrap();
        }
        let client = ClientConfig::default().serve(StreamableHttpClientTransport::from_uri(format!("{base}/mcp"))).await.unwrap();
        for (id, status) in [("a", "acquired"), ("b", "waiting"), ("c", "waiting")] {
            let reply = client.call_tool(CallToolRequestParams::new("hub_acquire_lock").with_arguments(json!({"session_id":id,"request_id":id,"resource":"/project/settings.json","ttl_seconds":60}).as_object().unwrap().clone())).await.unwrap();
            assert_eq!(reply.is_error, Some(false));
            assert_eq!(reply.structured_content.unwrap()["status"], status);
        }
        for _ in 0..2 {
            let reply = client.call_tool(CallToolRequestParams::new("hub_release_lock").with_arguments(json!({"session_id":"a","request_id":"a"}).as_object().unwrap().clone())).await.unwrap();
            assert_eq!(reply.is_error, Some(false));
        }
        let inbox: Value = http.get(format!("{base}/sessions/b/inbox")).send().await.unwrap().json().await.unwrap();
        assert_eq!(inbox["messages"].as_array().unwrap().len(), 1);
        assert_eq!(inbox["messages"][0]["payload"]["type"], "lock_granted");
        assert_eq!(inbox["messages"][0]["payload"]["request_id"], "b");
        let state: Value = http.get(format!("{base}/locks/requests/b?session_id=b")).send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
        assert_eq!(state["status"], "acquired");
        let c: Value = http.get(format!("{base}/sessions/c/inbox")).send().await.unwrap().json().await.unwrap();
        assert!(c["messages"].as_array().unwrap().is_empty());
        client.cancel().await.unwrap();
        cancellation.cancel(); server.await.unwrap();
    }).await.expect("Lock test exceeded its budget");
}
