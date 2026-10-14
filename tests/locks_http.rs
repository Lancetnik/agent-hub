use std::time::Duration;

use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::StreamableHttpClientTransport,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn lifecycle_only_database_upgrade_retains_manual_waiters_and_notifications() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = rusqlite::Connection::open(file.path()).unwrap();
        db.execute_batch("CREATE TABLE sessions (session_id TEXT PRIMARY KEY, runtime_id TEXT NOT NULL, display_name TEXT NOT NULL);
            INSERT INTO sessions VALUES('a','a','a'),('b','b','b');
            CREATE TABLE lock_requests (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT UNIQUE NOT NULL,
                session_id TEXT NOT NULL REFERENCES sessions(session_id), resource TEXT NOT NULL,
                ttl_seconds INTEGER NOT NULL, status TEXT NOT NULL, expires_at_ms INTEGER
            );").unwrap();
        let expiry = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64 + 60_000;
        db.execute("INSERT INTO lock_requests(request_id,session_id,resource,ttl_seconds,status,expires_at_ms) VALUES('a','a','file',60,'acquired',?1)", [expiry]).unwrap();
        db.execute_batch("INSERT INTO lock_requests(request_id,session_id,resource,ttl_seconds,status) VALUES('b','b','file',60,'waiting');").unwrap();
        drop(db);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let cancellation = CancellationToken::new();
        let app = agent_hub::router(cancellation.clone(), file.path()).unwrap();
        let shutdown = cancellation.clone();
        let server = tokio::spawn(async move { axum::serve(listener, app).with_graceful_shutdown(shutdown.cancelled_owned()).await.unwrap(); });
        let http = reqwest::Client::new();
        let before = http.get(format!("{base}/locks/requests/b?session_id=b")).send().await.unwrap().error_for_status().unwrap().json::<Value>().await.unwrap();
        assert_eq!(before["status"], "waiting");
        assert_eq!(before["notify"], true);
        http.post(format!("{base}/locks/release")).json(&json!({"session_id":"a","request_id":"a"})).send().await.unwrap().error_for_status().unwrap();
        let after = http.get(format!("{base}/locks/requests/b?session_id=b")).send().await.unwrap().error_for_status().unwrap().json::<Value>().await.unwrap();
        assert_eq!(after["status"], "acquired");
        let inbox = http.get(format!("{base}/sessions/b/inbox")).send().await.unwrap().json::<Value>().await.unwrap();
        assert_eq!(inbox["messages"][0]["payload"]["type"], "lock_granted");
        cancellation.cancel(); server.await.unwrap();
    }).await.expect("Legacy migration test timed out");
}

// Public seams: hook-facing HTTP acquire/wait/release and the durable inbox.
// A silent tool waiter must not create conversation input, including within one session.
#[tokio::test]
async fn silent_tool_waiters_share_fifo_without_waking_the_model() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let cancellation = CancellationToken::new();
        let app = agent_hub::router(cancellation.clone(), ":memory:").unwrap();
        let shutdown = cancellation.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).with_graceful_shutdown(shutdown.cancelled_owned()).await.unwrap();
        });
        let http = reqwest::Client::new();
        http.post(format!("{base}/sessions"))
            .json(&json!({"session_id":"a","runtime_id":"a","display_name":"a"}))
            .send().await.unwrap().error_for_status().unwrap();
        for (id, status) in [("edit-1", "acquired"), ("edit-2", "waiting")] {
            let response = http.post(format!("{base}/locks/acquire"))
                .json(&json!({"session_id":"a","request_id":id,"resource":"file","ttl_seconds":60,"notify":false}))
                .send().await.unwrap().error_for_status().unwrap();
            assert_eq!(response.json::<Value>().await.unwrap()["status"], status);
        }
        let waiter = tokio::spawn({
            let http = http.clone(); let base = base.clone();
            async move { http.get(format!("{base}/locks/requests/edit-2?session_id=a&wait_seconds=5"))
                .send().await.unwrap().error_for_status().unwrap().json::<Value>().await.unwrap() }
        });
        http.post(format!("{base}/locks/release"))
            .json(&json!({"session_id":"a","request_id":"edit-1"}))
            .send().await.unwrap().error_for_status().unwrap();
        assert_eq!(waiter.await.unwrap()["status"], "acquired");
        let inbox = http.get(format!("{base}/sessions/a/inbox"))
            .send().await.unwrap().json::<Value>().await.unwrap();
        assert!(inbox["messages"].as_array().unwrap().is_empty());
        cancellation.cancel(); server.await.unwrap();
    }).await.expect("Silent hook waiter hung");
}

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
