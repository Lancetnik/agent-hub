use std::{error::Error, path::Path, time::Duration};

use rmcp::{
    Peer, RoleClient, ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::StreamableHttpClientTransport,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

// Each start opens a new router/store; stop shuts down all server state before reopening SQLite.
struct Hub {
    base: String,
    cancellation: CancellationToken,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Hub {
    async fn start(database: &Path) -> Result<Self, Box<dyn Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let cancellation = CancellationToken::new();
        let app = agent_hub::router(cancellation.clone(), database)?;
        let shutdown = cancellation.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });
        Ok(Self {
            base,
            cancellation,
            server,
        })
    }

    async fn stop(self) -> Result<(), Box<dyn Error>> {
        self.cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.server).await???;
        Ok(())
    }

    async fn client(
        &self,
    ) -> Result<rmcp::service::RunningService<RoleClient, ClientConfig>, Box<dyn Error>> {
        Ok(ClientConfig::default()
            .serve(StreamableHttpClientTransport::from_uri(format!(
                "{}/mcp",
                self.base
            )))
            .await?)
    }
}

async fn call(
    client: &Peer<RoleClient>,
    tool: &str,
    arguments: Value,
) -> Result<Value, Box<dyn Error>> {
    let result = client
        .call_tool(
            CallToolRequestParams::new(tool.to_owned()).with_arguments(
                arguments
                    .as_object()
                    .ok_or("tool arguments must be an object")?
                    .clone(),
            ),
        )
        .await?;
    if result.is_error == Some(true) {
        return Err(format!("{tool}: {:?}", result.content).into());
    }
    result
        .structured_content
        .ok_or_else(|| format!("{tool} returned no structured content").into())
}

async fn demo() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("demo.sqlite3");
    let hub = Hub::start(&database).await?;
    for (id, name) in [("orchestrator", "Orchestrator"), ("worker", "Worker")] {
        reqwest::Client::new().post(format!("{}/sessions", hub.base))
            .json(&json!({"session_id": id, "runtime_id": format!("external-{id}"), "display_name": name}))
            .send().await?.error_for_status()?;
    }
    let orchestrator = hub.client().await?;
    let tools = orchestrator.list_all_tools().await?;
    for name in ["hub_send_message", "hub_read_inbox", "hub_ack_message"] {
        assert!(tools.iter().any(|tool| tool.name == name), "missing {name}");
    }
    let request = json!({
        "message_id": "request-1", "sender_id": "orchestrator", "recipient_id": "worker",
        "payload": {"text": "Compute 2 + 2"}, "correlation_id": "task-1",
    });
    let sent = call(&orchestrator, "hub_send_message", request.clone()).await?;
    println!(
        "1. MCP orchestrator → worker, committed: {}",
        sent["message"]
    );
    orchestrator.cancel().await?;
    hub.stop().await?;
    println!("2. Hub stopped before worker read; reopening the same SQLite database.");

    let hub = Hub::start(&database).await?;
    let orchestrator = hub.client().await?;
    let worker = hub.client().await?;
    assert_eq!(
        call(&worker, "hub_list_sessions", json!({})).await?["sessions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let inbox = call(&worker, "hub_read_inbox", json!({"session_id": "worker"})).await?;
    assert_eq!(
        inbox,
        json!({"messages": [sent["message"]], "next_cursor": null})
    );
    assert_eq!(
        call(&worker, "hub_read_inbox", json!({"session_id": "worker"})).await?,
        inbox
    );
    assert_eq!(
        call(&orchestrator, "hub_send_message", request.clone()).await?,
        sent
    );
    println!(
        "3. Sessions and pending request survived; repeated read and send created no loss or duplicate."
    );
    let ack = json!({"session_id": "worker", "message_id": "request-1"});
    let acknowledged = call(&worker, "hub_ack_message", ack.clone()).await?;
    assert_eq!(acknowledged["acknowledged"], true);
    assert_eq!(call(&worker, "hub_ack_message", ack).await?, acknowledged);
    let reply = call(
        &worker,
        "hub_send_message",
        json!({
            "message_id": "reply-1", "sender_id": "worker", "recipient_id": "orchestrator",
            "payload": {"result": 4}, "correlation_id": "task-1",
        }),
    )
    .await?;
    println!(
        "4. Worker ACKed twice and sent a correlated reply: {}",
        reply["message"]
    );
    orchestrator.cancel().await?;
    worker.cancel().await?;
    hub.stop().await?;
    println!("5. Hub stopped again; reopening SQLite to check ACK and reply.");

    let hub = Hub::start(&database).await?;
    let orchestrator = hub.client().await?;
    let worker = hub.client().await?;
    assert_eq!(
        call(&worker, "hub_read_inbox", json!({"session_id": "worker"})).await?,
        json!({"messages": [], "next_cursor": null})
    );
    assert_eq!(
        call(
            &orchestrator,
            "hub_read_inbox",
            json!({"session_id": "orchestrator"})
        )
        .await?,
        json!({"messages": [reply["message"]], "next_cursor": null})
    );
    let mut expected = sent;
    expected["message"]["acknowledged"] = json!(true);
    assert_eq!(
        call(&orchestrator, "hub_send_message", request).await?,
        expected
    );
    orchestrator.cancel().await?;
    worker.cancel().await?;
    hub.stop().await?;
    println!(
        "PASS: MCP send/read/ACK/reply; two Hub restarts preserved sessions, message IDs, ordering, ACK and correlation. Temporary demo database removed on exit."
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(Duration::from_secs(40), demo()).await?
}
