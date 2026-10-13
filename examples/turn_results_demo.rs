use std::{error::Error, path::Path, process::Command, time::Duration};

use rmcp::{
    Peer, RoleClient, ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::StreamableHttpClientTransport,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

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
            CallToolRequestParams::new(tool.to_owned())
                .with_arguments(arguments.as_object().ok_or("expected an object")?.clone()),
        )
        .await?;
    if result.is_error == Some(true) {
        return Err(format!("{tool}: {:?}", result.content).into());
    }
    result
        .structured_content
        .ok_or_else(|| "missing tool response".into())
}

async fn post_result(hub: &Hub, event: &Value) -> Result<Value, Box<dyn Error>> {
    Ok(reqwest::Client::new()
        .post(format!("{}/turn-results", hub.base))
        .json(event)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn demo() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("hub.sqlite3");
    let hub = Hub::start(&database).await?;
    reqwest::Client::new().post(format!("{}/sessions", hub.base))
        .json(&json!({"session_id": "orchestrator", "runtime_id": "external-orchestrator", "display_name": "Orchestrator"}))
        .send().await?.error_for_status()?;

    let worker_script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/codex_worker.py");
    let outbox = directory.path().join("outbox.sqlite3");
    let base = hub.base.clone();
    let cwd = directory.path().to_path_buf();
    println!(
        "1. External integration launches one real Codex turn; no completion tool is requested."
    );
    // The async executor remains available to serve the external integration's HTTP requests.
    let output = tokio::task::spawn_blocking(move || {
        Command::new("python3")
            .arg(worker_script)
            .args([
                "--url",
                &base,
                "--session-id",
                "worker",
                "--recipient-id",
                "orchestrator",
                "--correlation-id",
                "demo-assignment",
            ])
            .arg("--cwd")
            .arg(cwd)
            .arg("--outbox")
            .arg(outbox)
            .arg("Compute 2 + 2. Reply with the single digit only. Do not use tools.")
            .output()
    })
    .await??;
    if !output.status.success() {
        return Err(format!(
            "Worker failed: {}\n{}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        )
        .into());
    }
    let event: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(event["status"], "completed");
    assert_eq!(event["final_text"], "4");
    let orchestrator = hub.client().await?;
    assert_eq!(
        call(
            &orchestrator,
            "hub_lookup_session",
            json!({"session_id": "worker"})
        )
        .await?["session"]["runtime_id"],
        event["runtime_id"]
    );
    let inbox = call(
        &orchestrator,
        "hub_read_inbox",
        json!({"session_id": "orchestrator"}),
    )
    .await?;
    assert_eq!(inbox["messages"].as_array().unwrap().len(), 1);
    let message = &inbox["messages"][0];
    assert_eq!(message["payload"]["turn_id"], event["turn_id"]);
    assert_eq!(message["payload"]["final_text"], "4");
    assert_eq!(message["correlation_id"], "demo-assignment");
    println!(
        "2. Automatically registered worker; orchestrator reads correlated result through MCP: {message}"
    );
    assert_eq!(post_result(&hub, &event).await?["message"], *message);
    orchestrator.cancel().await?;
    hub.stop().await?;

    let hub = Hub::start(&database).await?;
    let orchestrator = hub.client().await?;
    assert_eq!(
        call(
            &orchestrator,
            "hub_read_inbox",
            json!({"session_id": "orchestrator"})
        )
        .await?,
        inbox
    );
    assert_eq!(post_result(&hub, &event).await?["message"], *message);
    call(
        &orchestrator,
        "hub_ack_message",
        json!({"session_id": "orchestrator", "message_id": message["message_id"]}),
    )
    .await?;
    assert_eq!(
        post_result(&hub, &event).await?["message"]["acknowledged"],
        true
    );
    assert_eq!(
        call(
            &orchestrator,
            "hub_read_inbox",
            json!({"session_id": "orchestrator"})
        )
        .await?["messages"],
        json!([])
    );
    orchestrator.cancel().await?;
    hub.stop().await?;
    println!(
        "PASS: real Codex result persisted automatically; restart, replay and ACK create no duplicate."
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(Duration::from_secs(240), demo()).await?
}
