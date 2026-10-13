use std::{
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rmcp::{
    Peer, RoleClient, ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::StreamableHttpClientTransport,
};
use serde_json::{Value, json};

async fn call(
    client: &Peer<RoleClient>,
    tool: &str,
    arguments: Value,
) -> Result<Value, Box<dyn Error>> {
    let result = client
        .call_tool(
            CallToolRequestParams::new(tool.to_owned())
                .with_arguments(arguments.as_object().unwrap().clone()),
        )
        .await?;
    if result.is_error == Some(true) {
        return Err(format!("{tool} failed: {:?}", result.content).into());
    }
    result
        .structured_content
        .ok_or_else(|| format!("{tool} returned no structured content").into())
}

async fn demo() -> Result<(), Box<dyn Error>> {
    let base = std::env::var("AGENT_HUB_URL").unwrap_or_else(|_| "http://127.0.0.1:7331".into());
    let base = base.trim_end_matches('/');
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let orchestrator_id = format!("demo-{nonce}-orchestrator");
    let worker_id = format!("demo-{nonce}-worker");
    let orchestrator = json!({
        "session_id": orchestrator_id,
        "runtime_id": format!("external-{nonce}-orchestrator"),
        "display_name": "Orchestrator",
    });
    let mut worker = json!({
        "session_id": worker_id,
        "runtime_id": format!("external-{nonce}-worker"),
        "display_name": "Worker",
    });
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    for session in [&orchestrator, &worker] {
        let registered: Value = http
            .post(format!("{base}/sessions"))
            .json(session)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(&registered, session);
        println!("HTTP registered: {registered}");
    }

    let client = ClientConfig::default()
        .serve(StreamableHttpClientTransport::from_uri(format!(
            "{base}/mcp"
        )))
        .await?;
    let tools = client.list_all_tools().await?;
    for name in ["hub_list_sessions", "hub_lookup_session"] {
        assert!(tools.iter().any(|tool| tool.name == name));
    }

    let listed = call(&client, "hub_list_sessions", json!({})).await?;
    let sessions = listed["sessions"]
        .as_array()
        .ok_or("sessions is not an array")?;
    assert!(sessions.contains(&orchestrator));
    assert!(sessions.contains(&worker));
    let count = sessions.len();
    println!("MCP found both sessions; registry contains {count} entries.");
    for session in [&orchestrator, &worker] {
        let found = call(
            &client,
            "hub_lookup_session",
            json!({"session_id": session["session_id"]}),
        )
        .await?;
        assert_eq!(&found["session"], session);
        println!("MCP lookup: {}", found["session"]);
    }

    worker["display_name"] = json!("Worker (updated)");
    let updated: Value = http
        .post(format!("{base}/sessions"))
        .json(&worker)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(updated, worker);

    // Reconnect to verify that registry state belongs to the Hub, not the MCP connection.
    client.cancel().await?;
    let client = ClientConfig::default()
        .serve(StreamableHttpClientTransport::from_uri(format!(
            "{base}/mcp"
        )))
        .await?;
    let listed = call(&client, "hub_list_sessions", json!({})).await?;
    let sessions = listed["sessions"]
        .as_array()
        .ok_or("sessions is not an array")?;
    assert_eq!(
        sessions.len(),
        count,
        "Re-registration must not add an entry"
    );
    assert!(sessions.contains(&orchestrator));
    assert!(sessions.contains(&worker));
    assert_eq!(
        sessions
            .iter()
            .filter(|session| session["session_id"] == worker_id)
            .count(),
        1,
    );
    let found = call(
        &client,
        "hub_lookup_session",
        json!({"session_id": worker_id}),
    )
    .await?;
    assert_eq!(found["session"], worker);
    println!(
        "PASS: HTTP re-registration updated the worker; registry still contains {count} entries after MCP reconnect."
    );
    client.cancel().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(Duration::from_secs(30), demo()).await?
}
