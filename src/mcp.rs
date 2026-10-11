use rmcp::{
    ServerHandler,
    handler::server::wrapper::{Json, Parameters},
    model::{Implementation, ServerCapabilities, ServerConfig},
    schemars, tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};

use crate::{
    messaging::{
        AckMessageRequest, AckMessageResponse, ReadInboxRequest, ReadInboxResponse,
        SendMessageRequest, SendMessageResponse,
    },
    sessions::Session,
    store::{Store, StoreError},
};

#[derive(Clone)]
pub struct AgentHub {
    store: Store,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub struct PingRequest {
    /// An arbitrary message echoed back to verify the MCP round trip.
    pub message: String,
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct PingResponse {
    pub message: String,
    pub server: &'static str,
    pub version: &'static str,
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct ListSessionsResponse {
    sessions: Vec<Session>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub struct LookupSessionRequest {
    /// Stable agent session ID from hub_list_sessions, not an MCP transport session ID.
    session_id: String,
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct LookupSessionResponse {
    session: Option<Session>,
}

#[tool_router]
impl AgentHub {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    #[tool(
        description = "Check connectivity to Agent Hub by echoing a message. Has no side effects.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    fn hub_ping(&self, Parameters(request): Parameters<PingRequest>) -> Json<PingResponse> {
        tracing::debug!("hub_ping called");
        Json(PingResponse {
            message: request.message,
            server: "agent-hub",
            version: env!("CARGO_PKG_VERSION"),
        })
    }

    #[tool(
        description = "List registered agent sessions, ordered by session_id. Use display_name to find a recipient and session_id to address it. Registration comes from an external HTTP integration; entries do not imply the runtime is online.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn hub_list_sessions(&self) -> Result<Json<ListSessionsResponse>, String> {
        Ok(Json(ListSessionsResponse {
            sessions: self.store.list_sessions().await.map_err(tool_error)?,
        }))
    }

    #[tool(
        description = "Look up a registered agent session by its exact stable session_id. Returns session: null when the ID is unknown. Has no side effects.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn hub_lookup_session(
        &self,
        Parameters(request): Parameters<LookupSessionRequest>,
    ) -> Result<Json<LookupSessionResponse>, String> {
        Ok(Json(LookupSessionResponse {
            session: self
                .store
                .lookup_session(request.session_id)
                .await
                .map_err(tool_error)?,
        }))
    }

    #[tool(
        description = "Send a durable direct message between registered agent sessions. Supply your sender_id and the recipient_id explicitly. Choose a globally unique message_id and reuse it with identical data on retries; changed data with the same ID is rejected. Optional correlation_id connects requests and replies. A successful result means Hub committed the message, not that the recipient read it or completed a task.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn hub_send_message(
        &self,
        Parameters(request): Parameters<SendMessageRequest>,
    ) -> Result<Json<SendMessageResponse>, String> {
        Ok(Json(SendMessageResponse {
            message: self.store.send(request).await.map_err(tool_error)?,
        }))
    }

    #[tool(
        description = "Read a registered session's unacknowledged inbox, ordered by persisted sequence. Reading does not acknowledge messages: they remain available until hub_ack_message. Omit cursor on every new poll to retry earlier unacknowledged messages; use next_cursor only for subsequent pages of the same scan. limit defaults to 100 and must be from 1 to 1000.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn hub_read_inbox(
        &self,
        Parameters(request): Parameters<ReadInboxRequest>,
    ) -> Result<Json<ReadInboxResponse>, String> {
        self.store
            .read_inbox(request)
            .await
            .map(Json)
            .map_err(tool_error)
    }

    #[tool(
        description = "Explicitly acknowledge acceptance of a message as its recipient. session_id must match the recipient_id. Repeated ACK succeeds. The message leaves the pending inbox but remains stored, and ACK survives a Hub restart. ACK does not establish successful completion of a task.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn hub_ack_message(
        &self,
        Parameters(request): Parameters<AckMessageRequest>,
    ) -> Result<Json<AckMessageResponse>, String> {
        self.store.ack(request).await.map(Json).map_err(tool_error)
    }
}

fn tool_error(error: StoreError) -> String {
    match error {
        StoreError::Invalid(message) => message,
        error => {
            tracing::error!(%error, "Hub database operation failed");
            "database operation failed".into()
        }
    }
}

#[tool_handler]
impl ServerHandler for AgentHub {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("agent-hub", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "Use hub_ping to verify connectivity, hub_list_sessions to find agent sessions, \
                 and hub_lookup_session to look up a stable session_id. External integrations \
                 register sessions over HTTP. Agent session IDs are independent of MCP connections. \
                 Use hub_send_message for durable direct messages, hub_read_inbox for pending messages, \
                 and hub_ack_message to explicitly confirm acceptance as the recipient. Reuse the \
                 message_id and data when retrying a send. Read does not ACK; omit cursor on a new poll. \
                 ACK is acceptance, not task success. Sessions, messages, and ACKs survive restarts.",
            )
    }
}
