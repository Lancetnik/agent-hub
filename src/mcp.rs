use rmcp::{
    ServerHandler,
    handler::server::wrapper::{Json, Parameters},
    model::{Implementation, ServerCapabilities, ServerConfig},
    schemars, tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};

use crate::sessions::{Session, SessionRegistry};

#[derive(Clone)]
pub struct AgentHub {
    registry: SessionRegistry,
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
    pub fn new(registry: SessionRegistry) -> Self {
        Self { registry }
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
    fn hub_list_sessions(&self) -> Json<ListSessionsResponse> {
        Json(ListSessionsResponse {
            sessions: self.registry.list(),
        })
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
    fn hub_lookup_session(
        &self,
        Parameters(request): Parameters<LookupSessionRequest>,
    ) -> Json<LookupSessionResponse> {
        Json(LookupSessionResponse {
            session: self.registry.lookup(&request.session_id),
        })
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
                 register sessions over HTTP. Agent session IDs are independent of MCP connections.",
            )
    }
}
