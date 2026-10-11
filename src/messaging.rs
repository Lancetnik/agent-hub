use rmcp::schemars;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SendMessageRequest {
    /// Caller-supplied globally unique ID. Reuse exactly the same data when retrying.
    pub message_id: String,
    /// Registered stable agent session ID, independent of the MCP connection.
    pub sender_id: String,
    /// Registered stable agent session ID of the recipient.
    pub recipient_id: String,
    /// Message content; any JSON value is accepted.
    pub payload: Value,
    /// Optional nonblank ID connecting a request and its replies.
    pub correlation_id: Option<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct Message {
    pub message_id: String,
    pub sender_id: String,
    pub recipient_id: String,
    pub payload: Value,
    pub correlation_id: Option<String>,
    /// Hub-wide persisted order. Values need not be consecutive within an inbox.
    pub sequence: i64,
    pub acknowledged: bool,
}

#[derive(Serialize, schemars::JsonSchema)]
pub(crate) struct SendMessageResponse {
    pub message: Message,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadInboxRequest {
    /// Registered recipient's stable session ID.
    pub session_id: String,
    /// Exclusive sequence cursor for pagination. Omit on every new poll to retry unacked messages.
    pub cursor: Option<i64>,
    /// Page size, from 1 to 1000; defaults to 100.
    pub limit: Option<u32>,
}

#[derive(Serialize, schemars::JsonSchema)]
pub(crate) struct ReadInboxResponse {
    pub messages: Vec<Message>,
    /// Pass as cursor for the next page. Null means this scan reached the end.
    pub next_cursor: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct AckMessageRequest {
    /// Must match the message's recipient. ACK confirms acceptance, not task success.
    pub session_id: String,
    pub message_id: String,
}

#[derive(Serialize, schemars::JsonSchema)]
pub(crate) struct AckMessageResponse {
    pub session_id: String,
    pub message_id: String,
    pub acknowledged: bool,
}
