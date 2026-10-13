# Agent Hub

A local prototype built with Rust, axum, and the official `rmcp` MCP SDK.
Four milestones are implemented: external integrations register sessions over
HTTP; agents discover each other and exchange durable messages through MCP;
real Codex worker turn results automatically enter the orchestrator's inbox and
continue its existing conversation with retained context. SQLite stores sessions,
messages, and acceptance acknowledgements (ACKs). Hub does not start sessions.

The target workflow is local use with a running Hub and ordinary Codex CLIs:
a task arrives in an existing conversation, a busy session accepts it after its
current turn finishes, and the worker result automatically continues the
orchestrator's conversation. Runtime integration detects completion; the model
need not call a completion tool.

Coordination with two workers, including a native Codex subagent, was verified
in clean CLI sessions. `SessionStart` explains how to finish a turn to receive
automatic responses; this workflow requires no separate Skill.
[Scenarios and verification results](docs/codex-coordination.md).

[Negotiation between two sessions after losing a shared-file edit](docs/codex-file-conflicts.md)
was also verified. For an observed edit conflict or an explicit agreement to
serialize a resource, [MCP leases with TTL and FIFO](docs/codex-locks.md) are
available: the waiting agent finishes its turn and automatically continues on
`lock_granted`. Default TTL is 120 seconds, range 1–600; the bridge releases
observed grants at turn completion. Coordination is cooperative; Hub cannot
prevent physical file writes after expiry. Routine local edits use local tools.
Checking Hub or acquiring a lock before every write is unnecessary; a peer
registration alone does not establish a conflict.

## Running Hub

Requires Rust 1.88+ and Cargo.

```sh
cargo run
```

The default listener is `127.0.0.1:7331`:

- `GET /health` — returns `ok`.
- `POST /sessions` — registers or updates a session.
- `POST /turn-results` — saves a terminal turn result as an addressed message.
- `GET /sessions/{session_id}/events` — SSE signal for available inbox entries.
- `GET /sessions/{session_id}/inbox?limit=100&cursor=...` — inbox for external adapters.
- `POST /sessions/{session_id}/ack` with `{"message_id":"..."}` — adapter ACK.
- `POST /locks/acquire`, `POST /locks/release` — acquire and release a lease.
- `GET /locks/requests/{request_id}?session_id=...` — inspect an owned request.
- `/mcp` — MCP over Streamable HTTP.
- `hub_ping({"message":"hello"})` — connectivity check.
- `hub_list_sessions({})` — registered sessions.
- `hub_lookup_session({"session_id":"worker"})` — lookup by stable ID.
- `hub_send_message({...})` — persist an addressed message.
- `hub_read_inbox({"session_id":"worker"})` — unacknowledged inbox entries.
- `hub_ack_message({"session_id":"worker","message_id":"request-1"})` — acceptance ACK.
- `hub_acquire_lock({"session_id":"worker","request_id":"worker:edit-1","resource":"/checkout/file","ttl_seconds":120})` — a lease or FIFO waiting.
- `hub_release_lock({"session_id":"worker","request_id":"worker:edit-1"})` — release or cancel waiting.

Configure the listener, database path, and logging:

```sh
AGENT_HUB_BIND=127.0.0.1:8000 AGENT_HUB_DB=/tmp/my-hub.sqlite3 RUST_LOG=agent_hub=debug,rmcp=info cargo run
```

Ctrl-C shuts down the HTTP server and MCP connections. The default database is
`agent-hub.sqlite3` in the current directory; reuse it after restart. Its parent
directory must exist. SQLite is bundled through rusqlite; no separate SQLite
installation is needed. Hub exits with an error if it cannot open/create the database.

## Session registration

An external hook or adapter submits three required string fields:

```sh
curl --fail-with-body http://127.0.0.1:7331/sessions \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"orchestrator","runtime_id":"external-thread-a","display_name":"Orchestrator"}'

curl --fail-with-body http://127.0.0.1:7331/sessions \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"worker","runtime_id":"external-thread-b","display_name":"Worker"}'
```

- `session_id`: stable Hub address selected by the external integration. Reuse
  it when registering again.
- `runtime_id`: the runtime's external session/conversation identifier.
- `display_name`: a display label; duplicate names are allowed.

Success returns HTTP 200 and the submitted record. Another POST with the same
session_id replaces the record, including runtime_id and display_name, without
a duplicate. Values are preserved verbatim; empty or whitespace-only strings
return HTTP 400 with `{"error":"<field> must not be blank"}`. Missing, mistyped,
or unknown fields return HTTP 422.

`hub_list_sessions` returns `{"sessions":[...]}` ordered by session_id. Once you
select a recipient by name, call `hub_lookup_session` with its ID: it returns
`{"session":{...}}` or `{"session":null}` for an unknown ID. Successful MCP tools
return JSON in structuredContent and text content. HTTP and separate MCP
connections share the same SQLite store.

## Direct messages

Both sessions must be registered. The sender supplies stable IDs explicitly:

```json
{
  "message_id": "request-1",
  "sender_id": "orchestrator",
  "recipient_id": "worker",
  "payload": {"text": "Compute 2 + 2"},
  "correlation_id": "task-1"
}
```

`hub_send_message` accepts these arguments and returns `{"message":{...}}` with
the supplied fields, durable sequence, and acknowledged flag. Payload may be
any JSON value, including null. Correlation is optional and returns null when
omitted. IDs must be nonblank; values are preserved verbatim. Tools reject
unknown fields and incorrect argument types.

- The sender chooses message_id, such as a UUID; it is unique across Hub. An
  identical retry returns the existing message with its sequence and current
  ACK. Different data with the same ID fail. ACK does not allow ID reuse.
- A reply uses a new message_id and may reuse the request's correlation_id.
- Successful send means Hub persisted the message. Unknown sender/recipient
  returns an error without a record. The orchestrator assesses task execution.

Read the worker's inbox:

```text
hub_read_inbox({"session_id":"worker","limit":100})
-> {"messages":[...],"next_cursor":null}
```

Inbox contains only unacknowledged messages ordered by sequence. Reading does
not ACK; a message can be read repeatedly, including after restart. For pages,
pass next_cursor as cursor to the next call: an exclusive lower sequence bound.
Default limit is 100, range 1–1000. Sequence numbers are global to Hub and may
have gaps in one recipient's inbox. `next_cursor: null` ends the current scan.
**Start every new scan without a cursor**, so previously read messages without
ACK are included again. Pagination does not freeze an inbox snapshot: ACKs and
new messages between calls can change subsequent pages.

```text
hub_ack_message({"session_id":"worker","message_id":"request-1"})
-> {"session_id":"worker","message_id":"request-1","acknowledged":true}
```

ACK confirms recipient acceptance and removes the message from inbox. Repeated
ACK succeeds. Another recipient or an unknown message fails. ACK survives
restart; the message itself remains in the database. ACK does not imply task
success. Operation errors return MCP tool results with isError=true and an explanation.

## Turn results

External integration reports turn completion over HTTP; no model tool call is needed:

```sh
curl --fail-with-body http://127.0.0.1:7331/turn-results \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"worker","runtime_id":"external-thread-b","turn_id":"turn-1","recipient_id":"orchestrator","correlation_id":"task-1","status":"completed","final_text":"4","artifact_refs":[],"error":null}'
```

Worker and orchestrator must be registered. Required nonblank string fields:
session_id, runtime_id, turn_id, recipient_id, correlation_id, and a terminal
status: completed, failed, or interrupted. `final_text` is a string or null;
`error` is runtime JSON or null. Both default to null when omitted. `artifact_refs`
is a string array, empty by default. Unknown/mistyped fields and nonterminal
status return HTTP 422; blank IDs, unknown sessions, and conflicting retries
return HTTP 400. Database errors return HTTP 500 with JSON error.

Success returns HTTP 200 with `{"message":{...}}`, available through
hub_read_inbox. Payload.type is turn_completed; payload contains runtime_id,
turn_id, status, final_text, artifact_refs, and error. Sender_id is the worker;
recipient_id and correlation_id come from the external integration. Status
means turn completion; the orchestrator assesses task success. Missing text is
not replaced by an invented successful response.

Hub derives message_id from `(session_id, runtime_id, turn_id)`. An identical
retry returns the same message, sequence, and current ACK, including after
restart. Different data for that tuple fail; changing recipient or correlation
does not create another result. A delayed event keeps its original runtime_id
even after registration has changed. Results use the same message table and
existing read/ACK operations.

## Ordinary Codex CLI sessions through hooks

With Hub running, install from the repository root:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/codex_hooks.py install
codex
```

In `/hooks`, trust the **Agent Hub ordinary CLI integration** commands, then
start a new codex or codex resume. Ordinary startup now automatically registers
the real CLI conversation and attaches delivery. Open another session in a
second terminal and ask the orchestrator to find it through hub_list_sessions
and send work through hub_send_message. The hook supplies the model's own UUID;
the session name comes from its current directory.

The worker automatically returns its turn result. An idle orchestrator starts
a new turn in the same conversation; a busy one first finishes its current turn.
The user retains ordinary CLI settings and approval control. The bridge attaches
to the existing shared daemon and does not launch a replacement session.
Installation preserves existing hooks/config.

Short core-flow check with two real CLIs and an owned temporary Hub:

```sh
.venv/bin/python scripts/check_codex_hooks.py --smoke --max-seconds 240
```

It verifies registration, an MCP assignment, worker completion without MCP calls,
and continuation of the original orchestrator conversation with retained context.

The full check uses two real interactive CLIs, temporary config, and its own
Hub. It consumes real turns and Codex quota:

```sh
.venv/bin/python scripts/check_codex_hooks.py
```

It covers hook registration, MCP assignment, automatic result, retained context,
a busy turn, Hub restart, reconnect, and user input. Uninstall with
`.venv/bin/python scripts/codex_hooks.py uninstall`. Installation, ACK, lifecycle,
and support limits: [CLI hook integration](docs/codex-hooks.md).

## External App Server client

Requires Python 3.11+ and an installed, authenticated Codex. The demo uses real
turns and quota, automatically building and starting a temporary Hub:

```sh
python3 scripts/check_codex_delivery.py
```

In a real LLM turn, the orchestrator sends an assignment through MCP. The worker
accepts it and finishes without a completion tool; its external adapter saves
the result. The orchestrator automatically starts another turn in the same
thread using retained context. The demo then checks a busy orchestrator, adapter
disconnect, Hub crash/restart, and resume of the original thread. Success ends
with PASS. Temporary databases are deleted; Codex history remains in its ordinary
store for thread/resume.

With a separately running Hub, use two terminals. Start the worker:

```sh
python3 scripts/codex_session.py --session-id worker \
  --state .agent-hub/worker.sqlite3 --report-results
```

Then the orchestrator:

```sh
python3 scripts/codex_session.py --session-id orchestrator \
  --state .agent-hub/orchestrator.sqlite3 \
  'Send worker a Hub message using hub_send_message: message_id=task-1, sender_id=orchestrator, recipient_id=worker, correlation_id=task-1, payload={"text":"Compute 2 + 2. Reply with the number only. Do not use tools."}. Then reply WAITING. When a turn_completed result arrives, report its final_text and do not send another message.'
```

The user starts these adapters; Hub does not manage Codex processes. Each state
file belongs to one session; restarting with the same file resumes its thread
without repeating the initial prompt. With report-results, a worker
returns a terminal result to each accepted message's sender, retaining correlation.
An orchestrator does not need that flag: its context determines subsequent actions.
Adapter stdout reports accepted message IDs, turn IDs, and final text.

SSE signals only that a read is needed. On connection, reconnect, and recovery
checks, the adapter scans durable inbox from the start, using cursors for pages.
A busy session first finishes its current turn. ACK follows runtime acceptance
of turn/start and local persistence of the turn ID. Retrying a lost HTTP ACK
uses the receipt without starting another turn. Details:
[persistent Codex adapter](docs/codex-delivery.md).

## Milestone 3 demo

Requires Python 3.11+ and installed, authenticated Codex. Runs real inference
and consumes quota:

```sh
cargo run --example turn_results_demo
```

It starts a temporary Hub and external adapter with a real Codex turn. The worker
registers automatically, computes 2 + 2, and result 4 appears in the orchestrator's
MCP inbox with assignment correlation. The demo checks repeated submission,
server restart with the same database, and retry after ACK. Success ends with PASS.

With Hub running and an orchestrator registered, run a worker separately:

```sh
python3 scripts/codex_worker.py \
  --session-id worker --recipient-id orchestrator --correlation-id task-1 \
  'Compute 2 + 2. Reply with the single digit only. Do not use tools.'
```

The adapter consumes App Server events, saves undelivered HTTP events in a local
outbox, and prints exact event JSON to stdout. Retry saved events without Codex:
`python3 scripts/codex_worker.py --retry`. Default outbox is
`.agent-hub/outbox.sqlite3`; override with --outbox. One new thread and one turn
per launch are supported; the worker uses read-only sandbox. Callback checks,
a Stop continuation example, and limits:
[Codex lifecycle integration](docs/codex-lifecycle.md).

## Milestone 2 demo

```sh
cargo run --example messaging_demo
```

This starts Hub on a free local port with a temporary SQLite database; a separate
cargo run is unnecessary. It registers two external sessions, sends an MCP
message, stops HTTP/MCP, and reopens the same database. The worker reads the
persisted message, ACKs, and replies with the same correlation_id. After a second
restart the demo checks the retained ACK and reply. Repeated read/send/ACK are
also checked; success ends with PASS. The database is deleted on exit. Integration
tests additionally perform two crash/restart cycles of a separate Hub process.

## Milestone 1 demo

With cargo run active, in another terminal:

```sh
cargo run --example sessions_demo
```

The demo sends real HTTP requests as an external integration, registers an
orchestrator and worker, and verifies discovery of the session MCP tools, list,
and lookup. It updates the worker name, reconnects through MCP, and checks the
updated record and unchanged session count. A fresh Hub ends with two records.
Each invocation uses fresh IDs and adds two sessions.

For another address:

```sh
AGENT_HUB_URL=http://127.0.0.1:8000 cargo run --example sessions_demo
```

This demonstrates server registration. Real runtime integration is verified by
turn_results_demo through the external Codex App Server adapter.

## Checking through Codex

With Hub running, in another terminal:

```sh
python3 scripts/check_codex.py
```

Check messaging tools through two temporary real Codex sessions:

```sh
python3 scripts/check_codex.py --messaging
```

This additionally registers two threads and checks send/read/ACK, retries, and
a correlated reply. It adds two session records and two messages to the selected
Hub database. Like the base probe, it calls tools directly through App Server;
it does not run an LLM turn or test model tool selection.

The script starts installed codex app-server, creates a temporary thread,
checks hub_ping discovery, and calls it through mcpServer/tool/call. The returned
random string must match its input. It does not run inference or change Codex
config files: only the started process receives its MCP override through -c.
Codex must support that API.

For another endpoint:

```sh
python3 scripts/check_codex.py --url http://127.0.0.1:8000/mcp
```

For a single ordinary session, pass the endpoint on startup:

```sh
codex -c 'mcp_servers.agent_hub.url="http://127.0.0.1:7331/mcp"'
```

Then ask: "Call hub_ping with message=hello." An already running Codex session
does not automatically load a server from this project's files.

## Verification

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
python3 -m unittest discover -s tests -p 'test_*.py'
```

Integration tests start a local HTTP server and cover health, tool discovery,
JSON responses, invalid argument types, session registration/update without
duplicates, shared storage across MCP connections, reconnect, and registration
validation. Both MCP lifecycles are covered: initialize version 2025-11-25 and
server/discover version 2026-07-28. Messaging tests start the real Hub binary with
a temporary database and cover two crash/restarts, retained sessions/messages/ACK,
correlated replies, addressing and ID conflicts, recipient validation for ACK,
inbox pagination, redelivery of unacknowledged messages, and concurrent retries
of one ID through two MCP connections.

Milestone 1 was verified on 2026-10-11 with Rust 1.97.1, axum 0.8.9, rmcp 3.5.1,
and Codex CLI 0.162.1: Cargo checks, registration demo, and direct hub_ping through
App Server passed. That probe covers MCP integration, not model-selected calls.
Milestone 4's full demo adds real turns.

Milestone 2 was verified in the same environment with rusqlite 0.40.2: five
integration tests, fmt/clippy, messaging_demo, and check_codex.py --messaging
passed. The demo reopened the database after two HTTP/MCP restarts; tests
performed two process crashes. Two temporary Codex sessions checked
send/read/ACK/reply directly through App Server without inference.

Milestone 3 checked HTTP results and MCP inbox, terminal statuses, nullable text,
runtime errors, artifact references, validation/conflicts, concurrent retries,
two crash/restarts, and retry after ACK. Five Python checks covered final-text
selection and outbox retention after failure/lost HTTP responses. The real
lifecycle probe captured SessionStart/Stop, including continuation from 4 to 5
within one turn. Turn_results_demo delivered a real worker result and checked
restart, retries, and ACK. A timeout probe persisted interrupted with null text;
the earlier direct messaging probe also passed.

Milestone 4 on Codex 0.162.1 passed nine Rust integration tests, 12 Python tests,
fmt/clippy. A real input probe returned 42 from retained context. The full demo
verified orchestrator-selected MCP, automatic worker results, and new turns in
the same orchestrator with 41, 43, 45, including busy-turn waiting, disconnect,
resume, and SSE after Hub crash/restart.

Ordinary CLI hook integration was also verified: two interactive codex sessions
registered, the orchestrator sent MCP assignments, and worker results continued
the original conversation with 41, 43, 45. A busy turn retained a result without
ACK. Hub restart and bridge reconnect preserved both CLIs. User input in the same
terminal returned 44. Twenty-three Python checks included runtime queue/history
recovery, ACK retries, and preserving user hooks/config.
[Events](docs/evidence/codex-cli-hooks.json).

The minimal local workflow was rechecked on 2026-10-12: 29 Python tests, nine Rust
tests, fmt/clippy, and a real --smoke passed. Worker completed without MCP calls
and its result continued the original orchestrator conversation.
[Events](docs/evidence/codex-prototype.json).

Leases were verified on 2026-10-13: **16 Rust**, **37 Python** tests, fmt/clippy,
and three isolated runs with two real Codex CLIs each passed. Checks included
explicit release, runtime release without model action, and TTL handoff while
the old owner was active. [Contract, results, evidence](docs/codex-locks.md).
Two additional runs with the earlier hints showed proactive lock selection
without Skills or an explicit lease request: both agents acquired, waited,
released through MCP, and preserved edits. After lease hints were removed from
SessionStart and tools scoped to an observed conflict or explicit request, the
same assignments produced **zero MCP calls** in both variants while preserving
both edits. The suite now has **38 Python tests**. Leases remain an optional
escalation. Self-triggered escalation after an actual edit conflict under this
new policy has not been verified. [Details](docs/codex-locks.md).

Historical evidence is preserved verbatim, including its original language.
Current documentation, instructions, and executable example prompts are English.
The English prompts were rechecked on 2026-10-14: routine edits made zero MCP
calls and explicit lease coordination retained automatic runtime release.
[English verification](docs/codex-locks.md).

## Prototype limits

SQLite persists sessions, messages, and ACKs. Registration does not establish
live presence; records have no TTL or automatic removal. External integration
saves turn results automatically. Ordinary CLI support uses hooks and the local
shared daemon, preserving user control and permissions. It uses experimental
thread/queue APIs in Codex 0.162.1. No-daemon, Windows, IDE, and Codex App paths
are unverified. The separate stdio client owns its thread, uses read-only
sandbox, and rejects interactive approval/input requests.

Delivery can repeat after a crash between runtime acceptance and a local receipt:
Hub and Codex have no shared transaction. Hub does not assess task success.
The prototype targets trusted local integrations of one user; sender_id and
session_id are not authenticated against a connection. Messages are not deleted,
and there is no dedicated tool to view acknowledged history. Agent session IDs
are independent of MCP transport sessions. Legacy MCP clients retain transport
session support; modern clients work without them. SDK support alone does not
ensure automatic LLM wake-up after a notification.

Sources:

- [Official MCP Rust SDK](https://github.com/modelcontextprotocol/rust-sdk)
- [Streamable HTTP example with axum](https://github.com/modelcontextprotocol/rust-sdk/blob/main/examples/servers/src/counter_streamhttp.rs)
- [SQLite through rusqlite](https://docs.rs/rusqlite/0.40.2/rusqlite/)
- [MCP in Codex](https://developers.openai.com/codex/mcp)
- [Codex App Server](https://developers.openai.com/codex/app-server)
