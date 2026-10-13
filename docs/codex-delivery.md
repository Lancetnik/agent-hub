# Delivering messages to an existing Codex session

The persistent external client `scripts/codex_session.py` connects a Hub inbox
to one Codex App Server thread. The user starts the client and chooses a stable
Hub address. Hub never starts a runtime or worker.

For ordinary interactive `codex`, use the [hook integration](codex-hooks.md):
it attaches to an already running CLI, registers it automatically, and leaves
control with the CLI. This document describes the separate external stdio client.

## Verified interface

On Codex CLI 0.162.1, `scripts/check_codex_input.py` sent two real turns to one
thread: the first remembered 37, and the second added 5 without receiving the
number again and returned 42. IDs and acceptance status are recorded in
[codex-input.json](evidence/codex-input.json).

The [official App Server contract](https://learn.chatgpt.com/docs/app-server)
describes `thread/resume`, `turn/start`, `turn/completed`, and stdio transport.
The adapter uses `turn/start` with the existing `threadId`, saves the returned
`turn.id`, and sends an ACK. The terminal event describes a turn result; the
orchestrator assesses task success. This path does not use `turn/steer`:
incoming messages wait for a busy turn to finish.

## Server contract

- `GET /sessions/{session_id}/events`: `text/event-stream`, with `inbox_changed`
  events carrying empty JSON `{}`. Each connection receives an initial event.
  New messages and retries of unacknowledged messages signal after the SQLite
  write. A keepalive every two seconds maintains the connection. The client reads
  inbox on a signal, after a busy turn completes, and on a 30-second recovery
  check. An unknown session returns HTTP 404.
- `GET /sessions/{session_id}/inbox?limit=100&cursor=...`: the existing MCP inbox
  contract, including order, an exclusive cursor, unacknowledged entries only,
  and `next_cursor`. Start each new scan without a cursor.
- `POST /sessions/{session_id}/ack`, body `{"message_id":"..."}`: the existing
  idempotent ACK with recipient validation. Reading and SSE do not acknowledge
  a message.

These HTTP endpoints use the same tables and operations as MCP tools. SSE stores
no event history and carries no message payload. After disconnect, a missed
signal, or notification queue overflow, the client rereads the durable inbox.
Encode the session ID as a single URL path component.

## External client state

`--state` selects a separate SQLite database for each session. It stores the Hub
address, thread ID, accepted messages and their turn IDs, ACK state, and result
outbox. Only one process can use a state file at a time: it is locked for the
duration of the client process.

On first launch, the client creates a persistent thread, saves its ID, and
registers it in Hub. Subsequent launches resume that thread. The initial prompt
runs only on creation; restarting the command does not submit it again. Each
incoming message enters context in full, including `message_id`, sender, and
correlation. While a turn is busy, the message stays in inbox without an ACK.

After the `turn/start` response, the client commits a local acceptance receipt
before acknowledging Hub. A lost ACK response is retried using that receipt.
Until acknowledgement succeeds, the adapter does not accept the next input.
Runtime results are processed independently of ACK delivery.

A worker with `--report-results` saves the result of a completed incoming turn
in its local outbox, addressed to the accepted message's sender. It preserves
`correlation_id`, falling back to `message_id` when correlation is absent.
`failed` and `interrupted`, nullable text, and runtime errors are forwarded just
as in the milestone 3 one-shot adapter. Saved HTTP events retry automatically.
On resume, the client reads persisted turns/items to recover missed terminal
results for accepted turns.

## Verification and reproduction

```sh
python3 scripts/check_codex_input.py --output /tmp/codex-input.json
python3 scripts/check_codex_delivery.py --output /tmp/codex-delivery.json
```

The full demo creates two real sessions: the orchestrator chooses an MCP tool
and sends a task; the worker finishes without a completion tool; the
orchestrator automatically starts another turn in the same thread and uses the
number remembered in its initial turn. Three scenarios cover an idle session,
a busy session with a result in unacknowledged inbox, and adapter disconnect
with a Hub crash/restart and resume of the original session. Results: 41, 43, 45.
Event capture: [codex-delivery.json](evidence/codex-delivery.json).

The HTTP/MCP test checks SSE signals, cursors, shared ACK, reconnect, and message
retention after reopening the database. Python tests cover runtime rejection
without ACK, busy-turn waiting, lost ACK responses and restart retry without a
second turn, result recovery, and identity protection.

## Delivery limits

- This client owns one stdio App Server and its thread. Ordinary CLI attachment
  is implemented separately by the [hook integration](codex-hooks.md). Restarting
  the external client can resume its thread. Concurrent control from another UI
  is unsupported; one Hub address must have one owner and one state file.
- There is a crash window between runtime acknowledgement and committing the
  local acceptance receipt. A crash or lost RPC response can cause a duplicate
  turn. Delivery permits repeats; there is no exactly-once execution guarantee.
  Tasks should account for `message_id` when performing external side effects.
- ACK means runtime acceptance. A later runtime error, interruption, or crash
  does not return the accepted message to inbox. The worker reports a terminal
  result when it is available in events or persisted history. The adapter does
  not invent a result if the runtime failed to persist it.
- Codex registrations and history persist without automatic cleanup. The local
  journal also grows. Preserve state/thread history for sessions you need to
  resume. Losing the initial prompt in the crash window after thread creation
  requires an explicit new message; the initial prompt is not retried.
- The prototype uses `read-only` sandbox and `approvalPolicy: never`, rejects
  interactive input/approval requests, and inherits the installed Codex config.
  Sessions use that Codex installation's model and settings.
- Enable `--report-results` for workers that answer incoming tasks. Incoming
  `turn_completed` results do not generate an automatic reply, preventing an
  endless result exchange. The flag is off by default for orchestrators.
- SSE and HTTP serve trusted local integrations without authentication. Recovery
  requires the Hub database, adapter state, and Codex history. Stopping the
  adapter closes its App Server and may interrupt an active turn.
