# Ordinary Codex CLI sessions through hooks

After installation, the user starts ordinary `codex`. The `SessionStart` hook
registers its runtime UUID in Hub and starts a background communication bridge.
The bridge attaches to the existing shared daemon over a Unix WebSocket. Hub
remains the communication service; the user owns CLI startup.

## Installation

Requires Python 3.11+, an installed and authenticated Codex, and a running Hub.
Verified on Codex CLI **0.162.1**. From this repository root:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/codex_hooks.py install --url http://127.0.0.1:7331
codex
```

In Codex, open `/hooks` and trust the **Agent Hub ordinary CLI integration**
commands for `SessionStart`, `Stop`, `SessionEnd`, `PreToolUse`, and `PostToolUse`.
The last two match only `apply_patch`. Then start a new `codex`
or resume a conversation with `codex resume`. Hook trust remains under user
control; the installer neither trusts other hooks nor bypasses review.

The installer adds its entries to `$CODEX_HOME/hooks.json` and the
`mcp_servers.agent_hub` endpoint to `config.toml`. `CODEX_HOME` defaults to
`~/.codex`. Existing hooks and settings are preserved; a conflicting MCP endpoint
causes an error. Reinstalling updates owned hooks without duplicates. Commands
contain absolute paths to Python and the script: reinstall after moving the
repository or environment. `--codex-home`, `--state-dir`, and `--socket` allow
a separate config, journal directory, or runtime socket.

Open two terminals with ordinary `codex`. `SessionStart` tells the model its own
`session_id`, matching the conversation UUID. `hub_list_sessions` discovers a
peer; `hub_send_message` sends it a task. The display name comes from the current
directory and may repeat; the address is the UUID.

Example orchestrator request: "Find the second session with `hub_list_sessions`
and ask it to compute 2 + 2 using `hub_send_message`, a fresh message_id, my
sender_id, and a correlation_id. Report the result when it arrives."
The worker receives incoming JSON with the original sender and correlation in
its own conversation. The integration automatically returns its turn result
to the sender.

`SessionStart` also explains waiting: after sending tasks, the orchestrator ends
its current turn and receives results in later turns. The worker answers with
an ordinary final response; sending that answer through MCP again would create
a second reply. This workflow does not require a separate Skill. Verification
with clean CLIs and native subagents is described in
[codex-coordination.md](codex-coordination.md).

For routine local edits, agents use local tools. `PreToolUse` automatically
acquires short file leases around `apply_patch`; `PostToolUse` releases them.
After contention, the hook waits, rejects the stale patch, and instructs the
model to reread before retrying. No Skill or model-side acquire/release call is
required. [Automatic file leases](codex-file-locks.md) describes coverage,
deadlines, and a real two-CLI verification. Explicit MCP leases remain an
escalation for an observed conflict or a request to serialize a broader resource.

## Acceptance and results

SSE signals that the bridge should read saved inbox entries. On connection,
reconnect, and recovery checks, the bridge scans inbox pages. While CLI is busy,
input remains in Hub without ACK until terminal completion of the current turn.
For an idle CLI, the bridge calls `thread/queue/add` on the existing thread. CLI
automatically runs the queued input as a new turn and retains context. If the
user starts a turn concurrently, the runtime manages the queue.

ACK follows the runtime `queuedSubmission` response and a local SQLite acceptance
receipt. It means queue acceptance, not task execution. The visible queue remains
under user control; cancelling an accepted queued input does not return its
message to Hub inbox. The bridge does not change the conversation's model,
sandbox, approvals, or instructions, and does not answer approval or user-input
requests.

Results come only from `turn/completed` with `completed`, `failed`, or
`interrupted`, completed items, and the runtime error. `Stop` starts a missing
bridge but is not terminal: another hook may continue the same turn.
`SessionEnd` stops the bridge while preserving state. This event describes
runtime session termination: with attached clients, closing a CLI window can
leave the session open. Archiving a conversation has verified `SessionEnd`
and bridge shutdown. Use `uninstall` to stop all integration bridges immediately.
The next `SessionStart` on resume reuses the UUID, acceptance receipts, and outbox.
Inputs with `payload.type=turn_completed` do not generate reciprocal automatic
results, preventing endless exchanges.

The same rule applies to `lock_granted` and `lock_expired`. The bridge binds
observed lease grants to a runtime turn, journals release in its outbox, and
releases on terminal completion even if the model forgot MCP release. A waiting
request survives between turns. TTL covers a lost bridge; the contract and real
two-CLI tests are in [codex-locks.md](codex-locks.md).

Before queue submission, the bridge saves an acceptance intent with the exact
input text. After a disconnect, it reconciles the runtime queue by
`clientUserMessageId` and history by userMessage, recovering acceptance after
a lost RPC response. Results persist before HTTP submission and retry after Hub
recovery. A bridge disconnect does not close the shared daemon or CLI.

## Verification

```sh
.venv/bin/python scripts/check_codex_hooks.py --output /tmp/codex-cli-hooks.json
```

The demo starts two **ordinary interactive CLIs** in PTYs with temporary
hooks/config, real authentication via a symlink to existing `auth.json`, and a
temporary Hub. It uses real turns and Codex quota. Only the isolated config
trusts its exact hook hashes; the user's config is unchanged.

Checks cover automatic registration, model-selected MCP calls, worker results,
continuation of the original orchestrator with a remembered number, an
unacknowledged result during a busy turn, Hub crash/restart, bridge reconnect,
and user input in the same CLI. Orchestrator answers: **41, 43, 45, 44**.
The demo also verifies retained read-only/never permissions and bridge shutdown
through `SessionEnd` when archiving temporary conversations. Successful events:
[codex-cli-hooks.json](evidence/codex-cli-hooks.json). Temporary config and
databases are deleted on exit.

## Removal and limits

```sh
.venv/bin/python scripts/codex_hooks.py uninstall
```

This removes owned integration entries and the unchanged owned MCP block, and
stops integration bridges. Local journals remain in `~/.codex/agent-hub` for
recovery. Delete them only after the required conversations finish; runtime
history and the Hub database are also needed for recovery. Hub session records
have no expiry and do not imply online presence.

The supported path is an ordinary CLI with the shared daemon on a local Unix
system. `--no-daemon`, Windows, IDE, and Codex App have not been verified through
this path. The bridge attaches only to a loaded thread; it does not create a
replacement or resume a conversation the user closed. Each `CODEX_HOME` needs
its own hook trust.

`thread/queue/*` is an experimental API in the tested version. Its availability
was verified with `codex app-server generate-json-schema --experimental` and
the real CLI demo, beyond documentation alone. Transport and the shared protocol
are described in [App Server](https://learn.chatgpt.com/docs/app-server), and
lifecycle/trust in [Hooks](https://learn.chatgpt.com/docs/hooks).

Hub, runtime, and journal have no shared transaction. If both runtime
queue/history and the receipt are lost after acceptance, duplicate execution
is possible; there is no exactly-once execution guarantee. Task side effects
should account for `message_id`. If terminal data are unavailable in both
events and history, the integration does not invent a result. Local journals
and history grow without cleanup. HTTP/MCP target trusted local integrations
of one user.
