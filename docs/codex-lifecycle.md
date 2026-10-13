# Connecting Codex turn results

Ordinary interactive CLI is supported by the separate
[hook integration](codex-hooks.md), which attaches to its shared daemon.
This document retains milestone 3's stdio adapter verification and limits.

Verified on 2026-10-11 with `codex-cli 0.162.1` for
[Prepare milestone 3: verify real Codex lifecycle callbacks](https://github.com/Lancetnik/agent-hub/issues/12)
and [Milestone 3: automatic delivery of worker turn results](https://github.com/Lancetnik/agent-hub/issues/7).

## Verified result source

The external adapter uses Codex App Server over stdio. It registers after
`thread/start`, binding the user-selected Hub address to the returned `thread.id`.
It reports a result only after `turn/completed`, copying `turn.id`, `turn.status`,
and `turn.error`. Text comes from the last completed `agentMessage` in that
thread/turn with phase `final_answer` or unknown phase `null`. Commentary is
excluded. Completed `fileChange` items provide artifact paths; Hub does not
receive the files themselves.

This follows the [official App Server contract](https://learn.chatgpt.com/docs/app-server)
and schemas generated from the installed binary:

```sh
codex --version
codex app-server generate-json-schema --out /tmp/agent-hub-codex-schema
```

`item/completed` supplies contents even when `turn.items` is only a summary or
empty. `completed`, `failed`, and `interrupted` are terminal turn statuses.
The orchestrator assesses task success; the adapter does not make that decision.

## Real callbacks

Reproduction requires Python 3.11+, an installed and authenticated Codex.
The probe runs real inference and consumes Codex quota.

```sh
python3 scripts/check_codex_lifecycle.py --output /tmp/codex-lifecycle.json
python3 scripts/check_codex_lifecycle.py --continue-once --output /tmp/codex-continuation.json
```

The probe creates a temporary CODEX_HOME with its own recorder and isolated
config. It reuses existing authentication via an auth.json symlink and the model
from user config; user settings and hook trust remain unchanged. Trust is written
only for the exact verified recorder definitions in temporary config. Other user
hooks and plugins are not loaded by this probe.

Observed:

- `SessionStart`: session_id matches the new root thread.id, `source: startup`.
- Ordinary `Stop`: the same session_id, turn ID, `last_assistant_message: "4"`,
  `stop_hook_active: false`, followed by `turn/completed` with `completed` status.
- Continuation through `Stop`: the first callback contains `"4"`; the recorder
  returns `decision: block`. The second callback for the same turn contains `"5"`
  and `stop_hook_active: true`. The adapter's terminal result is `"5"`.
- With unavailable networking, the turn ended `failed` without a `Stop` callback.
- An external worker with `--timeout 0.001` ended `interrupted` with
  `final_text: null`, and its HTTP result persisted in Hub. Reproduce with the
  worker command below and this additional option; exit code 1 is expected.

Sanitized continuation capture: [codex-lifecycle.json](evidence/codex-lifecycle.json).
IDs remain for correlation checks; cwd, transcript paths, and authentication
contents are excluded.

`Stop` permits null text and may continue a turn, as documented in
[Hooks](https://learn.chatgpt.com/docs/hooks) and verified by the continuation
experiment. It is therefore not treated as terminal. This milestone does not
install a separate HTTP lifecycle hook for ordinary CLI/IDE sessions.

## External worker and delivery

With Hub running, register the orchestrator first, then run the external adapter:

```sh
curl --fail-with-body http://127.0.0.1:7331/sessions \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"orchestrator","runtime_id":"external-orchestrator","display_name":"Orchestrator"}'

python3 scripts/codex_worker.py \
  --session-id worker --recipient-id orchestrator --correlation-id assignment-1 \
  'Compute 2 + 2. Reply with the single digit only. Do not use tools.'
```

Worker registration and result submission happen automatically over HTTP. The
prompt contains no instruction to report completion through MCP. The adapter's
caller, either a user or external orchestrator, selects the recipient, original
assignment correlation, and stable `--session-id`. A result prints as one JSON
event to stdout and can be saved and POSTed again to `/turn-results`.

Before HTTP submission the adapter commits registration and result to a local
SQLite outbox (`.agent-hub/outbox.sqlite3` by default; override with `--outbox`).
It deletes an entry after a confirmed HTTP response. Registration precedes
results. Undelivered events survive process exit:

```sh
python3 scripts/codex_worker.py --retry
```

Retry does not start Codex; it uses saved URLs and exact event data. A new worker
launch also retries accumulated events. There is no persistent background retry:
run `--retry` or the next worker after Hub recovery. Unknown recipients and data
conflicts retain the event in outbox and expose the error to the caller. Reuse
the same `--outbox` when retrying.

## Integration limits

- Each adapter call owns one external App Server launch, a new ephemeral root
  thread, and one turn. Hub does not own the Codex process. Reusing session_id
  updates registration to the new thread; the result ID includes both IDs.
- The worker uses read-only sandbox and `approvalPolicy: never`. This is a local
  prototype path; interactive approval/input requests are rejected.
- The worker inherits installed user Codex config except its MCP config override
  and disabled analytics. Hooks undergo ordinary Codex trust checks. The isolated
  lifecycle probe above uses its own config.
- `--timeout` (180 seconds by default) and Ctrl-C request `turn/interrupt`;
  the received terminal event supplies the result. `failed`/`interrupted` exit 1
  even after successful delivery. Exit 0 means normal turn completion and
  delivery, not task success.
- Missing final text remains null; artifact references may be empty. For legacy
  models, phase null cannot distinguish commentary from a final answer.
- A Codex or adapter crash before observing and journaling terminal data does not
  create an invented result. Previously persisted events can be retried.
- The next milestone implements a persistent external client, SSE, and automatic
  orchestrator continuation: [Codex delivery](codex-delivery.md). This stdio path
  does not attach to arbitrary existing CLI/IDE sessions.

## Full demo

```sh
cargo run --example turn_results_demo
```

The demo starts a temporary Hub, runs a real worker, reads final value 4 from the
orchestrator's inbox through MCP, and checks correlation. It then repeats the
event, restarts HTTP/MCP with the same database, repeats again, and ACKs receipt.
After ACK, another retry returns the existing acknowledged message. Success ends
with PASS; temporary database and outbox are removed on exit.
