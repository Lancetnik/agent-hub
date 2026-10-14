# Automatic file leases for Codex edits

With a running Hub and trusted installed hooks, `apply_patch` acquires file
leases without model MCP calls. No coordination Skill or per-project agent
instructions are required. Ordinary shell reads remain unchanged.

```text
apply_patch
    |
PreToolUse: canonical paths -> acquire short silent leases
    |
    +-- free ---------> run patch -> PostToolUse releases
    |
    +-- waiting ------> wait for release or deadline
                          |
                       release/cancel ticket
                          |
                       DENY original patch
                          |
                       agent rereads files and submits NEW apply_patch
```

Granting a previously busy file never automatically executes the original
patch. The hook returns a supported `PreToolUse` denial explaining that the
patch was not executed and asking the model to reread the named files, retain
their current changes, and prepare a new patch. This also applies if the patch
would still match: waiting itself triggers the rejection. The retry acquires
new leases under a new tool-call ID. A further contention can reject it again.
Rereading and retrying are model behavior, verified in the scenario below;
the hook does not generate or merge edits.

## Installation and limits on waiting

Restart Hub with the updated binary, then rerun the normal installer:

```sh
.venv/bin/python scripts/codex_hooks.py install
codex
```

Review and trust the new `PreToolUse` and `PostToolUse` entries in `/hooks`.
Both match `^apply_patch$`. Start a new conversation or use `codex resume`.
Existing hooks and settings are preserved, including during an upgrade from
the three lifecycle hooks. The installer does not auto-trust hooks.

Defaults in the installed `agent-hub/settings.json`:

| Setting | Default | Purpose |
| --- | --- | --- |
| `file_lock_wait_seconds` | 30 | Maximum total waiting budget for the hook; range 0–60. |
| `file_lock_ttl_seconds` | 30 | Lease lifetime starting on grant; range 1–600. |

The command-hook timeout is 75 seconds, longer than the allowed wait budget.
Routine HTTP requests use a two-second timeout. Hub unavailability, malformed
patch input, or an unavailable journal produces an explicit denial. A wait
deadline produces a denial and cancels the request. If a hook disappears, an
acquired lease expires by TTL and a silent queued request is automatically
cancelled after 60 seconds. The Hub deadline timer also handles startup recovery.

## Scope and state

Paths come from `Add File`, `Update File`, `Delete File`, and `Move to` patch
headers. The hook resolves them against its `cwd`, including symlinks, and
uses canonical absolute paths. Rename locks both the source and destination.
Multiple paths are acquired in sorted order. On contention, previously acquired
paths are released before waiting; the whole original patch is rejected.
Acquisition is not atomic across files, and the underlying patch operation is
not made transactional by these leases.

Requests use stable IDs derived from session, turn, tool call, and resource.
The local journal records the call before HTTP acquisition and binds acknowledged
tickets to the runtime turn. `PostToolUse` writes idempotent releases to the
outbox; terminal-turn cleanup and TTL provide fallbacks. A lost acquisition
response denies the edit; Hub still bounds the request's lifetime.

Silent `notify=false` tickets share the ordinary per-resource FIFO queue.
`GET /locks/requests/{id}?session_id=...&wait_seconds=...` subscribes before
reading SQLite, then waits on state-change hints and reconciles durable state.
There is no frequent status polling and no grant/expiry conversation input.
Distinct silent tool calls can queue within one session, including callers
whose hooks share a parent session ID. Existing explicitly acquired MCP leases
owned by that session are borrowed, without releasing them after each edit;
the manual owner remains responsible for serializing its child writers.

Session logs include `file_lock_waiting`, `file_lock_wait_finished`,
`file_locks_acquired`, `file_locks_released`, `file_lock_borrowed`, and
`file_lock_error`, with timestamps, turn/tool IDs, and relevant resources or
tickets. Existing SQLite request status shows acquired/waiting/released/expired/
cancelled and the acquired deadline.

## Verification

On 2026-10-14, Codex CLI **0.162.1**, two fresh ordinary CLIs in one disposable
checkout passed the controlled scenario. Personal Skills were disabled. Each
task requested a field edit using `apply_patch` and preservation of other fields;
neither task mentioned Hub, leases, recovery instructions, or negotiation.

The test fixture pauses alpha's production hook after acquisition and before
allowing its patch. Beta reads the initial file and requests the same resource.
After observing beta waiting, the fixture releases alpha. The production hooks
perform all acquisition, waiting, denial, and release; models create all edits.

Observed sequence: alpha saves `timeout_ms=2000`; beta's original patch is
denied after grant; beta rereads JSON containing alpha's change, then uses a
new `apply_patch` to set `max_retries=3`. Both changes and the label persist.
There are **zero model MCP calls**, **two work turns** plus two READY bootstrap
turns, and no extra conversation input. This verifies structured edits under
controlled contention, not natural selection of an edit tool or shell writes.

[Real runtime history and hook events](evidence/codex-file-lock-hooks.json).
The scenario passed twice. A separate two-CLI regression smoke also passed:
worker completion without MCP continued the original orchestrator dialogue
with its remembered context. [Completion smoke evidence](evidence/codex-file-lock-completion-smoke.json).
Reproduce with authenticated Codex and Python dependencies:

```sh
.venv/bin/python scripts/check_codex_file_locks.py \
  --output /tmp/codex-file-lock-hooks.json --max-seconds 240
```

Deterministic checks use controlled HTTP barriers and a controlled server clock:

- [test_codex_file_locks.py](../tests/test_codex_file_locks.py): acquisition before
  allow, post-tool release, wait then denial, fresh retry, deadline cancellation,
  canonical rename/multi-file paths, partial acquisition cleanup, journal failure,
  and borrowing an existing manual lease.
- [locks_http.rs](../tests/locks_http.rs): silent FIFO handoff and waiting without
  conversation input, including two tool calls from the same session; upgrading
  the previous SQLite schema preserves manual owners, waiters, and notifications.
- [locks.rs](../src/locks.rs): abandoned silent-waiter cancellation and compatibility
  with manual leases, alongside the existing lease state tests.
- [test_codex_hooks.py](../tests/test_codex_hooks.py): installer matching/timeouts,
  preservation of user hooks, and existing runtime completion/recovery behavior.

Final verification: **20 Rust tests**, **47 Python tests**, `cargo fmt --check`,
`cargo clippy --all-targets -- -D warnings`, and Python compilation passed.

## Coverage limits

Only structured `apply_patch` calls are intercepted. Shell commands and scripts
can write arbitrary files; their write sets cannot be inferred reliably from
command text. Native subagent tool-hook interception has not been separately
verified in this real scenario; same-session HTTP queueing is tested.

Hooks are not a filesystem enforcement boundary. A killed/timed-out callback
can fail without blocking the tool, and TTL cannot stop an already running
writer. Short per-operation leases also do not prevent semantic conflicts or
stale reads outside contention. Stronger enforcement would need controlled
writes or separate worktrees. Current [Codex hook coverage](https://learn.chatgpt.com/docs/hooks)
and [failure behavior](https://learn.chatgpt.com/docs/enterprise/managed-configuration)
describe these runtime limitations.
