# Resource leases with bounded lifetime

Model-requested MCP leases are an optional escalation for an observed overlapping-edit conflict
with another agent, or an explicit request to serialize a particular resource.
Routine edits use local tools; another registered session does not require
checking or acquiring a lease through MCP. Installed CLI hooks now acquire
[automatic file leases](codex-file-locks.md) for `apply_patch` independently of
the model; the inter-turn MCP workflow below remains available.

For this coordination, Hub maintains one owner and a FIFO queue **per resource**.
A waiting Codex finishes its turn; Hub grants the lease and resumes the same
conversation with `lock_granted`. The model need not poll inbox or wait for an
explicit peer message saying "I am done."

## Contract

```javascript
hub_acquire_lock({
  "session_id": "<UUID from SessionStart>",
  "request_id": "<UUID>:settings-edit-1",
  "resource": "/absolute/checkout/settings.json",
  "ttl_seconds": 120
})
```

The response includes those fields, status, and expires_at_ms (Unix milliseconds).
Default TTL is **120 seconds**, range **1–600 seconds**. It starts on grant;
time in queue does not consume TTL. A notifying MCP waiter has no queue deadline.

| Status | Meaning |
| --- | --- |
| `acquired` | Granted until expires_at_ms. |
| `waiting` | Persisted in queue; finish the turn and wait for notification. |
| `released` | The owner released it; this request will not be granted again. |
| `expired` | TTL elapsed; this request will not be granted again. |
| `cancelled` | Waiting was cancelled through release. |

Request_id is globally unique: use session_id and a fresh suffix for every new
critical section. Replaying **identical arguments** returns current status,
**never renews TTL**, and never requeues a terminal request. Changed arguments
with the same ID fail. One session cannot create two acquired/waiting requests
for the same resource through the notifying MCP workflow. Silent hook requests
can queue distinct tool calls from the same session.

After acquired or lock_granted, confirm current status by replaying acquire,
reread the latest file, preserve compatible changes, and finish writes before
expires_at_ms. On expired/released/cancelled, stop writing under that request;
a later attempt needs a new ID. Resource keys are compared literally: all writers
must use the same canonical absolute path, including the checkout. Hub does not
resolve symlink or path aliases itself.

```javascript
hub_release_lock({
  "session_id": "<your UUID>",
  "request_id": "<the same UUID>:settings-edit-1"
})
```

Release frees an acquired lease or cancels a waiting request. Repeats and release
of an expired request are safe: an old token cannot release the next owner's
lease. A different session_id fails validation; this is argument checking,
not connection authentication.

The ordinary CLI bridge binds an observed acquired MCP result to its runtime
turn and releases on completed, failed, or interrupted, even without model MCP
release. A resumed lock_granted also binds release to that turn. Waiting survives
the waiting turn's completion. Release intent is written to the local outbox
first; after restart, available runtime history recovers a missed release. If
a lost MCP response prevents observation, or the bridge disappears, TTL remains
the fallback. Arbitrary MCP clients without this bridge can release explicitly
and also receive bounded TTL.

## Persistence and delivery

SQLite holds requests, the unique owner, and arrival order. Handoff to the next
waiter and its durable lock_granted inbox event commit in one transaction. On
expiry, the old owner receives lock_expired. Notifications use stable IDs;
repeated release/expiry does not create another logical grant. Message IDs
starting with `hub-lock:` are reserved for these events.

A deadline timer wakes Hub at the nearest expires_at_ms; it does not poll.
Startup reaps expired owners. SSE signals durable inbox; the bridge reads it
and queues input in the idle existing CLI. A busy CLI receives notification
after its current turn ends. The bridge ACKs and skips a stale grant instead
of starting work under it. The model also confirms current status before writes,
since expiry can occur after runtime queue acceptance.

Lock_granted and lock_expired do not generate automatic turn_completed replies
to the previous owner, avoiding extra reciprocal turns after an editing turn.

HTTP equivalents are available for diagnostics:

- `POST /locks/acquire`: the same arguments as the acquire tool.
- `POST /locks/release`: the same arguments as the release tool.
- `GET /locks/requests/{request_id}?session_id=...`: inspect an owned request.

Hooks use `notify=false` on acquire and `wait_seconds` (0–60) on inspection.
These requests share resource FIFO but do not create `lock_granted` or
`lock_expired` conversation messages. A silent waiter is cancelled after 60
seconds if abandoned. An automatic edit can borrow its session's existing
notifying acquired lease; it returns that original ticket and does not release
it in `PostToolUse`. This avoids waiting on the model's own explicit lease.

State shows whether a particular request is acquired and its deadline. Unrelated
resources can continue independently.

## Real protocol verification

2026-10-13, Codex CLI **0.162.1**: three separate runs with two new ordinary CLIs
in one temporary git checkout each. Hub, CODEX_HOME, journals, and hooks were
temporary; global settings and user sessions were unchanged. Personal Skills
were disabled, with no extra Skill or subagents. Prompts explicitly required
the lease workflow. Each run was bounded to 300 seconds, with at most two CLIs
active. Barriers only delayed commands; agents reread and edited the target JSON.

| Scenario | Check | Result |
| --- | --- | --- |
| Explicit release | A holds the lease; B ends its waiting turn. A edits timeout and releases. B resumes, rereads, and edits retries. | PASS: both edits and label retained; 3 work turns, 6 acquire/release calls, 1 grant. |
| Forgotten release | A edits timeout and completes without hub_release_lock. Bridge releases before TTL. | PASS: both edits retained; 3 work turns, 5 acquire/release calls, 1 grant. Observed A completion to B grant turn start: 48 ms. |
| TTL with active old owner | A is held by a barrier for 30 seconds. Without status polling during the wait, the timer grants B while A is still active. A then sees expired and tries stale release. | PASS: B remains acquired; A does not write, B saves retries=3 and other initial fields. 4 work turns, including expiry notification to A. |

In the first run, observed completion of A's MCP release to B's turn start took
34 ms. These are individual runtime-observer measurements, not an SLA.

Excluding READY bootstrap, ordinary lease workflows used **3** work turns versus
**12** in the earlier [negotiation scenario](codex-file-conflicts.md), with no
negotiation messages. Prompts and ordering differ: the earlier experiment began
after a lost edit, while leases prevented it with cooperative writers. This
shows fewer exchanges, **not proven token or cost savings**. New evidence includes
runtime tokenUsage.total/last with cached input. The earlier experiment has no
comparable counters. Cached input is included in inputTokens; do not add it again.

Evidence: [release](evidence/codex-lock-release.json),
[runtime release](evidence/codex-lock-runtime-release.json),
[expiry](evidence/codex-lock-expiry.json).

A separate regression smoke with two new CLIs also passed: the worker made
**zero MCP calls**, result 4 continued the original orchestrator with answer 41,
retaining 37 from context, and SessionEnd cleanup passed.
[Smoke evidence](evidence/codex-lock-regression-smoke.json).

## Proactive lease selection with the earlier hints

Before the routine-edit policy changed, **two further real runs** used two fresh
CLIs each on the same version. Nine personal Skills were disabled; there was no
extra Skill or project AGENTS.md. Installed SessionStart and tool descriptions
remained: the hook conditionally explained leases ("When using Hub leases")
without requiring a lease before each edit. This tested absence of additional
user task instructions, not absence of all Hub context.

Both agents received assignments almost simultaneously in one temporary
checkout. There was no controlled writer, FIFO, injected save, or prescribed
edit method; only a single-line settings.json and git baseline remained. First
work turns overlapped by **11.992 seconds** and **14.234 seconds** respectively.

The following task descriptions are English translations; historical captures
preserve the original prompts and responses verbatim.

| Variant | Assignments | Observation |
| --- | --- | --- |
| `natural` | "Set timeout_ms=2000 in settings.json. Preserve other fields and verify the result." B received the equivalent task for max_retries=3. No Hub, lease, or peer mention. | PASS for edit preservation: both independently acquired TTL=120. B edited first; A received waiting, finished, resumed on grant, and reread B's edit. Both explicitly released through MCP. 3 work turns, 5 MCP calls. |
| `peer-aware` | The same tasks plus "Another independent Codex session is working in this checkout concurrently; preserve its changes." No coordination method specified. | PASS for edit preservation: both chose leases; A also listed sessions. The same waiting/grant/release sequence, 3 work turns, 6 MCP calls. |

Both final files had timeout_ms=2000, max_retries=3, label=fixture. SQLite held
two released requests; inbox and runtime queue were empty. Exact prompts, tool
selection/results, and real read/write commands are in
[natural evidence](evidence/codex-lock-natural.json) and
[peer-aware evidence](evidence/codex-lock-peer-aware.json).

These observations revealed unnecessary proactive use: both acquired/released
before any conflict for routine compatible edits. Natural added five MCP calls,
peer-aware six. Correct final contents do not justify that cost for routine work.
Lease guidance was therefore removed from standard SessionStart, and the tool
description and optional Skill scoped to an observed conflict or explicit
serialization request. Messaging guidance now keeps routine work local.
Earlier evidence remains for comparison.

One run per variant does not establish behavior across models. Tool-only
selection with all hook hints removed, and native subagents independently
selecting leases, were not tested here. Hooks/Skill were unchanged between these
two historical variants.

## Routine edits after changing the hints

The same assignments were repeated in two fresh isolated Codex 0.162.1 runs,
without Skills or AGENTS.md. `--expect-local-only` checks runtime history for
zero MCP calls; this expectation **is not added to agent prompts**. Assignments
and the single-line JSON matched the historical variants, with no helper/barrier.

| Variant | MCP before | MCP after | Result after the change |
| --- | --- | --- | --- |
| `natural`: no peer mention | 5 | **0** | PASS: timeout=2000, retries=3, label retained. 2 work turns, 16.906 seconds overlap. |
| `peer-aware`: concurrent peer mentioned | 6 | **0** | PASS: both edits retained. 2 work turns, 17.224 seconds overlap. |

Neither run created a lock request; queue and inbox were empty. Agents used
ordinary read/edit/verify commands. Evidence:
[natural](evidence/codex-lock-on-demand-natural.json),
[peer-aware](evidence/codex-lock-on-demand-peer-aware.json).

The explicit runtime-release scenario was also repeated with the new policy:
PASS, 3 work turns, 5 MCP calls. A held the lease, B ended its waiting turn and
received a grant after A's terminal completion. A made no MCP release; the bridge
released it. Both edits persisted.
[Explicit evidence](evidence/codex-lock-on-demand-explicit.json).

These are model guidance changes, not an API prohibition: the same tools remain
available. Two examples show removal of extra calls in those tasks, not a
universal guarantee for every model/situation. Tool descriptions still occupy
MCP catalog context, so zero calls do not mean zero integration context cost.
The frequency of real conflicts is not measured. **Self-triggered escalation
after an actual edit conflict under the new policy has not been verified.**

To receive updated descriptions and SessionStart without the old lease guidance,
restart Hub and open new Codex conversations. Previous hints may remain in an
older conversation's history.

## Verification of English prompts

On 2026-10-14, after translating the executable prompts, two fresh bounded runs
on Codex CLI 0.162.1 passed:

- `natural --expect-local-only`: zero MCP calls, two work turns, both edits
  retained. [English natural evidence](evidence/codex-lock-english-natural.json).
- `runtime-release`: three work turns, five MCP calls, waiting/grant delivery,
  automatic bridge release without owner MCP release, and both edits retained.
  [English explicit evidence](evidence/codex-lock-english-explicit.json).

Each used two temporary CLIs, no extra Skills, and a 240-second overall deadline.
Historical captures above remain unchanged rather than being translated in place.

## Reproduction and tests

```sh
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
python3 -m unittest discover -s tests -p 'test_*.py'

# Requires installed/authenticated Codex and Python dependencies; uses real turns.
.venv/bin/python scripts/check_codex_locks.py --scenario release \
  --output /tmp/hub-lock-release.json
.venv/bin/python scripts/check_codex_locks.py --scenario runtime-release \
  --output /tmp/hub-lock-runtime-release.json
.venv/bin/python scripts/check_codex_locks.py --scenario expiry \
  --output /tmp/hub-lock-expiry.json
# Observe tool selection without a helper, barrier, or lease instructions:
.venv/bin/python scripts/check_codex_locks.py --scenario natural --max-seconds 240 \
  --output /tmp/hub-lock-natural.json
.venv/bin/python scripts/check_codex_locks.py --scenario peer-aware --max-seconds 240 \
  --output /tmp/hub-lock-peer-aware.json
# Require ordinary work without MCP calls under the current policy:
.venv/bin/python scripts/check_codex_locks.py --scenario natural --expect-local-only \
  --max-seconds 240 --output /tmp/hub-lock-on-demand-natural.json
.venv/bin/python scripts/check_codex_locks.py --scenario peer-aware --expect-local-only \
  --max-seconds 240 --output /tmp/hub-lock-on-demand-peer-aware.json
```

Before automatic file hooks were added, this revision passed **16 Rust** and
**38 Python** tests, fmt/clippy. Server
implementation: [locks.rs](../src/locks.rs), schema in [store.rs](../src/store.rs),
routes in [lib.rs](../src/lib.rs), tools in [mcp.rs](../src/mcp.rs). Lease-to-turn
binding and durable release are in LeaseJournal in
[codex_worker.py](../scripts/codex_worker.py), used by the
[CLI bridge](../scripts/codex_hooks.py) and
[external adapter](../scripts/codex_session.py). Waiting policy lives in the
acquire tool description and optional coordination Skill. Standard SessionStart
provides identity and messaging rules, keeping routine edits local.

Added checks:

- [src/locks.rs](../src/locks.rs): five controlled-clock tests for expiry, full
  waiter TTL, no renewal, cancellation, concurrent acquisition, unrelated
  resources, validation, foreign release, and disk restart.
- [tests/locks_http.rs](../tests/locks_http.rs): MCP FIFO/dedup/stale release;
  the independent TTL=1 timer wakes a waiter through SSE without release or
  status polling. Waiting is synchronized by reading SSE.
- [tests/test_codex_hooks.py](../tests/test_codex_hooks.py): automatic release,
  waiting/grant lifecycle, history recovery, stale-grant skip, no reciprocal
  result, and SessionStart identity/messaging without advertising leases.
- [tests/test_codex_session.py](../tests/test_codex_session.py): external adapter
  boundaries: stale-grant skip and release without a reply to the prior owner.

## Limits and recommendations

Coordination is **advisory** in a trusted local Hub; participants cooperate.
TTL cannot interrupt an already running filesystem writer or guarantee no old
owner writes after expiry. Strong enforcement needs a controlled writer with
fencing or separate worktrees. The tests demonstrate cooperative agent behavior,
not mandatory compliance by every model.

Prepare analysis outside the lease. Hold it only for rereading, editing, and a
short check. Start with 120 seconds; keep long commands outside the section.
Use one resource at a time initially. There is no renewal or atomic multi-resource
acquisition. Offline waiters are not removed automatically: once granted, their
TTL expires and the queue advances. Requests/status records persist without cleanup.

Native children are not separately registered owners. The parent acquires,
coordinates its child, and waits for it before release. The bridge does not
observe child lease calls as part of the parent's turn. Lease usage by children
was not separately tested here.

Leases do not combine turns into a task. If a Hub-assigned worker ends its first
turn while waiting, its automatic result is **partial**, not task success. A
later grant turn does not automatically report back to that original task sender:
it needs an explicit correlated final message or a future request-to-task link.
The tested workflow is two independently instructed sessions. Completion without
MCP remains supported for ordinary incoming tasks without a lease wait.

Lease instructions live in their MCP tool descriptions; the
[optional Skill](../skills/agent-hub-coordination/SKILL.md) covers escalation after
a conflict or explicit request. Standard SessionStart does not advertise leases
before file edits. Tool availability does not guarantee model selection; Hub
does not infer semantic edit conflicts. Automatic structured-edit interception
is a separate mechanism described in [codex-file-locks.md](codex-file-locks.md).
