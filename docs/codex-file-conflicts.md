# Two sessions editing the same file

Through MCP, agents can exchange requirements, choose an editor, and verify a
combined result. Hub provides messages and automatic turn results. Agents
themselves detect lost edits, select an editor, and merge contents; Hub does not
watch file changes or prevent writes.

[TTL leases with automatic wake-up](codex-locks.md) are now available to coordinate
conflicting edits. The experiment below retains results from the earlier
messaging workflow without leases.

## Real verification

2026-10-13, Codex CLI **0.162.1**. Three completed runs with two new independent
CLIs each, without subagents, Skills, or AGENTS.md. Both sessions share a temporary
git checkout with `workspace-write`/`never`. Registrations have the same display_name
`project`; addressing uses two distinct runtime UUIDs. Hub, CODEX_HOME, journals,
and project files are temporary. Global config, existing user conversations,
and working trees were unchanged; the fixture creates no commits.

`settings.json` starts with `timeout_ms=1000`, `max_retries=1`, and `label=fixture`.
Agent A is asked to set timeout to 2000 and B to set retries to 3. They prepare
complete file versions from the same initial snapshot. Agents invoke the
controlled writer themselves; it only delays saving their prepared text through
FIFO barriers:

1. Both read initial values and prepare their versions.
2. A's write produces `timeout_ms=2000`, `max_retries=1`.
3. B's stale snapshot produces `timeout_ms=1000`, `max_retries=3`.
   A's edit is genuinely lost.
4. Both can read the result and continue.

The harness provides no ready-made resolution and does not repair the target
after the collision. Codex sessions perform recovery. The writer models saving
a stale version; it does not imply that ordinary `apply_patch` always behaves
this way. A deterministic test verifies the injection mechanism without LLMs or sleeps.

| Variant | Result |
| --- | --- |
| Ordinary verification of each assignment | PASS for preserving both changes. A saw the lost timeout, reread current data, and restored it while preserving retries=3. No MCP communication. |
| Explicit request to negotiate through Hub | PASS. Five MCP messages, exchanged requirements, an agreed editor, and two successful checks. The wording "save without further changes" caused B to rewrite the same bytes unnecessarily. |
| Clarification: B only reads and verifies | PASS. Five MCP messages. After agreement, only A changed settings.json; B read it and ran verification. Both confirmed timeout=2000, retries=3, label=fixture. |

The final run took approximately **46 seconds** from lost-edit injection to
finished negotiation and both checks. This measures one complete LLM workflow,
not HTTP/MCP latency or a recovery-time guarantee.

There was an earlier unsuccessful bootstrap: the new git project waited for
TUI trust and CLI never registered. The harness now explicitly trusts only its
temporary checkout. That run was not counted as an agent-behavior test.

## Usage

Example instructions for two participants:

> When your edits overlap in one file in a shared checkout, contact each other
> through Agent Hub, exchange requirements, and agree on one editor. That editor
> rereads the current file, combines compatible changes, and reports after
> verification. Others only read and verify. Finish your turn while waiting.

This workflow is in the [optional Skill](../skills/agent-hub-coordination/SKILL.md).
The experiments supplied these rules through user prompts; automatic Skill
activation on a conflict was not tested separately.

The result confirms that MCP negotiation is possible. Automatic selection of MCP
after observing another edit was not demonstrated: in the first variant the
agents simply read and repaired the file. An editor agreement is cooperative
and does not protect against a third or nonparticipating writer.

These were compatible changes to different JSON fields. Contradictory function
edits, Git merge conflicts, a selected editor crashing, and continuous competing
writes were not tested. Incompatible requirements need a priority decision,
not communication alone.

## Reproduction

Requires authenticated Codex and the project's Python dependencies. Run scenarios
sequentially; they consume real Codex quota.

```sh
.venv/bin/python scripts/check_codex_file_conflict.py \
  --max-seconds 360 --output /tmp/hub-file-baseline.json

.venv/bin/python scripts/check_codex_file_conflict.py --guided \
  --max-seconds 360 --output /tmp/hub-file-coordinated.json

python3 -W error::ResourceWarning -m unittest discover -s tests \
  -p 'test_codex_file_conflict.py'
```

The harness has overall and barrier/scenario deadlines, stops after more than
16 observed runtime turn starts, and disables subagents. Cleanup stops only its
owned temporary CLIs, daemon, Hub, and blocked writer processes. Initial
snapshots, the lost-edit checkpoint, peer messages, runtime history, and both
verified final snapshots are saved. Historical captures retain their original
language; the current script's instructions are English.

- [Without an explicit coordination request](evidence/codex-file-conflict-baseline.json)
- [Negotiation with ambiguous save wording](evidence/codex-file-conflict-guided.json)
- [Negotiation with a single editor](evidence/codex-file-conflict-coordinated.json)
