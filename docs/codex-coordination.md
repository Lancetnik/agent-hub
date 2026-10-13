# Coordination between clean Codex sessions

For local use with a running Hub, a separate Skill is optional. Agents need
clear waiting and response rules; these are included in `SessionStart` in
`scripts/codex_hooks.py`.

## What was verified

2026-10-13, Codex CLI **0.162.1**, four bounded runs: 11 new ordinary CLI sessions
and four native subagents in total, one child per run. Each run used temporary
Hub/SQLite, CODEX_HOME, fixtures, and read-only/never CLIs. User hooks/config and
existing conversations were unchanged. The personal `~/.agents/skills` directory
was disabled in the **temporary** config: CODEX_HOME alone does not exclude
personal Skills. Fixture projects had no AGENTS.md or coordination instructions.

The first two and final scenarios used the same ordinary assignment, without
MCP tool names or prepared message IDs: find worker-a and worker-b through Agent
Hub, ask them to calculate values from their own data.json, and combine results.
One worker delegated computation to a native subagent. Worker results: **23**
and **109**; the final value with the number from initial context: **169**.
Workers/subagents read the fixtures; the orchestrator did not.

| Scenario | Observed behavior |
| --- | --- |
| Previous hook, no Skill | Correct final value 169. The orchestrator slept for 50 seconds, then read inbox. Both workers additionally sent their answers through MCP. |
| Previous hook + short Skill | PASS. Two automatic results continued the conversation. Workers made no MCP calls; the orchestrator did not poll inbox. The child returned its result to the parent. |
| Child asked for clean context and Hub task submission | PASS for submission/calculation: final value 37. The parent passed its sender_id to the child. The child sent a task as the parent, then read inbox manually; the worker also sent a manual reply. Automatic delivery was not confirmed in this scenario. |
| Refined hook, no Skill | PASS with strict checks: two automatic results, retained context, a native child, zero worker MCP calls, and zero orchestrator inbox reads. |

The first run exited with a **harness error** while checking for a subagent:
the runtime persisted child completion but omitted the spawn call from the
parent's turn.items. The script now also discovers children from metadata of
its own temporary conversations; results and completion still come from the
runtime API. The original FAIL remains in evidence. Harness cleanup was also
fixed: a PTY exit timeout no longer prevents shutdown of other owned CLIs and Hub.

## Changes

Transport and runtime completion were unchanged. Three rules were added to
`SessionStart` context:

1. Send tasks and finish the current turn; automatic input arrives in a later
   turn when CLI is idle. Sleeping or polling inbox in an active turn delays it.
2. Put the worker answer in its final response: the bridge sends the result.
   Sending that answer manually creates an additional message.
3. Match incoming `payload.type=turn_completed` to the task; retain partial
   results in the conversation and finish the turn while other results are pending.

Runtime integration still detects worker completion even when the model calls
no MCP tool. These hints guide model behavior; they do not make instruction
following deterministic.

In the final run, observed worker `turn/completed` to orchestrator
`turn/started` with the matching result took **58 ms** and **38 ms**. With the
Skill: **67 ms** and **45 ms**. These are single-observer measurements in those
runs, including delivery and the new turn's start, not an SLA or LLM generation time.

## Subagents and Skills

A native child successfully performs part of a worker assignment and returns
its answer to the parent. Hub automatically sends the **registered parent's**
final response. It does not create a separate addressable registration or bridge
for the native child.

A child can send through MCP as its registered parent if the parent supplies
sender_id. Replies address the parent. This does not make the child a separate
Hub worker; the probe showed extra manual ACK/reads and duplicate replies. For
ordinary coordination, keep external Hub communication in the parent CLI and
give the child a specific piece of work. Codex also documents separate
SubagentStart/SubagentStop events and no SessionEnd for children:
[official hooks documentation](https://learn.chatgpt.com/docs/hooks).

The [optional Skill](../skills/agent-hub-coordination/SKILL.md) captures the
verified workflow for correlation, partial results, and children. In the
experiment the orchestrator explicitly invoked `$agent-hub-coordination` and
workers independently read it for incoming tasks. You can copy it to
`~/.codex/skills/agent-hub-coordination/` or the selected CODEX_HOME's skills
directory. The hook installer does not install it.

One run per variant does not establish consistency across models. These were
small read-only tasks; long edits, file conflicts, failed tasks, and autonomous
worker creation were not tested here. The sender prompt explicitly requested
`fork_turns=none`, but the API snapshot omitted the spawn call and its options,
so the precise fork mode is not verified by the saved trace. A separate child
conversation and its MCP calls are confirmed by runtime history.

Separate verification of two sessions writing one shared file and negotiating
through MCP: [codex-file-conflicts.md](codex-file-conflicts.md).

## Reproduction

From the repository root, with Python dependencies installed and Codex authenticated:

```sh
.venv/bin/python scripts/check_codex_coordination.py \
  --require-automatic --max-seconds 360 --output /tmp/hub-coordination.json

.venv/bin/python scripts/check_codex_coordination.py \
  --skill skills/agent-hub-coordination/SKILL.md \
  --max-seconds 360 --output /tmp/hub-coordination-skill.json

.venv/bin/python scripts/check_codex_coordination.py \
  --scenario subagent-sender --max-seconds 270 \
  --output /tmp/hub-subagent-sender.json
```

These are **real LLM turns** consuming quota. Run them sequentially. The main
scenario starts three CLIs and one child; sender starts two CLIs and one child.
Temporary config limits concurrent children to one per parent; each assignment
requests exactly one child. There is an overall deadline and scenario deadlines.
Checks read runtime events and history rather than deciding a race with fixed
sleeps. Evidence retains historical variants with the old hook; a new run uses
the current SessionStart instructions. Historical captures retain their original
language; current scripts use English prompts.

Evidence contains user messages, visible answers, commands, MCP/child actions,
bridge events, and temporary IDs. It excludes auth.json contents and reasoning items.

Saved captures:

- [Previous hook without Skill, including harness FAIL](evidence/codex-coordination-bare.json)
- [Previous hook with Skill](evidence/codex-coordination-skill.json)
- [Submission from a clean child](evidence/codex-coordination-subagent.json)
- [Refined hook without Skill](evidence/codex-coordination-hooks.json)
