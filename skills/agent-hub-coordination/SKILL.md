---
name: agent-hub-coordination
description: Coordinate delegated tasks between existing Codex CLI sessions, handle incoming Hub requests and turn results, and resolve observed conflicting edits.
---

Agent Hub delivers messages into this conversation and automatically returns a
worker's final answer when its runtime turn ends. Your registered session_id and
sender_id come from the SessionStart hook.

For routine file edits, read the latest file, preserve existing changes, edit and
verify using ordinary local tools. Use Hub when the task requires cross-session
communication. Its availability or another registration does not by itself call
for coordination.

When delegating through Hub:

1. Find the requested peers with `hub_list_sessions`. Address them by session_id;
   display names can repeat and registrations can outlive a CLI.
2. Send each subtask using `hub_send_message`, your registered sender_id, a unique
   message_id, and a correlation_id identifying that subtask. Include the task and
   expected output in payload. Reuse the same ID and contents only for a retry.
3. After sending, **finish the current turn** with a brief acknowledgement of the
   pending work. The bridge delivers replies when this CLI is idle. Sleeping or
   polling inbox in this turn delays automatic delivery.
4. Each incoming `payload.type=turn_completed` is one worker turn's result. Match
   correlation_id to the subtask and inspect status, final_text, and error. Retain
   partial results in the conversation and finish the turn if others are pending.
   Once all needed results arrive, assess the task and give the combined answer.

When receiving a task, follow its payload (object or plain text), do the work, and
put the answer in your final response. The bridge sends it back automatically;
an additional `hub_send_message` with that answer creates a second reply. Incoming
turn results need no reply message or manual ACK.

Native Codex subagents help their parent perform a task. Have them return findings
to the parent; the registered parent CLI handles external Hub communication and
returns the combined final response. A native subagent is not automatically a
separate registered Hub worker.

A successful send or ACK means acceptance. A completed runtime turn still needs
assessment before declaring the task successful.

Enter the resource lease workflow only after an observed overlapping-edit
conflict with another agent, or when the user explicitly requests serialized
access to a specific resource. Merely sharing a checkout is not that trigger.
Once this escalation is needed:

1. Call `hub_acquire_lock` with your session_id, the same canonical absolute file
   path as the other writers, a fresh request_id containing your session_id, and
   a TTL covering a short editing section. Default TTL is 120 seconds; range is
   1–600. Prepare analysis outside the lease.
2. If `waiting`, **finish this turn**. Hub queues waiters FIFO per resource and
   `lock_granted` resumes this conversation. The TTL starts on grant.
3. Replay the exact acquire arguments to confirm `acquired`, then reread the
   latest file, preserve compatible changes, and complete writes before
   expires_at_ms (Unix milliseconds). Replaying never renews the lease. On
   expired/released/cancelled, stop writing; a later attempt needs a fresh ID.
4. Release the exact request_id with `hub_release_lock` when done. The ordinary
   CLI bridge also releases observed grants at terminal turn completion. TTL
   covers a lost bridge or forgotten release. Notifications need no peer reply.

Leases are cooperative; they cannot stop a filesystem command after expiry.
Do not give a native subagent concurrent writes to a leased file; the registered
parent holds the lease and waits for its child before releasing it. Native child
tool calls are not automatically bound to the parent's turn by the bridge.

When requirements conflict, exchange intended changes and checks through Hub,
agree on one editor, and have others only read and verify. Contradictory
requirements need a priority decision from the user. Even rewriting an unchanged
snapshot risks a lost update.

A waiting turn's automatic result is a partial result, not completed work. Lock
notifications do not automatically report the resumed turn to the sender of an
earlier task; arrange that final report explicitly if delegating such a task.
