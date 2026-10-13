# Agent Hub

Shared terminology for communication between agent sessions.

## Language

**Hub**:
A communication service where agent sessions register, discover each other, and exchange messages.

**Agent session**:
A participant representing an agent conversation with its own context.
_Avoid_: MCP transport session

**Session ID**:
A stable address for an agent session in Hub. Preserves its identity across registration updates and connection changes.

**External runtime ID**:
The session or conversation identifier in the system executing the agent, such as a Codex thread ID.

**Orchestrator**:
An agent session that delegates work to other sessions and makes decisions based on their responses.

**Worker**:
An agent session performing work assigned by an orchestrator.

**Turn**:
One cycle of processing an input in an agent session.
_Avoid_: task

**Turn result**:
The terminal status, available final text, artifact references, and error details from a finished turn. The orchestrator uses these data to assess task success.

**Terminal turn status**:
Runtime-confirmed completion, failure, or interruption of a turn. Normal completion does not imply task success.

**Task**:
Work with an expected outcome assigned to a session. Completing it may require several turns.
_Avoid_: turn

**Message**:
An addressed communication from one session to another, with a payload and its own identifier.

**Inbox**:
Messages addressed to a session whose acceptance it has not yet acknowledged.

**Acknowledgement (ACK)**:
The recipient's explicit confirmation that it accepted a message. Does not imply successful task execution.

**Correlation ID**:
A shared identifier linking related messages, such as a request and its reply.

**Resource**:
A shared object whose access participants coordinate using one agreed key, such as a file in a particular checkout.

**Lease (lock with TTL)**:
A temporary exclusive right to modify a resource by agreement with other participants. Ends on release or expiry; does not itself prevent physical writes.

**Lock request**:
A distinct attempt to acquire a resource lease. Has its own identifier, may wait for a grant, and remains distinguishable from later requests by the same participant.
