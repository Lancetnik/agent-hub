"""Runtime receipt boundaries and restart recovery, without inference."""

import json
import sys
import tempfile
import unittest
from http.client import IncompleteRead
from pathlib import Path
from unittest.mock import Mock
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from codex_session import CodexSession, Journal


def message(message_id="message-1"):
    return {"message_id": message_id, "sender_id": "orchestrator", "recipient_id": "worker",
            "payload": {"text": "Compute 2 + 2"}, "correlation_id": "assignment-1",
            "sequence": 1, "acknowledged": False}


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "worker.sqlite3"
        self.journal = Journal(self.path)
        self.journal.bind("worker", "thread")
        self.app = Mock()
        self.app.request.return_value = {"turn": {"id": "turn", "status": "inProgress"}}
        self.app.receive.side_effect = TimeoutError
        self.hub = Mock(session_id="worker", url="http://hub")
        self.hub.inbox.return_value = iter([message()])
        self.session = CodexSession(self.app, "thread", self.hub, self.journal,
                                    report_results=True, emit=Mock())

    def tearDown(self):
        self.journal.close()
        self.directory.cleanup()

    def test_rejected_runtime_input_stays_pending_without_ack_or_receipt(self):
        self.app.request.side_effect = RuntimeError("runtime rejected input")
        with self.assertRaises(RuntimeError):
            self.session.deliver(message())
        self.hub.ack.assert_not_called()
        self.assertIsNone(self.journal.receipt("message-1"))

    def test_lost_ack_response_retries_saved_receipt_after_restart_without_starting_a_turn(self):
        self.hub.ack.side_effect = URLError("response lost after commit")
        with self.assertRaises(URLError):
            self.session.deliver(message())
        self.assertEqual(self.journal.receipt("message-1"), ("turn",))
        self.assertEqual(self.session.active_turn, "turn")
        self.journal.close()
        self.journal = Journal(self.path)
        self.hub.ack.side_effect = None
        # The process restarts; its durable receipt is the authority for ACK retry.
        session = CodexSession(self.app, "thread", self.hub, self.journal, emit=Mock())
        session.active_turn = "turn"
        session.tick()
        self.assertEqual(self.app.request.call_count, 1)
        self.assertEqual(self.hub.ack.call_count, 2)
        self.assertEqual(self.journal.unacked(), [])

    def test_busy_session_does_not_read_or_ack_and_delivers_after_its_terminal_event(self):
        self.session.active_turn = "busy"
        self.session.tick()
        self.hub.inbox.assert_not_called()
        self.hub.ack.assert_not_called()
        # Unrelated runtime completions must not release the target session.
        self.session.observe({"method": "turn/completed", "params": {
            "threadId": "other", "turn": {"id": "busy", "status": "completed"},
        }})
        self.assertEqual(self.session.active_turn, "busy")
        self.session.observe({"method": "turn/completed", "params": {
            "threadId": "thread", "turn": {"id": "busy", "status": "completed", "error": None},
        }})
        self.session.next_poll = 0
        self.session.tick()
        self.app.request.assert_called_once()
        self.hub.ack.assert_called_once_with("message-1")

    def test_truncated_http_ack_is_retried_without_restarting_the_runtime(self):
        self.hub.ack.side_effect = IncompleteRead(b'{"acknowledged":')
        self.session.tick()
        self.assertEqual(self.journal.unacked(), ["message-1"])
        self.assertEqual(self.session.active_turn, "turn")
        self.app.close.assert_not_called()
        self.hub.ack.side_effect = None
        self.session.next_poll = 0
        self.session.tick()
        self.assertEqual(self.journal.unacked(), [])
        self.assertEqual(self.app.request.call_count, 1)

    def test_runtime_completion_queues_result_once_even_if_terminal_is_replayed(self):
        self.session.deliver(message())
        self.session.observe({"method": "item/completed", "params": {
            "threadId": "thread", "turnId": "turn",
            "item": {"id": "final", "type": "agentMessage", "phase": "final_answer", "text": "4"},
        }})
        terminal = {"method": "turn/completed", "params": {
            "threadId": "thread", "turn": {"id": "turn", "status": "completed", "error": None},
        }}
        self.session.observe(terminal)
        self.session.observe(terminal)
        rows = self.journal.db.execute("SELECT url, body FROM pending").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "http://hub/turn-results")
        result = json.loads(rows[0][1])
        self.assertEqual((result["final_text"], result["recipient_id"], result["correlation_id"]),
                         ("4", "orchestrator", "assignment-1"))
        self.assertIsNone(self.journal.unfinished("turn"))
        self.assertIsNone(self.session.active_turn)

    def test_receipt_after_restart_can_recover_a_missing_completion_from_runtime_items(self):
        self.session.deliver(message())
        self.journal.close()
        self.journal = Journal(self.path)
        recovered = CodexSession(self.app, "thread", self.hub, self.journal,
                                 report_results=True, emit=Mock())
        recovered.observe({"method": "item/completed", "params": {
            "threadId": "thread", "turnId": "turn",
            "item": {"id": "final", "type": "agentMessage", "phase": "final_answer", "text": "4"},
        }})
        recovered.observe({"method": "turn/completed", "params": {
            "threadId": "thread", "turn": {"id": "turn", "status": "interrupted", "error": None},
        }})
        result = json.loads(self.journal.db.execute("SELECT body FROM pending").fetchone()[0])
        self.assertEqual(result["status"], "interrupted")
        self.assertEqual(result["final_text"], "4")

    def test_state_file_cannot_be_reused_for_a_different_session(self):
        with self.assertRaises(ValueError):
            self.journal.bind("other", "thread")


if __name__ == "__main__":
    unittest.main()
