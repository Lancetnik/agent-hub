"""Runtime mapping and crash/retry checks; no inference or network access required."""

import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from codex_worker import Outbox, TurnCollector


class RuntimeMappingTests(unittest.TestCase):
    def item(self, collector, item, thread="thread", turn="turn"):
        collector.observe({"method": "item/completed", "params": {
            "threadId": thread, "turnId": turn, "item": item,
        }})

    def result(self, collector, status="completed", error=None):
        return collector.result({"threadId": "thread", "turn": {
            "id": "turn", "status": status, "items": [], "error": error,
        }}, "worker", "orchestrator", "assignment")

    def test_stop_continuation_replaces_intermediate_final_and_ignores_commentary(self):
        collector = TurnCollector("thread")
        self.item(collector, {"id": "first", "type": "agentMessage", "phase": "final_answer", "text": "4"})
        self.item(collector, {"id": "comment", "type": "agentMessage", "phase": "commentary", "text": "Continuing"})
        self.item(collector, {"id": "last", "type": "agentMessage", "phase": "final_answer", "text": "5"})
        self.item(collector, {"id": "foreign", "type": "agentMessage", "phase": "final_answer", "text": "wrong"}, thread="other")
        self.item(collector, {"id": "foreign-turn", "type": "agentMessage", "phase": "final_answer", "text": "wrong"}, turn="other")
        result = self.result(collector)
        self.assertEqual(result["final_text"], "5")
        self.assertEqual((result["runtime_id"], result["turn_id"], result["correlation_id"]), ("thread", "turn", "assignment"))

    def test_commentary_is_not_a_result_and_failure_details_are_preserved(self):
        collector = TurnCollector("thread")
        self.item(collector, {"id": "comment", "type": "agentMessage", "phase": "commentary", "text": "Working"})
        error = {"message": "Inference failed", "codexErrorInfo": "other", "additionalDetails": "provider detail"}
        for status in ["completed", "failed", "interrupted"]:
            result = self.result(collector, status, error if status == "failed" else None)
            self.assertIsNone(result["final_text"])
            self.assertEqual(result["status"], status)
            self.assertEqual(result["error"], error if status == "failed" else None)
            self.assertNotIn("task_success", result)
        with self.assertRaises(ValueError):
            self.result(collector, "inProgress")

    def test_legacy_unphased_text_and_completed_file_changes(self):
        collector = TurnCollector("thread")
        self.item(collector, {"id": "first", "type": "agentMessage", "phase": "final_answer", "text": "Earlier answer"})
        self.item(collector, {"id": "legacy", "type": "agentMessage", "phase": None, "text": "Saved report"})
        self.item(collector, {"id": "files", "type": "fileChange", "changes": [{"path": "/tmp/report.md"}]})
        result = self.result(collector)
        self.assertEqual(result["final_text"], "Saved report")
        self.assertEqual(result["artifact_refs"], ["/tmp/report.md"])


class OutboxTests(unittest.TestCase):
    def test_offline_event_survives_restart_and_registration_precedes_result(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outbox.sqlite3"
            outbox = Outbox(path)
            registration = {"session_id": "worker", "runtime_id": "thread", "display_name": "Worker"}
            event = {"turn_id": "turn", "final_text": "4"}
            outbox.enqueue("registration", "http://hub/sessions", registration)
            outbox.enqueue("result", "http://hub/turn-results", event)
            with patch("codex_worker.urlopen", side_effect=URLError("offline")):
                with self.assertRaises(URLError):
                    outbox.flush()
            outbox.close()
            delivered = []

            def accept(request, timeout):
                delivered.append((request.full_url, json.loads(request.data)))
                return io.StringIO('{"ok":true}')

            outbox = Outbox(path)
            with patch("codex_worker.urlopen", side_effect=accept):
                outbox.flush()
                outbox.flush()
            outbox.close()
            self.assertEqual(delivered, [("http://hub/sessions", registration), ("http://hub/turn-results", event)])

    def test_lost_response_replays_the_exact_committed_event(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outbox.sqlite3"
            outbox = Outbox(path)
            event = {"turn_id": "turn", "status": "completed", "final_text": "🦀"}
            outbox.enqueue("result", "http://hub/turn-results", event)
            attempts = []

            def lost_response(request, timeout):
                attempts.append(request.data)
                raise URLError("response lost after the server committed")

            with patch("codex_worker.urlopen", side_effect=lost_response):
                with self.assertRaises(URLError):
                    outbox.flush()
            outbox.close()

            def accepted(request, timeout):
                attempts.append(request.data)
                return io.StringIO('{"message":{"acknowledged":true}}')

            outbox = Outbox(path)
            outbox.enqueue("result", "http://hub/turn-results", event)
            with self.assertRaises(ValueError):
                outbox.enqueue("result", "http://hub/turn-results", {**event, "final_text": "changed"})
            with patch("codex_worker.urlopen", side_effect=accepted):
                outbox.flush()
            self.assertEqual(outbox.db.execute("SELECT count(*) FROM pending").fetchone()[0], 0)
            outbox.close()
            self.assertEqual(len(attempts), 2)
            self.assertEqual(attempts[0], attempts[1])


if __name__ == "__main__":
    unittest.main()
