"""Ordinary CLI delivery: runtime receipts, recovery, and ownership of user settings."""

import io
import json
import queue
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from check_codex import AppServer
from codex_hooks import CliBridge, CliJournal, client_id, hook, input_text, install, uninstall


def message(payload=None):
    return {"message_id": "assignment", "sender_id": "orchestrator", "recipient_id": "worker",
            "correlation_id": "task", "payload": payload or {"text": "Compute 2 + 2"}}


def user_item():
    return {"type": "userMessage", "id": "different-runtime-item-id",
            "content": [{"type": "text", "text": input_text(message())}]}


class CliDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "journal.sqlite3"
        self.journal = CliJournal(self.path)
        self.journal.register({"session_id": "worker", "runtime_id": "thread", "display_name": "worker"})
        self.app = Mock()
        self.app.receive.side_effect = TimeoutError
        self.app.request.return_value = {"queuedSubmission": {"id": "queue", "clientUserMessageId": client_id("assignment")}}
        self.hub = Mock(session_id="worker", url="http://hub")
        self.bridge = CliBridge(self.app, self.hub, self.journal, Mock())

    def tearDown(self):
        self.journal.close()
        self.directory.cleanup()

    def test_runtime_rejection_never_acknowledges_input(self):
        self.app.request.side_effect = RuntimeError("queue rejected")
        with self.assertRaises(RuntimeError):
            self.bridge.deliver(message())
        self.hub.ack.assert_not_called()
        self.assertFalse(self.journal.accepted("assignment"))

    def test_busy_cli_does_not_queue_or_acknowledge(self):
        self.app.request.return_value = {"thread": {"status": {"type": "active"}}}
        self.bridge.tick()
        self.assertEqual(self.app.request.call_args.args[0], "thread/read")
        self.hub.inbox.assert_not_called()
        self.hub.ack.assert_not_called()

    def test_lost_ack_retries_receipt_after_restart_without_resubmitting(self):
        self.hub.ack.side_effect = URLError("ACK response lost")
        with self.assertRaises(URLError):
            self.bridge.deliver(message())
        self.journal.close()
        self.journal = CliJournal(self.path)
        self.bridge = CliBridge(self.app, self.hub, self.journal, Mock())
        self.app.reset_mock()
        self.hub.ack.side_effect = None
        self.bridge.deliver(message())
        self.app.request.assert_not_called()
        self.hub.ack.assert_called_with("assignment")
        self.assertEqual(self.journal.pending_acks(), [])

    def test_history_recovers_acceptance_after_lost_queue_response(self):
        self.journal.prepare(message())
        turn = {"id": "turn", "status": "completed", "items": [user_item(),
                {"type": "agentMessage", "id": "answer", "phase": "final_answer", "text": "4"}]}
        self.bridge.recover([turn], [])
        self.bridge.deliver(message())
        self.app.request.assert_not_called()
        row = self.journal.db.execute("SELECT body FROM pending").fetchone()
        self.assertEqual(json.loads(row[0])["final_text"], "4")
        self.assertEqual(json.loads(row[0])["recipient_id"], "orchestrator")
        self.assertEqual(json.loads(row[0])["correlation_id"], "task")

    def test_pending_runtime_queue_recovers_acceptance_without_resubmitting(self):
        self.journal.prepare(message())
        self.bridge.recover([], [{"id": "queue", "clientUserMessageId": client_id("assignment")}])
        self.bridge.deliver(message())
        self.app.request.assert_not_called()

    def test_notification_before_queue_response_and_duplicate_terminal_report_once(self):
        def accept(*args):
            self.bridge.observe({"method": "item/started", "params": {
                "threadId": "thread", "turnId": "turn", "item": user_item()}})
            return {"queuedSubmission": {"id": "queue", "clientUserMessageId": client_id("assignment")}}
        self.app.request.side_effect = accept
        self.bridge.deliver(message())
        self.bridge.observe({"method": "item/completed", "params": {"threadId": "thread", "turnId": "turn",
                             "item": {"type": "agentMessage", "id": "answer", "text": "4", "phase": "final_answer"}}})
        terminal = {"method": "turn/completed", "params": {"threadId": "thread",
                    "turn": {"id": "turn", "status": "failed", "error": {"message": "runtime failed"}}}}
        self.bridge.observe(terminal)
        self.bridge.observe(terminal)
        rows = self.journal.db.execute("SELECT body FROM pending").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0][0])["status"], "failed")
        self.assertEqual(json.loads(rows[0][0])["error"], {"message": "runtime failed"})

    def test_results_do_not_generate_reciprocal_results(self):
        incoming = message({"type": "turn_completed", "final_text": "4"})
        self.journal.prepare(incoming)
        self.journal.observe_input("turn", {"type": "userMessage", "content": [{"type": "text", "text": input_text(incoming)}]})
        self.bridge.finish({"threadId": "thread", "turn": {"id": "turn", "status": "completed"}})
        self.assertEqual(self.journal.db.execute("SELECT COUNT(*) FROM pending").fetchone()[0], 0)

    def test_observer_leaves_approval_request_to_cli(self):
        observer = AppServer.__new__(AppServer)
        observer.messages = queue.Queue()
        observer.messages.put({"id": 7, "method": "item/commandExecution/requestApproval", "params": {}})
        observer.answer_requests = False
        observer.send = Mock()
        observer.receive(0.1)
        observer.send.assert_not_called()


class InstallationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.args = SimpleNamespace(codex_home=str(self.root), state_dir=None, url="http://localhost:7331", socket=None)

    def tearDown(self):
        self.directory.cleanup()

    def test_install_reinstall_and_remove_preserve_user_hooks_and_config(self):
        config = 'model="my-model"\n[hooks.state.user]\ntrusted_hash="existing"\n'
        other = {"matcher": "startup", "hooks": [{"type": "command", "command": "user-hook"}]}
        hooks = {"hooks": {"SessionStart": [other], "Stop": [], "OtherEvent": [other]}}
        (self.root / "config.toml").write_text(config)
        (self.root / "hooks.json").write_text(json.dumps(hooks))
        # The dependency is only used by the connection, not configuration generation.
        with patch.dict(sys.modules, {"websockets.sync.client": SimpleNamespace(unix_connect=Mock())}):
            install(self.args)
            install(self.args)
        installed = json.loads((self.root / "hooks.json").read_text())
        self.assertEqual(installed["hooks"]["SessionStart"][0], other)
        self.assertEqual(len(installed["hooks"]["SessionStart"]), 2)
        self.assertEqual(installed["hooks"]["OtherEvent"], [other])
        self.assertEqual((self.root / "config.toml").read_text().count("trusted_hash"), 1)
        uninstall(self.args)
        self.assertEqual(json.loads((self.root / "hooks.json").read_text()), hooks)
        self.assertEqual((self.root / "config.toml").read_text(), config)

    def test_conflicting_mcp_configuration_is_preserved(self):
        config = '[mcp_servers.agent_hub]\ncommand="existing-server"\n'
        (self.root / "config.toml").write_text(config)
        with patch.dict(sys.modules, {"websockets.sync.client": SimpleNamespace(unix_connect=Mock())}):
            with self.assertRaises(ValueError):
                install(self.args)
        self.assertEqual((self.root / "config.toml").read_text(), config)
        self.assertFalse((self.root / "hooks.json").exists())

    def test_session_end_stops_bridge_without_creating_a_journal_or_inventing_a_result(self):
        settings = self.root / "settings.json"
        settings.write_text(json.dumps({"state_dir": str(self.root), "url": "http://hub"}))
        session_id = "00000000-0000-4000-8000-000000000001"
        event = io.StringIO(json.dumps({"session_id": session_id, "hook_event_name": "SessionEnd"}))
        with patch("codex_hooks.sys.stdin", event), patch("codex_hooks.sys.stdout", io.StringIO()), \
                patch("codex_hooks.stop_bridge") as stop:
            hook(settings)
        stop.assert_called_once_with(self.root / (session_id + ".pid"), settings)
        self.assertEqual(list(self.root.glob("*.sqlite3")), [])


if __name__ == "__main__":
    unittest.main()
