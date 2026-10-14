"""Public command-hook contract against a controlled external HTTP boundary."""

import json
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/codex_hooks.py"


class FileLockHookTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name).resolve()
        self.session_id = str(uuid.uuid4())
        self.requests = []
        self.respond = lambda method, path, body: (200, {
            **(body or {}), "status": "released" if path == "/locks/release" else "acquired"})
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def handle_request(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"]))) if "Content-Length" in self.headers else None
                owner.requests.append((self.command, self.path, body))
                status, result = owner.respond(self.command, self.path, body)
                encoded = json.dumps(result).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
            do_POST = do_GET = handle_request
            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.settings = self.root / "settings.json"
        self.settings.write_text(json.dumps({"url": f"http://127.0.0.1:{self.server.server_port}",
            "state_dir": str(self.root), "codex_home": str(self.root), "file_lock_wait_seconds": 3}))
        self.patch = "*** Begin Patch\n*** Update File: settings.txt\n@@\n-old\n+new\n*** End Patch"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=2)
        self.directory.cleanup()

    def run_hook(self, name, *, tool_id="edit-1", patch=None):
        event = {"hook_event_name": name, "session_id": self.session_id, "turn_id": "turn",
            "tool_use_id": tool_id, "tool_name": "apply_patch", "cwd": str(self.root),
            "tool_input": {"command": self.patch if patch is None else patch}}
        process = subprocess.run([sys.executable, str(SCRIPT), "hook", "--settings", str(self.settings)],
            input=json.dumps(event), text=True, capture_output=True, timeout=10)
        self.assertEqual(process.returncode, 0, process.stderr)
        return json.loads(process.stdout)

    def test_free_file_is_locked_before_allow_and_released_after_the_tool(self):
        self.assertEqual(self.run_hook("PreToolUse"), {})
        acquisitions = [b for _, p, b in self.requests if p == "/locks/acquire"]
        self.assertEqual(len(acquisitions), 1)
        self.assertEqual(acquisitions[0]["resource"], str(self.root / "settings.txt"))
        self.assertIs(acquisitions[0]["notify"], False)
        self.assertFalse(any(p == "/locks/release" for _, p, _ in self.requests))
        self.assertEqual(self.run_hook("PostToolUse"), {})
        released = [b["request_id"] for _, p, b in self.requests if p == "/locks/release"]
        self.assertEqual(released, [acquisitions[0]["request_id"]])

    def test_an_existing_manual_lease_is_borrowed_without_releasing_it(self):
        self.respond = lambda method, path, body: (200, {**body, "status": "acquired", "notify": True, "request_id": "manual-ticket"})
        self.assertEqual(self.run_hook("PreToolUse"), {})
        self.assertEqual(self.run_hook("PostToolUse"), {})
        self.assertFalse(any(p == "/locks/release" for _, p, _ in self.requests))

    def test_patch_with_outer_whitespace_still_acquires_its_file_lease(self):
        self.assertEqual(self.run_hook("PreToolUse", patch="\n" + self.patch + "\n\n"), {})
        self.assertEqual(len([p for _, p, _ in self.requests if p == "/locks/acquire"]), 1)

    def test_wait_deadline_cancels_the_ticket_and_rejects_the_patch(self):
        ticket = {}
        def respond(method, path, body):
            if path == "/locks/acquire":
                ticket.update(body)
            return 200, {**(body or ticket), "status": "released" if path == "/locks/release" else "waiting"}
        self.respond = respond
        result = self.run_hook("PreToolUse")["hookSpecificOutput"]
        self.assertEqual(result["permissionDecision"], "deny")
        self.assertIn("deadline", result["permissionDecisionReason"])
        self.assertTrue(any(p == "/locks/release" for _, p, _ in self.requests))

    def test_multi_file_rename_uses_canonical_paths_and_releases_all_leases(self):
        (self.root / "real").mkdir()
        (self.root / "alias").symlink_to(self.root / "real", target_is_directory=True)
        patch = ("*** Begin Patch\n*** Update File: z.txt\n*** Move to: alias/a.txt\n@@\n-old\n+new\n"
                 "*** Add File: added file.txt\n+hello\n*** Delete File: gone.txt\n*** End Patch")
        self.assertEqual(self.run_hook("PreToolUse", patch=patch), {})
        paths = [b["resource"] for _, p, b in self.requests if p == "/locks/acquire"]
        self.assertEqual(paths, [str(self.root / "added file.txt"), str(self.root / "gone.txt"),
                                str(self.root / "real/a.txt"), str(self.root / "z.txt")])
        self.run_hook("PostToolUse")
        self.assertEqual(len([p for _, p, _ in self.requests if p == "/locks/release"]), 4)

    def test_hub_error_after_partial_acquisition_releases_prior_files_and_denies(self):
        def respond(method, path, body):
            if path == "/locks/acquire" and body["resource"].endswith("z.txt"):
                return 503, {"error": "Hub unavailable"}
            return 200, {**body, "status": "acquired"}
        self.respond = respond
        patch = "*** Begin Patch\n*** Add File: a.txt\n+a\n*** Add File: z.txt\n+z\n*** End Patch"
        self.assertEqual(self.run_hook("PreToolUse", patch=patch)["hookSpecificOutput"]["permissionDecision"], "deny")
        self.assertEqual(len([p for _, p, _ in self.requests if p == "/locks/release"]), 1)

    def test_unavailable_journal_explicitly_denies_instead_of_failing_the_hook(self):
        (self.root / (self.session_id + ".sqlite3")).mkdir()
        result = self.run_hook("PreToolUse")["hookSpecificOutput"]
        self.assertEqual(result["permissionDecision"], "deny")
        self.assertFalse(self.requests)

    def test_contended_patch_waits_then_is_denied_and_a_fresh_attempt_can_proceed(self):
        entered, released = threading.Event(), threading.Event()
        target = self.root / "settings.txt"
        target.write_text("old\n")
        ticket = {}

        def respond(method, path, body):
            if path == "/locks/acquire":
                ticket.update(body)
                return 200, {**body, "status": "waiting"}
            if method == "GET":
                entered.set()
                if not released.wait(5):
                    return 500, {"error": "Test barrier was not released"}
                return 200, {**ticket, "status": "acquired"}
            return 200, {**body, "status": "released"}

        self.respond = respond
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.run_hook, "PreToolUse")
            try:
                self.assertTrue(entered.wait(2), "Hook never waited for the busy file")
                self.assertFalse(pending.done(), "Hook returned before the other writer released")
                target.write_text("peer change\n")
            finally:
                released.set()
            output = pending.result(timeout=5)["hookSpecificOutput"]
        self.assertEqual(output["permissionDecision"], "deny")
        self.assertIn("Reread", output["permissionDecisionReason"])
        self.assertIn(str(target), output["permissionDecisionReason"])
        self.assertEqual(target.read_text(), "peer change\n")
        self.assertEqual(len([p for _, p, _ in self.requests if p == "/locks/release"]), 1)
        self.respond = lambda method, path, body: (200, {**(body or {}), "status": "acquired"})
        self.assertEqual(self.run_hook("PreToolUse", tool_id="edit-2"), {})


if __name__ == "__main__":
    unittest.main()
