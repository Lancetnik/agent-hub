"""The real-CLI experiment must reliably inject a lost update before negotiation."""

import json
import os
import select
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from check_codex_file_conflict import INITIAL, WRITER


class ControlledCollisionTests(unittest.TestCase):
    def test_two_stale_snapshots_lose_first_edit_at_explicit_barriers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "settings.json"
            target.write_text(json.dumps(INITIAL))
            script = root / "collision_writer.py"
            script.write_text(WRITER)
            candidates = {"alpha": {"timeout_ms": 2000, "max_retries": 1, "label": "fixture"},
                          "beta": {"timeout_ms": 1000, "max_retries": 3, "label": "fixture"}}
            children = {}

            def waiting(role, stage):
                child = children[role]
                self.assertTrue(select.select([child.stdout], [], [], 5)[0], "Writer did not reach its barrier")
                self.assertEqual(child.stdout.readline().strip(), f"WAITING:{role}:{stage}")

            def release(role, stage):
                fd = os.open(root / f"{role}.{stage}.fifo", os.O_WRONLY | os.O_NONBLOCK)
                try:
                    os.write(fd, b"1")
                finally:
                    os.close(fd)

            try:
                for role, candidate in candidates.items():
                    (root / f"{role}.json").write_text(json.dumps(candidate))
                    for stage in ["write", "verify"]:
                        os.mkfifo(root / f"{role}.{stage}.fifo")
                    children[role] = subprocess.Popen([sys.executable, str(script), role, f"{role}.json"],
                                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                for role in children:
                    waiting(role, "write")
                    self.assertEqual(json.loads((root / f"{role}.write.ready").read_text())["observed"], INITIAL)
                release("alpha", "write")
                waiting("alpha", "verify")
                self.assertEqual(json.loads(target.read_text()), candidates["alpha"])
                release("beta", "write")
                waiting("beta", "verify")
                self.assertEqual(json.loads(target.read_text()), candidates["beta"])
                self.assertEqual(json.loads(target.read_text())["timeout_ms"], 1000, "No lost update was injected")
                for role, child in children.items():
                    release(role, "verify")
                    stdout, stderr = child.communicate(timeout=5)
                    self.assertEqual(child.returncode, 0, stderr)
                    self.assertIn('"max_retries": 3', stdout)
            finally:
                for child in children.values():
                    if child.poll() is None:
                        child.kill()
                    child.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
