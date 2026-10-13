#!/usr/bin/env python3
"""Force a stale-save collision in two real CLIs sharing a temporary checkout.

Compare ordinary task verification with explicitly requested Hub coordination.
Writes are confined to a disposable project; no user hooks/config are modified.
"""

import argparse
import json
import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path

from check_codex_coordination import Experiment, final_text, summarize
from check_codex_hooks import Terminal
from codex_hooks import CliJournal
from codex_session import HubClient


INITIAL = {"timeout_ms": 1000, "max_retries": 1, "label": "fixture"}
EXPECTED = {"timeout_ms": 2000, "max_retries": 3, "label": "fixture"}

# This helper controls when an agent's already-prepared snapshot reaches disk.
# It never generates edits or resolves the conflict for the agents.
WRITER = '''import json, os, pathlib, select, sys
root = pathlib.Path(__file__).resolve().parent
role, proposal = sys.argv[1:]
candidate = json.loads((root / proposal).read_text())
observed = json.loads((root / "settings.json").read_text())
(root / (role + ".writer-pid")).write_text(str(os.getpid()))
def barrier(stage, body):
    path = root / (role + "." + stage + ".fifo")
    fd = os.open(path, os.O_RDWR | os.O_NONBLOCK)
    try:
        (root / (role + "." + stage + ".ready")).write_text(json.dumps(body))
        print("WAITING:" + role + ":" + stage, flush=True)
        if not select.select([fd], [], [], 150)[0]:
            raise TimeoutError("Controlled writer barrier timed out")
        if os.read(fd, 1) != b"1":
            raise RuntimeError("Invalid barrier release")
    finally:
        os.close(fd)
barrier("write", {"observed": observed, "candidate": candidate})
(root / "settings.json").write_text(json.dumps(candidate, indent=2) + "\\n")
barrier("verify", {"saved": candidate})
print("Current settings.json:")
print((root / "settings.json").read_text())
'''

VERIFIER = '''import json, pathlib, sys
root = pathlib.Path(__file__).resolve().parent
actual = json.loads((root / "settings.json").read_text())
(root / (sys.argv[1] + ".verified.json")).write_text(json.dumps(actual))
assert actual == {"timeout_ms": 2000, "max_retries": 3, "label": "fixture"}, actual
print("FILE_VERIFIED")
'''


class FileConflictExperiment(Experiment):
    def __init__(self, codex, root):
        super().__init__(codex, root)
        self.config.write_text(self.config.read_text().replace("[agents]\n", "[agents]\nenabled=false\n", 1))
        self.project = root / "project"
        self.project.mkdir()
        with self.config.open("a") as config:
            config.write("\n[projects." + json.dumps(str(self.project.resolve())) + "]\ntrust_level=\"trusted\"\n")
        self.role_ids, self.checkpoints = {}, []
        self.target = self.project / "settings.json"
        self.target.write_text(json.dumps(INITIAL, indent=2) + "\n")
        (self.project / "collision_writer.py").write_text(WRITER)
        (self.project / "verify_settings.py").write_text(VERIFIER)
        for role in ["alpha", "beta"]:
            for stage in ["write", "verify"]:
                os.mkfifo(self.project / f"{role}.{stage}.fifo")
        subprocess.run(["git", "init", "-q", str(self.project)], check=True)
        subprocess.run(["git", "-C", str(self.project), "add", "settings.json"], check=True)

    def registered_ids(self):
        ids = set()
        for path in self.state.glob("*.sqlite3"):
            journal = CliJournal(path)
            try:
                if journal.registration():
                    ids.add(journal.registration()["session_id"])
            finally:
                journal.close()
        return ids

    def registrations(self):
        # Both normal CLI registrations have the same display_name (project).
        # Addressing stays by UUID; the test labels each sequentially born CLI.
        return {role: thread for role, thread in self.role_ids.items() if thread in self.registered_ids()}

    def launch_shared(self, role):
        before = self.registered_ids()
        self.terminals[role] = Terminal(self.codex, self.env, self.project,
                                        "Reply READY only. Do not use tools.", sandbox="workspace-write")
        born = self.wait(lambda: self.registered_ids() - before, timeout=90)
        assert len(born) == 1, "Unexpected extra CLI registration"
        self.role_ids[role] = born.pop()

    def checkpoint(self, name):
        snapshot = {"name": name, "seconds": round(time.monotonic() - self.started, 3),
                    "settings": json.loads(self.target.read_text())}
        self.checkpoints.append(snapshot)
        print(name + ": " + json.dumps(snapshot["settings"]), flush=True)

    def release(self, role, stage):
        # The ready marker is written after the reader opened this FIFO.
        fd = os.open(self.project / f"{role}.{stage}.fifo", os.O_WRONLY | os.O_NONBLOCK)
        try:
            os.write(fd, b"1")
        finally:
            os.close(fd)

    def idle(self):
        if sum(e.get("method") == "turn/started" for e in self.trace) > 16:
            raise RuntimeError("Stopped coordination loop after 16 runtime turns")
        return all(self.app.request("thread/read", {"threadId": t})["thread"]["status"]["type"] == "idle"
                   for t in self.registered.values())

    def __exit__(self, *args):
        try:
            super().__exit__(*args)
        finally:
            # Only our helper processes, identified by their absolute script path.
            for path in self.project.glob("*.writer-pid"):
                pid = int(path.read_text())
                found = subprocess.run(["ps", "-p", str(pid), "-o", "args="], text=True, capture_output=True)
                if str(self.project / "collision_writer.py") in found.stdout:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass


def collide(experiment, *, guided):
    for role in ["alpha", "beta"]:
        experiment.launch_shared(role)
    experiment.ready()
    experiment.checkpoint("initial")
    for role, field, value in [("alpha", "timeout_ms", 2000), ("beta", "max_retries", 3)]:
        text = (f"Edit settings.json: set {field}={value} and preserve other fields. Work independently. "
                f"This fixture delays saves: first read settings.json, prepare the complete JSON "
                f"with only your edit in proposal-{role}.json, then apply it exactly once with "
                f"python3 {experiment.project}/collision_writer.py {role} proposal-{role}.json. "
                "The command waits for fixture barriers and prints the current file after saving. "
                "Wait for it to finish. Leave collision_writer.py, FIFOs, and ready files unchanged. "
                "After the command, verify that your edit actually persisted.")
        if guided:
            text += (" Another independent Codex session works in this checkout. After the command, use Agent Hub "
                     "to find the other participant (its session_id differs from yours), exchange settings.json "
                     "requirements, and agree on one editor. That editor rereads the current file, saves both "
                     "compatible changes, and reports the result to the peer through Hub. "
                     "Until you agree on an editor, make no further settings.json writes. The other participant "
                     "only reads and verifies; even rewriting identical text is unnecessary. Finish turns "
                     "while waiting for messages. If both volunteer as editor, choose the smaller session_id. "
                     "Include your session_id in every message_id and use a separate correlation_id for negotiation. "
                     f"After agreement, verify using python3 {experiment.project}/verify_settings.py {role} "
                     "(leave the helper and its verified files unchanged) and finish with FILE_VERIFIED; "
                     "send no further messages in response to a successful verification acknowledgement.")
        else:
            text += " If needed, repair your edit while preserving the other current fields."
        experiment.prompt(role, text)
    experiment.wait(lambda: all((experiment.project / f"{r}.write.ready").exists()
                               for r in ["alpha", "beta"]), timeout=100)
    for role, expected in [("alpha", {**INITIAL, "timeout_ms": 2000}), ("beta", {**INITIAL, "max_retries": 3})]:
        prepared = json.loads((experiment.project / f"{role}.write.ready").read_text())
        assert prepared == {"observed": INITIAL, "candidate": expected}, "Agents did not prepare independent snapshots"
    experiment.checkpoint("both_read_initial")
    experiment.release("alpha", "write")
    experiment.wait(lambda: (experiment.project / "alpha.verify.ready").exists(), timeout=20)
    assert json.loads(experiment.target.read_text()) == {**INITIAL, "timeout_ms": 2000}
    experiment.checkpoint("alpha_saved")
    experiment.release("beta", "write")
    experiment.wait(lambda: (experiment.project / "beta.verify.ready").exists(), timeout=20)
    assert json.loads(experiment.target.read_text()) == {**INITIAL, "max_retries": 3}
    experiment.checkpoint("beta_overwrote_alpha")
    for role in ["alpha", "beta"]:
        experiment.release(role, "verify")

    def settled():
        if not experiment.idle():
            return False
        if any(list(HubClient(experiment.hub.url, t).inbox()) for t in experiment.registered.values()):
            return False
        if any(experiment.app.request("thread/queue/list", {"threadId": t})["data"] for t in experiment.registered.values()):
            return False
        if guided and not all((experiment.project / f"{r}.verified.json").exists()
                              and json.loads((experiment.project / f"{r}.verified.json").read_text()) == EXPECTED
                              for r in ["alpha", "beta"]):
            return False
        # At least one post-READY turn per participant must have finished.
        return all(len(experiment.turns(t)) >= 2 for t in experiment.registered.values())
    experiment.wait(settled, timeout=180)
    experiment.checkpoint("settled")
    captured = experiment.capture()
    captured["checkpoints"] = experiment.checkpoints
    captured["final_settings"] = json.loads(experiment.target.read_text())
    summary = summarize(captured)
    peer_sends = [c for calls in summary["mcp_calls"].values() for c in calls
                  if c["tool"] == "hub_send_message" and c["status"] == "completed" and not c["error"]
                  and {c["arguments"]["sender_id"], c["arguments"]["recipient_id"]} == set(experiment.registered.values())]
    captured["peer_send_count"] = len(peer_sends)
    captured["both_changes_preserved"] = captured["final_settings"] == EXPECTED
    captured["guided"] = guided
    assert (experiment.project / "collision_writer.py").read_text() == WRITER
    assert (experiment.project / "verify_settings.py").read_text() == VERIFIER
    if guided:
        assert peer_sends, "No peer coordination happened through MCP"
        assert captured["both_changes_preserved"], "A compatible edit was lost"
        captured["verified_snapshots"] = {r: json.loads((experiment.project / f"{r}.verified.json").read_text())
                                          for r in ["alpha", "beta"]}
        for thread in experiment.registered.values():
            texts = [p["text"] for t in captured["threads"][thread]["turns"] for item in t["items"]
                     if item["type"] == "userMessage" for p in item["content"] if p["type"] == "text"]
            assert any(text.startswith("Incoming Agent Hub message:\n") for text in texts), "One peer never received a Hub message"
        assert all(any(final_text(t).strip() == "FILE_VERIFIED" for t in experiment.turns(thread))
                   for thread in experiment.registered.values()), "Both peers did not confirm verification"
    print("RESULT: preserved=" + str(captured["both_changes_preserved"]) + ", peer sends=" + str(len(peer_sends)), flush=True)
    return captured


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--output", required=True)
    parser.add_argument("--guided", action="store_true", help="Explicitly ask peers to agree on one editor via Hub")
    parser.add_argument("--max-seconds", type=int, default=360)
    args = parser.parse_args()
    if args.max_seconds <= 0:
        parser.error("max-seconds must be positive")
    def timeout(*_):
        raise TimeoutError("Shared-file test exceeded its overall time budget")
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(args.max_seconds)
    repo = Path(__file__).resolve().parents[1]
    subprocess.run(["cargo", "build", "--bin", "agent-hub"], cwd=repo, check=True)
    evidence = {"version": subprocess.check_output([args.codex, "--version"], text=True).strip(), "guided": args.guided}
    with tempfile.TemporaryDirectory(prefix="hub-file-", dir="/tmp") as directory:
        with FileConflictExperiment(args.codex, Path(directory)) as experiment:
            try:
                evidence["evidence"] = collide(experiment, guided=args.guided)
                evidence["status"] = "passed" if evidence["evidence"]["both_changes_preserved"] else "lost_edit"
            except Exception as error:
                evidence["status"], evidence["error"] = "failed", str(error)
                if experiment.app:
                    evidence["evidence"] = experiment.capture()
                    evidence["evidence"]["checkpoints"] = experiment.checkpoints
                    evidence["evidence"]["final_settings"] = json.loads(experiment.target.read_text())
                raise
            finally:
                Path(args.output).write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")
    print("Evidence: " + args.output, flush=True)


if __name__ == "__main__":
    main()
