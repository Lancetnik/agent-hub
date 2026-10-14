#!/usr/bin/env python3
"""Two real CLIs: automatic leases, contended patch denial, reread and retry.

The fixture pauses one hook AFTER production acquisition and BEFORE allowing
the edit. It only controls timing; models generate patches and handle errors.
Uses a disposable checkout/CODEX_HOME, two CLIs, no coordination Skill or MCP
instructions. Requires real Codex authorization and costs four bounded turns.
"""

import argparse
import json
import os
import shlex
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

from check_codex_coordination import summarize
from check_codex_file_conflict import EXPECTED, FileConflictExperiment
from codex_session import HubClient


def verify(capture):
    assert capture["final_settings"] == EXPECTED, capture["final_settings"]
    assert all(not calls for calls in capture["summary"]["mcp_calls"].values()), "Models called MCP"
    events = capture["bridge_events"]["beta"]
    rejected = next(e for e in events if e["type"] == "file_lock_error" and "Reread" in e["error"])
    retry = next(e for e in events if e["type"] == "file_locks_acquired")
    assert rejected["tool_use_id"] != retry["tool_use_id"], "Old call was replayed"
    items = capture["threads"][capture["registered"]["beta"]]["turns"][-1]["items"]
    written = next(n for n, i in enumerate(items) if i["type"] == "fileChange" and i["id"] == retry["tool_use_id"])
    reads = [i for i in items[:written] if i["type"] == "commandExecution" and "settings.json" in i.get("command", "")]
    snapshots = []
    for item in reads:
        try:
            snapshots.append(json.loads(item.get("aggregatedOutput", "")))
        except json.JSONDecodeError:
            pass
    assert {"timeout_ms": 1000, "max_retries": 1, "label": "fixture"} in snapshots, "No initial read"
    assert {"timeout_ms": 2000, "max_retries": 1, "label": "fixture"} in snapshots, "No reread of alpha's saved edit before retry"
    assert not any(i["type"] == "fileChange" and i["id"] == rejected["tool_use_id"] for i in items), "Rejected patch executed"
    capture["verified"] = {"reread_before_retry": True, "rejected_patch_not_executed": True,
        "old_tool_use_id": rejected["tool_use_id"], "retry_tool_use_id": retry["tool_use_id"], "both_changes_preserved": True}


GATE_HOOK = '''import contextlib, io, json, os, pathlib, select, sys
script, settings = sys.argv[1:]
sys.path.insert(0, str(pathlib.Path(script).parent))
from codex_hooks import hook
root = pathlib.Path(__file__).resolve().parent
event = json.load(sys.stdin)
sys.stdin = io.StringIO(json.dumps(event))
output = io.StringIO()
with contextlib.redirect_stdout(output):
    hook(settings)
result = json.loads(output.getvalue())
alpha = root / "alpha.session"
if (event["hook_event_name"] == "PreToolUse" and not result and alpha.exists()
        and event["session_id"] == alpha.read_text() and not (root / "alpha.gated").exists()):
    fd = os.open(root / "edit.fifo", os.O_RDWR | os.O_NONBLOCK)
    try:
        (root / "alpha.gated").write_text(json.dumps(event))
        if not select.select([fd], [], [], 25)[0] or os.read(fd, 1) != b"1":
            result = {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny",
                      "permissionDecisionReason": "Test fixture gate timed out; patch was not executed"}}
    finally:
        os.close(fd)
print(json.dumps(result))
'''


class HookLockExperiment(FileConflictExperiment):
    def configure_hooks(self):
        wrapper = self.root / "gate_hook.py"
        wrapper.write_text(GATE_HOOK)
        os.mkfifo(self.root / "edit.fifo")
        path = self.home / "hooks.json"
        document = json.loads(path.read_text())
        entry = document["hooks"]["PreToolUse"][-1]
        entry["hooks"][0]["command"] = shlex.join([sys.executable, str(wrapper),
            str(Path(__file__).resolve().parent / "codex_hooks.py"), str(self.state / "settings.json")])
        path.write_text(json.dumps(document))

    def file_events(self, role):
        path = self.state / (self.registered[role] + ".jsonl")
        return [json.loads(line) for line in path.read_text().splitlines() if line.startswith("{")]

    def release_gate(self):
        fd = os.open(self.root / "edit.fifo", os.O_WRONLY | os.O_NONBLOCK)
        try:
            os.write(fd, b"1")
        finally:
            os.close(fd)


def demo(codex, output):
    repo = Path(__file__).resolve().parents[1]
    subprocess.run(["cargo", "build", "--bin", "agent-hub"], cwd=repo, check=True)
    with tempfile.TemporaryDirectory(prefix="hub-edit-", dir="/tmp") as directory:
        with HookLockExperiment(codex, Path(directory)) as experiment:
            for role in ["alpha", "beta"]:
                experiment.launch_shared(role)
            experiment.ready()
            (experiment.root / "alpha.session").write_text(experiment.registered["alpha"])
            experiment.prompt("alpha", "Read settings.json. Use apply_patch to set timeout_ms to 2000, "
                "preserving all other fields. Verify the saved file. Do not edit other files.")
            experiment.wait(lambda: (experiment.root / "alpha.gated").exists(), timeout=60)
            experiment.prompt("beta", "Read settings.json. Use apply_patch to set max_retries to 3, "
                "preserving all other fields. Verify the saved file. Do not edit other files.")
            experiment.wait(lambda: any(e["type"] == "file_lock_waiting" for e in experiment.file_events("beta")), timeout=20)
            print("PASS: both real CLIs automatically acquired/requested leases; beta is waiting.", flush=True)
            experiment.release_gate()
            experiment.wait(lambda: experiment.idle()
                and all(len(experiment.turns(t)) == 2 for t in experiment.registered.values()), timeout=100)
            capture = experiment.capture()
            capture["version"] = subprocess.check_output([codex, "--version"], text=True).strip()
            capture["final_settings"] = json.loads(experiment.target.read_text())
            capture["fixture_barrier"] = "Pause alpha after production hook acquisition, before apply_patch"
            capture["summary"] = summarize(capture)
            verify(capture)
            capture["final_tickets"] = {}
            for role, thread in experiment.registered.items():
                for event in experiment.file_events(role):
                    if event["type"] == "file_locks_released":
                        for request_id in event["request_ids"]:
                            ticket = HubClient(experiment.hub.url, thread).lock_status(request_id)
                            assert ticket["status"] == "released", "File lock remained active"
                            capture["final_tickets"][request_id] = ticket
            for thread in experiment.registered.values():
                assert not list(HubClient(experiment.hub.url, thread).inbox()), "Hook leaked conversation input"
            Path(output).write_text(json.dumps(capture, indent=2, ensure_ascii=False) + "\n")
            print("PASS: beta received the hook denial, reread/retried; both edits persisted, zero MCP calls.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--output", required=True)
    parser.add_argument("--max-seconds", type=int, default=240)
    args = parser.parse_args()
    if args.max_seconds <= 0:
        parser.error("max-seconds must be positive")
    def timeout(*_):
        raise TimeoutError("Automatic hook lock check exceeded its time budget")
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(args.max_seconds)
    demo(args.codex, args.output)
