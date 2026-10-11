#!/usr/bin/env python3
"""Verify context-preserving external input and waiting for a busy Codex thread."""

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

from check_codex import AppServer
from codex_worker import TurnCollector, initialize


def probe(codex="codex", output=None):
    version = subprocess.check_output([codex, "--version"], text=True).strip()
    with tempfile.TemporaryDirectory(prefix="hub-input-") as directory:
        app = AppServer(codex, None, command=[codex, "app-server", "--listen", "stdio://",
                        "-c", "analytics.enabled=false", "-c", "mcp_servers={}"])
        try:
            initialize(app)
            thread = app.request("thread/start", {
                "cwd": directory, "ephemeral": True,
                "approvalPolicy": "never", "sandbox": "read-only",
            })["thread"]["id"]
            collector = TurnCollector(thread)
            app.on_notification = collector.observe
            first = app.request("turn/start", {"threadId": thread, "input": [{
                "type": "text", "text": "Remember the secret number 37. Reply READY only. Do not use tools.",
            }]})["turn"]
            assert first["status"] == "inProgress", first
            # External input arrives while this turn is busy. Keep it outside runtime.
            pending = "What is the secret number plus 5? Reply with the number only. Do not use tools."
            terminal = app.wait_notification("turn/completed", lambda p: p["turn"]["id"] == first["id"])
            first_result = collector.result(terminal, "probe", "observer", "first")
            assert first_result["status"] == "completed", first_result
            second = app.request("turn/start", {"threadId": thread, "input": [{
                "type": "text", "text": pending,
            }]})["turn"]
            terminal = app.wait_notification("turn/completed", lambda p: p["turn"]["id"] == second["id"])
            result = collector.result(terminal, "probe", "observer", "second")
            assert second["id"] != first["id"]
            assert result["status"] == "completed" and result["final_text"] == "42", result
            evidence = {"version": version, "thread_id": thread,
                        "first_turn_id": first["id"], "second_turn_id": second["id"],
                        "acceptance_status": second["status"], "final_text": result["final_text"],
                        "busy_policy": "external queue until turn/completed"}
            if output:
                Path(output).write_text(json.dumps(evidence, indent=2) + "\n")
            print(json.dumps(evidence, indent=2), flush=True)
            print("PASS: external input starts a distinct turn in the same thread and retains context.")
            return evidence
        finally:
            app.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--output")
    args = parser.parse_args()
    probe(args.codex, args.output)
