#!/usr/bin/env python3
"""Run the complete prototype with two real Codex sessions and recovery checks."""

import argparse
import json
import os
import shlex
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.request import urlopen

from codex_session import CodexSession, HubClient, Journal


class Hub:
    def __init__(self, binary, root):
        self.binary, self.root = binary, root
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.address = "127.0.0.1:" + str(listener.getsockname()[1])
        self.url = "http://" + self.address
        self.process = None

    def start(self):
        self.process = subprocess.Popen([str(self.binary)], env={**os.environ,
            "AGENT_HUB_BIND": self.address, "AGENT_HUB_DB": str(self.root / "hub.sqlite3"),
            "RUST_LOG": "error"}, stdout=subprocess.DEVNULL)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("Hub exited during startup")
            try:
                with urlopen(self.url + "/health", timeout=0.2) as response:
                    if response.read() == b"ok":
                        return
            except OSError:
                time.sleep(0.02)
        raise TimeoutError("Hub did not start")

    def stop(self):
        if self.process:
            self.process.kill()
            self.process.wait(timeout=5)
            self.process = None


def pump(sessions, predicate, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for session in sessions:
            session.tick()
        if predicate():
            return
    raise TimeoutError("The expected real runtime result did not arrive")


def assignment(key, calculation):
    return {"message_id": key, "sender_id": "orchestrator", "recipient_id": "worker",
            "correlation_id": key, "payload": {"text":
                f"Compute {calculation}. Reply with the single integer only. Do not use tools."}}


def send_prompt(request, ending):
    return ("For this turn only, call the agent_hub hub_send_message tool exactly once with "
            + json.dumps(request) + ". Then " + ending
            + " For future incoming Hub turn_completed messages, reply only with the sum of "
              "the secret number from our first turn and the integer payload.final_text. "
              "Do not send messages in response to turn_completed.")


def demo(codex, output):
    repo = Path(__file__).resolve().parents[1]
    subprocess.run(["cargo", "build", "--bin", "agent-hub"], cwd=repo, check=True)
    version = subprocess.check_output([codex, "--version"], text=True).strip()
    with tempfile.TemporaryDirectory(prefix="hub-delivery-") as directory:
        root = Path(directory)
        hub = Hub(repo / "target/debug/agent-hub", root)
        orchestrator = worker = None
        journals = []
        events = {"orchestrator": [], "worker": []}

        def emit(role):
            def record(event):
                events[role].append(event)
                print(role + ": " + json.dumps(event, ensure_ascii=False), flush=True)
            return record

        def answer(expected):
            return any(e["type"] == "turn_completed" and e["status"] == "completed"
                       and e["final_text"] == expected for e in events["orchestrator"])

        try:
            hub.start()
            orchestrator_journal = Journal(root / "orchestrator.sqlite3")
            worker_journal = Journal(root / "worker.sqlite3")
            journals.extend([orchestrator_journal, worker_journal])
            orchestrator = CodexSession.connect(codex, hub.url, "orchestrator", orchestrator_journal,
                                                root, emit=emit("orchestrator"))
            worker = CodexSession.connect(codex, hub.url, "worker", worker_journal,
                                          root, report_results=True, emit=emit("worker"))
            thread_id = orchestrator.thread_id
            command_turns = set()

            def observe(notification):
                if notification.get("method") == "item/started":
                    params = notification["params"]
                    if params.get("item", {}).get("type") == "commandExecution":
                        command_turns.add(params["turnId"])
                orchestrator.observe(notification)

            orchestrator.app.on_notification = observe
            print("1. A real orchestrator turn assigns work via MCP; worker needs no completion tool.", flush=True)
            first = assignment("idle-assignment", "2 + 2")
            orchestrator.start_text("Remember our secret number 37. " + send_prompt(first, "reply WAITING only."))
            pump([orchestrator], lambda: not orchestrator.active_turn)
            assert list(worker.hub.inbox())[0]["message_id"] == first["message_id"]
            # Worker begins only after the orchestrator is idle in this first scenario.
            pump([worker, orchestrator], lambda: answer("41"))
            assert not list(orchestrator.hub.inbox()), "Accepted result should be ACKed"
            print("PASS: worker result 4 automatically continued the existing orchestrator with context -> 41.", flush=True)

            print("2. Hold a real orchestrator turn busy while the worker finishes.", flush=True)
            gate = root / "busy-release"
            second = assignment("busy-assignment", "3 + 3")
            command = "python3 -c " + shlex.quote(
                "import pathlib,time; p=pathlib.Path(" + repr(str(gate)) + "); "
                "exec('while not p.exists(): time.sleep(0.1)'); print('released')")
            busy_turn = orchestrator.start_text(send_prompt(second,
                "run this exact shell command and wait until it exits: " + command
                + ". Afterward reply BUSY_DONE only. The command belongs only to this turn."))
            pump([orchestrator], lambda: busy_turn in command_turns, timeout=60)
            pump([worker, orchestrator], lambda: bool(list(orchestrator.hub.inbox())))
            pending = list(orchestrator.hub.inbox())[0]
            assert pending["correlation_id"] == "busy-assignment", pending
            assert orchestrator.active_turn == busy_turn, events["orchestrator"]
            assert orchestrator_journal.receipt(pending["message_id"]) is None
            assert not pending["acknowledged"]
            gate.write_text("release\n")
            pump([worker, orchestrator], lambda: answer("43"))
            print("PASS: result 6 stayed unacknowledged while busy, then started a separate turn -> 43.", flush=True)

            print("3. Disconnect the adapter, restart Hub, finish worker, and resume the same orchestrator.", flush=True)
            third = assignment("reconnect-assignment", "4 + 4")
            orchestrator.start_text(send_prompt(third, "reply OFFLINE_READY only."))
            pump([orchestrator], lambda: not orchestrator.active_turn)
            assert list(worker.hub.inbox())[0]["message_id"] == third["message_id"]
            orchestrator.close()
            orchestrator = None
            hub.stop()
            hub.start()
            pump([worker], lambda: bool(list(HubClient(hub.url, "orchestrator").inbox())))
            stored = list(HubClient(hub.url, "orchestrator").inbox())[0]
            assert stored["payload"]["final_text"] == "8" and not stored["acknowledged"], stored
            orchestrator = CodexSession.connect(codex, hub.url, "orchestrator", orchestrator_journal,
                                                root, emit=emit("orchestrator"))
            assert orchestrator.thread_id == thread_id
            pump([worker, orchestrator], lambda: answer("45"))
            pump([worker, orchestrator], lambda: worker.signals.connections >= 2, timeout=15)
            assert not list(orchestrator.hub.inbox())
            print("PASS: SSE reconnected; durable result 8 continued the resumed original thread -> 45.", flush=True)
            evidence = {"version": version, "orchestrator_thread_id": thread_id,
                        "worker_thread_id": worker.thread_id,
                        "worker_sse_connections": worker.signals.connections,
                        "events": events, "scenarios": ["idle", "busy", "adapter and Hub restart"]}
            if output:
                Path(output).write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n")
            print("PASS: full worker -> Hub -> existing orchestrator flow, busy queue, and recovery.", flush=True)
        finally:
            for session in [orchestrator, worker]:
                if session:
                    session.close()
            for journal in journals:
                journal.close()
            hub.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--output")
    args = parser.parse_args()
    demo(args.codex, args.output)
