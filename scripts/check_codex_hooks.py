#!/usr/bin/env python3
"""Exercise hooks in two ordinary interactive Codex CLIs, with isolated configuration."""

import argparse
import fcntl
import json
import os
import pty
import select
import shlex
import signal
import sqlite3
import struct
import subprocess
import sys
import tempfile
import termios
import time
import tomllib
from pathlib import Path
from types import SimpleNamespace

from check_codex import AppServer
from check_codex_delivery import Hub
from codex_daemon import DaemonClient
from codex_hooks import CliJournal, install, uninstall
from codex_session import HubClient
from codex_worker import initialize


class Terminal:
    def __init__(self, codex, environment, cwd, prompt):
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.execvpe(codex, [codex, "--no-alt-screen", "-C", str(cwd),
                              "-s", "read-only", "-a", "never", prompt], environment)
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 140, 0, 0))
        self.tail = bytearray()

    def drain(self):
        if select.select([self.fd], [], [], 0)[0]:
            try:
                data = os.read(self.fd, 65536)
            except OSError:
                return
            self.tail.extend(data)
            del self.tail[:-8000]
            if b"\x1b[6n" in data:
                os.write(self.fd, b"\x1b[1;1R")

    def prompt(self, text):
        os.write(self.fd, b"\x1b[200~" + text.encode() + b"\x1b[201~\r")

    def close(self):
        # Close the PTY first: on macOS an exiting child can wait for its tty to drain.
        os.close(self.fd)
        try:
            os.kill(self.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if os.waitpid(self.pid, os.WNOHANG)[0]:
                return
            time.sleep(0.05)
        raise TimeoutError("The demo CLI did not exit after its PTY was closed")


def stop_demo_daemon(home):
    # The daemon is detached. Target only binaries inside this temporary CODEX_HOME.
    prefix = str(home.resolve() / "packages/app-server-daemon") + "/"
    listing = subprocess.check_output(["ps", "-axo", "pid=,args="], text=True)
    for line in listing.splitlines():
        pid, command = line.strip().split(None, 1)
        if command.startswith(prefix):
            try:
                os.kill(int(pid), signal.SIGKILL)
            except ProcessLookupError:
                pass


def demo(codex, output):
    repo = Path(__file__).resolve().parents[1]
    subprocess.run(["cargo", "build", "--bin", "agent-hub"], cwd=repo, check=True)
    version = subprocess.check_output([codex, "--version"], text=True).strip()
    # A short home is required by the daemon's Unix-domain socket path limit.
    with tempfile.TemporaryDirectory(prefix="hub-hooks-", dir="/tmp") as directory:
        root = Path(directory)
        home, state = root / "home", root / "state"
        home.mkdir()
        source = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
        if (source / "auth.json").exists():
            (home / "auth.json").symlink_to(source / "auth.json")
        model = tomllib.loads((source / "config.toml").read_text()).get("model") if (source / "config.toml").exists() else None
        config = home / "config.toml"
        config.write_text("analytics.enabled=false\n" + ("model=" + json.dumps(model) + "\n" if model else "")
                          + "\n[projects." + json.dumps(str(root)) + "]\ntrust_level=\"trusted\"\n")
        env = {**os.environ, "CODEX_HOME": str(home), "TERM": "xterm-256color"}
        hub, terminals, app = Hub(repo / "target/debug/agent-hub", root), [], None
        command_threads = set()
        args = SimpleNamespace(codex_home=str(home), state_dir=str(state), url=hub.url, socket=None)

        def wait(predicate, timeout=180):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                for terminal in terminals:
                    terminal.drain()
                value = predicate()
                if value:
                    return value
                time.sleep(0.03)
            raise TimeoutError("Ordinary CLI demo timed out")

        def sessions():
            result = {}
            for path in state.glob("*.sqlite3"):
                journal = CliJournal(path)
                registration = journal.registration()
                journal.close()
                if registration:
                    result[registration["display_name"]] = registration["session_id"]
            return result

        def turns(thread):
            return app.request("thread/read", {"threadId": thread, "includeTurns": True})["thread"]["turns"]

        def answered(thread, expected):
            return any(t["status"] == "completed" and any(i["type"] == "agentMessage" and
                       i.get("phase") in {None, "final_answer"} and i["text"] == expected
                       for i in t["items"]) for t in turns(thread))

        def events(thread):
            path = state / (thread + ".jsonl")
            return [json.loads(line) for line in path.read_text().splitlines() if line.startswith("{")] if path.exists() else []

        def assign(key, calculation, ending):
            request = {"message_id": key, "sender_id": orchestrator, "recipient_id": worker,
                       "correlation_id": key, "payload": {"text":
                       f"Compute {calculation}. Reply with the single integer only. Do not use tools."}}
            text = ("Call agent_hub hub_send_message exactly once with " + json.dumps(request) + ". Then "
                    + ending + " For future incoming Hub turn_completed messages, reply only with the sum of "
                    "our original secret and the integer payload.final_text. Do not send messages in reply to results.")
            terminals[1].prompt(text)

        try:
            hub.start()
            install(args)
            # Only this isolated demo pre-trusts its own commands. User installs use /hooks.
            trust = AppServer(codex, None, command=[codex, "app-server", "--listen", "stdio://"], environment=env)
            try:
                initialize(trust)
                listing = trust.request("hooks/list", {"cwds": [str(root)]})
                with config.open("a") as file:
                    for hook in listing["data"][0]["hooks"]:
                        assert str(repo / "scripts/codex_hooks.py") in hook["command"]
                        file.write("\n[hooks.state." + json.dumps(hook["key"]) + "]\ntrusted_hash="
                                   + json.dumps(hook["currentHash"]) + "\n")
            finally:
                trust.close()
            for role, prompt in [("worker", "For future incoming Hub requests, follow payload.text and reply with the result. "
                                 "The integration reports results automatically. Reply WORKER_READY only now."),
                                 ("orchestrator", "Remember the secret number 37 for this conversation. Reply READY only. Do not use tools.")]:
                cwd = root / role
                cwd.mkdir()
                terminals.append(Terminal(codex, env, cwd, prompt))
            registered = wait(lambda: sessions() if len(sessions()) == 2 else None)
            worker, orchestrator = registered["worker"], registered["orchestrator"]
            app = DaemonClient(home / "app-server-control/app-server-control.sock")
            initialize(app)
            def observe(notification):
                params = notification.get("params", {})
                if notification["method"] == "item/started" and params.get("item", {}).get("type") == "commandExecution":
                    command_threads.add(params["threadId"])
            app.on_notification = observe
            for thread in [worker, orchestrator]:
                resumed = app.request("thread/resume", {"threadId": thread})
                assert resumed["approvalPolicy"] == "never"
                assert resumed["sandbox"]["type"] == "readOnly"
            wait(lambda: answered(worker, "WORKER_READY") and answered(orchestrator, "READY"))
            wait(lambda: all(any(e["type"] == "bridge_connected" for e in events(t)) for t in [worker, orchestrator]))
            print("PASS: SessionStart registered two ordinary CLIs and attached their existing threads.", flush=True)

            assign("hooks-idle", "2 + 2", "reply WAITING only.")
            wait(lambda: answered(orchestrator, "41"))
            assert not list(HubClient(hub.url, orchestrator).inbox())
            print("PASS: a user prompt called MCP; worker result 4 woke the same idle CLI -> 41.", flush=True)

            gate = root / "release"
            command = "python3 -c " + shlex.quote("import pathlib,time; p=pathlib.Path(" + repr(str(gate))
                       + "); exec('while not p.exists(): time.sleep(0.1)'); print('released')")
            assign("hooks-busy", "3 + 3", "run this shell command and wait for its exit: " + command
                   + ". This command belongs only to this turn. Then reply BUSY_DONE only.")
            # Running commands aren't included in persisted turn items until completion.
            wait(lambda: app.request("thread/read", {"threadId": orchestrator})["thread"]["status"]["type"] == "active"
                         and orchestrator in command_threads)
            pending = wait(lambda: next((m for m in HubClient(hub.url, orchestrator).inbox()
                           if m["correlation_id"] == "hooks-busy"), None))
            assert not pending["acknowledged"]
            assert app.request("thread/read", {"threadId": orchestrator})["thread"]["status"]["type"] == "active"
            with sqlite3.connect(state / (orchestrator + ".sqlite3")) as db:
                assert not db.execute("SELECT queue_id FROM inputs WHERE message_id=?", (pending["message_id"],)).fetchone()
            gate.write_text("release\n")
            wait(lambda: answered(orchestrator, "43"))
            print("PASS: result 6 remained unacknowledged while CLI was busy, then woke it -> 43.", flush=True)

            # Pause only the bridge. Keep both interactive CLIs and their daemon alive.
            bridge_pid = int((state / (orchestrator + ".pid")).read_text())
            os.kill(bridge_pid, signal.SIGSTOP)
            try:
                hub.stop()
                hub.start()
                assign("hooks-reconnect", "4 + 4", "reply OFFLINE_READY only.")
                stored = wait(lambda: next((m for m in HubClient(hub.url, orchestrator).inbox()
                              if m["correlation_id"] == "hooks-reconnect"), None))
                assert stored["payload"]["final_text"] == "8" and not stored["acknowledged"]
                os.kill(bridge_pid, signal.SIGUSR1)
            finally:
                os.kill(bridge_pid, signal.SIGCONT)
            wait(lambda: answered(orchestrator, "45"))
            wait(lambda: sum(e["type"] == "bridge_connected" for e in events(orchestrator)) >= 2)
            assert not list(HubClient(hub.url, orchestrator).inbox())
            print("PASS: Hub restart and bridge reconnection preserved both CLI threads -> 45.", flush=True)

            terminals[1].prompt("What is our original secret plus 7? Reply with the number only. Do not use tools.")
            wait(lambda: answered(orchestrator, "44"))
            print("PASS: user can still type into the same ordinary CLI -> 44.", flush=True)
            # Archiving our disposable sessions explicitly ends them, including observers.
            for thread in [worker, orchestrator]:
                app.request("thread/archive", {"threadId": thread})
            wait(lambda: not list(state.glob("*.pid")), timeout=20)
            print("PASS: SessionEnd on archiving the disposable threads stopped their bridges.", flush=True)
            evidence = {"version": version, "orchestrator_thread_id": orchestrator, "worker_thread_id": worker,
                        "scenarios": ["hook registration", "idle", "busy", "Hub restart and bridge reconnect",
                                      "CLI user input", "SessionEnd"],
                        "permissions": {"approvalPolicy": "never", "sandbox": "readOnly", "preserved": True},
                        "events": {role: events(thread) for role, thread in registered.items()}}
            if output:
                Path(output).write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n")
        finally:
            (root / "release").touch()
            uninstall(args)
            if app:
                app.close()
            stop_demo_daemon(home)
            for terminal in terminals:
                terminal.close()
            hub.stop()
    print("PASS: full ordinary CLI hook flow and isolated cleanup.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--output")
    args = parser.parse_args()
    demo(args.codex, args.output)
