#!/usr/bin/env python3
"""Install Codex lifecycle hooks and attach Hub delivery to ordinary running CLI sessions."""

import argparse
import fcntl
import hashlib
import json
import os
import shlex
import signal
import subprocess
import sys
import threading
import time
import tomllib
import uuid
from http.client import HTTPException
from pathlib import Path

from codex_daemon import DaemonClient
from codex_session import HubClient, InboxSignals
from codex_worker import Outbox, TurnCollector, initialize

MARKER = "Agent Hub ordinary CLI integration"
EVENTS = ("SessionStart", "Stop", "SessionEnd")
HTTP_FAILURES = (OSError, HTTPException, json.JSONDecodeError)


def input_text(message):
    return "Incoming Agent Hub message:\n" + json.dumps(message, ensure_ascii=False, sort_keys=True)


def client_id(message_id):
    return "agent-hub:" + hashlib.sha256(message_id.encode()).hexdigest()


class CliJournal(Outbox):
    def __init__(self, path):
        super().__init__(path)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS registration (singleton INTEGER PRIMARY KEY CHECK(singleton=1), body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS inputs (
                message_id TEXT PRIMARY KEY, message TEXT NOT NULL, input_text TEXT UNIQUE NOT NULL,
                queue_id TEXT, turn_id TEXT UNIQUE,
                acknowledged INTEGER NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0
            );
        """)

    def register(self, session):
        with self.db:
            self.db.execute("INSERT INTO registration VALUES(1, ?) ON CONFLICT(singleton) DO UPDATE SET body=excluded.body",
                            (json.dumps(session),))

    def registration(self):
        row = self.db.execute("SELECT body FROM registration").fetchone()
        return json.loads(row[0]) if row else None

    def prepare(self, message):
        encoded = json.dumps(message, ensure_ascii=False, sort_keys=True)
        existing = self.db.execute("SELECT message FROM inputs WHERE message_id=?", (message["message_id"],)).fetchone()
        if existing and existing[0] != encoded:
            raise ValueError("The same Hub message ID has different contents")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO inputs(message_id,message,input_text) VALUES(?,?,?)",
                            (message["message_id"], encoded, input_text(message)))

    def accepted(self, message_id):
        row = self.db.execute("SELECT queue_id,turn_id FROM inputs WHERE message_id=?", (message_id,)).fetchone()
        return bool(row and any(row))

    def queued(self, message_id, queue_id):
        with self.db:
            self.db.execute("UPDATE inputs SET queue_id=? WHERE message_id=?", (queue_id, message_id))

    def observe_input(self, turn_id, item):
        if item["type"] != "userMessage":
            return
        texts = [part["text"] for part in item["content"] if part["type"] == "text"]
        for text in texts:
            with self.db:
                self.db.execute("UPDATE inputs SET turn_id=? WHERE input_text=? AND turn_id IS NULL", (turn_id, text))

    def pending_acks(self):
        return [row[0] for row in self.db.execute(
            "SELECT message_id FROM inputs WHERE acknowledged=0 AND (queue_id IS NOT NULL OR turn_id IS NOT NULL)")]

    def acked(self, message_id):
        with self.db:
            self.db.execute("UPDATE inputs SET acknowledged=1 WHERE message_id=?", (message_id,))

    def unfinished(self, turn_id):
        row = self.db.execute("SELECT message FROM inputs WHERE turn_id=? AND completed=0", (turn_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def completed(self, turn_id):
        with self.db:
            self.db.execute("UPDATE inputs SET completed=1 WHERE turn_id=?", (turn_id,))

    def recover_queue(self, entries):
        pending = {client_id(row[0]): row[0] for row in self.db.execute(
            "SELECT message_id FROM inputs WHERE queue_id IS NULL AND turn_id IS NULL")}
        for entry in entries:
            message_id = pending.get(entry.get("clientUserMessageId"))
            if message_id:
                self.queued(message_id, entry["id"])


class CliBridge:
    def __init__(self, app, hub, journal, emit):
        self.app, self.hub, self.journal, self.emit = app, hub, journal, emit
        self.thread_id = journal.registration()["runtime_id"]
        self.collector = TurnCollector(self.thread_id)
        self.next_poll = 0
        self.signals = None
        app.on_notification = self.observe

    def observe(self, notification):
        params = notification.get("params", {})
        if params.get("threadId") != self.thread_id:
            return
        method = notification["method"]
        if method in {"item/started", "item/completed"}:
            self.journal.observe_input(params["turnId"], params["item"])
        self.collector.observe(notification)
        if method == "turn/completed":
            self.finish(params)

    def recover(self, turns, queued):
        self.journal.recover_queue(queued)
        for turn in turns:
            for item in turn.get("items", []):
                self.observe({"method": "item/completed", "params": {
                    "threadId": self.thread_id, "turnId": turn["id"], "item": item,
                }})
            if turn["status"] in {"completed", "failed", "interrupted"} and self.journal.unfinished(turn["id"]):
                self.finish({"threadId": self.thread_id, "turn": turn})

    def finish(self, terminal):
        turn_id = terminal["turn"]["id"]
        message = self.journal.unfinished(turn_id)
        result = self.collector.result(terminal, self.hub.session_id,
                                       message["sender_id"] if message else self.hub.session_id,
                                       (message.get("correlation_id") or message["message_id"]) if message else turn_id)
        if message:
            # Automatically return request results, but avoid reciprocal replies to results.
            payload = message["payload"]
            if not isinstance(payload, dict) or payload.get("type") != "turn_completed":
                self.journal.enqueue("result:" + json.dumps([self.hub.session_id, self.thread_id, turn_id]),
                                     self.hub.url + "/turn-results", result)
            self.journal.completed(turn_id)
        self.emit({"type": "turn_completed", "thread_id": self.thread_id, "turn_id": turn_id,
                   "status": result["status"], "final_text": result["final_text"]})

    def deliver(self, message):
        self.journal.prepare(message)
        if not self.journal.accepted(message["message_id"]):
            response = self.app.request("thread/queue/add", {
                "threadId": self.thread_id, "clientUserMessageId": client_id(message["message_id"]),
                "input": [{"type": "text", "text": input_text(message)}],
            })["queuedSubmission"]
            if response["clientUserMessageId"] != client_id(message["message_id"]):
                raise RuntimeError("Runtime accepted a different queued message")
            self.journal.queued(message["message_id"], response["id"])
            self.emit({"type": "message_accepted", "message_id": message["message_id"],
                       "thread_id": self.thread_id, "queue_id": response["id"]})
        self.hub.ack(message["message_id"])
        self.journal.acked(message["message_id"])

    def tick(self):
        for _ in range(32):
            try:
                self.app.receive(0.01)
            except TimeoutError:
                break
        self.app.notifications.clear()
        now = time.monotonic()
        if now < self.next_poll and not (self.signals and self.signals.dirty.is_set()):
            return
        self.next_poll = now + 1
        if self.signals:
            self.signals.dirty.clear()
        try:
            self.journal.flush()
            for message_id in self.journal.pending_acks():
                self.hub.ack(message_id)
                self.journal.acked(message_id)
            # Read live status: the user can start a turn from the CLI at any time.
            thread = self.app.request("thread/read", {"threadId": self.thread_id})["thread"]
            if thread["status"]["type"] != "idle":
                return
            for message in self.hub.inbox():
                self.deliver(message)
                break
        except HTTP_FAILURES as error:
            self.emit({"type": "hub_retry", "error": str(error)})

    def close(self):
        if self.signals:
            self.signals.close()
        self.app.close()


def config_root(path=None):
    return Path(path or os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).expanduser().resolve()


def session_paths(settings, session_id):
    # Runtime UUIDs give stable Hub addresses and safe per-session filenames.
    uuid.UUID(session_id)
    base = Path(settings["state_dir"]) / session_id
    return Path(str(base) + ".sqlite3"), Path(str(base) + ".lock"), Path(str(base) + ".pid"), Path(str(base) + ".jsonl")


def ensure_bridge(settings_path, settings, session_id):
    _, lock_path, _, log_path = session_paths(settings, session_id)
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
    with log_path.open("ab") as log:
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "bridge",
                          "--settings", str(settings_path), "--session-id", session_id],
                         stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)


def stop_bridge(pid_path, settings_path):
    try:
        pid = int(pid_path.read_text())
        command = subprocess.check_output(["ps", "-p", str(pid), "-o", "args="], text=True, timeout=1).strip()
        if (str(Path(__file__).resolve()) + " bridge " in command
                and str(settings_path) in command):
            os.kill(pid, signal.SIGTERM)
    except (OSError, ValueError, subprocess.SubprocessError):
        pass


def hook(settings_path):
    settings = json.loads(Path(settings_path).read_text())
    event = json.load(sys.stdin)
    session_id = event["session_id"]
    database, _, pid_path, _ = session_paths(settings, session_id)
    if event["hook_event_name"] == "SessionEnd":
        stop_bridge(pid_path, Path(settings_path))
        print("{}")
        return
    journal = CliJournal(database)
    try:
        if event["hook_event_name"] == "SessionStart":
            registration = {"session_id": session_id, "runtime_id": session_id,
                            "display_name": Path(event["cwd"]).name or session_id[:8]}
            journal.register(registration)
            journal.enqueue("register:" + hashlib.sha256(json.dumps(registration, sort_keys=True).encode()).hexdigest(),
                            settings["url"] + "/sessions", registration)
        if journal.registration():
            ensure_bridge(settings_path, settings, session_id)
        try:
            journal.flush(timeout=2)
        except HTTP_FAILURES:
            pass  # The bridge retries durable registration after Hub recovers.
        if event["hook_event_name"] == "SessionStart":
            print(json.dumps({"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext":
                f"Your Agent Hub session_id and sender_id are {session_id}. Use hub_list_sessions to find peers and "
                "hub_send_message to contact them. Incoming Hub requests are delivered automatically in this CLI "
                "conversation. The integration reports turn results automatically; no completion tool is needed."}}))
        else:
            print("{}")  # Stop is a reconnect hint, never treated as terminal completion.
    finally:
        journal.close()


def run_bridge(settings_path, session_id):
    settings = json.loads(Path(settings_path).read_text())
    database, lock_path, pid_path, _ = session_paths(settings, session_id)
    stopped, reconnect = threading.Event(), threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    signal.signal(signal.SIGUSR1, lambda *_: reconnect.set())

    def emit(event):
        print(json.dumps(event, ensure_ascii=False), flush=True)

    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        pid_path.write_text(str(os.getpid()))
        journal = CliJournal(database)
        try:
            while not stopped.is_set():
                bridge = app = None
                try:
                    app = DaemonClient(settings.get("socket_path") or
                                       Path(settings["codex_home"]) / "app-server-control/app-server-control.sock")
                    initialize(app)
                    loaded, cursor = [], None
                    while True:
                        page = app.request("thread/loaded/list", {"cursor": cursor})
                        loaded.extend(page["data"])
                        cursor = page.get("nextCursor")
                        if not cursor:
                            break
                    if session_id not in loaded:
                        raise RuntimeError("The registered CLI thread is not loaded; waiting for its user to resume it")
                    # No overrides: preserve the CLI's model, sandbox, approvals, and instructions.
                    thread = app.request("thread/resume", {"threadId": session_id})["thread"]
                    bridge = CliBridge(app, HubClient(settings["url"], session_id), journal, emit)
                    queued, cursor = [], None
                    while True:
                        page = app.request("thread/queue/list", {"threadId": session_id, "cursor": cursor})
                        queued.extend(page["data"])
                        cursor = page.get("nextCursor")
                        if not cursor:
                            break
                    bridge.recover(thread.get("turns", []), queued)
                    for notification in list(app.notifications):
                        bridge.observe(notification)
                    app.notifications.clear()
                    bridge.signals = InboxSignals(bridge.hub)
                    reconnect.clear()
                    emit({"type": "bridge_connected", "session_id": session_id, "thread_id": thread["id"]})
                    while not stopped.is_set() and not reconnect.is_set():
                        bridge.tick()
                except (OSError, RuntimeError, HTTPException) as error:
                    emit({"type": "bridge_retry", "error": str(error)})
                finally:
                    if bridge:
                        bridge.close()
                    elif app:
                        app.close()
                stopped.wait(1)
        finally:
            try:
                journal.flush(timeout=2)
            except HTTP_FAILURES:
                pass
            journal.close()
            pid_path.unlink(missing_ok=True)


def install(args):
    # Fail before touching config if the runtime transport dependency is unavailable.
    from websockets.sync.client import unix_connect  # noqa: F401

    root = config_root(args.codex_home)
    root.mkdir(parents=True, exist_ok=True)
    state_dir = Path(args.state_dir).expanduser().resolve() if args.state_dir else root / "agent-hub"
    state_dir.mkdir(parents=True, exist_ok=True)
    settings_path = state_dir / "settings.json"
    config_path = root / "config.toml"
    config_text = config_path.read_text() if config_path.exists() else ""
    config = tomllib.loads(config_text)
    url = args.url.rstrip("/")
    existing_mcp = config.get("mcp_servers", {}).get("agent_hub")
    if existing_mcp is not None and (existing_mcp.get("url") != url + "/mcp" or existing_mcp.get("enabled") is False):
        raise ValueError("Existing mcp_servers.agent_hub uses a different configuration; choose its URL or reconcile it first")
    block = ("\n# BEGIN " + MARKER + "\n[mcp_servers.agent_hub]\nurl = "
             + json.dumps(url + "/mcp") + "\n# END " + MARKER + "\n")
    previous = json.loads(settings_path.read_text()) if settings_path.exists() else {}
    settings = {"url": url, "codex_home": str(root), "state_dir": str(state_dir),
                "socket_path": args.socket, "config_block": previous.get("config_block", "")}
    if not existing_mcp:
        settings["config_block"] = block
    command = shlex.join([sys.executable, str(Path(__file__).resolve()), "hook", "--settings", str(settings_path)])
    hooks_path = root / "hooks.json"
    document = json.loads(hooks_path.read_text()) if hooks_path.exists() else {}
    hooks = document.setdefault("hooks", {})
    settings["added_events"] = previous.get("added_events", [name for name in EVENTS if name not in hooks])
    for name in EVENTS:
        entries = [entry for entry in hooks.get(name, []) if entry.get("description") != MARKER]
        entries.append({"description": MARKER, "hooks": [{"type": "command", "command": command,
                        "statusMessage": MARKER, "timeout": 3 if name == "SessionEnd" else 10}]})
        hooks[name] = entries
    # All validation precedes mutations. Existing hook entries and config remain intact.
    settings_path.write_text(json.dumps(settings, indent=2) + "\n")
    hooks_path.write_text(json.dumps(document, indent=2) + "\n")
    if not existing_mcp:
        config_path.write_text(config_text + block)
    print(f"Installed hooks in {hooks_path}. In Codex, review and trust only these commands through /hooks.")
    print("Launch ordinary codex; SessionStart registers it and attaches delivery automatically.")


def uninstall(args):
    root = config_root(args.codex_home)
    state_dir = Path(args.state_dir).expanduser().resolve() if args.state_dir else root / "agent-hub"
    settings_path = state_dir / "settings.json"
    settings = json.loads(settings_path.read_text()) if settings_path.exists() else {}
    hooks_path = root / "hooks.json"
    if hooks_path.exists():
        document = json.loads(hooks_path.read_text())
        for name in EVENTS:
            entries = document.get("hooks", {}).get(name)
            if entries is not None:
                document["hooks"][name] = [entry for entry in entries if entry.get("description") != MARKER]
                if not document["hooks"][name] and name in settings.get("added_events", []):
                    del document["hooks"][name]
        hooks_path.write_text(json.dumps(document, indent=2) + "\n")
    if settings_path.exists():
        config_path = root / "config.toml"
        block = settings.get("config_block")
        if block and config_path.exists():
            text = config_path.read_text()
            if block in text:
                config_path.write_text(text.replace(block, "", 1))
    # Keep durable receipts/outboxes for recovery; stop only our session bridge processes.
    for pid_path in state_dir.glob("*.pid"):
        stop_bridge(pid_path, settings_path)
    print("Removed Agent Hub hook entries and its unmodified MCP config block. Durable state retained.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ["install", "uninstall"]:
        sub = commands.add_parser(name)
        sub.add_argument("--codex-home")
        sub.add_argument("--state-dir")
        if name == "install":
            sub.add_argument("--url", default="http://127.0.0.1:7331")
            sub.add_argument("--socket", help="Optional explicit Unix socket for the existing CLI runtime")
    for name in ["hook", "bridge"]:
        sub = commands.add_parser(name)
        sub.add_argument("--settings", required=True)
        if name == "bridge":
            sub.add_argument("--session-id", required=True)
    args = parser.parse_args()
    try:
        if args.command == "install":
            install(args)
        elif args.command == "uninstall":
            uninstall(args)
        elif args.command == "hook":
            hook(args.settings)
        else:
            run_bridge(args.settings, args.session_id)
    except (OSError, ValueError, ImportError) as error:
        print(f"Codex hook integration failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
