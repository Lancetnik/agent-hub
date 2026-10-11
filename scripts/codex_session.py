#!/usr/bin/env python3
"""External persistent Codex client: durable Hub inbox -> existing thread -> runtime ACK."""

import argparse
import fcntl
import json
import sys
import threading
import time
from http.client import HTTPException
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from check_codex import AppServer
from codex_worker import Outbox, TurnCollector, initialize


class HubClient:
    def __init__(self, url, session_id):
        self.url = url.rstrip("/")
        self.session_id = session_id
        self.base = self.url + "/sessions/" + quote(session_id, safe="")

    def request(self, path, body=None):
        request = Request(self.base + path, headers={"Content-Type": "application/json"},
                          data=json.dumps(body).encode() if body is not None else None)
        with urlopen(request, timeout=10) as response:
            return json.load(response)

    def inbox(self):
        cursor = None
        while True:
            query = {"limit": 100}
            if cursor is not None:
                query["cursor"] = cursor
            page = self.request("/inbox?" + urlencode(query))
            yield from page["messages"]
            cursor = page["next_cursor"]
            if cursor is None:
                return

    def ack(self, message_id):
        result = self.request("/ack", {"message_id": message_id})
        if result != {"session_id": self.session_id, "message_id": message_id,
                      "acknowledged": True}:
            raise RuntimeError("Hub did not confirm the recipient ACK")


class InboxSignals:
    """SSE carries wake-up hints only. Reconnect and keepalives trigger a fresh inbox scan."""

    def __init__(self, hub):
        self.hub = hub
        self.dirty = threading.Event()
        self.dirty.set()
        self.stopped = threading.Event()
        self.connections = 0
        self.response = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        delay = 0.1
        while not self.stopped.is_set():
            try:
                request = Request(self.hub.base + "/events", headers={"Accept": "text/event-stream"})
                with urlopen(request, timeout=5) as response:
                    self.response = response
                    self.connections += 1
                    self.dirty.set()
                    delay = 0.1
                    for line in response:
                        if self.stopped.is_set():
                            return
                        if line.startswith(b"data:") or line.startswith(b":"):
                            self.dirty.set()
            except (OSError, HTTPException, ValueError):
                # Timeout, dropped connection, or Hub restart: inbox survives independently.
                self.dirty.set()
            finally:
                self.response = None
            if self.stopped.wait(delay):
                return
            delay = min(delay * 2, 5)

    def close(self):
        self.stopped.set()
        # Keepalive/timeout lets the reader exit without blocking the runtime owner.
        self.thread.join(timeout=6)


class Journal(Outbox):
    """Persist runtime receipts before ACK, so ACK retry never starts a second turn."""

    def __init__(self, path):
        super().__init__(path)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS identity (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                session_id TEXT NOT NULL, thread_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deliveries (
                message_id TEXT PRIMARY KEY, turn_id TEXT NOT NULL UNIQUE,
                message TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0,
                completed INTEGER NOT NULL DEFAULT 0
            );
        """)

    def identity(self):
        return self.db.execute("SELECT session_id, thread_id FROM identity").fetchone()

    def bind(self, session_id, thread_id):
        existing = self.identity()
        if existing and existing != (session_id, thread_id):
            raise ValueError("State file belongs to a different Hub session or Codex thread")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO identity VALUES (1, ?, ?)", (session_id, thread_id))

    def accepted(self, message, turn_id):
        with self.db:
            self.db.execute("INSERT INTO deliveries(message_id, turn_id, message) VALUES (?, ?, ?)",
                            (message["message_id"], turn_id, json.dumps(message, ensure_ascii=False)))

    def receipt(self, message_id):
        return self.db.execute("SELECT turn_id FROM deliveries WHERE message_id = ?",
                               (message_id,)).fetchone()

    def unacked(self):
        return [row[0] for row in self.db.execute(
            "SELECT message_id FROM deliveries WHERE acknowledged = 0 ORDER BY rowid")]

    def mark_acked(self, message_id):
        with self.db:
            self.db.execute("UPDATE deliveries SET acknowledged = 1 WHERE message_id = ?", (message_id,))

    def unfinished(self, turn_id):
        row = self.db.execute("SELECT message FROM deliveries WHERE turn_id = ? AND completed = 0",
                              (turn_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def mark_completed(self, turn_id):
        with self.db:
            self.db.execute("UPDATE deliveries SET completed = 1 WHERE turn_id = ?", (turn_id,))


class CodexSession:
    def __init__(self, app, thread_id, hub, journal, *, report_results=False, emit=None):
        self.app, self.thread_id, self.hub, self.journal = app, thread_id, hub, journal
        self.report_results = report_results
        self.emit = emit or (lambda event: print(json.dumps(event, ensure_ascii=False), flush=True))
        self.collector = TurnCollector(thread_id)
        self.active_turn = None
        self.terminals = {}
        self.signals = None
        self.next_poll = 0
        self.app.on_notification = self.observe

    @classmethod
    def connect(cls, codex, url, session_id, journal, cwd, *, report_results=False, emit=None):
        identity = journal.identity()
        if identity and identity[0] != session_id:
            raise ValueError("Use a separate state file for each Hub session")
        app = AppServer(codex, url + "/mcp", command=[
            codex, "app-server", "--listen", "stdio://", "-c", "analytics.enabled=false",
            "-c", 'mcp_servers={agent_hub={url=' + json.dumps(url.rstrip("/") + "/mcp") + '}}',
        ])
        session = None
        try:
            initialize(app)
            params = {"cwd": str(Path(cwd).resolve()), "approvalPolicy": "never", "sandbox": "read-only"}
            if identity:
                params["threadId"] = identity[1]
            thread = app.request("thread/resume" if identity else "thread/start", params)["thread"]
            journal.bind(session_id, thread["id"])
            hub = HubClient(url, session_id)
            session = cls(app, thread["id"], hub, journal, report_results=report_results, emit=emit)
            # A restart can miss terminal events. Persisted runtime history supplies their contents.
            for turn in thread.get("turns", []):
                for item in turn.get("items", []):
                    session.collector.observe({"method": "item/completed", "params": {
                        "threadId": thread["id"], "turnId": turn["id"], "item": item,
                    }})
                if turn["status"] in {"completed", "failed", "interrupted"}:
                    session.observe({"method": "turn/completed", "params": {
                        "threadId": thread["id"], "turn": turn,
                    }})
                elif thread.get("status", {}).get("type") == "active":
                    session.active_turn = turn["id"]
            journal.enqueue("register:" + thread["id"], hub.url + "/sessions", {
                "session_id": session_id, "runtime_id": thread["id"], "display_name": session_id,
            })
            journal.flush()
            session.signals = InboxSignals(hub)
            session.emit({"type": "session_ready", "session_id": session_id,
                          "thread_id": thread["id"], "resumed": bool(identity)})
            return session
        except BaseException:
            if session:
                session.close()
            else:
                app.close()
            raise

    def observe(self, notification):
        self.collector.observe(notification)
        params = notification.get("params", {})
        if params.get("threadId") != self.thread_id:
            return
        method = notification["method"]
        if method == "turn/started":
            self.active_turn = params["turn"]["id"]
        elif method == "turn/completed":
            turn_id = params["turn"]["id"]
            self.terminals[turn_id] = params
            if self.active_turn == turn_id:
                self.active_turn = None
            self.finish(params)

    def finish(self, terminal):
        turn_id = terminal["turn"]["id"]
        message = self.journal.unfinished(turn_id)
        result = self.collector.result(terminal, self.hub.session_id,
                                       message["sender_id"] if message else self.hub.session_id,
                                       (message.get("correlation_id") or message["message_id"]) if message else turn_id)
        if message:
            if self.report_results:
                key = "result:" + json.dumps([self.hub.session_id, self.thread_id, turn_id])
                self.journal.enqueue(key, self.hub.url + "/turn-results", result)
            self.journal.mark_completed(turn_id)
        self.emit({"type": "turn_completed", "thread_id": self.thread_id, "turn_id": turn_id,
                   "status": result["status"], "final_text": result["final_text"]})

    def start_text(self, text):
        if self.active_turn:
            raise RuntimeError("The existing session is busy")
        turn = self.app.request("turn/start", {"threadId": self.thread_id,
                                "input": [{"type": "text", "text": text}]})["turn"]
        turn_id = turn["id"]
        if turn_id not in self.terminals:
            self.active_turn = turn_id
        return turn_id

    def deliver(self, message):
        receipt = self.journal.receipt(message["message_id"])
        if not receipt:
            # Include IDs and correlation in the model's context, rather than just the payload.
            text = "Incoming Agent Hub message:\n" + json.dumps(message, ensure_ascii=False)
            turn_id = self.start_text(text)
            # There is an unavoidable crash window between runtime acceptance and this commit.
            self.journal.accepted(message, turn_id)
            if turn_id in self.terminals:
                self.finish(self.terminals[turn_id])
            self.emit({"type": "message_accepted", "message_id": message["message_id"],
                       "thread_id": self.thread_id, "turn_id": turn_id})
        self.hub.ack(message["message_id"])
        self.journal.mark_acked(message["message_id"])

    def tick(self):
        # Consume runtime events while the SSE reader runs separately. All SQLite/RPC writes
        # remain on this owner thread, including callbacks observed during a request.
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
            # Retry the saved receipt even while its accepted turn is still running.
            for message_id in self.journal.unacked():
                self.hub.ack(message_id)
                self.journal.mark_acked(message_id)
            if not self.active_turn:
                for message in self.hub.inbox():
                    self.deliver(message)
                    if self.active_turn:
                        break
        except (OSError, HTTPException, json.JSONDecodeError) as error:
            self.emit({"type": "hub_retry", "error": str(error)})

    def close(self):
        if self.signals:
            self.signals.close()
        self.app.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:7331")
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--state", required=True, help="One SQLite state file per session; reuse on restart")
    parser.add_argument("--cwd", default=".")
    parser.add_argument("--report-results", action="store_true", help="Reply to each input's sender on turn completion")
    parser.add_argument("prompt", nargs="?", help="Initial input, used only for a newly created thread")
    args = parser.parse_args()
    if not args.session_id.strip():
        parser.error("session-id must not be blank")
    state = Path(args.state)
    state.parent.mkdir(parents=True, exist_ok=True)
    # A second owner could submit the same inbox message before either receipt is saved.
    with Path(str(state) + ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("Another adapter owns this state file")
        journal = Journal(state)
        try:
            while True:
                session = None
                try:
                    fresh = journal.identity() is None
                    session = CodexSession.connect(args.codex, args.url, args.session_id, journal,
                                                   args.cwd, report_results=args.report_results)
                    if fresh and args.prompt:
                        session.start_text(args.prompt)
                    while True:
                        session.tick()
                except (OSError, HTTPException, RuntimeError, json.JSONDecodeError) as error:
                    print(f"Adapter reconnecting: {error}", file=sys.stderr, flush=True)
                finally:
                    if session:
                        session.close()
                time.sleep(1)
        except KeyboardInterrupt:
            return 0
        finally:
            journal.close()


if __name__ == "__main__":
    sys.exit(main())
