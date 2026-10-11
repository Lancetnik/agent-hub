#!/usr/bin/env python3
"""Externally launch one Codex turn and deliver its terminal result to Agent Hub."""

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from check_codex import AppServer


class Outbox:
    """Commit before sending; a lost HTTP response is safe to retry with the same body."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        self.db.execute("""CREATE TABLE IF NOT EXISTS pending (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT NOT NULL UNIQUE, url TEXT NOT NULL, body TEXT NOT NULL
        )""")
        self.db.commit()

    def enqueue(self, key, url, body):
        encoded = json.dumps(body, sort_keys=True, ensure_ascii=False)
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            existing = self.db.execute(
                "SELECT url, body FROM pending WHERE key = ?", (key,)
            ).fetchone()
            if existing and existing != (url, encoded):
                raise ValueError("Outbox key already contains different data")
            self.db.execute(
                "INSERT OR IGNORE INTO pending(key, url, body) VALUES (?, ?, ?)",
                (key, url, encoded),
            )

    def flush(self, timeout=10):
        for sequence, url, body in self.db.execute(
            "SELECT sequence, url, body FROM pending ORDER BY sequence"
        ).fetchall():
            request = Request(url, data=body.encode(), headers={"Content-Type": "application/json"})
            with urlopen(request, timeout=timeout) as response:
                # Consume the response before forgetting the durable event.
                json.load(response)
            with self.db:
                self.db.execute("DELETE FROM pending WHERE sequence = ?", (sequence,))

    def close(self):
        self.db.close()


class TurnCollector:
    """Use completed items for text, and only a terminal event for completion."""

    def __init__(self, thread_id):
        self.thread_id = thread_id
        self.items = {}

    def observe(self, notification):
        if notification["method"] != "item/completed":
            return
        params = notification["params"]
        if params["threadId"] != self.thread_id:
            return
        items = self.items.setdefault(params["turnId"], {})
        item = params["item"]
        items[item["id"]] = item

    def result(self, terminal, session_id, recipient_id, correlation_id):
        if terminal["threadId"] != self.thread_id:
            raise ValueError("Completion belongs to another thread")
        turn = terminal["turn"]
        if turn["status"] not in {"completed", "failed", "interrupted"}:
            raise ValueError("Expected a terminal runtime status")
        items = list(self.items.get(turn["id"], {}).values())
        texts = [i["text"] for i in items if i["type"] == "agentMessage"
                 and i.get("phase") in {"final_answer", None}]
        artifacts = sorted({change["path"] for i in items if i["type"] == "fileChange"
                            for change in i["changes"]})
        return {
            "session_id": session_id, "runtime_id": self.thread_id, "turn_id": turn["id"],
            "recipient_id": recipient_id, "correlation_id": correlation_id,
            "status": turn["status"], "final_text": texts[-1] if texts else None,
            "artifact_refs": artifacts, "error": turn.get("error"),
        }


def initialize(app):
    app.request("initialize", {
        "clientInfo": {"name": "agent_hub_worker", "version": "0.1.0"},
        "capabilities": {"experimentalApi": True},
    })
    app.send({"method": "initialized", "params": {}})


def run_worker(args, outbox):
    # The user invokes this external integration; the Hub server owns no runtime process.
    app = AppServer(args.codex, args.url.rstrip("/") + "/mcp", command=[
        args.codex, "app-server", "--listen", "stdio://",
        "-c", "analytics.enabled=false",
        "-c", 'mcp_servers={agent_hub={url=' + json.dumps(args.url.rstrip("/") + "/mcp") + '}}',
    ])
    try:
        initialize(app)
        thread = app.request("thread/start", {
            "cwd": str(Path(args.cwd).resolve()), "ephemeral": True,
            "approvalPolicy": "never", "sandbox": "read-only",
        })["thread"]["id"]
        registration = {"session_id": args.session_id, "runtime_id": thread,
                        "display_name": args.display_name or args.session_id}
        outbox.enqueue("register:" + thread, args.url.rstrip("/") + "/sessions", registration)
        try:
            outbox.flush()
        except (URLError, TimeoutError) as error:
            print(f"Registration queued in {outbox.path}: {error}", file=sys.stderr)
        collector = TurnCollector(thread)
        app.on_notification = collector.observe
        turn = app.request("turn/start", {
            "threadId": thread, "input": [{"type": "text", "text": args.prompt}],
        })["turn"]["id"]
        try:
            terminal = app.wait_notification(
                "turn/completed", lambda p: p["threadId"] == thread and p["turn"]["id"] == turn,
                timeout=args.timeout,
            )
        except (KeyboardInterrupt, TimeoutError):
            app.request("turn/interrupt", {"threadId": thread, "turnId": turn}, timeout=10)
            terminal = app.wait_notification(
                "turn/completed", lambda p: p["threadId"] == thread and p["turn"]["id"] == turn,
                timeout=10,
            )
        event = collector.result(terminal, args.session_id, args.recipient_id, args.correlation_id)
        key = "result:" + json.dumps([args.session_id, thread, turn])
        outbox.enqueue(key, args.url.rstrip("/") + "/turn-results", event)
        # Also write the exact event to stdout so the user can inspect or replay it.
        print(json.dumps(event, ensure_ascii=False), flush=True)
        outbox.flush()
        return 0 if event["status"] == "completed" else 1
    finally:
        app.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:7331")
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--session-id")
    parser.add_argument("--recipient-id")
    parser.add_argument("--correlation-id")
    parser.add_argument("--display-name")
    parser.add_argument("--cwd", default=".")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--outbox", default=".agent-hub/outbox.sqlite3")
    parser.add_argument("--retry", action="store_true", help="Only retry previously queued HTTP events")
    parser.add_argument("prompt", nargs="?")
    args = parser.parse_args()
    if not args.retry:
        for name in ["session_id", "recipient_id", "correlation_id", "prompt"]:
            if not getattr(args, name) or not getattr(args, name).strip():
                parser.error(f"{name} is required and must not be blank")
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    outbox = Outbox(args.outbox)
    try:
        if args.retry:
            outbox.flush()
            return 0
        return run_worker(args, outbox)
    except (OSError, RuntimeError, ValueError, TimeoutError) as error:
        if isinstance(error, HTTPError):
            detail = error.read().decode(errors="replace")
        else:
            detail = str(error)
        print(f"Worker integration failed: {detail}\nPending HTTP events: {outbox.path}\n"
              f"Retry: python3 scripts/codex_worker.py --outbox {outbox.path} --retry", file=sys.stderr)
        return 1
    finally:
        outbox.close()


if __name__ == "__main__":
    sys.exit(main())
