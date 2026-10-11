#!/usr/bin/env python3
"""Verify MCP discovery and a tool call through Codex, without running an LLM turn."""

import argparse
import json
import queue
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, urlopen


class AppServer:
    def __init__(self, codex, url, *, command=None, environment=None, on_notification=None):
        self.next_id = 0
        self.messages = queue.Queue()
        self.notifications = []
        self.on_notification = on_notification
        self.stderr = tempfile.TemporaryFile(mode="w+t")
        try:
            self.process = subprocess.Popen(
                command or [
                    codex,
                    "app-server",
                    "--listen",
                    "stdio://",
                    "-c",
                    "mcp_servers={agent_hub={url="
                    + json.dumps(url)
                    + ",required=true}}",
                    "-c",
                    "analytics.enabled=false",
                ],
                env=environment,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self.stderr,
                text=True,
                bufsize=1,
            )
        except OSError:
            self.stderr.close()
            raise
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.process.stdout:
            try:
                self.messages.put(json.loads(line))
            except json.JSONDecodeError:
                self.messages.put(RuntimeError("Codex emitted invalid JSON"))
                break
        self.messages.put(RuntimeError("Codex app-server closed its output"))

    def send(self, message):
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def request(self, method, params, timeout=40):
        self.next_id += 1
        request_id = self.next_id
        self.send({"id": request_id, "method": method, "params": params})
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Timed out waiting for {method}")
            message = self.receive(remaining)
            if "method" in message:
                continue
            if message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(f"{method}: {json.dumps(message['error'])}")
                return message["result"]

    def receive(self, timeout):
        try:
            message = self.messages.get(timeout=timeout)
        except queue.Empty as error:
            raise TimeoutError("Timed out waiting for Codex") from error
        if isinstance(message, Exception):
            raise message
        if "method" in message:
            if "id" in message:
                # These probes do not approve tools, input requests, or elicitation.
                self.send({"id": message["id"], "error": {
                    "code": -32601, "message": "Unsupported in Agent Hub integration",
                }})
            else:
                self.notifications.append(message)
                if self.on_notification:
                    self.on_notification(message)
        return message

    def wait_notification(self, method, predicate=lambda params: True, timeout=120):
        deadline = time.monotonic() + timeout
        while True:
            for index, message in enumerate(self.notifications):
                if message["method"] == method and predicate(message["params"]):
                    return self.notifications.pop(index)["params"]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Timed out waiting for {method}")
            self.receive(remaining)

    def close(self):
        if self.process.poll() is None:
            try:
                self.process.stdin.close()
            except BrokenPipeError:
                pass
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
        self.process.stdout.close()
        self.stderr.close()


def check_messaging(app, orchestrator_thread, url):
    worker = app.request(
        "thread/start",
        {
            "cwd": str(Path(__file__).resolve().parents[1]),
            "ephemeral": True,
            "approvalPolicy": "never",
            "sandbox": "read-only",
        },
    )
    worker_thread = worker["thread"]["id"]
    nonce = "codex-messages-" + uuid.uuid4().hex
    orchestrator_id, worker_id = nonce + "-orchestrator", nonce + "-worker"
    for session_id, thread_id in [
        (orchestrator_id, orchestrator_thread), (worker_id, worker_thread)
    ]:
        session = {"session_id": session_id, "runtime_id": thread_id, "display_name": session_id}
        request = Request(
            urljoin(url.rstrip("/") + "/", "../sessions"),
            data=json.dumps(session).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=10) as response:
            if json.load(response) != session:
                raise RuntimeError("Unexpected HTTP registration result")

    def call(thread_id, tool, arguments):
        result = app.request(
            "mcpServer/tool/call",
            {"threadId": thread_id, "server": "agent_hub", "tool": tool, "arguments": arguments},
        )
        if result.get("isError") or not isinstance(result.get("structuredContent"), dict):
            raise RuntimeError(f"{tool} failed: {json.dumps(result)}")
        return result["structuredContent"]

    arguments = {
        "message_id": nonce + "-request", "sender_id": orchestrator_id, "recipient_id": worker_id,
        "payload": {"text": "Посчитай 2 + 2"}, "correlation_id": nonce,
    }
    sent = call(orchestrator_thread, "hub_send_message", arguments)
    if call(orchestrator_thread, "hub_send_message", arguments) != sent:
        raise RuntimeError("Repeated send changed the message")
    expected = {"messages": [sent["message"]], "next_cursor": None}
    for _ in range(2):
        if call(worker_thread, "hub_read_inbox", {"session_id": worker_id}) != expected:
            raise RuntimeError("Worker inbox did not retain the pending request")
    ack_args = {"session_id": worker_id, "message_id": arguments["message_id"]}
    for _ in range(2):
        if call(worker_thread, "hub_ack_message", ack_args) != {**ack_args, "acknowledged": True}:
            raise RuntimeError("Worker ACK returned an unexpected result")
    if call(worker_thread, "hub_read_inbox", {"session_id": worker_id})["messages"]:
        raise RuntimeError("ACKed request remained in the inbox")
    reply = call(worker_thread, "hub_send_message", {
        "message_id": nonce + "-reply", "sender_id": worker_id, "recipient_id": orchestrator_id,
        "payload": {"result": 4}, "correlation_id": nonce,
    })
    if call(orchestrator_thread, "hub_read_inbox", {"session_id": orchestrator_id}) != {
        "messages": [reply["message"]], "next_cursor": None,
    }:
        raise RuntimeError("Orchestrator did not receive the correlated reply")
    print("PASS: two Codex threads used MCP send/read/ACK/reply; retries preserved the message and ACK.")


def check(codex, url, messaging=False):
    app = AppServer(codex, url)
    try:
        app.request(
            "initialize",
            {
                "clientInfo": {"name": "agent_hub_probe", "version": "0.1.0"},
                "capabilities": {"experimentalApi": True},
            },
        )
        app.send({"method": "initialized", "params": {}})
        thread = app.request(
            "thread/start",
            {
                "cwd": str(Path(__file__).resolve().parents[1]),
                "ephemeral": True,
                "approvalPolicy": "never",
                "sandbox": "read-only",
            },
        )
        thread_id = thread["thread"]["id"]
        inventory = app.request(
            "mcpServerStatus/list",
            {"threadId": thread_id, "serverName": "agent_hub", "detail": "toolsAndAuthOnly"},
        )
        servers = [server for server in inventory["data"] if server["name"] == "agent_hub"]
        if len(servers) != 1 or "hub_ping" not in servers[0]["tools"]:
            raise RuntimeError("Codex did not discover agent_hub.hub_ping")
        print("Codex discovered hub_ping over Streamable HTTP.", flush=True)

        nonce = "codex-probe-" + uuid.uuid4().hex
        result = app.request(
            "mcpServer/tool/call",
            {
                "threadId": thread_id,
                "server": "agent_hub",
                "tool": "hub_ping",
                "arguments": {"message": nonce},
            },
        )
        content = result.get("structuredContent")
        if result.get("isError") or not isinstance(content, dict):
            raise RuntimeError("hub_ping did not return a successful structured result")
        if content.get("message") != nonce or content.get("server") != "agent-hub":
            raise RuntimeError("hub_ping returned an unexpected response")
        print(json.dumps(content, ensure_ascii=False, indent=2))
        print("PASS: Codex called the Rust MCP tool. No LLM turn was started.")
        if messaging:
            check_messaging(app, thread_id, url)
    finally:
        app.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:7331/mcp")
    parser.add_argument("--codex", default="codex", help="Path to the Codex CLI executable")
    parser.add_argument("--messaging", action="store_true", help="Also check messaging through two temporary Codex threads")
    args = parser.parse_args()
    try:
        check(args.codex, args.url, args.messaging)
    except (OSError, RuntimeError, TimeoutError, KeyError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
