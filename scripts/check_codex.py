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


class AppServer:
    def __init__(self, codex, url):
        self.next_id = 0
        self.messages = queue.Queue()
        self.stderr = tempfile.TemporaryFile(mode="w+t")
        try:
            self.process = subprocess.Popen(
                [
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
            try:
                message = self.messages.get(timeout=remaining)
            except queue.Empty as error:
                raise TimeoutError(f"Timed out waiting for {method}") from error
            if isinstance(message, Exception):
                raise message
            if "method" in message:
                if "id" in message:
                    # This probe never needs approvals, user input, or elicitation.
                    self.send(
                        {
                            "id": message["id"],
                            "error": {"code": -32601, "message": "Unsupported in MCP probe"},
                        }
                    )
                continue
            if message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(f"{method}: {json.dumps(message['error'])}")
                return message["result"]

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


def check(codex, url):
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
    finally:
        app.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:7331/mcp")
    parser.add_argument("--codex", default="codex", help="Path to the Codex CLI executable")
    args = parser.parse_args()
    try:
        check(args.codex, args.url)
    except (OSError, RuntimeError, TimeoutError, KeyError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
