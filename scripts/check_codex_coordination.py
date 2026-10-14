#!/usr/bin/env python3
"""Probe natural Hub coordination in fresh real CLIs, including native subagents.

Uses a temporary Hub, CODEX_HOME and read-only fixture projects. Costs real Codex
turns. Never installs hooks/skills in the user's home or touches existing threads.
"""

import argparse
import json
import os
import signal
import subprocess
import tempfile
import time
import tomllib
from pathlib import Path
from types import SimpleNamespace

from check_codex import AppServer
from check_codex_delivery import Hub
from check_codex_hooks import Terminal, stop_demo_daemon
from codex_daemon import DaemonClient
from codex_hooks import CliJournal, install, uninstall
from codex_worker import initialize


VISIBLE_ITEMS = {"userMessage", "agentMessage", "mcpToolCall", "collabAgentToolCall", "commandExecution", "fileChange"}


def visible_turn(turn):
    return {**turn, "items": [item for item in turn.get("items", []) if item["type"] in VISIBLE_ITEMS]}


def final_text(turn):
    return "\n".join(i["text"] for i in turn.get("items", [])
                     if i["type"] == "agentMessage" and i.get("phase") in {None, "final_answer"})


def answer_matches(turn, expected):
    if turn["status"] != "completed":
        return False
    text = final_text(turn).strip()
    if text.startswith("```") and text.endswith("```"):
        text = text.partition("\n")[2][:-3].strip()
    try:
        return json.loads(text) == expected
    except json.JSONDecodeError:
        return False


def summarize(capture):
    calls, automatic, latencies = {}, {}, []
    started, completed = {}, {}
    for event in capture["trace"]:
        method, params = event.get("method"), event.get("params", {})
        if method in {"turn/started", "turn/completed"}:
            key = (params["threadId"], params["turn"]["id"])
            (started if method == "turn/started" else completed)[key] = event["seconds"]
    for thread, snapshot in capture["threads"].items():
        role = snapshot["role"]
        calls[role], automatic[role] = [], 0
        for turn in snapshot["turns"]:
            for item in turn["items"]:
                if item["type"] == "mcpToolCall":
                    calls[role].append({"tool": item["tool"], "arguments": item["arguments"],
                                        "status": item["status"], "error": item.get("error")})
                elif item["type"] == "userMessage":
                    for part in item["content"]:
                        prefix = "Incoming Agent Hub message:\n"
                        if part["type"] != "text" or not part["text"].startswith(prefix):
                            continue
                        message = json.loads(part["text"][len(prefix):])
                        payload = message["payload"]
                        if isinstance(payload, dict) and payload.get("type") == "turn_completed":
                            automatic[role] += 1
                            origin = completed.get((message["sender_id"], payload["turn_id"]))
                            delivery = started.get((thread, turn["id"]))
                            if origin is not None and delivery is not None:
                                latencies.append({"sender": message["sender_id"], "recipient": thread,
                                                  "seconds": round(delivery - origin, 3)})
    return {"mcp_calls": calls, "automatic_result_inputs": automatic,
            "completion_to_recipient_turn_started": latencies,
            "native_subagents": sum(bool(t["parent_thread_id"]) for t in capture["threads"].values()),
            "runtime_turns": sum(len(t["turns"]) for t in capture["threads"].values())}


class Experiment:
    def __init__(self, codex, root, *, skill=None, require_automatic=False):
        self.codex, self.root = codex, root
        self.home, self.state = root / "home", root / "state"
        self.home.mkdir()
        source = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
        if (source / "auth.json").exists():
            (self.home / "auth.json").symlink_to(source / "auth.json")
        original = tomllib.loads((source / "config.toml").read_text()) if (source / "config.toml").exists() else {}
        self.config = self.home / "config.toml"
        self.config.write_text("analytics.enabled=false\n"
                               + ("model=" + json.dumps(original["model"]) + "\n" if original.get("model") else "")
                               + "\n[agents]\nmax_concurrent_threads_per_session=1\n"
                               + "\n[projects." + json.dumps(str(root)) + "]\ntrust_level=\"trusted\"\n")
        self.env = {**os.environ, "CODEX_HOME": str(self.home), "TERM": "xterm-256color"}
        self.hub = Hub(Path(__file__).resolve().parents[1] / "target/debug/agent-hub", root)
        self.args = SimpleNamespace(codex_home=str(self.home), state_dir=str(self.state), url=self.hub.url, socket=None)
        self.terminals, self.registered, self.app = {}, {}, None
        self.started = time.monotonic()
        self.trace, self.disabled_skills = [], []
        self.skill = skill
        self.require_automatic = require_automatic

    def __enter__(self):
        try:
            self.hub.start()
            install(self.args)
            self.configure_hooks()
            trust = AppServer(self.codex, None, command=[self.codex, "app-server", "--listen", "stdio://"], environment=self.env)
            try:
                initialize(trust)
                skills = trust.request("skills/list", {"cwds": [str(self.root)], "forceReload": True})
                # CODEX_HOME alone does not hide ~/.agents/skills. Disable those in
                # the temporary config so personal coordination skills cannot help.
                for entry in skills["data"]:
                    for item in entry["skills"]:
                        if item["scope"] != "system" and item["enabled"]:
                            trust.request("skills/config/write", {"path": item["path"], "enabled": False})
                            self.disabled_skills.append(item["name"])
                listing = trust.request("hooks/list", {"cwds": [str(self.root)]})
                with self.config.open("a") as file:
                    for hook in listing["data"][0]["hooks"]:
                        assert "scripts/codex_hooks.py" in hook["command"]
                        file.write("\n[hooks.state." + json.dumps(hook["key"]) + "]\ntrusted_hash="
                                   + json.dumps(hook["currentHash"]) + "\n")
            finally:
                trust.close()
            if self.skill:
                directory = self.home / "skills" / "agent-hub-coordination"
                directory.mkdir(parents=True)
                (directory / "SKILL.md").write_text(Path(self.skill).read_text())
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def configure_hooks(self):
        """Optional isolated fixture barriers, installed before trusting test-owned hooks."""


    def launch(self, role, fixture=None):
        cwd = self.root / role
        cwd.mkdir()
        if fixture is not None:
            (cwd / "data.json").write_text(json.dumps(fixture) + "\n")
        self.terminals[role] = Terminal(self.codex, self.env, cwd, "Reply READY only. Do not use tools.")

    def registrations(self):
        registrations = {}
        for path in self.state.glob("*.sqlite3"):
            journal = CliJournal(path)
            try:
                entry = journal.registration()
                if entry:
                    registrations[entry["display_name"]] = entry["session_id"]
            finally:
                journal.close()
        return registrations

    def wait(self, predicate, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for terminal in self.terminals.values():
                terminal.drain()
            result = predicate()
            if result:
                return result
            if self.app:
                try:
                    self.app.receive(0.05)
                except TimeoutError:
                    pass
                self.app.notifications.clear()
            else:
                # Bootstrap has no runtime connection yet; only wait for hooks.
                time.sleep(0.05)
        raise TimeoutError("Coordination scenario exceeded its time budget")

    def ready(self):
        self.registered = self.wait(lambda: self.registrations()
                                    if len(self.registrations()) == len(self.terminals) else None, timeout=90)
        self.app = DaemonClient(self.home / "app-server-control/app-server-control.sock", on_notification=self.observe)
        initialize(self.app)
        for thread in self.registered.values():
            self.app.request("thread/resume", {"threadId": thread})
        self.wait(lambda: all(any(t["status"] == "completed" and final_text(t) == "READY"
                                 for t in self.turns(thread)) for thread in self.registered.values()), timeout=90)
        print("READY: " + ", ".join(self.registered), flush=True)

    def observe(self, notification):
        params = notification.get("params", {})
        method = notification["method"]
        if method in {"turn/started", "turn/completed", "thread/started", "thread/tokenUsage/updated"}:
            if "turn" in params:
                params = {**params, "turn": visible_turn(params["turn"])}
            self.trace.append({"seconds": round(time.monotonic() - self.started, 3),
                               "method": method, "params": params})
        elif method == "item/completed" and params["item"]["type"] in {"mcpToolCall", "collabAgentToolCall"}:
            self.trace.append({"seconds": round(time.monotonic() - self.started, 3),
                               "method": method, "params": params})

    def turns(self, thread):
        return self.app.request("thread/read", {"threadId": thread, "includeTurns": True})["thread"]["turns"]

    def prompt(self, role, text):
        self.trace.append({"seconds": round(time.monotonic() - self.started, 3), "prompt_role": role, "text": text})
        self.terminals[role].prompt(text)

    def capture(self):
        threads = dict(self.registered)
        captured = {}
        # Some runtime builds persist wait calls but omit spawn calls from parent
        # turn.items. Use owned temporary session metadata for discovery only;
        # completion and answers still come from runtime events/history.
        parents = {}
        for path in (self.home / "sessions").rglob("*.jsonl"):
            with path.open() as file:
                first = json.loads(file.readline())
            if first["type"] == "session_meta":
                meta = first["payload"]
                source = meta.get("source")
                if isinstance(source, dict):
                    spawn = source.get("subagent", {}).get("thread_spawn", {})
                    if spawn.get("parent_thread_id") in self.registered.values():
                        parents[meta["id"]] = spawn["parent_thread_id"]
                        threads["native-subagent:" + meta["id"]] = meta["id"]
        while threads:
            role, thread = threads.popitem()
            if thread in captured:
                continue
            snapshot = self.app.request("thread/read", {"threadId": thread, "includeTurns": True})["thread"]
            captured[thread] = {"role": role, "parent_thread_id": snapshot.get("parentThreadId") or parents.get(thread),
                                "metadata_parent_thread_id": parents.get(thread),
                                "source": snapshot.get("source"), "turns": [visible_turn(t) for t in snapshot["turns"]]}
            for turn in snapshot["turns"]:
                for item in turn["items"]:
                    if item["type"] == "collabAgentToolCall" and item["tool"] == "spawnAgent":
                        for child in item["receiverThreadIds"]:
                            threads[role + "/subagent"] = child
        bridges = {}
        for role, thread in self.registered.items():
            path = self.state / (thread + ".jsonl")
            bridges[role] = [json.loads(line) for line in path.read_text().splitlines() if line.startswith("{")]
        return {"registered": self.registrations(), "threads": captured, "trace": self.trace,
                "bridge_events": bridges, "disabled_personal_skills": self.disabled_skills,
                "extra_skill": bool(self.skill),
                "processes": {"hub": self.hub.process.pid, "cli": {r: t.pid for r, t in self.terminals.items()}}}

    def __exit__(self, *_):
        try:
            if self.app:
                for thread in self.app.request("thread/loaded/list", {}, timeout=5)["data"]:
                    try:
                        snapshot = self.app.request("thread/read", {"threadId": thread, "includeTurns": True}, timeout=5)["thread"]
                        for turn in snapshot.get("turns", []):
                            if turn["status"] == "inProgress":
                                self.app.request("turn/interrupt", {"threadId": thread, "turnId": turn["id"]}, timeout=5)
                        self.app.request("thread/archive", {"threadId": thread}, timeout=5)
                    except (RuntimeError, TimeoutError):
                        pass
        finally:
            try:
                uninstall(self.args)
            finally:
                if self.app:
                    self.app.close()
                try:
                    stop_demo_daemon(self.home)
                finally:
                    try:
                        for terminal in self.terminals.values():
                            try:
                                terminal.close()
                            except (TimeoutError, ChildProcessError):
                                # SIGKILL was already sent by Terminal.close; keep
                                # cleaning up the other owned processes.
                                pass
                    finally:
                        self.hub.stop()


def multi_worker(experiment):
    experiment.launch("worker-a", [{"quantity": 11}, {"quantity": 7}, {"quantity": 5}])
    experiment.launch("worker-b", [{"quantity": 3, "price": 19}, {"quantity": 4, "price": 13}])
    experiment.launch("orchestrator")
    experiment.ready()
    prompt = ("For this task, remember 37. Find the already open worker-a and worker-b sessions through Agent Hub "
              "and give them two independent tasks. Worker-a: read data.json in its directory, sum quantity, "
              "and answer with the number. Worker-b: have exactly one native Codex subagent read its data.json "
              "and sum quantity * price; wait for its response and answer with the number. The subagent must not "
              "create any other subagents. Do not read the workers' files or calculate their results yourself. "
              "Once both results arrive, output JSON with fields a, b, and total (37+a+b). "
              "Submitting a task is not a result: provide the final answer after both workers respond.")
    if experiment.skill:
        prompt = "$agent-hub-coordination " + prompt
    experiment.prompt("orchestrator", prompt)
    expected = {"a": 23, "b": 109, "total": 169}
    def answered():
        return any(answer_matches(t, expected) for t in experiment.turns(experiment.registered["orchestrator"]))
    experiment.wait(answered, timeout=210)
    capture = experiment.capture()
    assert any(t["parent_thread_id"] == experiment.registered["worker-b"] for t in capture["threads"].values()), "No native subagent ran"
    if experiment.skill or experiment.require_automatic:
        summary = summarize(capture)
        assert summary["automatic_result_inputs"]["orchestrator"] == 2, "Results did not arrive as automatic inputs"
        assert not summary["mcp_calls"]["worker-a"] and not summary["mcp_calls"]["worker-b"], "Workers manually replied"
        assert not any(c["tool"] == "hub_read_inbox" for c in summary["mcp_calls"]["orchestrator"]), "Orchestrator polled inbox"
    print("PASS: two worker replies and native subagent -> " + json.dumps(expected), flush=True)
    return capture


def subagent_sender(experiment):
    experiment.launch("worker-direct", [{"quantity": 13}, {"quantity": 17}])
    experiment.launch("orchestrator")
    experiment.ready()
    experiment.prompt("orchestrator", "Remember 7. Create exactly one native Codex subagent with clean context "
                      "(fork_turns=none). Have it find the already open worker-direct session through Agent Hub "
                      "and send a task: read data.json in that worker's directory and answer with the sum of quantity. "
                      "The subagent submits the task and returns acceptance details to you; "
                      "ask it not to create other subagents. Do not submit the task or read the worker's file yourself. "
                      "When worker-direct's result arrives, output JSON with fields worker and total (7+worker).")
    def answered():
        return any(answer_matches(t, {"worker": 30, "total": 37})
                   for t in experiment.turns(experiment.registered["orchestrator"]))
    experiment.wait(answered, timeout=150)
    capture = experiment.capture()
    calls = summarize(capture)["mcp_calls"]
    child_calls = [c for role, entries in calls.items() if role.startswith("native-subagent:") for c in entries
                   if c["tool"] == "hub_send_message"]
    assert child_calls and any(c["arguments"]["sender_id"] == experiment.registered["orchestrator"]
                              and c["status"] == "completed" for c in child_calls), "Native child did not send on behalf of its parent"
    print("PASS: clean native subagent sent a Hub task on behalf of parent -> 37.", flush=True)
    return capture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--output", required=True)
    parser.add_argument("--skill", help="Optional SKILL.md installed only into the temporary CODEX_HOME")
    parser.add_argument("--scenario", choices=["multi-worker", "subagent-sender"], default="multi-worker")
    parser.add_argument("--require-automatic", action="store_true", help="Also require automatic results and no manual replies/polling")
    parser.add_argument("--max-seconds", type=int, default=360)
    args = parser.parse_args()
    if args.max_seconds <= 0:
        parser.error("max-seconds must be positive")
    def timeout(*_):
        raise TimeoutError("Coordination check exceeded its overall time budget")
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(args.max_seconds)
    repo = Path(__file__).resolve().parents[1]
    subprocess.run(["cargo", "build", "--bin", "agent-hub"], cwd=repo, check=True)
    evidence = {"version": subprocess.check_output([args.codex, "--version"], text=True).strip(),
                "scenario": args.scenario, "extra_skill": bool(args.skill)}
    with tempfile.TemporaryDirectory(prefix="hub-coord-", dir="/tmp") as directory:
        with Experiment(args.codex, Path(directory), skill=args.skill, require_automatic=args.require_automatic) as experiment:
            try:
                scenario = multi_worker if args.scenario == "multi-worker" else subagent_sender
                evidence["evidence"] = scenario(experiment)
                evidence["summary"] = summarize(evidence["evidence"])
                evidence["status"] = "passed"
            except Exception as error:
                evidence["status"], evidence["error"] = "failed", str(error)
                if experiment.app:
                    evidence["evidence"] = experiment.capture()
                    evidence["summary"] = summarize(evidence["evidence"])
                raise
            finally:
                Path(args.output).write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")
    print("Evidence: " + args.output, flush=True)


if __name__ == "__main__":
    main()
