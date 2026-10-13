#!/usr/bin/env python3
"""Exercise TTL leases in two ordinary Codex CLIs in a disposable shared checkout.

Uses real Codex turns and quota, two CLIs at most, temporary hooks/config, and no
coordination Skill. Explicit lease cases use owner barriers; natural/peer-aware
cases let agents choose tools and edits freely, without any helper or barrier.
"""

import argparse
import json
import signal
import sqlite3
import subprocess
import tempfile
from pathlib import Path

from check_codex_coordination import final_text, summarize
from check_codex_file_conflict import EXPECTED, INITIAL, FileConflictExperiment
from codex_hooks import CliJournal
from codex_session import HubClient


GATE = '''import os, pathlib, select, sys
root = pathlib.Path(__file__).resolve().parent
role = sys.argv[1]
(root / (role + ".writer-pid")).write_text(str(os.getpid()))
fd = os.open(root / (role + ".write.fifo"), os.O_RDWR | os.O_NONBLOCK)
try:
    (root / (role + ".write.ready")).write_text("ready")
    print("WAITING:" + role, flush=True)
    if not select.select([fd], [], [], 150)[0]:
        raise TimeoutError("Lease test barrier timed out")
    if os.read(fd, 1) != b"1":
        raise RuntimeError("Invalid barrier release")
finally:
    os.close(fd)
print("BARRIER_RELEASED")
'''


def exercise(experiment, scenario):
    gate = experiment.project / "collision_writer.py"
    gate.write_text(GATE)
    for role in ["alpha", "beta"]:
        experiment.launch_shared(role)
    experiment.ready()
    ids = experiment.registered
    requests = {role: thread + ":lease-test" for role, thread in ids.items()}
    ttl = 30 if scenario == "expiry" else 60
    resource = str(experiment.target.resolve())
    clients = {r: HubClient(experiment.hub.url, t) for r, t in ids.items()}

    def acquire(role, seconds):
        return json.dumps({"session_id": ids[role], "request_id": requests[role],
                           "resource": resource, "ttl_seconds": seconds})

    def ticket(role):
        return clients[role].lock_status(requests[role])

    def done(role, text):
        return any(t["status"] == "completed" and final_text(t).strip() == text
                   for t in experiment.turns(ids[role]))

    common = ("This tests Agent Hub leases in a shared checkout. Coordinate using only hub_acquire_lock and "
              "hub_release_lock; send no messages and do not poll inbox. "
              "Leave fixture scripts, FIFOs, and ready files unchanged. ")
    owner = (common + f"First call hub_acquire_lock with exactly these arguments: {acquire('alpha', ttl)}. "
             f"If acquired, run python3 {gate} alpha and wait for it to finish. ")
    if scenario == "expiry":
        owner += (f"After the command, replay hub_acquire_lock with the same arguments: {acquire('alpha', ttl)}. "
                  "The fixture deliberately waits for your TTL to expire; expect expired. "
                  "Make no changes to settings.json and do not request a new lease. "
                  f"Call hub_release_lock with session_id={ids['alpha']} and request_id={requests['alpha']} "
                  "to test safe release of an old token. Finish with EXPIRED_STOPPED. "
                  "If lock_expired arrives later, reply EXPIRED_STOPPED without tools.")
    else:
        owner += (f"Replay hub_acquire_lock with the same arguments {acquire('alpha', ttl)} to confirm "
                  "acquired. Reread settings.json and set timeout_ms=2000, preserving other fields. "
                  "Verify the saved file. ")
        if scenario == "runtime-release":
            owner += "For this test, do NOT call hub_release_lock: the bridge will release at turn completion. "
        else:
            owner += (f"Release through hub_release_lock with session_id={ids['alpha']} "
                      f"and request_id={requests['alpha']}. ")
        owner += "Finish with LOCK_A_DONE."
    experiment.prompt("alpha", owner)
    experiment.wait(lambda: (experiment.project / "alpha.write.ready").exists(), timeout=90)
    assert ticket("alpha")["status"] == "acquired"
    experiment.checkpoint("alpha_holds_lease_and_is_active")

    waiter = (common + f"Call hub_acquire_lock with arguments {acquire('beta', 60)}. "
              "If waiting, finish this turn with WAITING_FOR_LOCK. Do not sleep or poll; "
              "lock_granted will resume you. On notification, replay the same acquire to confirm acquired. ")
    if scenario == "expiry":
        waiter += f"Then run python3 {gate} beta and wait for it to finish. "
    waiter += ("While acquired, reread current settings.json and set max_retries=3, preserving other fields. "
               "Verify the saved file. "
               f"Release through hub_release_lock with session_id={ids['beta']} "
               f"and request_id={requests['beta']}. Finish with LOCK_B_DONE. "
               "Lease notifications need no messages to other participants.")
    experiment.prompt("beta", waiter)
    experiment.wait(lambda: (ticket("beta") or {}).get("status") == "waiting"
                    and done("beta", "WAITING_FOR_LOCK"), timeout=60)
    experiment.checkpoint("beta_waiting_turn_ended")
    if scenario == "expiry":
        # No lock status/acquire/release calls while waiting for TTL. The Hub's
        # deadline scheduler, SSE and CLI bridge must do the handoff themselves.
        experiment.wait(lambda: (experiment.project / "beta.write.ready").exists(), timeout=60)
        assert ticket("alpha")["status"] == "expired"
        assert ticket("beta")["status"] == "acquired"
        assert experiment.app.request("thread/read", {"threadId": ids["alpha"]})["thread"]["status"]["type"] == "active"
        experiment.checkpoint("ttl_handoff_while_old_owner_is_active")
        experiment.release("alpha", "write")
        experiment.wait(lambda: done("alpha", "EXPIRED_STOPPED"), timeout=60)
        assert ticket("beta")["status"] == "acquired", "Old owner released the next owner's lease"
        assert json.loads(experiment.target.read_text()) == INITIAL, "Expired owner wrote the file"
        experiment.checkpoint("stale_release_keeps_beta_lease")
        experiment.release("beta", "write")
    else:
        experiment.release("alpha", "write")

    def settled():
        if not done("alpha", "EXPIRED_STOPPED" if scenario == "expiry" else "LOCK_A_DONE") or not done("beta", "LOCK_B_DONE"):
            return False
        return (experiment.idle() and all(not list(client.inbox()) for client in clients.values())
                and all(not experiment.app.request("thread/queue/list", {"threadId": t})["data"] for t in ids.values()))

    experiment.wait(settled, timeout=90)
    experiment.wait(lambda: all(ticket(r)["status"] in {"released", "expired"} for r in ids), timeout=10)
    experiment.checkpoint("settled")
    capture = experiment.capture()
    capture["checkpoints"] = experiment.checkpoints
    capture["final_settings"] = json.loads(experiment.target.read_text())
    capture["tickets"] = {r: ticket(r) for r in ids}
    capture["summary"] = summary = summarize(capture)
    capture["token_usage"] = {}
    for event in capture["trace"]:
        if event.get("method") == "thread/tokenUsage/updated":
            capture["token_usage"][event["params"]["threadId"]] = event["params"]["tokenUsage"]
    notices = {r: [] for r in ids}
    for thread in capture["threads"].values():
        for turn in thread["turns"]:
            for item in turn["items"]:
                if item["type"] != "userMessage":
                    continue
                for part in item["content"]:
                    prefix = "Incoming Agent Hub message:\n"
                    if part["type"] == "text" and part["text"].startswith(prefix):
                        notices[thread["role"]].append(json.loads(part["text"][len(prefix):])["payload"]["type"])
    capture["notifications"] = notices
    assert notices["beta"] == ["lock_granted"], notices
    assert notices["alpha"] == (["lock_expired"] if scenario == "expiry" else []), notices
    assert not any(summary["automatic_result_inputs"].values()), "Notification generated an automatic result reply"
    assert all(c["tool"] in {"hub_acquire_lock", "hub_release_lock"} and not c["error"]
               for calls in summary["mcp_calls"].values() for c in calls)
    if scenario == "runtime-release":
        assert not any(c["tool"] == "hub_release_lock" for c in summary["mcp_calls"]["alpha"])
        assert capture["tickets"]["alpha"]["status"] == "released", "TTL expiry masked missing runtime release"
    expected = {**INITIAL, "max_retries": 3} if scenario == "expiry" else EXPECTED
    assert capture["final_settings"] == expected
    assert gate.read_text() == GATE
    print("RESULT: " + scenario + ", file=" + json.dumps(expected) +
          ", runtime turns=" + str(summary["runtime_turns"]), flush=True)
    return capture


def observe_choice(experiment, *, peer_aware):
    """Observe tool selection without prescribing Hub, leases, or an edit method."""
    # Remove controlled-writer fixtures. Only the real target and its git baseline
    # remain: no injected save, ready marker, helper or hidden edit policy.
    for path in experiment.project.iterdir():
        if path.name not in {"settings.json", ".git"}:
            path.unlink()
    experiment.target.write_text(json.dumps(INITIAL) + "\n")
    subprocess.run(["git", "-C", str(experiment.project), "add", "settings.json"], check=True)
    for role in ["alpha", "beta"]:
        experiment.launch_shared(role)
    experiment.ready()
    experiment.checkpoint("initial")
    for role, field, value in [("alpha", "timeout_ms", 2000), ("beta", "max_retries", 3)]:
        text = f"Set {field}={value} in settings.json. Preserve other fields and verify the result."
        if peer_aware:
            text += (" Another independent Codex session is working in this checkout concurrently; "
                     "preserve its changes.")
        experiment.prompt(role, text)

    def settled():
        if not experiment.idle():
            return False
        for thread in experiment.registered.values():
            turns = experiment.turns(thread)
            if len(turns) < 2 or turns[-1]["status"] not in {"completed", "failed", "interrupted"}:
                return False
            # Wait for the bridge to observe the latest terminal and drain its
            # durable writes too, so a late automatic message is not overlooked.
            events = [json.loads(line) for line in (experiment.state / (thread + ".jsonl")).read_text().splitlines()
                      if line.startswith("{")]
            if not any(e.get("type") == "turn_completed" and e.get("turn_id") == turns[-1]["id"] for e in events):
                return False
            journal = CliJournal(experiment.state / (thread + ".sqlite3"))
            try:
                if journal.db.execute("SELECT COUNT(*) FROM pending").fetchone()[0]:
                    return False
            finally:
                journal.close()
            if list(HubClient(experiment.hub.url, thread).inbox()):
                return False
            if experiment.app.request("thread/queue/list", {"threadId": thread})["data"]:
                return False
        return True

    experiment.wait(settled, timeout=180)
    experiment.checkpoint("settled")
    capture = experiment.capture()
    capture["checkpoints"] = experiment.checkpoints
    capture["final_settings"] = json.loads(experiment.target.read_text())
    capture["summary"] = summary = summarize(capture)
    capture["both_changes_preserved"] = capture["final_settings"] == EXPECTED
    capture["peer_awareness_in_prompt"] = peer_aware
    capture["used_locks"] = any(c["tool"] == "hub_acquire_lock" for calls in summary["mcp_calls"].values() for c in calls)
    capture["token_usage"] = {e["params"]["threadId"]: e["params"]["tokenUsage"] for e in capture["trace"]
                              if e.get("method") == "thread/tokenUsage/updated"}
    with sqlite3.connect(experiment.root / "hub.sqlite3") as db:
        db.row_factory = sqlite3.Row
        capture["lock_requests"] = [dict(row) for row in db.execute("SELECT * FROM lock_requests ORDER BY sequence")]
    intervals = []
    for thread, snapshot in capture["threads"].items():
        first = snapshot["turns"][1]
        times = {e["method"]: e["seconds"] for e in capture["trace"]
                 if e.get("method") in {"turn/started", "turn/completed"}
                 and e["params"]["threadId"] == thread and e["params"]["turn"]["id"] == first["id"]}
        intervals.append(times)
    capture["first_work_turn_overlap_seconds"] = round(max(0, min(t["turn/completed"] for t in intervals)
                                                            - max(t["turn/started"] for t in intervals)), 3)
    assert capture["first_work_turn_overlap_seconds"] > 0, "Agent work did not actually overlap"
    assert all(t["turns"][-1]["status"] == "completed" for t in capture["threads"].values())
    print("RESULT: preserved=" + str(capture["both_changes_preserved"]) +
          ", used locks=" + str(capture["used_locks"]) +
          ", overlap=" + str(capture["first_work_turn_overlap_seconds"]), flush=True)
    return capture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--output", required=True)
    parser.add_argument("--scenario", choices=["release", "runtime-release", "expiry", "natural", "peer-aware"], required=True)
    parser.add_argument("--max-seconds", type=int, default=300)
    parser.add_argument("--expect-local-only", action="store_true",
                        help="Fail a natural/peer-aware probe if it calls any MCP tool; this expectation is not added to agent prompts")
    args = parser.parse_args()
    if args.max_seconds <= 0:
        parser.error("max-seconds must be positive")
    if args.expect_local_only and args.scenario not in {"natural", "peer-aware"}:
        parser.error("expect-local-only applies only to natural and peer-aware")

    def timeout(*_):
        raise TimeoutError("Lease experiment exceeded its overall time budget")

    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(args.max_seconds)
    repo = Path(__file__).resolve().parents[1]
    subprocess.run(["cargo", "build", "--bin", "agent-hub"], cwd=repo, check=True)
    evidence = {"version": subprocess.check_output([args.codex, "--version"], text=True).strip(),
                "scenario": args.scenario, "expect_local_only": args.expect_local_only}
    with tempfile.TemporaryDirectory(prefix="hub-lease-", dir="/tmp") as directory:
        with FileConflictExperiment(args.codex, Path(directory)) as experiment:
            try:
                evidence["evidence"] = (observe_choice(experiment, peer_aware=args.scenario == "peer-aware")
                                        if args.scenario in {"natural", "peer-aware"} else exercise(experiment, args.scenario))
                evidence["status"] = "passed" if evidence["evidence"].get("both_changes_preserved", True) else "lost_edit"
                if args.expect_local_only and any(evidence["evidence"]["summary"]["mcp_calls"].values()):
                    evidence["status"] = "unexpected_hub_use"
            except Exception as error:
                evidence["status"], evidence["error"] = "failed", str(error)
                if experiment.app:
                    evidence["evidence"] = experiment.capture()
                    evidence["evidence"]["checkpoints"] = experiment.checkpoints
                    evidence["evidence"]["final_settings"] = json.loads(experiment.target.read_text())
                raise
            finally:
                Path(args.output).write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")
    print("Evidence: " + args.output, flush=True)
    if evidence["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
