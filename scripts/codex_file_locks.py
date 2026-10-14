"""Automatic leases around structured Codex edits; no model-side lock calls."""

import hashlib
import json
import sqlite3
import time
from http.client import HTTPException
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


def patch_resources(event):
    command = event["tool_input"]["command"]
    if not isinstance(command, str):
        raise ValueError("apply_patch command must be a string")
    lines = command.strip().splitlines()
    if not lines or lines[0] != "*** Begin Patch" or lines[-1] != "*** End Patch":
        raise ValueError("Cannot identify file paths in this apply_patch input")
    resources = set()
    for line in lines:
        for prefix in ("*** Add File: ", "*** Update File: ", "*** Delete File: ", "*** Move to: "):
            if line.startswith(prefix):
                path = line[len(prefix):]
                if not path:
                    raise ValueError("Empty file path in apply_patch")
                resources.add(str((Path(event["cwd"]) / path).resolve()))
                break
    if not resources:
        raise ValueError("No file paths in apply_patch")
    return sorted(resources)


def request(settings, path, body=None, *, timeout=2):
    req = Request(settings["url"].rstrip("/") + path, headers={"Content-Type": "application/json"},
                  data=json.dumps(body).encode() if body is not None else None)
    with urlopen(req, timeout=timeout) as response:
        return json.load(response)


def log(settings, event, kind, **fields):
    path = Path(settings["state_dir"]) / (event["session_id"] + ".jsonl")
    try:
        with path.open("a") as file:
            file.write(json.dumps({"type": kind, "timestamp_ms": int(time.time() * 1000),
                "turn_id": event.get("turn_id"), "tool_use_id": event.get("tool_use_id"), **fields}) + "\n")
    except OSError:
        pass  # Diagnostics must not turn an explicit denial into a failed hook.


def denied(reason):
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny",
        "permissionDecisionReason": "Agent Hub did not allow this patch: " + str(reason)}}


def release_call(settings, event, journal, *, keep=None):
    rows = journal.db.execute("""SELECT f.turn_id,f.request_id FROM file_lock_calls f
        JOIN held_locks h ON h.turn_id=f.turn_id AND h.request_id=f.request_id
        WHERE f.tool_use_id=? AND h.closed=0 AND f.request_id!=?""",
        (event["tool_use_id"], keep or "")).fetchall()
    for turn_id, request_id in rows:
        journal.enqueue("lock-release:" + json.dumps([event["session_id"], request_id, turn_id]),
                        settings["url"].rstrip("/") + "/locks/release",
                        {"session_id": event["session_id"], "request_id": request_id})
        with journal.db:
            journal.db.execute("UPDATE held_locks SET closed=1 WHERE turn_id=? AND request_id=?", (turn_id, request_id))
    journal.flush(timeout=2)
    if rows:
        log(settings, event, "file_locks_released", request_ids=[row[1] for row in rows])


def pre_edit(settings, event, journal):
    resources = patch_resources(event)
    wait_seconds = settings.get("file_lock_wait_seconds", 30)
    if not isinstance(wait_seconds, int) or not 0 <= wait_seconds <= 60:
        raise ValueError("file_lock_wait_seconds must be between 0 and 60")
    deadline = time.monotonic() + wait_seconds
    for resource in resources:
        digest = hashlib.sha256(json.dumps([event["session_id"], event["turn_id"],
            event["tool_use_id"], resource]).encode()).hexdigest()
        request_id = "hub-file:" + digest
        with journal.db:
            journal.db.execute("INSERT OR IGNORE INTO file_lock_calls VALUES(?,?,?,?)",
                (event["tool_use_id"], event["turn_id"], request_id, resource))
        ticket = request(settings, "/locks/acquire", {"session_id": event["session_id"],
            "request_id": request_id, "resource": resource,
            "ttl_seconds": settings.get("file_lock_ttl_seconds", 30), "notify": False})
        if ticket["session_id"] != event["session_id"]:
            raise ValueError("Hub returned a different file lock request")
        if ticket["request_id"] != request_id:
            if ticket.get("notify") is True and ticket["status"] == "acquired" and ticket["resource"] == resource:
                log(settings, event, "file_lock_borrowed", resource=resource, request_id=ticket["request_id"])
                continue
            raise ValueError("Hub returned a different file lock request")
        journal.hold_lock(event["turn_id"], request_id)
        if ticket["status"] == "waiting":
            log(settings, event, "file_lock_waiting", resource=resource, request_id=request_id)
            # Never keep earlier file leases while waiting on another resource.
            release_call(settings, event, journal, keep=request_id)
            remaining = max(0, int(deadline - time.monotonic()))
            if remaining:
                ticket = request(settings, "/locks/requests/" + quote(request_id, safe="") + "?"
                    + urlencode({"session_id": event["session_id"], "wait_seconds": remaining}), timeout=remaining + 2)
            log(settings, event, "file_lock_wait_finished", resource=resource, status=ticket["status"])
            if ticket["status"] == "acquired":
                outcome = "The resource became available."
            elif ticket["status"] == "waiting":
                outcome = "The wait deadline ended."
            else:
                outcome = "Waiting ended with lock status " + ticket["status"] + "."
            raise ValueError(f"Another writer held {resource}. {outcome} The pending patch was NOT executed. "
                "Reread these files, preserve their current changes, and submit a NEW apply_patch: " + ", ".join(resources))
        if ticket["status"] != "acquired":
            raise ValueError("This tool call's file lock is already closed. Reread the file and submit a NEW apply_patch.")
    log(settings, event, "file_locks_acquired", resources=resources)
    return {}


def handle(settings, event, journal):
    if event.get("tool_name") != "apply_patch":
        return {}
    journal.db.execute("""CREATE TABLE IF NOT EXISTS file_lock_calls (
        tool_use_id TEXT NOT NULL, turn_id TEXT NOT NULL, request_id TEXT PRIMARY KEY, resource TEXT NOT NULL
    )""")
    journal.db.commit()
    try:
        if event["hook_event_name"] == "PostToolUse":
            release_call(settings, event, journal)
            return {}
        return pre_edit(settings, event, journal)
    except (OSError, HTTPException, ValueError, TypeError, KeyError, sqlite3.DatabaseError) as error:
        try:
            release_call(settings, event, journal)
        except (OSError, HTTPException, ValueError, sqlite3.DatabaseError):
            pass  # Durable release and TTL remain available to the bridge.
        log(settings, event, "file_lock_error", error=str(error))
        if event["hook_event_name"] == "PostToolUse":
            return {}
        return denied(error)
