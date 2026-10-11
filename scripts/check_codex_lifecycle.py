#!/usr/bin/env python3
"""Capture real Codex hooks and terminal events in an isolated temporary setup."""

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

from check_codex import AppServer
from codex_worker import TurnCollector, initialize


def probe(codex, output, continue_once):
    version = subprocess.check_output([codex, "--version"], text=True).strip()
    with tempfile.TemporaryDirectory(prefix="hub-lifecycle-") as directory:
        root = Path(directory)
        codex_home = root / "codex-home"
        codex_home.mkdir()
        user_home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
        # Reuse sign-in through a read-only link; never copy or print credentials.
        auth = user_home / "auth.json"
        if auth.exists():
            (codex_home / "auth.json").symlink_to(auth)
        model = None
        if (user_home / "config.toml").exists():
            model = tomllib.loads((user_home / "config.toml").read_text()).get("model")
        callbacks = root / "callbacks.jsonl"
        recorder = root / "recorder.py"
        recorder.write_text(
            "import json,sys\nfrom pathlib import Path\ne=json.load(sys.stdin)\n"
            "with Path(sys.argv[1]).open('a') as f: f.write(json.dumps(e)+'\\n')\n"
            "response={}\n"
            "if sys.argv[2]=='yes' and e['hook_event_name']=='Stop' and not e['stop_hook_active']:\n"
            "    response={'decision':'block','reason':'Reply with the single digit 5. Do not use tools.'}\n"
            "print(json.dumps(response))\n"
        )
        command = shlex.join([sys.executable, str(recorder), str(callbacks),
                              "yes" if continue_once else "no"])
        (codex_home / "hooks.json").write_text(json.dumps({"hooks": {
            name: [{"hooks": [{"type": "command", "command": command}]}]
            for name in ["SessionStart", "Stop", "Interrupt"]
        }}))
        config = codex_home / "config.toml"
        config.write_text("analytics.enabled=false\n" +
                          ("model=" + json.dumps(model) + "\n" if model else ""))

        def start():
            app = AppServer(codex, None, command=[codex, "app-server", "--listen", "stdio://"],
                            environment={**os.environ, "CODEX_HOME": str(codex_home)})
            try:
                initialize(app)
            except BaseException:
                app.close()
                raise
            return app

        app = start()
        try:
            listing = app.request("hooks/list", {"cwds": [str(root)]})
            hooks = listing["data"][0]["hooks"]
            assert len(hooks) == 3, listing
            with config.open("a") as target:
                for hook in hooks:
                    # Trust only this probe's exact, locally reviewed recorder command.
                    assert hook["command"] == command
                    target.write("\n[hooks.state." + json.dumps(hook["key"]) + "]\n"
                                 "trusted_hash=" + json.dumps(hook["currentHash"]) + "\n")
        finally:
            app.close()

        app = start()
        try:
            listing = app.request("hooks/list", {"cwds": [str(root)]})
            assert all(h["trustStatus"] == "trusted" for h in listing["data"][0]["hooks"])
            thread = app.request("thread/start", {
                "cwd": str(root), "ephemeral": True, "approvalPolicy": "never", "sandbox": "read-only",
            })["thread"]["id"]
            collector = TurnCollector(thread)
            app.on_notification = collector.observe
            turn = app.request("turn/start", {"threadId": thread, "input": [{
                "type": "text", "text": "Compute 2 + 2. Reply with the single digit only. Do not use tools.",
            }]})["turn"]["id"]
            terminal = app.wait_notification("turn/completed", lambda p: p["turn"]["id"] == turn)
            result = collector.result(terminal, "worker", "orchestrator", "assignment-1")
            captured = [json.loads(line) for line in callbacks.read_text().splitlines()]
            starts = [e for e in captured if e["hook_event_name"] == "SessionStart"]
            stops = [e for e in captured if e["hook_event_name"] == "Stop"]
            assert starts and starts[0]["session_id"] == thread, captured
            assert stops and all(e["session_id"] == thread and e["turn_id"] == turn for e in stops)
            assert result["status"] == "completed", result
            assert result["final_text"] == ("5" if continue_once else "4"), result
            if continue_once:
                assert len(stops) >= 2 and stops[0]["last_assistant_message"] == "4", stops
                assert stops[-1]["stop_hook_active"] is True, stops
            # Save identifiers and result data only; omit credentials, cwd, transcript paths.
            evidence = {"version": version, "continued_by_stop": continue_once, "callbacks": [
                {k: e[k] for k in ["hook_event_name", "session_id", "turn_id", "source",
                                  "last_assistant_message", "stop_hook_active"] if k in e}
                for e in captured
            ], "terminal": terminal, "result": result}
            if output:
                Path(output).write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n")
            print(json.dumps(evidence, indent=2, ensure_ascii=False))
            print("PASS: real SessionStart/Stop and a terminal runtime result agree.")
        finally:
            app.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--output")
    parser.add_argument("--continue-once", action="store_true")
    args = parser.parse_args()
    try:
        probe(args.codex, args.output, args.continue_once)
    except (OSError, RuntimeError, TimeoutError, AssertionError) as error:
        print(f"Lifecycle probe failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
