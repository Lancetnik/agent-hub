"""Connect to the existing Codex CLI daemon; never launch or replace the runtime."""

import json
import os
import queue
import threading
from pathlib import Path

from check_codex import AppServer


class DaemonClient(AppServer):
    def __init__(self, socket_path=None, *, on_notification=None):
        from websockets.exceptions import WebSocketException
        from websockets.sync.client import unix_connect

        home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
        self.socket_path = str(socket_path or home / "app-server-control/app-server-control.sock")
        self.next_id = 0
        self.messages = queue.Queue()
        self.notifications = []
        self.on_notification = on_notification
        # The CLI remains the UI owner for approval/input requests.
        self.answer_requests = False
        self.transport_errors = WebSocketException
        try:
            self.socket = unix_connect(self.socket_path, open_timeout=10, close_timeout=1,
                                       max_size=16 * 1024 * 1024)
        except WebSocketException as error:
            raise RuntimeError(f"Codex daemon connection failed: {error}") from error
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        try:
            for message in self.socket:
                self.messages.put(json.loads(message))
        except Exception as error:
            self.messages.put(RuntimeError(f"Codex daemon connection failed: {error}"))
        finally:
            self.messages.put(RuntimeError("Codex daemon disconnected"))

    def send(self, message):
        try:
            self.socket.send(json.dumps(message))
        except self.transport_errors as error:
            raise RuntimeError(f"Codex daemon send failed: {error}") from error

    def close(self):
        self.socket.close()
        self.reader.join(timeout=1)
