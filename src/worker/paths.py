from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

from ..common.config_base import BaseConfigManager, DEFAULT_BASE_DIR


class WorkerConfigManager(BaseConfigManager):
    def __init__(self) -> None:
        super().__init__(config_filename="config.json")

    @property
    def pid_path(self) -> Path:
        return self._config_dir / "proxy-ssh.pid"

    def write_pid(self) -> None:
        self.ensure_dirs()
        self.pid_path.write_text(str(os.getpid()))

    def remove_pid(self) -> None:
        if self.pid_path.exists():
            self.pid_path.unlink()

    def check_process_running(self) -> tuple[bool, int | None]:
        if not self.pid_path.exists():
            return False, None
        try:
            pid = int(self.pid_path.read_text().strip())
            os.kill(pid, 0)
            return True, pid
        except (ProcessLookupError, ValueError, PermissionError):
            return False, None

    def stop_process(self) -> None:
        running, pid = self.check_process_running()
        if not running or pid is None:
            return
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            try:
                os.kill(pid, 0)
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        except (ProcessLookupError, PermissionError):
            pass
        self.remove_pid()
