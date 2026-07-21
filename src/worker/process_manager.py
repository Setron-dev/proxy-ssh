from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("proxy-ssh.process")


class ClientProcessManager:
    """Manages proxy-ssh-client as a child process."""

    def __init__(
        self,
        client_binary: str,
        client_config_path: str,
    ) -> None:
        self._client_binary = client_binary
        self._client_config_path = client_config_path
        self._process: asyncio.subprocess.Process | None = None
        self._should_run = False
        self._monitor_task: asyncio.Task | None = None
        self._restart_count = 0
        self._last_exit_code: int | None = None

    @property
    def running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    @property
    def pid(self) -> int | None:
        return self._process.pid if self._process else None

    @property
    def restart_count(self) -> int:
        return self._restart_count

    @property
    def last_exit_code(self) -> int | None:
        return self._last_exit_code

    async def start(self) -> None:
        if not Path(self._client_binary).exists():
            logger.error("client binary not found: %s", self._client_binary)
            return
        if not Path(self._client_config_path).exists():
            logger.error("client config not found: %s", self._client_config_path)
            return
        self._should_run = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("client process manager started")

    async def stop(self) -> None:
        self._should_run = False
        self._kill_client()
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("client process manager stopped")

    async def restart(self) -> None:
        self._kill_client()
        await asyncio.sleep(1)
        await self._spawn_client()

    def _kill_client(self) -> None:
        if self._process and self._process.returncode is None:
            try:
                self._process.send_signal(signal.SIGTERM)
                try:
                    asyncio.get_event_loop().run_until_complete(
                        asyncio.wait_for(self._process.wait(), timeout=5)
                    )
                except (asyncio.TimeoutError, RuntimeError):
                    try:
                        self._process.kill()
                    except ProcessLookupError:
                        pass
            except (ProcessLookupError, OSError):
                pass
            logger.info("client process killed PID %s", self.pid)
        self._process = None

    async def _spawn_client(self) -> None:
        cmd = [self._client_binary, "--config", self._client_config_path]
        logger.info("starting client: %s", " ".join(cmd))
        try:
            self._process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            logger.info("client process started PID %d", self._process.pid)
        except Exception as exc:
            logger.error("failed to start client: %s", exc)
            self._process = None

    async def _monitor_loop(self) -> None:
        while self._should_run:
            if self._process is None or self._process.returncode is not None:
                if self._process is not None:
                    self._last_exit_code = self._process.returncode
                    logger.warning(
                        "client exited with code %s, restarting...",
                        self._last_exit_code,
                    )
                    self._restart_count += 1
                    delay = min(5 * (2 ** min(self._restart_count, 6)), 120)
                    await asyncio.sleep(delay)

                if self._should_run:
                    await self._spawn_client()
            else:
                await asyncio.sleep(2)

    async def update_config(self, client_config_path: str) -> None:
        """Update config path and restart client."""
        self._client_config_path = client_config_path
        if self.running:
            await self.restart()
