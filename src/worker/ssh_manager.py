from __future__ import annotations

import asyncio
import logging
import subprocess
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger("proxy-ssh.ssh")


class TunnelStatus(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


@dataclass
class SSHTunnelInfo:
    pid: int | None = None
    status: TunnelStatus = TunnelStatus.DISCONNECTED
    uptime: float = 0.0
    started_at: float | None = None
    last_error: str = ""
    reconnect_count: int = 0


class SSHManager:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        auth_method: str,
        key_path: str = "",
        key_passphrase: str = "",
        password: str = "",
        local_forward_port: int = 4096,
        remote_forward_port: int = 4096,
        on_status_change: Optional[Callable[[TunnelStatus], Awaitable[None]]] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._auth_method = auth_method
        self._key_path = key_path
        self._key_passphrase = key_passphrase
        self._password = password
        self._local_forward_port = local_forward_port
        self._remote_forward_port = remote_forward_port
        self._on_status_change = on_status_change
        self._process: subprocess.Popen | None = None
        self._info = SSHTunnelInfo()
        self._should_run = False
        self._monitor_task: asyncio.Task | None = None

    @property
    def status(self) -> TunnelStatus:
        return self._info.status

    @property
    def info(self) -> SSHTunnelInfo:
        return self._info

    async def start(self) -> None:
        self._should_run = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def stop(self) -> None:
        self._should_run = False
        self._kill_process()
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

    def _build_ssh_command(self) -> list[str]:
        cmd = [
            "ssh", "-N",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "ConnectTimeout=15",
            "-o", "BatchMode=yes",
            "-p", str(self._port),
            "-L", f"127.0.0.1:{self._local_forward_port}:127.0.0.1:{self._remote_forward_port}",
        ]
        if self._auth_method == "key" and self._key_path:
            key = Path(self._key_path).expanduser()
            if key.exists():
                cmd.extend(["-i", str(key)])
        cmd.append(f"{self._username}@{self._host}")
        return cmd

    def _start_process(self) -> bool:
        cmd = self._build_ssh_command()
        logger.info("starting SSH tunnel: %s@%s:%d", self._username, self._host, self._port)
        try:
            self._process = subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL,
            )
            self._info.pid = self._process.pid
            logger.info("SSH process started PID %d", self._process.pid)
            return True
        except FileNotFoundError:
            logger.error("ssh command not found")
            self._info.last_error = "ssh command not found"
            return False
        except Exception as exc:
            logger.error("failed to start SSH: %s", exc)
            self._info.last_error = str(exc)
            return False

    def _kill_process(self) -> None:
        if self._process and self._process.poll() is None:
            try:
                self._process.terminate()
                try:
                    self._process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                    self._process.wait(timeout=3)
            except Exception:
                pass
            logger.info("SSH process killed PID %s", self._info.pid)
        self._process = None
        self._info.pid = None

    async def _monitor_loop(self) -> None:
        while self._should_run:
            if self._info.status in (TunnelStatus.DISCONNECTED, TunnelStatus.RECONNECTING):
                await self._set_status(
                    TunnelStatus.CONNECTING if self._info.status == TunnelStatus.DISCONNECTED
                    else TunnelStatus.RECONNECTING
                )
                success = self._start_process()
                if success:
                    await asyncio.sleep(3)
                    if self._process and self._process.poll() is None:
                        await self._set_status(TunnelStatus.CONNECTED)
                        self._info.started_at = time.time()
                        logger.info("SSH tunnel established")
                    else:
                        self._info.last_error = "SSH exited during startup"
                        logger.error("SSH tunnel failed to start")
                        await self._set_status(TunnelStatus.RECONNECTING)
                        self._info.reconnect_count += 1
                        delay = min(5 * (2 ** min(self._info.reconnect_count, 6)), 120)
                        logger.info("reconnecting in %ds...", delay)
                        await asyncio.sleep(delay)
                else:
                    await self._set_status(TunnelStatus.RECONNECTING)
                    self._info.reconnect_count += 1
                    delay = min(5 * (2 ** min(self._info.reconnect_count, 6)), 120)
                    await asyncio.sleep(delay)
            elif self._info.status == TunnelStatus.CONNECTED:
                if self._process and self._process.poll() is None:
                    await asyncio.sleep(5)
                else:
                    exit_code = self._process.returncode if self._process else None
                    self._info.last_error = f"SSH exited code {exit_code}"
                    logger.warning("SSH tunnel disconnected (exit code: %s)", exit_code)
                    self._process = None
                    self._info.pid = None
                    self._info.reconnect_count += 1
                    await self._set_status(TunnelStatus.RECONNECTING)
                    delay = min(5 * (2 ** min(self._info.reconnect_count, 6)), 120)
                    logger.info("reconnecting in %ds...", delay)
                    await asyncio.sleep(delay)

    async def _set_status(self, new_status: TunnelStatus) -> None:
        old = self._info.status
        self._info.status = new_status
        if old != new_status:
            logger.info("SSH status: %s -> %s", old.value, new_status.value)
            if self._on_status_change:
                try:
                    await self._on_status_change(new_status)
                except Exception:
                    pass
