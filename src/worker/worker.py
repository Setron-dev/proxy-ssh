from __future__ import annotations

import asyncio
import signal
import sys
import time

from ..common.config_base import setup_logging
from .config import WorkerConfig, WorkerConfigManager
from .ssh_manager import SSHManager, SSHTunnelInfo, TunnelStatus
from .process_manager import ClientProcessManager
from .status import print_status, format_uptime


class RelayWorker:
    def __init__(self, config: WorkerConfig, config_manager: WorkerConfigManager) -> None:
        self._config = config
        self._cm = config_manager
        self._ssh: SSHManager | None = None
        self._client_mgr: ClientProcessManager | None = None
        self._stop_event = asyncio.Event()
        self._start_time: float = 0
        self._logger = None

    async def run(self) -> None:
        import logging
        self._logger = logging.getLogger("proxy-ssh.worker")
        self._start_time = time.time()

        self._ssh = SSHManager(
            host=self._config.ssh.host, port=self._config.ssh.port,
            username=self._config.ssh.username, auth_method=self._config.ssh.auth_method,
            key_path=self._config.ssh.key_path, key_passphrase=self._config.ssh.key_passphrase,
            password=self._config.ssh.password, local_forward_port=self._config.ssh.local_forward_port,
            remote_forward_port=self._config.ssh.remote_forward_port,
            on_status_change=self._on_ssh_status,
        )

        self._client_mgr = ClientProcessManager(
            client_binary=str(self._cm.client_binary_path),
            client_config_path=str(self._cm.client_config_path),
        )

        self._logger.info("starting proxy-ssh worker...")
        self._logger.info("ssh: %s@%s:%d", self._config.ssh.username, self._config.ssh.host, self._config.ssh.port)
        self._logger.info("server: %s", self._config.server.url)
        self._logger.info("service port: %d", self._config.service_port)

        self._cm.write_pid()
        ssh_task = asyncio.create_task(self._ssh.start())
        client_task = asyncio.create_task(self._client_mgr.start())

        try:
            await self._stop_event.wait()
        finally:
            self._logger.info("shutting down...")
            if self._client_mgr:
                await self._client_mgr.stop()
            await self._ssh.stop()
            client_task.cancel()
            ssh_task.cancel()
            for t in [client_task, ssh_task]:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            self._cm.remove_pid()
            self._logger.info("proxy-ssh worker stopped")

    async def stop(self) -> None:
        self._stop_event.set()

    async def _on_ssh_status(self, status: TunnelStatus) -> None:
        if status == TunnelStatus.CONNECTED:
            self._logger.info("ssh tunnel established")
        elif status == TunnelStatus.RECONNECTING:
            self._logger.warning("ssh tunnel disconnected, reconnecting...")

    def get_ssh_info(self) -> SSHTunnelInfo:
        if self._ssh:
            info = self._ssh.info
            if info.started_at:
                info.uptime = time.time() - info.started_at
            return info
        return SSHTunnelInfo()

    def get_client_running(self) -> bool:
        return self._client_mgr.running if self._client_mgr else False

    def get_client_restarts(self) -> int:
        return self._client_mgr.restart_count if self._client_mgr else 0
