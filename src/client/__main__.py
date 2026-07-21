from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from ..common.config_base import setup_logging
from ..common.json_lib import _ensure_websockets
from .config import ClientConfigManager
from .connector import ServerConnector
from .relay import TCPRelay


__version__ = "1.0.0"


class RelayClient:
    def __init__(self, config_path: str) -> None:
        self._cm = ClientConfigManager(config_path)
        self._config = self._cm.load()
        self._connector: ServerConnector | None = None
        self._relay: TCPRelay | None = None
        self._stop_event = asyncio.Event()
        self._logger = None

    async def run(self) -> None:
        import logging
        self._logger = logging.getLogger("proxy-ssh-client")
        setup_logging(self._config.log_level)

        self._logger.info("starting proxy-ssh-client...")
        self._logger.info("server: %s", self._config.server_url)
        self._logger.info("upstream port: %d", self._config.upstream_port)

        self._connector = ServerConnector(
            server_url=self._config.server_url,
            auth_token=self._config.auth_token,
            on_message=self._on_message,
            tls_verify=self._config.tls_verify,
        )
        self._relay = TCPRelay(
            send_callback=self._connector.send,
            upstream_port=self._config.upstream_port,
        )

        server_task = asyncio.create_task(self._connector.run())
        try:
            await self._stop_event.wait()
        finally:
            self._logger.info("shutting down client...")
            if self._connector:
                await self._connector.stop()
            if self._relay:
                await self._relay.cleanup_all()
            server_task.cancel()
            try:
                await server_task
            except (asyncio.CancelledError, Exception):
                pass
            self._logger.info("proxy-ssh-client stopped")

    async def stop(self) -> None:
        self._stop_event.set()

    async def _on_message(self, msg):
        if self._relay:
            await self._relay.handle(msg)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="proxy-ssh-client",
        description="proxy-ssh-client - Internal relay client (managed by proxy-ssh)",
    )
    parser.add_argument("--config", required=True, help="Path to client config file")
    parser.add_argument("--version", action="version", version=f"proxy-ssh-client v{__version__}")
    args = parser.parse_args()

    _ensure_websockets()
    client = RelayClient(args.config)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown():
        loop.create_task(client.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    try:
        loop.run_until_complete(client.run())
    except KeyboardInterrupt:
        loop.run_until_complete(client.stop())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
