from __future__ import annotations

import asyncio
import base64
import json
import logging
import signal
import sys

from connection_manager import ConnectionManager
from worker_bridge import WorkerBridge
from client_listener import ClientListener
from protocol import MessageType, RelayMessage

logger = logging.getLogger("relay.server")


def load_config(path: str = "config.json") -> dict:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning("config not found, using defaults")
        return {}


class RelayServer:
    def __init__(self, config: dict) -> None:
        self._config = config
        self._cm = ConnectionManager(
            client_idle_timeout=config.get("timeouts", {}).get("client_idle", 300),
        )

        ws_cfg = config.get("worker_ws", {})
        tls_cfg = config.get("tls", {})
        self._bridge = WorkerBridge(
            connection_manager=self._cm,
            listen_path=ws_cfg.get("path", "/relay"),
            auth_token=ws_cfg.get("auth_token", ""),
            tls_certfile=tls_cfg.get("certfile", ""),
            tls_keyfile=tls_cfg.get("keyfile", ""),
            host=ws_cfg.get("host", "0.0.0.0"),
            port=ws_cfg.get("port", 8443),
        )

        cl_cfg = config.get("client_listener", {})
        self._listener = ClientListener(
            connection_manager=self._cm,
            worker_bridge=self._bridge,
            host=cl_cfg.get("host", "0.0.0.0"),
            port=cl_cfg.get("port", 3128),
        )

        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        await self._bridge.start()
        await self._listener.start()
        self._tasks.append(asyncio.create_task(self._cm.cleanup_idle()))
        self._tasks.append(asyncio.create_task(self._response_relay()))
        self._tasks.append(asyncio.create_task(self._health_check()))
        logger.info("relay server started")

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await self._listener.stop()
        await self._bridge.stop()
        await self._cm.close_all()
        logger.info("relay server stopped")

    async def _response_relay(self) -> None:
        while True:
            if not self._bridge.worker_connected or not self._bridge._authenticated.is_set():
                await asyncio.sleep(0.5)
                continue

            pending = list(self._bridge._data_queue.keys())
            for request_id in pending:
                queue = self._bridge._data_queue.get(request_id)
                if queue is None:
                    continue

                while not queue.empty():
                    msg = await queue.get()

                    if msg.msg_type == MessageType.DATA:
                        data = base64.b64decode(msg.payload_data) if msg.payload_data else b""
                        if data:
                            await self._cm.send_to_client(request_id, data)

                    elif msg.msg_type == MessageType.END:
                        await self._cm.close_client(request_id)
                        await self._bridge.unregister_queue(request_id)
                        break

                    elif msg.msg_type == MessageType.ERROR:
                        logger.error("worker error for %s: %s", request_id, msg.error_msg)
                        await self._cm.close_client(request_id)
                        await self._bridge.unregister_queue(request_id)
                        break

            await asyncio.sleep(0.005)

    async def _health_check(self) -> None:
        while True:
            await asyncio.sleep(10)
            logger.info(
                "health: active_clients=%d worker_connected=%s",
                self._cm.active_count,
                self._bridge.worker_connected,
            )


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    config = load_config("config.json")
    server = RelayServer(config)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await server.start()
    await stop_event.wait()
    await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
