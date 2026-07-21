#!/usr/bin/env python3
"""
Standalone relay: Server + built-in Worker in one process.
Runs on Daytona. Client connects to TCP port 3128.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("standalone")

from protocol import MessageType, RelayMessage, new_request_id

CLIENT_PORT = 3128
AUTH_TOKEN = "standalone-relay-2026"


class InlineWorker:
    """Built-in worker that connects to the server's WS on localhost."""

    def __init__(self, ws_port: int = 8443):
        self._ws_port = ws_port
        self._ws = None
        self._active: dict[str, asyncio.StreamWriter] = {}
        self._send_lock = asyncio.Lock()

    async def run(self):
        import websockets.asyncio.client
        url = f"ws://127.0.0.1:{self._ws_port}"
        while True:
            try:
                logger.info("[worker] connecting to %s", url)
                async with websockets.asyncio.client.connect(
                    url, ping_interval=20, ping_timeout=10, max_size=None,
                ) as ws:
                    self._ws = ws
                    await ws.send(RelayMessage.make_auth(AUTH_TOKEN).to_json())
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    msg = RelayMessage.from_json(raw if isinstance(raw, bytes) else raw.encode())
                    if msg.msg_type != MessageType.AUTH_OK:
                        logger.error("[worker] auth failed: %s", msg.error_msg)
                        await asyncio.sleep(3)
                        continue
                    logger.info("[worker] authenticated")
                    async for raw in ws:
                        msg = RelayMessage.from_json(raw if isinstance(raw, bytes) else raw.encode())
                        if msg.msg_type == MessageType.PING:
                            await ws.send(RelayMessage.make_pong().to_json())
                        elif msg.msg_type == MessageType.REQUEST:
                            asyncio.create_task(self._handle_request(msg))
                        elif msg.msg_type == MessageType.CLOSE:
                            await self._close(msg.request_id)
            except Exception as exc:
                logger.error("[worker] connection lost: %s", exc)
            self._ws = None
            await asyncio.sleep(2)

    async def _handle_request(self, msg: RelayMessage):
        conn_id = msg.request_id
        initial = base64.b64decode(msg.payload_data) if msg.payload_data else b""

        host, port = self._parse_target(initial)
        if not host:
            logger.warning("[worker] cannot parse target for %s", conn_id)
            await self._send_error(conn_id, "cannot determine target host")
            return

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=20,
            )
        except Exception as exc:
            logger.error("[worker] connect to %s:%d failed: %s", host, port, exc)
            await self._send_error(conn_id, str(exc))
            return

        self._active[conn_id] = writer
        if initial:
            writer.write(initial)
            await writer.drain()

        try:
            while True:
                data = await asyncio.wait_for(reader.read(65536), timeout=30)
                if not data:
                    break
                await self._send_data(conn_id, data)
        except (asyncio.TimeoutError, ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as exc:
            logger.error("[worker] relay error %s: %s", conn_id, exc)
        finally:
            writer.close()
            self._active.pop(conn_id, None)
            await self._send_end(conn_id)

    def _parse_target(self, data: bytes) -> tuple[str, int]:
        try:
            header_block = data.split(b"\r\n\r\n", 1)[0]
            lines = header_block.split(b"\r\n")
            host = ""
            for line in lines[1:]:
                lower = line.lower()
                if lower.startswith(b"host:"):
                    host = line[5:].strip().decode("utf-8", errors="ignore")
                    break
            if not host:
                return "", 0
            if ":" in host:
                h, p = host.rsplit(":", 1)
                if p.isdigit():
                    return h, int(p)
            return host, 80
        except Exception:
            pass
        return "", 0

    def _ws_alive(self) -> bool:
        if not self._ws:
            return False
        try:
            return self._ws.state.name == "OPEN"
        except AttributeError:
            return getattr(self._ws, "open", True)

    async def _send_data(self, conn_id: str, data: bytes):
        if not self._ws_alive():
            return
        try:
            async with self._send_lock:
                await self._ws.send(RelayMessage.make_data(
                    conn_id, base64.b64encode(data).decode(),
                ).to_json())
        except Exception:
            pass

    async def _send_end(self, conn_id: str):
        if not self._ws_alive():
            return
        try:
            async with self._send_lock:
                await self._ws.send(RelayMessage.make_end(conn_id).to_json())
        except Exception:
            pass

    async def _send_error(self, conn_id: str, error: str):
        if not self._ws_alive():
            return
        try:
            async with self._send_lock:
                await self._ws.send(RelayMessage.make_error(conn_id, error).to_json())
        except Exception:
            pass

    async def _close(self, conn_id: str):
        writer = self._active.pop(conn_id, None)
        if writer and not writer.is_closing():
            try:
                writer.close()
            except Exception:
                pass


async def main():
    ws_port = 8443

    config = {
        "client_listener": {"host": "0.0.0.0", "port": CLIENT_PORT},
        "worker_ws": {
            "host": "0.0.0.0", "port": ws_port,
            "path": "/relay", "auth_token": AUTH_TOKEN,
        },
        "tls": {"enabled": False, "certfile": "", "keyfile": ""},
        "timeouts": {"client_idle": 300, "worker_response": 60},
    }

    sys.path.insert(0, ".")
    from relay_server import RelayServer

    server = RelayServer(config)
    await server.start()
    logger.info("=== Relay server ready on port %d ===", CLIENT_PORT)

    worker = InlineWorker(ws_port=ws_port)
    worker_task = asyncio.create_task(worker.run())

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    worker_task.cancel()
    await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
