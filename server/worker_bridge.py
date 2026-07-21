from __future__ import annotations

import asyncio
import logging
import ssl
from typing import TYPE_CHECKING

import websockets
import websockets.asyncio.server

from protocol import MessageType, RelayMessage

if TYPE_CHECKING:
    from connection_manager import ConnectionManager

logger = logging.getLogger("relay.worker_bridge")


class WorkerBridge:
    def __init__(
        self,
        connection_manager: ConnectionManager,
        listen_path: str = "/relay",
        auth_token: str = "",
        tls_certfile: str = "",
        tls_keyfile: str = "",
        host: str = "0.0.0.0",
        port: int = 8443,
    ) -> None:
        self._cm = connection_manager
        self._listen_path = listen_path
        self._auth_token = auth_token
        self._host = host
        self._port = port
        self._worker: websockets.asyncio.server.ServerConnection | None = None
        self._server: websockets.asyncio.server.serve | None = None
        self._authenticated = asyncio.Event()
        self._data_queue: dict[str, asyncio.Queue[RelayMessage]] = {}
        self._lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()

        self._ssl_ctx: ssl.SSLContext | None = None
        if tls_certfile and tls_keyfile:
            self._ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            self._ssl_ctx.load_cert_chain(tls_certfile, tls_keyfile)

    @property
    def worker_connected(self) -> bool:
        return self._worker is not None and self._worker.open

    async def start(self) -> None:
        self._server = await websockets.asyncio.server.serve(
            self._handle_worker_connection,
            self._host,
            self._port,
            ssl=self._ssl_ctx,
            ping_interval=20,
            ping_timeout=10,
            max_size=None,
        )
        logger.info("worker bridge listening on %s:%d", self._host, self._port)

    async def stop(self) -> None:
        if self._worker:
            await self._worker.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_worker_connection(
        self, ws: websockets.asyncio.server.ServerConnection
    ) -> None:
        logger.info("worker connected from %s", ws.remote_address)

        if self._worker is not None and self._worker.open:
            logger.warning("replacing existing worker connection")
            try:
                await self._worker.close(1000, "replaced")
            except Exception:
                pass

        self._worker = ws
        self._authenticated.clear()

        try:
            async for raw in ws:
                msg = RelayMessage.from_json(raw if isinstance(raw, bytes) else raw.encode())
                await self._process_worker_message(msg)
        except websockets.ConnectionClosed as exc:
            logger.info("worker disconnected: %s", exc)
        except Exception as exc:
            logger.error("worker connection error: %s", exc, exc_info=True)
        finally:
            self._worker = None
            self._authenticated.clear()
            logger.info("worker connection cleaned up")

    async def _process_worker_message(self, msg: RelayMessage) -> None:
        if msg.msg_type == MessageType.AUTH_OK:
            self._authenticated.set()
            logger.info("worker authenticated successfully")
            return

        if msg.msg_type == MessageType.AUTH_FAIL:
            logger.error("worker auth failed: %s", msg.error_msg)
            return

        if not self._authenticated.is_set():
            if msg.msg_type == MessageType.AUTH and msg.token == self._auth_token:
                await self._send_raw(RelayMessage.make_auth_ok())
                self._authenticated.set()
                logger.info("worker authenticated via token")
            elif msg.msg_type == MessageType.AUTH:
                await self._send_raw(RelayMessage.make_auth_fail("invalid token"))
            return

        if msg.msg_type in (MessageType.DATA, MessageType.END, MessageType.ERROR):
            queue = self._data_queue.get(msg.request_id)
            if queue:
                await queue.put(msg)
            else:
                logger.warning("received %s for unknown request_id=%s", msg.msg_type.value, msg.request_id)

        elif msg.msg_type == MessageType.PONG:
            pass

    async def register_queue(self, request_id: str) -> asyncio.Queue[RelayMessage]:
        q: asyncio.Queue[RelayMessage] = asyncio.Queue()
        async with self._lock:
            self._data_queue[request_id] = q
        return q

    async def unregister_queue(self, request_id: str) -> None:
        async with self._lock:
            self._data_queue.pop(request_id, None)

    async def send_to_worker(self, msg: RelayMessage) -> bool:
        if not self.worker_connected or not self._authenticated.is_set():
            logger.warning("cannot send: worker not connected/authenticated")
            return False
        return await self._send_raw(msg)

    async def _send_raw(self, msg: RelayMessage) -> bool:
        if not self._worker or not self._worker.open:
            return False
        try:
            async with self._send_lock:
                await self._worker.send(msg.to_json())
            return True
        except Exception as exc:
            logger.error("send to worker failed: %s", exc)
            return False
