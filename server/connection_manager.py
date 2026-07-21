from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger("relay.connection_manager")


@dataclass
class ClientConnection:
    request_id: str
    client_id: str
    writer: asyncio.StreamWriter
    connected_at: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)

    def touch(self) -> None:
        self.last_activity = time.time()

    @property
    def idle_time(self) -> float:
        return time.time() - self.last_activity


class ConnectionManager:
    def __init__(self, client_idle_timeout: float = 300) -> None:
        self._clients: dict[str, ClientConnection] = {}
        self._lock = asyncio.Lock()
        self._client_idle_timeout = client_idle_timeout

    @property
    def active_count(self) -> int:
        return len(self._clients)

    async def register(self, conn: ClientConnection) -> None:
        async with self._lock:
            self._clients[conn.request_id] = conn
        logger.info(
            "client registered: request_id=%s client_id=%s",
            conn.request_id, conn.client_id,
        )

    async def unregister(self, request_id: str) -> ClientConnection | None:
        async with self._lock:
            return self._clients.pop(request_id, None)

    async def get(self, request_id: str) -> ClientConnection | None:
        async with self._lock:
            return self._clients.get(request_id)

    async def send_to_client(self, request_id: str, data: bytes) -> bool:
        conn = await self.get(request_id)
        if conn is None:
            logger.warning("send_to_client: unknown request_id=%s", request_id)
            return False
        try:
            conn.writer.write(data)
            await conn.writer.drain()
            conn.touch()
            return True
        except (ConnectionError, OSError, asyncio.CancelledError) as exc:
            logger.error("send_to_client failed: request_id=%s error=%s", request_id, exc)
            return False

    async def close_client(self, request_id: str) -> None:
        conn = await self.unregister(request_id)
        if conn is None:
            return
        try:
            conn.writer.close()
            await conn.writer.wait_closed()
        except Exception:
            pass
        logger.info("client connection closed: request_id=%s", request_id)

    async def cleanup_idle(self) -> None:
        while True:
            await asyncio.sleep(30)
            now = time.time()
            async with self._lock:
                idle = [
                    rid for rid, c in self._clients.items()
                    if c.idle_time > self._client_idle_timeout
                ]
            for rid in idle:
                logger.info("closing idle client: request_id=%s", rid)
                await self.close_client(rid)

    async def close_all(self) -> None:
        async with self._lock:
            ids = list(self._clients.keys())
        for rid in ids:
            await self.close_client(rid)
