from __future__ import annotations

import asyncio
import base64
import logging
from typing import Awaitable, Callable

from ..common.protocol import MessageType, RelayMessage

logger = logging.getLogger("proxy-ssh-client.relay")


class TCPRelay:
    """Transparent TCP relay - forwards all bytes bidirectionally without modification."""

    BUFFER_SIZE = 65536

    def __init__(
        self,
        send_callback: Callable[[RelayMessage], Awaitable[bool]],
        upstream_port: int = 4096,
    ) -> None:
        self._send = send_callback
        self._upstream_port = upstream_port
        self._connections: dict[str, asyncio.StreamWriter] = {}
        self._tasks: dict[str, asyncio.Task] = {}

    async def handle(self, msg: RelayMessage) -> None:
        if msg.msg_type == MessageType.REQUEST:
            asyncio.create_task(self._on_new_connection(msg))
        elif msg.msg_type == MessageType.DATA:
            await self._on_data(msg)
        elif msg.msg_type == MessageType.CLOSE:
            await self._on_close(msg)

    async def _on_new_connection(self, msg: RelayMessage) -> None:
        conn_id = msg.request_id
        logger.info("new connection: id=%s", conn_id)
        task = asyncio.create_task(self._relay_loop(conn_id, msg))
        self._tasks[conn_id] = task

    async def _on_data(self, msg: RelayMessage) -> None:
        writer = self._connections.get(msg.request_id)
        if writer and not writer.is_closing():
            try:
                data = base64.b64decode(msg.payload_data) if msg.payload_data else b""
                if data:
                    writer.write(data)
                    await writer.drain()
            except Exception as exc:
                logger.error("data forward failed: %s", exc)
                await self._close_connection(msg.request_id)

    async def _on_close(self, msg: RelayMessage) -> None:
        await self._close_connection(msg.request_id)

    async def _relay_loop(self, conn_id: str, init_msg: RelayMessage) -> None:
        reader: asyncio.StreamReader | None = None
        writer: asyncio.StreamWriter | None = None
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", self._upstream_port)
            self._connections[conn_id] = writer
            logger.info("upstream connected: id=%s", conn_id)

            initial_data = base64.b64decode(init_msg.payload_data) if init_msg.payload_data else b""
            if initial_data:
                writer.write(initial_data)
                await writer.drain()

            upstream_to_server = asyncio.create_task(
                self._pipe_upstream_to_server(conn_id, reader)
            )
            self._tasks[f"{conn_id}_upstream"] = upstream_to_server
            await upstream_to_server

        except Exception as exc:
            logger.error("relay error: id=%s err=%s", conn_id, exc)
            await self._send(RelayMessage.make_error(conn_id, str(exc)))
        finally:
            if writer and not writer.is_closing():
                try:
                    writer.close()
                except Exception:
                    pass
            self._connections.pop(conn_id, None)
            self._tasks.pop(conn_id, None)
            self._tasks.pop(f"{conn_id}_upstream", None)
            logger.debug("relay cleaned up: id=%s", conn_id)

    async def _pipe_upstream_to_server(self, conn_id: str, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                chunk = await reader.read(self.BUFFER_SIZE)
                if not chunk:
                    break
                ok = await self._send(RelayMessage.make_data(
                    request_id=conn_id,
                    data=base64.b64encode(chunk).decode(),
                ))
                if not ok:
                    logger.warning("send to server failed, closing: %s", conn_id)
                    break
        except asyncio.CancelledError:
            pass
        except (ConnectionError, asyncio.IncompleteReadError):
            logger.info("upstream closed: %s", conn_id)
        except asyncio.TimeoutError:
            logger.info("upstream timeout: %s", conn_id)
        except Exception as exc:
            logger.error("upstream read error: %s", exc)

        await self._send(RelayMessage.make_end(conn_id))

    async def _close_connection(self, conn_id: str) -> None:
        writer = self._connections.pop(conn_id, None)
        if writer and not writer.is_closing():
            try:
                writer.close()
            except Exception:
                pass
        task = self._tasks.pop(conn_id, None)
        upstream_task = self._tasks.pop(f"{conn_id}_upstream", None)
        if upstream_task and not upstream_task.done():
            upstream_task.cancel()
        if task and not task.done():
            task.cancel()

    async def cleanup_all(self) -> None:
        for conn_id in list(self._connections.keys()):
            await self._close_connection(conn_id)
