from __future__ import annotations

import asyncio
import base64
import logging
from typing import TYPE_CHECKING

from protocol import (
    RelayMessage,
    new_request_id,
)

if TYPE_CHECKING:
    from connection_manager import ConnectionManager
    from worker_bridge import WorkerBridge

logger = logging.getLogger("relay.client_listener")

BUFFER_SIZE = 65536


class ClientListener:
    def __init__(
        self,
        connection_manager: ConnectionManager,
        worker_bridge: WorkerBridge,
        host: str = "0.0.0.0",
        port: int = 3128,
    ) -> None:
        self._cm = connection_manager
        self._bridge = worker_bridge
        self._host = host
        self._port = port
        self._server: asyncio.Server | None = None
        self._client_counter = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client,
            self._host,
            self._port,
        )
        logger.info("client listener on %s:%d", self._host, self._port)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._client_counter += 1
        client_id = f"client-{self._client_counter}"
        peer = writer.get_extra_info("peername")
        conn_id = new_request_id()
        logger.info("new client: %s from %s (conn=%s)", client_id, peer, conn_id)

        from connection_manager import ClientConnection
        conn = ClientConnection(
            request_id=conn_id, client_id=client_id,
            writer=writer,
        )
        await self._cm.register(conn)

        try:
            first_chunk = await asyncio.wait_for(reader.read(BUFFER_SIZE), timeout=10)
            if not first_chunk:
                await self._cm.close_client(conn_id)
                return

            req_msg = RelayMessage.make_request(
                request_id=conn_id,
                client_id=client_id,
                data=base64.b64encode(first_chunk).decode(),
            )

            if not await self._bridge.send_to_worker(req_msg):
                logger.error("failed to send to worker: %s", conn_id)
                await self._cm.close_client(conn_id)
                return

            await self._relay_client_to_worker(reader, conn_id, client_id)

        except asyncio.TimeoutError:
            logger.warning("client timeout: %s", client_id)
        except (ConnectionError, asyncio.IncompleteReadError):
            logger.info("client disconnected early: %s", client_id)
        except Exception as exc:
            logger.error("client error: %s", exc, exc_info=True)
        finally:
            await self._cm.close_client(conn_id)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _relay_client_to_worker(
        self, reader: asyncio.StreamReader, conn_id: str, client_id: str
    ) -> None:
        try:
            while True:
                chunk = await asyncio.wait_for(reader.read(BUFFER_SIZE), timeout=300)
                if not chunk:
                    logger.info("client %s sent EOF", client_id)
                    await self._bridge.send_to_worker(RelayMessage.make_close(conn_id))
                    break

                msg = RelayMessage.make_data(
                    request_id=conn_id,
                    data=base64.b64encode(chunk).decode(),
                )
                if not await self._bridge.send_to_worker(msg):
                    logger.error("send to worker failed: %s", conn_id)
                    break

        except asyncio.TimeoutError:
            logger.info("client %s idle timeout", client_id)
            await self._bridge.send_to_worker(RelayMessage.make_close(conn_id))
        except (ConnectionError, asyncio.IncompleteReadError):
            logger.info("client %s disconnected", client_id)
            await self._bridge.send_to_worker(RelayMessage.make_close(conn_id))
        except Exception as exc:
            logger.error("relay error: %s", exc)
            await self._bridge.send_to_worker(RelayMessage.make_close(conn_id))
