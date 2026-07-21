from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from ..common.json_lib import _ensure_websockets, _dumps
from ..common.protocol import MessageType, RelayMessage

logger = logging.getLogger("proxy-ssh-client.connector")


class ServerConnector:
    def __init__(
        self,
        server_url: str,
        auth_token: str,
        on_message: Callable[[RelayMessage], Awaitable[None]],
        tls_verify: bool = True,
        ping_interval: int = 20,
        reconnect_delay: int = 5,
        max_reconnect_delay: int = 60,
    ) -> None:
        self._server_url = server_url
        self._auth_token = auth_token
        self._on_message = on_message
        self._tls_verify = tls_verify
        self._ping_interval = ping_interval
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._ws = None
        self._connected = asyncio.Event()
        self._authenticated = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._should_run = False
        self._reconnect_count = 0
        self._last_error = ""

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._ws.open and self._authenticated.is_set()

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    @property
    def last_error(self) -> str:
        return self._last_error

    async def run(self) -> None:
        _ensure_websockets()
        import websockets.asyncio.client
        self._should_run = True
        delay = self._reconnect_delay
        while self._should_run:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._last_error = str(exc)
                logger.error("connection lost: %s", exc)
                self._reconnect_count += 1
            if not self._should_run:
                break
            logger.info("reconnecting in %ds...", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._max_reconnect_delay)
        self._connected.clear()
        self._authenticated.clear()

    async def _connect_and_listen(self) -> None:
        import websockets.asyncio.client
        import ssl as _ssl
        ssl_ctx = None
        if self._server_url.startswith("wss://"):
            ssl_ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
            if not self._tls_verify:
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = _ssl.CERT_NONE
        logger.info("connecting to %s", self._server_url)
        async with websockets.asyncio.client.connect(
            self._server_url, ssl=ssl_ctx,
            ping_interval=self._ping_interval, ping_timeout=10, max_size=None,
        ) as ws:
            self._ws = ws
            self._connected.set()
            await ws.send(RelayMessage.make_auth(self._auth_token).to_json())
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            resp = RelayMessage.from_json(raw if isinstance(raw, bytes) else raw.encode())
            if resp.msg_type == MessageType.AUTH_OK:
                self._authenticated.set()
                self._last_error = ""
                logger.info("authenticated with server")
            else:
                self._last_error = f"auth failed: {resp.error_msg}"
                logger.error("authentication failed: %s", resp.error_msg)
                return
            async for raw in ws:
                msg = RelayMessage.from_json(raw if isinstance(raw, bytes) else raw.encode())
                if msg.msg_type == MessageType.PING:
                    async with self._send_lock:
                        await ws.send(RelayMessage.make_pong().to_json())
                else:
                    await self._on_message(msg)

    async def send(self, msg: RelayMessage) -> bool:
        if not self.connected:
            return False
        try:
            async with self._send_lock:
                await self._ws.send(msg.to_json())
            return True
        except Exception as exc:
            logger.error("send failed: %s", exc)
            self._last_error = str(exc)
            return False

    async def stop(self) -> None:
        self._should_run = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
