#!/usr/bin/env python3
"""
proxy-ssh - SSH Tunnel + WebSocket Relay Client
A single-file relay service that manages SSH tunnels and connects to a relay server.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import getpass
import hashlib
import json
import logging
import os
import platform
import secrets
import signal
import socket
import stat
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

websockets = None  # lazy import, required only at runtime


def _ensure_websockets():
    global websockets
    if websockets is None:
        try:
            import websockets as _ws
            import websockets.asyncio.client
            websockets = _ws
        except ImportError:
            print("ERROR: websockets not installed. Run: pip install websockets")
            sys.exit(1)

try:
    import orjson as _json_lib

    def _dumps(obj: Any) -> bytes:
        return _json_lib.dumps(obj)

    def _loads(raw: bytes) -> dict:
        return _json_lib.loads(raw)
except ImportError:
    import json as _json_lib

    def _dumps(obj: Any) -> bytes:
        return _json_lib.dumps(obj).encode()

    def _loads(raw: bytes) -> dict:
        return _json_lib.loads(raw)


__version__ = "1.0.0"
__program__ = "proxy-ssh"


# ============================================================
# PROTOCOL
# ============================================================

class MessageType(str, Enum):
    REQUEST = "request"
    DATA = "data"
    CLOSE = "close"
    END = "end"
    ERROR = "error"
    PING = "ping"
    PONG = "pong"
    AUTH = "auth"
    AUTH_OK = "auth_ok"
    AUTH_FAIL = "auth_fail"


def new_request_id() -> str:
    return uuid.uuid4().hex[:16]


@dataclass
class RelayMessage:
    msg_type: MessageType
    request_id: str = ""
    payload_data: str = ""
    client_id: str = ""
    protocol: str = ""
    status: int = 0
    http_method: str = ""
    path: str = ""
    msg_headers: dict[str, str] = field(default_factory=dict)
    error_msg: str = ""
    token: str = ""

    def to_json(self) -> bytes:
        d: dict[str, Any] = {"type": self.msg_type.value}
        if self.request_id:
            d["request_id"] = self.request_id
        if self.client_id:
            d["client_id"] = self.client_id
        if self.protocol:
            d["protocol"] = self.protocol
        if self.payload_data:
            d["data"] = self.payload_data
        if self.status:
            d["status"] = self.status
        if self.http_method:
            d["method"] = self.http_method
        if self.path:
            d["path"] = self.path
        if self.msg_headers:
            d["headers"] = self.msg_headers
        if self.error_msg:
            d["error"] = self.error_msg
        if self.token:
            d["token"] = self.token
        return _dumps(d)

    @classmethod
    def from_json(cls, raw: bytes) -> RelayMessage:
        d = _loads(raw)
        return cls(
            msg_type=MessageType(d.get("type", "data")),
            request_id=d.get("request_id", ""),
            payload_data=d.get("data", ""),
            client_id=d.get("client_id", ""),
            protocol=d.get("protocol", ""),
            status=d.get("status", 0),
            http_method=d.get("method", ""),
            path=d.get("path", ""),
            msg_headers=d.get("headers", {}),
            error_msg=d.get("error", ""),
            token=d.get("token", ""),
        )

    @classmethod
    def make_request(cls, request_id: str, client_id: str = "", data: str = "") -> RelayMessage:
        return cls(msg_type=MessageType.REQUEST, request_id=request_id, client_id=client_id,
                   payload_data=data)

    @classmethod
    def make_data(cls, request_id: str, data: str) -> RelayMessage:
        return cls(msg_type=MessageType.DATA, request_id=request_id, payload_data=data)

    @classmethod
    def make_close(cls, request_id: str) -> RelayMessage:
        return cls(msg_type=MessageType.CLOSE, request_id=request_id)

    @classmethod
    def make_end(cls, request_id: str) -> RelayMessage:
        return cls(msg_type=MessageType.END, request_id=request_id)

    @classmethod
    def make_error(cls, request_id: str, error: str) -> RelayMessage:
        return cls(msg_type=MessageType.ERROR, request_id=request_id, error_msg=error)

    @classmethod
    def make_auth(cls, token: str) -> RelayMessage:
        return cls(msg_type=MessageType.AUTH, token=token)

    @classmethod
    def make_auth_ok(cls) -> RelayMessage:
        return cls(msg_type=MessageType.AUTH_OK)

    @classmethod
    def make_auth_fail(cls, error: str = "authentication failed") -> RelayMessage:
        return cls(msg_type=MessageType.AUTH_FAIL, error_msg=error)

    @classmethod
    def make_ping(cls) -> RelayMessage:
        return cls(msg_type=MessageType.PING)

    @classmethod
    def make_pong(cls) -> RelayMessage:
        return cls(msg_type=MessageType.PONG)


# ============================================================
# CONFIG
# ============================================================

APP_NAME = "proxy-ssh"

if platform.system() == "Windows":
    DEFAULT_BASE_DIR = Path(os.environ.get("APPDATA", "~")) / APP_NAME
else:
    DEFAULT_BASE_DIR = Path.home() / ".config" / APP_NAME


def _get_machine_key() -> str:
    system = platform.system()
    if system == "Linux":
        machine_id = ""
        try:
            machine_id = Path("/etc/machine-id").read_text().strip()
        except Exception:
            pass
        user = getpass.getuser()
        return hashlib.sha256(f"{machine_id}:{user}".encode()).hexdigest()
    elif system == "Darwin":
        serial = ""
        try:
            serial = os.popen(
                "ioreg -rd1 -c IOPlatformExpertDevice 2>/dev/null "
                "| grep IOPlatformSerialNumber | awk '{print $NF}' | tr -d '\"'"
            ).read().strip()
        except Exception:
            pass
        user = getpass.getuser()
        return hashlib.sha256(f"{serial}:{user}".encode()).hexdigest()
    else:
        return hashlib.sha256(f"{platform.node()}:{getpass.getuser()}".encode()).hexdigest()


def _xor_encrypt(data: bytes, key: bytes) -> bytes:
    return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))


def _derive_key(password: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)


def encrypt_secret(plaintext: str, machine_key: str) -> str:
    salt = secrets.token_bytes(16)
    key = _derive_key(machine_key, salt)
    iv = secrets.token_bytes(16)
    encrypted = _xor_encrypt(plaintext.encode("utf-8"), key)
    return (salt + iv + encrypted).hex()


def decrypt_secret(ciphertext: str, machine_key: str) -> str:
    try:
        raw = bytes.fromhex(ciphertext)
        salt = raw[:16]
        _iv = raw[16:32]
        encrypted = raw[32:]
        key = _derive_key(machine_key, salt)
        return _xor_encrypt(encrypted, key).decode("utf-8")
    except Exception:
        return ""


@dataclass
class SSHConfig:
    host: str = ""
    port: int = 22
    username: str = ""
    auth_method: str = "key"
    key_path: str = ""
    key_passphrase: str = ""
    password: str = ""
    local_forward_port: int = 4096
    remote_forward_port: int = 4096


@dataclass
class ServerConfig:
    url: str = ""
    auth_token: str = ""
    tls_verify: bool = True


@dataclass
class WorkerConfig:
    ssh: SSHConfig = field(default_factory=SSHConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    version: str = __version__
    first_run: bool = True
    log_level: str = "INFO"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> WorkerConfig:
        ssh_data = d.get("ssh", {})
        server_data = d.get("server", {})
        return cls(
            ssh=SSHConfig(**{k: v for k, v in ssh_data.items() if k in SSHConfig.__dataclass_fields__}),
            server=ServerConfig(**{k: v for k, v in server_data.items() if k in ServerConfig.__dataclass_fields__}),
            version=d.get("version", __version__),
            first_run=d.get("first_run", True),
            log_level=d.get("log_level", "INFO"),
        )


class ConfigManager:
    def __init__(self) -> None:
        self._config_dir = DEFAULT_BASE_DIR
        self._config_path = DEFAULT_BASE_DIR / "config.json"
        self._machine_key = _get_machine_key()
        self._config: WorkerConfig | None = None

    @property
    def config_dir(self) -> Path:
        return self._config_dir

    @property
    def config_path(self) -> Path:
        return self._config_path

    @property
    def log_path(self) -> Path:
        return self._config_dir / "logs" / "proxy-ssh.log"

    @property
    def fingerprint_path(self) -> Path:
        return self._config_dir / "fingerprint"

    def ensure_dirs(self) -> None:
        self._config_dir.mkdir(parents=True, exist_ok=True)
        (self._config_dir / "logs").mkdir(exist_ok=True)

    def exists(self) -> bool:
        return self._config_path.exists()

    def load(self) -> WorkerConfig:
        if self._config:
            return self._config
        if not self._config_path.exists():
            raise FileNotFoundError(f"Config not found: {self._config_path}")
        with open(self._config_path, "r") as f:
            raw = json.load(f)
        self._config = WorkerConfig.from_dict(raw)
        if self._config.ssh.password:
            self._config.ssh.password = decrypt_secret(self._config.ssh.password, self._machine_key)
        if self._config.ssh.key_passphrase:
            self._config.ssh.key_passphrase = decrypt_secret(self._config.ssh.key_passphrase, self._machine_key)
        if self._config.server.auth_token:
            self._config.server.auth_token = decrypt_secret(self._config.server.auth_token, self._machine_key)
        return self._config

    def save(self, config: WorkerConfig) -> None:
        self.ensure_dirs()
        data = config.to_dict()
        if config.ssh.password:
            data["ssh"]["password"] = encrypt_secret(config.ssh.password, self._machine_key)
        if config.ssh.key_passphrase:
            data["ssh"]["key_passphrase"] = encrypt_secret(config.ssh.key_passphrase, self._machine_key)
        if config.server.auth_token:
            data["server"]["auth_token"] = encrypt_secret(config.server.auth_token, self._machine_key)
        with open(self._config_path, "w") as f:
            json.dump(data, f, indent=2)
        os.chmod(self._config_path, stat.S_IRUSR | stat.S_IWUSR)
        self._config = config

    def delete(self) -> None:
        if self._config_path.exists():
            self._config_path.unlink()
        self._config = None


# ============================================================
# LOGGING
# ============================================================

def setup_logging(level: str = "INFO", log_path: Path | None = None) -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    if root.handlers:
        root.handlers.clear()
    fmt = logging.Formatter(
        fmt="[%(asctime)s] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(getattr(logging, level.upper(), logging.INFO))
    console.setFormatter(fmt)
    root.addHandler(console)
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        fh.setLevel(getattr(logging, level.upper(), logging.INFO))
        fh.setFormatter(fmt)
        root.addHandler(fh)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


# ============================================================
# SSH MANAGER
# ============================================================

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
        self._logger = logging.getLogger("proxy-ssh.ssh")

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
        self._logger.info("starting SSH tunnel: %s@%s:%d", self._username, self._host, self._port)
        try:
            self._process = subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL,
            )
            self._info.pid = self._process.pid
            self._logger.info("SSH process started PID %d", self._process.pid)
            return True
        except FileNotFoundError:
            self._logger.error("ssh command not found")
            self._info.last_error = "ssh command not found"
            return False
        except Exception as exc:
            self._logger.error("failed to start SSH: %s", exc)
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
            self._logger.info("SSH process killed PID %s", self._info.pid)
        self._process = None
        self._info.pid = None

    async def _monitor_loop(self) -> None:
        while self._should_run:
            if self._info.status in (TunnelStatus.DISCONNECTED, TunnelStatus.RECONNECTING):
                await self._set_status(TunnelStatus.CONNECTING if self._info.status == TunnelStatus.DISCONNECTED else TunnelStatus.RECONNECTING)
                success = self._start_process()
                if success:
                    await asyncio.sleep(3)
                    if self._process and self._process.poll() is None:
                        await self._set_status(TunnelStatus.CONNECTED)
                        self._info.started_at = time.time()
                        self._logger.info("SSH tunnel established")
                    else:
                        self._info.last_error = "SSH exited during startup"
                        self._logger.error("SSH tunnel failed to start")
                        await self._set_status(TunnelStatus.RECONNECTING)
                        self._info.reconnect_count += 1
                        delay = min(5 * (2 ** min(self._info.reconnect_count, 6)), 120)
                        self._logger.info("reconnecting in %ds...", delay)
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
                    self._logger.warning("SSH tunnel disconnected (exit code: %s)", exit_code)
                    self._process = None
                    self._info.pid = None
                    self._info.reconnect_count += 1
                    await self._set_status(TunnelStatus.RECONNECTING)
                    delay = min(5 * (2 ** min(self._info.reconnect_count, 6)), 120)
                    self._logger.info("reconnecting in %ds...", delay)
                    await asyncio.sleep(delay)

    async def _set_status(self, new_status: TunnelStatus) -> None:
        old = self._info.status
        self._info.status = new_status
        if old != new_status:
            self._logger.info("SSH status: %s -> %s", old.value, new_status.value)
            if self._on_status_change:
                try:
                    await self._on_status_change(new_status)
                except Exception:
                    pass


# ============================================================
# SERVER CONNECTOR
# ============================================================

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
        self._ws: websockets.asyncio.client.ClientConnection | None = None
        self._connected = asyncio.Event()
        self._authenticated = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._should_run = False
        self._reconnect_count = 0
        self._last_error = ""
        self._logger = logging.getLogger("proxy-ssh.connector")

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
        self._should_run = True
        delay = self._reconnect_delay
        while self._should_run:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._last_error = str(exc)
                self._logger.error("connection lost: %s", exc)
                self._reconnect_count += 1
            if not self._should_run:
                break
            self._logger.info("reconnecting in %ds...", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._max_reconnect_delay)
        self._connected.clear()
        self._authenticated.clear()

    async def _connect_and_listen(self) -> None:
        import ssl
        ssl_ctx = None
        if self._server_url.startswith("wss://"):
            ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            if not self._tls_verify:
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE
        self._logger.info("connecting to %s", self._server_url)
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
                self._logger.info("authenticated with server")
            else:
                self._last_error = f"auth failed: {resp.error_msg}"
                self._logger.error("authentication failed: %s", resp.error_msg)
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
            self._logger.error("send failed: %s", exc)
            self._last_error = str(exc)
            return False

    async def stop(self) -> None:
        self._should_run = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass


# ============================================================
# TRANSPARENT TCP RELAY
# ============================================================

class TCPRelay:
    """Transparent TCP relay - forwards all bytes bidirectionally without modification."""

    BUFFER_SIZE = 65536

    def __init__(self, send_callback: Callable[[RelayMessage], Awaitable[bool]], upstream_port: int = 4096) -> None:
        self._send = send_callback
        self._upstream_port = upstream_port
        self._connections: dict[str, asyncio.StreamWriter] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._logger = logging.getLogger("proxy-ssh.relay")

    async def handle(self, msg: RelayMessage) -> None:
        if msg.msg_type == MessageType.REQUEST:
            asyncio.create_task(self._on_new_connection(msg))
        elif msg.msg_type == MessageType.DATA:
            await self._on_data(msg)
        elif msg.msg_type == MessageType.CLOSE:
            await self._on_close(msg)

    async def _on_new_connection(self, msg: RelayMessage) -> None:
        conn_id = msg.request_id
        self._logger.info("new connection: id=%s", conn_id)
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
                self._logger.error("data forward failed: %s", exc)
                await self._close_connection(msg.request_id)

    async def _on_close(self, msg: RelayMessage) -> None:
        await self._close_connection(msg.request_id)

    async def _relay_loop(self, conn_id: str, init_msg: RelayMessage) -> None:
        reader: asyncio.StreamReader | None = None
        writer: asyncio.StreamWriter | None = None
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", self._upstream_port)
            self._connections[conn_id] = writer
            self._logger.info("upstream connected: id=%s", conn_id)

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
            self._logger.error("relay error: id=%s err=%s", conn_id, exc)
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
            self._logger.debug("relay cleaned up: id=%s", conn_id)

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
                    self._logger.warning("send to server failed, closing: %s", conn_id)
                    break
        except asyncio.CancelledError:
            pass
        except (ConnectionError, asyncio.IncompleteReadError):
            self._logger.info("upstream closed: %s", conn_id)
        except asyncio.TimeoutError:
            self._logger.info("upstream timeout: %s", conn_id)
        except Exception as exc:
            self._logger.error("upstream read error: %s", exc)

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


# ============================================================
# STATUS
# ============================================================

class StatusCollector:
    @staticmethod
    def format_uptime(seconds: float) -> str:
        if seconds <= 0:
            return "N/A"
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        parts.append(f"{secs}s")
        return " ".join(parts)

    @staticmethod
    def check_process_running() -> tuple[bool, int | None]:
        pid_file = DEFAULT_BASE_DIR / "proxy-ssh.pid"
        if not pid_file.exists():
            return False, None
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)
            return True, pid
        except (ProcessLookupError, ValueError, PermissionError):
            return False, None

    @staticmethod
    def write_pid() -> None:
        pid_file = DEFAULT_BASE_DIR / "proxy-ssh.pid"
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.write_text(str(os.getpid()))

    @staticmethod
    def remove_pid() -> None:
        pid_file = DEFAULT_BASE_DIR / "proxy-ssh.pid"
        if pid_file.exists():
            pid_file.unlink()

    @staticmethod
    def print_status(ssh_info: SSHTunnelInfo, server_connected: bool, server_error: str = "", reconnect_count: int = 0) -> None:
        colors = {
            "connected": "\033[92m", "connecting": "\033[93m",
            "reconnecting": "\033[93m", "disconnected": "\033[91m", "failed": "\033[91m",
        }
        R = "\033[0m"
        sc = colors.get(ssh_info.status.value, "")
        srv_c = "\033[92m" if server_connected else "\033[91m"
        print()
        print("  \033[1mproxy-ssh Status\033[0m")
        print("  " + "─" * 40)
        print()
        print("  \033[1mSSH Tunnel\033[0m")
        print(f"  Status:     {sc}{ssh_info.status.value.upper()}{R}")
        print(f"  PID:        {ssh_info.pid or 'N/A'}")
        print(f"  Uptime:     {StatusCollector.format_uptime(ssh_info.uptime)}")
        print(f"  Reconnects: {ssh_info.reconnect_count}")
        if ssh_info.last_error:
            print(f"  Last Error: {ssh_info.last_error[:80]}")
        print()
        print("  \033[1mServer\033[0m")
        print(f"  Status:     {srv_c}{'CONNECTED' if server_connected else 'DISCONNECTED'}{R}")
        print(f"  Reconnects: {reconnect_count}")
        if server_error:
            print(f"  Last Error: {server_error[:80]}")
        print()


# ============================================================
# DOCTOR
# ============================================================

class CheckResult:
    def __init__(self, name: str, passed: bool, message: str, fix: str = "") -> None:
        self.name = name
        self.passed = passed
        self.message = message
        self.fix = fix

    def __str__(self) -> str:
        icon = "\033[92m✓\033[0m" if self.passed else "\033[91m✗\033[0m"
        return f"  {icon} {self.name}: {self.message}"


class Doctor:
    def __init__(self, config_manager: ConfigManager) -> None:
        self._cm = config_manager

    async def run(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        results.append(self._check_ssh())
        results.append(self._check_config())
        if self._cm.exists():
            results.append(self._check_key_permissions())
            results.extend(await self._check_server())
        results.append(self._check_internet())
        return results

    def _check_ssh(self) -> CheckResult:
        try:
            r = subprocess.run(["ssh", "-V"], capture_output=True, text=True, timeout=5)
            version = r.stderr.strip() if r.stderr else r.stdout.strip()
            return CheckResult("SSH Client", True, version)
        except FileNotFoundError:
            return CheckResult("SSH Client", False, "not found", "apt install openssh-client / brew install openssh")
        except Exception as exc:
            return CheckResult("SSH Client", False, str(exc))

    def _check_config(self) -> CheckResult:
        if not self._cm.exists():
            return CheckResult("Configuration", False, "not found", "Run: proxy-ssh setup")
        try:
            config = self._cm.load()
            if not config.ssh.host:
                return CheckResult("Configuration", False, "SSH host not set", "Run: proxy-ssh setup")
            if not config.server.url:
                return CheckResult("Configuration", False, "server URL not set", "Run: proxy-ssh setup")
            return CheckResult("Configuration", True, f"host={config.ssh.host} user={config.ssh.username}")
        except Exception as exc:
            return CheckResult("Configuration", False, str(exc), "Run: proxy-ssh setup")

    def _check_key_permissions(self) -> CheckResult:
        try:
            config = self._cm.load()
            if config.ssh.auth_method == "key" and config.ssh.key_path:
                key = Path(config.ssh.key_path).expanduser()
                if not key.exists():
                    return CheckResult("SSH Key", False, f"not found: {key}", "Run: proxy-ssh setup")
                mode = key.stat().st_mode
                if mode & 0o077:
                    return CheckResult("SSH Key Permissions", False, f"too open: {oct(mode)}", f"chmod 600 {key}")
                return CheckResult("SSH Key", True, str(key))
            return CheckResult("SSH Key", True, "using password auth")
        except Exception as exc:
            return CheckResult("SSH Key", False, str(exc))

    async def _check_server(self) -> list[CheckResult]:
        results = []
        try:
            from urllib.parse import urlparse
            config = self._cm.load()
            parsed = urlparse(config.server.url)
            host = parsed.hostname or ""
            port = parsed.port or 443
            result = await asyncio.get_event_loop().run_in_executor(None, self._check_tcp, host, port)
            results.append(result)
        except Exception as exc:
            results.append(CheckResult("Server", False, str(exc)))
        return results

    @staticmethod
    def _check_tcp(host: str, port: int) -> CheckResult:
        try:
            sock = socket.create_connection((host, port), timeout=10)
            sock.close()
            return CheckResult("Server Reachable", True, f"{host}:{port}")
        except socket.timeout:
            return CheckResult("Server Reachable", False, f"timeout {host}:{port}", "Check server address/firewall")
        except Exception as exc:
            return CheckResult("Server Reachable", False, str(exc))

    @staticmethod
    def _check_internet() -> CheckResult:
        try:
            sock = socket.create_connection(("1.1.1.1", 53), timeout=5)
            sock.close()
            return CheckResult("Internet", True, "reachable")
        except Exception:
            return CheckResult("Internet", False, "no connection", "Check network")


# ============================================================
# WORKER
# ============================================================

class RelayWorker:
    def __init__(self, config: WorkerConfig, config_manager: ConfigManager) -> None:
        self._config = config
        self._cm = config_manager
        self._ssh: SSHManager | None = None
        self._connector: ServerConnector | None = None
        self._relay: TCPRelay | None = None
        self._stop_event = asyncio.Event()
        self._start_time: float = 0
        self._logger = logging.getLogger("proxy-ssh.worker")

    async def run(self) -> None:
        self._start_time = time.time()
        self._ssh = SSHManager(
            host=self._config.ssh.host, port=self._config.ssh.port,
            username=self._config.ssh.username, auth_method=self._config.ssh.auth_method,
            key_path=self._config.ssh.key_path, key_passphrase=self._config.ssh.key_passphrase,
            password=self._config.ssh.password, local_forward_port=self._config.ssh.local_forward_port,
            remote_forward_port=self._config.ssh.remote_forward_port,
            on_status_change=self._on_ssh_status,
        )
        self._connector = ServerConnector(
            server_url=self._config.server.url, auth_token=self._config.server.auth_token,
            on_message=self._on_message, tls_verify=self._config.server.tls_verify,
        )
        self._relay = TCPRelay(
            send_callback=self._connector.send,
            upstream_port=self._config.ssh.local_forward_port,
        )
        self._logger.info("starting proxy-ssh worker...")
        self._logger.info("ssh: %s@%s:%d", self._config.ssh.username, self._config.ssh.host, self._config.ssh.port)
        self._logger.info("server: %s", self._config.server.url)
        StatusCollector.write_pid()
        ssh_task = asyncio.create_task(self._ssh.start())
        server_task = asyncio.create_task(self._connector.run())
        try:
            await self._stop_event.wait()
        finally:
            self._logger.info("shutting down...")
            await self._connector.stop()
            await self._ssh.stop()
            if self._relay:
                await self._relay.cleanup_all()
            server_task.cancel()
            ssh_task.cancel()
            for t in [server_task, ssh_task]:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            StatusCollector.remove_pid()
            self._logger.info("proxy-ssh worker stopped")

    async def stop(self) -> None:
        self._stop_event.set()

    async def _on_ssh_status(self, status: TunnelStatus) -> None:
        if status == TunnelStatus.CONNECTED:
            self._logger.info("ssh tunnel established")
        elif status == TunnelStatus.RECONNECTING:
            self._logger.warning("ssh tunnel disconnected, reconnecting...")

    async def _on_message(self, msg: RelayMessage) -> None:
        if self._relay:
            await self._relay.handle(msg)

    def get_ssh_info(self) -> SSHTunnelInfo:
        if self._ssh:
            info = self._ssh.info
            if info.started_at:
                info.uptime = time.time() - info.started_at
            return info
        return SSHTunnelInfo()

    def get_server_connected(self) -> bool:
        return self._connector.connected if self._connector else False

    def get_server_error(self) -> str:
        return self._connector.last_error if self._connector else ""

    def get_server_reconnect_count(self) -> int:
        return self._connector.reconnect_count if self._connector else 0


# ============================================================
# SETUP WIZARD
# ============================================================

class SetupWizard:
    def __init__(self, config_manager: ConfigManager) -> None:
        self._cm = config_manager

    async def run(self, reset: bool = False) -> WorkerConfig:
        print()
        print("  \033[1m╔══════════════════════════════════════╗\033[0m")
        print("  \033[1m║        proxy-ssh - Setup             ║\033[0m")
        print("  \033[1m╚══════════════════════════════════════╝\033[0m")
        print()
        if self._cm.exists() and not reset:
            print("  Configuration already exists.")
            print("  Run 'proxy-ssh setup --reset' to reconfigure.\n")
            return self._cm.load()
        if reset:
            print("  \033[93mResetting configuration...\033[0m\n")
            self._cm.delete()
        ssh = self._collect_ssh()
        server = self._collect_server()
        config = WorkerConfig(ssh=ssh, server=server, first_run=False)
        print("\n  \033[93mVerifying SSH connection...\033[0m")
        ok, msg = await self._verify_ssh(ssh)
        if ok:
            print(f"  \033[92m✓ {msg}\033[0m")
        else:
            print(f"  \033[91m✗ {msg}\033[0m")
            resp = input("  Continue anyway? [y/N]: ").strip().lower()
            if resp != "y":
                print("  Setup cancelled.")
                return self._cm.load()
        self._cm.save(config)
        print(f"\n  \033[92m✓ Configuration saved!\033[0m")
        print(f"  Location: {self._cm.config_path}\n")
        print("  Next step: proxy-ssh start\n")
        return config

    def _collect_ssh(self) -> SSHConfig:
        print("  \033[1mSSH Configuration\033[0m")
        print("  " + "─" * 36 + "\n")
        host = input("  SSH Host: ").strip()
        while not host:
            print("  \033[91mHost cannot be empty.\033[0m")
            host = input("  SSH Host: ").strip()
        port_str = input("  SSH Port [22]: ").strip()
        port = int(port_str) if port_str.isdigit() else 22
        username = input("  SSH Username: ").strip()
        while not username:
            print("  \033[91mUsername cannot be empty.\033[0m")
            username = input("  SSH Username: ").strip()
        print("\n  Authentication:")
        print("    1) Private Key")
        print("    2) Password\n")
        while True:
            choice = input("  Select [1]: ").strip() or "1"
            if choice in ("1", "2"):
                break
            print("  \033[91mInvalid choice.\033[0m")
        auth_method = "key" if choice == "1" else "password"
        key_path, key_passphrase, password = "", "", ""
        if auth_method == "key":
            default_key = str(Path.home() / ".ssh" / "id_rsa")
            key_path = input(f"  Key Path [{default_key}]: ").strip() or default_key
            if Path(key_path).expanduser().exists():
                if input("  Key has passphrase? [y/N]: ").strip().lower() == "y":
                    key_passphrase = getpass("  Key Passphrase: ")
        else:
            password = getpass("  SSH Password: ")
        port_str = input("\n  Forward Port [4096]: ").strip()
        fwd = int(port_str) if port_str.isdigit() else 4096
        print()
        return SSHConfig(host=host, port=port, username=username, auth_method=auth_method,
                         key_path=key_path, key_passphrase=key_passphrase, password=password,
                         local_forward_port=fwd, remote_forward_port=fwd)

    def _collect_server(self) -> ServerConfig:
        print("  \033[1mServer Configuration\033[0m")
        print("  " + "─" * 36 + "\n")
        url = input("  Server URL (wss://...): ").strip()
        while not url:
            print("  \033[91mURL cannot be empty.\033[0m")
            url = input("  Server URL (wss://...): ").strip()
        token = input("  Auth Token: ").strip()
        while not token:
            print("  \033[91mToken cannot be empty.\033[0m")
            token = input("  Auth Token: ").strip()
        tls = input("  Verify TLS? [Y/n]: ").strip().lower() != "n"
        print()
        return ServerConfig(url=url, auth_token=token, tls_verify=tls)

    @staticmethod
    async def _verify_ssh(ssh: SSHConfig) -> tuple[bool, str]:
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
               "-o", "StrictHostKeyChecking=accept-new", "-p", str(ssh.port)]
        if ssh.auth_method == "key" and ssh.key_path:
            cmd.extend(["-i", ssh.key_path])
        cmd.extend([f"{ssh.username}@{ssh.host}", "echo", "proxy-ssh-ok"])
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
            if proc.returncode == 0:
                return True, f"connected to {ssh.host}"
            err = stderr.decode().strip()
            if "Permission denied" in err:
                return False, "authentication failed"
            return False, f"failed: {err[:100]}"
        except asyncio.TimeoutError:
            return False, "connection timeout"
        except FileNotFoundError:
            return False, "ssh not found"
        except Exception as exc:
            return False, str(exc)


# ============================================================
# CLI
# ============================================================

def cmd_setup(args: argparse.Namespace) -> None:
    cm = ConfigManager()
    cm.ensure_dirs()
    setup_logging("INFO")
    asyncio.run(SetupWizard(cm).run(reset=args.reset))


def cmd_start(_args: argparse.Namespace) -> None:
    _ensure_websockets()
    cm = ConfigManager()
    if not cm.exists():
        print("\033[91m  No configuration found. Run 'proxy-ssh setup' first.\033[0m\n")
        sys.exit(1)
    running, pid = StatusCollector.check_process_running()
    if running:
        print(f"\033[93m  Already running (PID {pid}).\033[0m\n")
        sys.exit(1)
    config = cm.load()
    setup_logging(config.log_level, cm.log_path)
    print("\033[92m  Starting proxy-ssh...\033[0m")
    worker = RelayWorker(config, cm)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    def _shutdown():
        loop.create_task(worker.stop())
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)
    try:
        loop.run_until_complete(worker.run())
    except KeyboardInterrupt:
        print("\n\033[93m  Shutting down...\033[0m")
        loop.run_until_complete(worker.stop())
    finally:
        loop.close()


def cmd_stop(_args: argparse.Namespace) -> None:
    running, pid = StatusCollector.check_process_running()
    if not running:
        print("\033[93m  No proxy-ssh running.\033[0m")
        return
    print(f"\033[93m  Stopping (PID {pid})...\033[0m")
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(2)
        try:
            os.kill(pid, 0)
            os.kill(pid, signal.SIGKILL)
            print("\033[92m  Force killed.\033[0m")
        except ProcessLookupError:
            print("\033[92m  Stopped.\033[0m")
    except PermissionError:
        print(f"\033[91m  Permission denied for PID {pid}.\033[0m")
    except ProcessLookupError:
        print("\033[92m  Already stopped.\033[0m")
    StatusCollector.remove_pid()


def cmd_restart(args: argparse.Namespace) -> None:
    cmd_stop(args)
    time.sleep(1)
    cmd_start(args)


def cmd_status(_args: argparse.Namespace) -> None:
    running, pid = StatusCollector.check_process_running()
    if not running:
        print("\n  \033[1mproxy-ssh Status\033[0m")
        print("  " + "─" * 40)
        print("\n  \033[91m  NOT RUNNING\033[0m\n")
        print("  Start with: proxy-ssh start\n")
        return
    print(f"\n  \033[92m  Running (PID {pid})\033[0m")
    info = SSHTunnelInfo()
    StatusCollector.print_status(info, False)


def cmd_logs(args: argparse.Namespace) -> None:
    log_path = ConfigManager().log_path
    if not log_path.exists():
        print("\033[93m  No logs found.\033[0m")
        return
    if args.follow:
        print(f"\033[90m  Following {log_path} (Ctrl+C to stop)\033[0m\n")
        try:
            with open(log_path, "r") as f:
                f.seek(0, 2)
                while True:
                    line = f.readline()
                    if line:
                        print(line.rstrip())
                    else:
                        time.sleep(0.1)
        except KeyboardInterrupt:
            print()
    else:
        with open(log_path, "r") as f:
            lines = f.readlines()
        for line in lines[-50:]:
            print(line.rstrip())


def cmd_doctor(_args: argparse.Namespace) -> None:
    cm = ConfigManager()
    setup_logging("WARNING")
    print("\n  \033[1mproxy-ssh Doctor\033[0m")
    print("  " + "─" * 40 + "\n")
    results = asyncio.run(Doctor(cm).run())
    failed = sum(1 for r in results if not r.passed)
    for r in results:
        print(r)
    print()
    if failed == 0:
        print("  \033[92mAll checks passed!\033[0m")
    else:
        print(f"  \033[91m{failed} issue(s) found.\033[0m\n")
        print("  \033[1mFixes:\033[0m")
        for r in results:
            if not r.passed and r.fix:
                print(f"    - {r.fix}")
    print()


def cmd_config(_args: argparse.Namespace) -> None:
    cm = ConfigManager()
    if not cm.exists():
        print("\033[91m  No configuration. Run 'proxy-ssh setup' first.\033[0m\n")
        sys.exit(1)
    config = cm.load()
    print("\n  \033[1mConfiguration\033[0m")
    print("  " + "─" * 40 + "\n")
    print(f"  SSH Host:       {config.ssh.host}:{config.ssh.port}")
    print(f"  SSH User:       {config.ssh.username}")
    print(f"  Auth Method:    {config.ssh.auth_method}")
    print(f"  Key Path:       {config.ssh.key_path or 'N/A'}")
    print(f"  Forward Port:   {config.ssh.local_forward_port}")
    print(f"  Server URL:     {config.server.url}")
    print(f"  TLS Verify:     {config.server.tls_verify}")
    print(f"  Log Level:      {config.log_level}")
    print(f"  Config:         {cm.config_path}")
    print(f"  Logs:           {cm.log_path}\n")


def cmd_reset(_args: argparse.Namespace) -> None:
    cm = ConfigManager()
    if not cm.exists():
        print("\033[93m  No configuration to reset.\033[0m")
        return
    resp = input("  \033[93mDelete all configuration? [y/N]: \033[0m")
    if resp.strip().lower() != "y":
        print("  Cancelled.")
        return
    cmd_stop(_args)
    cm.delete()
    print("  \033[92mConfiguration deleted.\033[0m")
    print("  Run 'proxy-ssh setup' to reconfigure.")


def cmd_version(_args: argparse.Namespace) -> None:
    print(f"\n  proxy-ssh v{__version__}\n")


def cmd_update(_args: argparse.Namespace) -> None:
    print("\033[93m  Update not yet implemented.\033[0m\n")


def main() -> None:
    parser = argparse.ArgumentParser(prog="proxy-ssh", description="proxy-ssh - SSH Tunnel + WebSocket Relay")
    sub = parser.add_subparsers(dest="command", help="Commands")

    p = sub.add_parser("setup", help="Configure proxy-ssh")
    p.add_argument("--reset", action="store_true", help="Reset configuration")

    sub.add_parser("start", help="Start proxy-ssh")
    sub.add_parser("stop", help="Stop proxy-ssh")
    sub.add_parser("restart", help="Restart proxy-ssh")
    sub.add_parser("status", help="Show status")

    p = sub.add_parser("logs", help="View logs")
    p.add_argument("-f", "--follow", action="store_true", help="Follow logs")

    sub.add_parser("doctor", help="Run diagnostics")
    sub.add_parser("config", help="Show configuration")
    sub.add_parser("reset", help="Delete configuration")
    sub.add_parser("update", help="Update proxy-ssh")
    sub.add_parser("version", help="Show version")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)

    cmds = {
        "setup": cmd_setup, "start": cmd_start, "stop": cmd_stop,
        "restart": cmd_restart, "status": cmd_status, "logs": cmd_logs,
        "doctor": cmd_doctor, "config": cmd_config, "reset": cmd_reset,
        "update": cmd_update, "version": cmd_version,
    }
    handler = cmds.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
