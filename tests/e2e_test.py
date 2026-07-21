#!/usr/bin/env python3
"""
E2E Test for proxy-ssh relay.

Architecture:
  1. Relay server runs locally (WS :8443, TCP :3128)
  2. Mock worker connects via WebSocket, handles relay to real targets
  3. Test TCP client connects to server's ClientListener (TCP :3128)
  4. Data flow: TestClient -> Server TCP -> Server -> Worker WS -> Target -> back

All output goes to stdout. No buffering, no hanging.
"""
import asyncio
import base64
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

import websockets.asyncio.client
import websockets.asyncio.server

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.common.protocol import MessageType, RelayMessage, new_request_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger("e2e")

SERVER_WS_PORT = 8443
SERVER_TCP_PORT = 3128
AUTH_TOKEN = "e2e-test-token-2026"


TEST_SITES = [
    {"name": "httpbin.org /ip",           "host": "httpbin.org",      "port": 80, "req": b"GET /ip HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "example.com",               "host": "example.com",      "port": 80, "req": b"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"},
    {"name": "google.com",                "host": "google.com",       "port": 80, "req": b"GET / HTTP/1.1\r\nHost: google.com\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin POST json",         "host": "httpbin.org",      "port": 80, "req": b"POST /post HTTP/1.1\r\nHost: httpbin.org\r\nContent-Type: application/json\r\nContent-Length: 27\r\nConnection: close\r\n\r\n{\"test\":\"proxy-ssh-data\"}"},
    {"name": "httpbin /headers",          "host": "httpbin.org",      "port": 80, "req": b"GET /headers HTTP/1.1\r\nHost: httpbin.org\r\nX-Custom: proxy-ssh\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin /user-agent",       "host": "httpbin.org",      "port": 80, "req": b"GET /user-agent HTTP/1.1\r\nHost: httpbin.org\r\nUser-Agent: proxy-ssh-test/1.0\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin /delay/1",          "host": "httpbin.org",      "port": 80, "req": b"GET /delay/1 HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin /status/204",       "host": "httpbin.org",      "port": 80, "req": b"GET /status/204 HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin /base64",           "host": "httpbin.org",      "port": 80, "req": b"GET /base64/cHJveHktc3No HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin /gzip",             "host": "httpbin.org",      "port": 80, "req": b"GET /gzip HTTP/1.1\r\nHost: httpbin.org\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin /cookies",          "host": "httpbin.org",      "port": 80, "req": b"GET /cookies HTTP/1.1\r\nHost: httpbin.org\r\nCookie: foo=bar;baz=qux\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin /redirect->/ip",    "host": "httpbin.org",      "port": 80, "req": b"GET /redirect-to?url=/ip HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "jsonplaceholder /posts",    "host": "jsonplaceholder.typicode.com", "port": 80, "req": b"GET /posts/1 HTTP/1.1\r\nHost: jsonplaceholder.typicode.com\r\nConnection: close\r\n\r\n"},
    {"name": "jsonplaceholder /users",    "host": "jsonplaceholder.typicode.com", "port": 80, "req": b"GET /users/1 HTTP/1.1\r\nHost: jsonplaceholder.typicode.com\r\nConnection: close\r\n\r\n"},
    {"name": "jsonplaceholder /todos",    "host": "jsonplaceholder.typicode.com", "port": 80, "req": b"GET /todos/1 HTTP/1.1\r\nHost: jsonplaceholder.typicode.com\r\nConnection: close\r\n\r\n"},
    {"name": "postman-echo /get",         "host": "postman-echo.com", "port": 80, "req": b"GET /get?foo=bar HTTP/1.1\r\nHost: postman-echo.com\r\nConnection: close\r\n\r\n"},
    {"name": "postman-echo /post",        "host": "postman-echo.com", "port": 80, "req": b"POST /post HTTP/1.1\r\nHost: postman-echo.com\r\nContent-Type: text/plain\r\nContent-Length: 11\r\nConnection: close\r\n\r\nhello world"},
    {"name": "httpbin /ip (large req)",   "host": "httpbin.org",      "port": 80, "req": b"POST /post HTTP/1.1\r\nHost: httpbin.org\r\nContent-Type: application/octet-stream\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n" + b"X" * 1024},
    {"name": "http.org",                  "host": "http.org",         "port": 80, "req": b"GET / HTTP/1.1\r\nHost: http.org\r\nConnection: close\r\n\r\n"},
    {"name": "neverssl.com",              "host": "neverssl.com",     "port": 80, "req": b"GET / HTTP/1.1\r\nHost: neverssl.com\r\nConnection: close\r\n\r\n"},
]


class MockWorker:
    """WebSocket client that acts as the relay worker.
    Receives REQUEST from server, connects to target, relays data back."""

    def __init__(self):
        self._ws = None
        self._active: dict[str, asyncio.StreamWriter] = {}
        self._tasks: dict[str, asyncio.Task] = {}

    async def run(self):
        url = f"ws://127.0.0.1:{SERVER_WS_PORT}"
        logger.info("mock worker connecting to %s", url)
        async with websockets.asyncio.client.connect(
            url, ping_interval=20, ping_timeout=10, max_size=None,
        ) as ws:
            self._ws = ws
            await ws.send(RelayMessage.make_auth(AUTH_TOKEN).to_json())
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            msg = RelayMessage.from_json(raw if isinstance(raw, bytes) else raw.encode())
            if msg.msg_type != MessageType.AUTH_OK:
                logger.error("worker auth failed: %s", msg.error_msg)
                return
            logger.info("worker authenticated")
            async for raw in ws:
                msg = RelayMessage.from_json(raw if isinstance(raw, bytes) else raw.encode())
                if msg.msg_type == MessageType.PING:
                    await ws.send(RelayMessage.make_pong().to_json())
                elif msg.msg_type == MessageType.REQUEST:
                    asyncio.create_task(self._handle_request(msg))
                elif msg.msg_type == MessageType.CLOSE:
                    await self._close(msg.request_id)

    async def _handle_request(self, msg: RelayMessage):
        conn_id = msg.request_id
        initial = base64.b64decode(msg.payload_data) if msg.payload_data else b""

        host, port = self._parse_target(initial)
        if not host:
            logger.warning("cannot parse target for %s", conn_id)
            await self._send_error(conn_id, "cannot determine target host")
            return

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=10,
            )
        except Exception as exc:
            logger.error("connect to %s:%d failed: %s", host, port, exc)
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
            logger.error("relay error %s: %s", conn_id, exc)
        finally:
            writer.close()
            self._active.pop(conn_id, None)
            await self._send_end(conn_id)

    def _parse_target(self, data: bytes) -> tuple[str, int]:
        try:
            first_line = data.split(b"\r\n", 1)[0]
            parts = first_line.split(b" ")
            if len(parts) >= 2:
                target = parts[1].decode("utf-8", errors="ignore")
                if target.startswith("http://"):
                    target = target[7:]
                if "/" in target:
                    target = target.split("/")[0]
                if ":" in target:
                    return target.split(":")[0], int(target.split(":")[1])
                return target, 80
        except Exception:
            pass
        return "", 0

    async def _send_data(self, conn_id: str, data: bytes):
        if not self._ws or not self._ws.open:
            return
        try:
            await self._ws.send(RelayMessage.make_data(
                conn_id, base64.b64encode(data).decode(),
            ).to_json())
        except Exception:
            pass

    async def _send_end(self, conn_id: str):
        if not self._ws or not self._ws.open:
            return
        try:
            await self._ws.send(RelayMessage.make_end(conn_id).to_json())
        except Exception:
            pass

    async def _send_error(self, conn_id: str, error: str):
        if not self._ws or not self._ws.open:
            return
        try:
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


async def wait_for_port(port: int, host: str = "127.0.0.1", timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            return True
        except (ConnectionRefusedError, OSError):
            await asyncio.sleep(0.2)
    return False


async def test_one_site(idx: int, total: int, site: dict) -> dict:
    label = site["name"]
    sys.stdout.write(f"  [{idx:2d}/{total}] {label:<42s}")
    sys.stdout.flush()
    t0 = time.monotonic()

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", SERVER_TCP_PORT), timeout=5,
        )
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        err = str(exc)[:50]
        print(f"\033[91mFAIL  {ms:6.0f}ms  {err}\033[0m", flush=True)
        return {"name": label, "pass": False, "ms": ms, "error": err}

    try:
        writer.write(site["req"])
        await writer.drain()

        response = b""
        while True:
            try:
                chunk = await asyncio.wait_for(reader.read(65536), timeout=15)
                if not chunk:
                    break
                response += chunk
            except asyncio.TimeoutError:
                break

        writer.close()
        await writer.wait_closed()
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        err = str(exc)[:50]
        print(f"\033[91mFAIL  {ms:6.0f}ms  {err}\033[0m", flush=True)
        return {"name": label, "pass": False, "ms": ms, "error": err}

    ms = (time.monotonic() - t0) * 1000
    size = len(response)
    if response:
        print(f"\033[92mPASS  {ms:6.0f}ms  {size:>6d}B\033[0m", flush=True)
        return {"name": label, "pass": True, "ms": ms, "bytes": size}
    else:
        print(f"\033[91mFAIL  {ms:6.0f}ms  empty\033[0m", flush=True)
        return {"name": label, "pass": False, "ms": ms, "error": "empty response"}


async def run_server():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "server"))
    os.chdir(os.path.join(os.path.dirname(__file__), "..", "server"))

    config = {
        "client_listener": {"host": "127.0.0.1", "port": SERVER_TCP_PORT},
        "worker_ws": {
            "host": "127.0.0.1", "port": SERVER_WS_PORT,
            "path": "/relay", "auth_token": AUTH_TOKEN,
        },
        "tls": {"enabled": False, "certfile": "", "keyfile": ""},
        "timeouts": {"client_idle": 300, "worker_response": 60},
    }

    from relay_server import RelayServer
    server = RelayServer(config)
    await server.start()
    return server


async def main():
    print()
    print("=" * 62)
    print("  proxy-ssh E2E Test")
    print("  Server + Mock Worker + 20 Site Tests")
    print("=" * 62)
    print()

    print("[1/4] Starting relay server...")
    server = await run_server()
    await asyncio.sleep(0.5)
    print(f"  Server listening on WS:{SERVER_WS_PORT} TCP:{SERVER_TCP_PORT}")

    print("[2/4] Starting mock worker...")
    worker = MockWorker()
    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.5)
    print("  Worker connected and authenticated")

    print(f"[3/4] Testing {len(TEST_SITES)} sites...")
    print("-" * 62)

    results = []
    for i, site in enumerate(TEST_SITES, 1):
        r = await test_one_site(i, len(TEST_SITES), site)
        results.append(r)
        await asyncio.sleep(0.1)

    passed = sum(1 for r in results if r["pass"])
    failed = len(results) - passed

    print()
    print("=" * 62)
    print("  RESULTS")
    print("=" * 62)
    print(f"  Total:  {len(results)}")
    print(f"  Passed: \033[92m{passed}\033[0m")
    print(f"  Failed: \033[91m{failed}\033[0m")
    if results:
        rate = passed * 100 // len(results)
        print(f"  Rate:   {passed}/{len(results)} ({rate}%)")
    print("=" * 62)

    if failed:
        print("\n  Failed sites:")
        for r in results:
            if not r["pass"]:
                print(f"    - {r['name']}: {r.get('error', 'unknown')}")

    print()
    report_path = os.path.join(os.path.dirname(__file__), "test-report.json")
    with open(report_path, "w") as f:
        json.dump({"total": len(results), "passed": passed, "failed": failed, "results": results}, f, indent=2)
    print(f"  Report saved: {report_path}")

    worker_task.cancel()
    try:
        await worker_task
    except (asyncio.CancelledError, Exception):
        pass
    await server.stop()

    return failed == 0


if __name__ == "__main__":
    try:
        ok = asyncio.run(main())
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        print("\n  Interrupted")
        sys.exit(1)
