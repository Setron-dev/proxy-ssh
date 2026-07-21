#!/usr/bin/env python3
"""
Integration tests: 20 sites, various protocols.
Run after server is up and tunnel is established.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import websockets
import websockets.asyncio.client

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.common.protocol import MessageType, RelayMessage, new_request_id

TEST_SITES = [
    {"name": "httpbin.org (HTTP)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"GET /ip HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "example.com (HTTP)", "host": "example.com", "port": 80, "proto": "http",
     "request": b"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"},
    {"name": "google.com (HTTP)", "host": "google.com", "port": 80, "proto": "http",
     "request": b"GET / HTTP/1.1\r\nHost: google.com\r\nConnection: close\r\n\r\n"},
    {"name": "jsonplaceholder (HTTP)", "host": "jsonplaceholder.typicode.com", "port": 80, "proto": "http",
     "request": b"GET /posts/1 HTTP/1.1\r\nHost: jsonplaceholder.typicode.com\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin.org/ip (HTTP)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"GET /ip HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin.org/headers (HTTP)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"GET /headers HTTP/1.1\r\nHost: httpbin.org\r\nX-Test: proxy-ssh\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin.org/user-agent (HTTP)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"GET /user-agent HTTP/1.1\r\nHost: httpbin.org\r\nUser-Agent: proxy-ssh-test/1.0\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin.org/get (HTTP POST)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"POST /post HTTP/1.1\r\nHost: httpbin.org\r\nContent-Type: application/json\r\nContent-Length: 27\r\nConnection: close\r\n\r\n{\"test\":\"proxy-ssh-data\"}"},
    {"name": "neverssl.com (HTTP)", "host": "neverssl.com", "port": 80, "proto": "http",
     "request": b"GET / HTTP/1.1\r\nHost: neverssl.com\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin.org/base64 (HTTP)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"GET /base64/cHJveHktc3NoLXRlc3Q HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "DNS tcpbin (TCP)", "host": "tcpbin.mjs.plus", "port": 4242, "proto": "raw",
     "request": b"HELLO PROXY-SSH\n"},
    {"name": "Daytime TCP (TCP)", "host": "tcpbin.mjs.plus", "port": 17, "proto": "raw",
     "request": b"\r\n"},
    {"name": "Echo TCP (TCP)", "host": "tcpbin.mjs.plus", "port": 7, "proto": "raw",
     "request": b"proxy-ssh-echo-test\n"},
    {"name": "Postman echo (HTTP)", "host": "postman-echo.com", "port": 80, "proto": "http",
     "request": b"GET /get HTTP/1.1\r\nHost: postman-echo.com\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin.org/delay (HTTP)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"GET /delay/1 HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin.org/status/200 (HTTP)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"GET /status/200 HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin.org/redirect (HTTP)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"GET /redirect-to?url=/ip HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin.org/cookies (HTTP)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"GET /cookies/set?test=proxyssh HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n"},
    {"name": "httpbin.org/gzip (HTTP)", "host": "httpbin.org", "port": 80, "proto": "http",
     "request": b"GET /gzip HTTP/1.1\r\nHost: httpbin.org\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n"},
    {"name": "TCP echo large (TCP)", "host": "tcpbin.mjs.plus", "port": 7, "proto": "raw",
     "request": b"A" * 1024 + b"\n"},
]


@dataclass
class TestResult:
    name: str
    passed: bool
    duration_ms: float = 0.0
    response_size: int = 0
    error: str = ""


@dataclass
class TestReport:
    results: list[TestResult] = field(default_factory=list)
    total: int = 0
    passed: int = 0
    failed: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "results": [
                {"name": r.name, "passed": r.passed, "duration_ms": r.duration_ms,
                 "response_size": r.response_size, "error": r.error}
                for r in self.results
            ],
        }


class RelayTestClient:
    def __init__(self, server_url: str, auth_token: str) -> None:
        self._server_url = server_url
        self._auth_token = auth_token
        self._ws = None
        self._authenticated = asyncio.Event()
        self._responses: dict[str, asyncio.Event] = {}
        self._response_data: dict[str, bytes] = {}
        self._send_lock = asyncio.Lock()

    async def connect(self) -> bool:
        try:
            self._ws = await websockets.asyncio.client.connect(
                self._server_url, ping_interval=20, ping_timeout=10, max_size=None,
            )
            await self._ws.send(RelayMessage.make_auth(self._auth_token).to_json())
            raw = await asyncio.wait_for(self._ws.recv(), timeout=10)
            msg = RelayMessage.from_json(raw if isinstance(raw, bytes) else raw.encode())
            if msg.msg_type == MessageType.AUTH_OK:
                self._authenticated.set()
                asyncio.create_task(self._listen())
                return True
            return False
        except Exception as exc:
            print(f"  Connect failed: {exc}")
            return False

    async def _listen(self) -> None:
        try:
            async for raw in self._ws:
                msg = RelayMessage.from_json(raw if isinstance(raw, bytes) else raw.encode())
                if msg.msg_type == MessageType.DATA:
                    data = base64.b64decode(msg.payload_data) if msg.payload_data else b""
                    if msg.request_id in self._response_data:
                        self._response_data[msg.request_id] += data
                    else:
                        self._response_data[msg.request_id] = data
                elif msg.msg_type == MessageType.END:
                    if msg.request_id in self._responses:
                        self._responses[msg.request_id].set()
                elif msg.msg_type == MessageType.PING:
                    await self._ws.send(RelayMessage.make_pong().to_json())
        except websockets.ConnectionClosed:
            pass
        except Exception:
            pass

    async def send_request(self, request_id: str, host: str, port: int, data: bytes) -> bytes:
        self._responses[request_id] = asyncio.Event()
        self._response_data[request_id] = b""
        payload = base64.b64encode(data).decode()
        await self._ws.send(RelayMessage.make_request(request_id, data=payload).to_json())
        try:
            await asyncio.wait_for(self._responses[request_id].wait(), timeout=15)
        except asyncio.TimeoutError:
            pass
        return self._response_data.pop(request_id, b"")

    async def close(self, request_id: str) -> None:
        try:
            await self._ws.send(RelayMessage.make_close(request_id).to_json())
        except Exception:
            pass
        self._responses.pop(request_id, None)
        self._response_data.pop(request_id, None)

    async def disconnect(self) -> None:
        if self._ws:
            await self._ws.close()


async def run_test(client: RelayTestClient, site: dict) -> TestResult:
    name = site["name"]
    request_data = site["request"]
    t0 = time.monotonic()
    try:
        conn_id = new_request_id()
        response = await client.send_request(conn_id, site["host"], site["port"], request_data)
        duration = (time.monotonic() - t0) * 1000
        await client.close(conn_id)
        if response:
            return TestResult(name=name, passed=True, duration_ms=duration, response_size=len(response))
        return TestResult(name=name, passed=False, duration_ms=duration, error="empty response")
    except Exception as exc:
        duration = (time.monotonic() - t0) * 1000
        return TestResult(name=name, passed=False, duration_ms=duration, error=str(exc)[:120])


async def main() -> None:
    server_url = os.environ.get("SERVER_WS", "ws://127.0.0.1:8443/relay")
    auth_token = os.environ.get("AUTH_TOKEN", "CHANGE_ME_TO_A_SECURE_TOKEN")

    print()
    print("  \033[1mproxy-ssh Integration Tests\033[0m")
    print("  " + "\u2500" * 50)
    print(f"  Server: {server_url}")
    print(f"  Sites:  {len(TEST_SITES)}")
    print()

    client = RelayTestClient(server_url, auth_token)
    connected = await client.connect()
    if not connected:
        print("  \033[91mFailed to connect to server!\033[0m")
        report = TestReport(total=0, passed=0, failed=0)
        report.results = [TestResult(name="connection", passed=False, error="failed to connect")]
        _save_report(report)
        sys.exit(1)

    print("  \033[92mConnected to server.\033[0m\n")

    report = TestReport()
    for i, site in enumerate(TEST_SITES, 1):
        sys.stdout.write(f"  [{i:2d}/{len(TEST_SITES)}] {site['name']:<45s}")
        sys.stdout.flush()
        result = await run_test(client, site)
        report.results.append(result)
        report.total += 1
        if result.passed:
            report.passed += 1
            print(f"  \033[92mPASS\033[0m  {result.duration_ms:6.0f}ms  {result.response_size}B")
        else:
            report.failed += 1
            print(f"  \033[91mFAIL\033[0m  {result.duration_ms:6.0f}ms  {result.error[:60]}")

    await client.disconnect()

    print()
    print("  " + "\u2500" * 50)
    print(f"  Total: {report.total}  Passed: \033[92m{report.passed}\033[0m  Failed: \033[91m{report.failed}\033[0m")
    print()

    _save_report(report)
    if report.failed > 0:
        sys.exit(1)


def _save_report(report: TestReport) -> None:
    path = "test-report.json"
    with open(path, "w") as f:
        json.dump(report.to_dict(), f, indent=2)
    print(f"  Report saved: {path}")


if __name__ == "__main__":
    asyncio.run(main())
