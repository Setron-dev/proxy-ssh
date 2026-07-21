#!/usr/bin/env python3
"""
Local client proxy.
Forwards all TCP connections to Daytona worker via SSH tunnel.
Run this on your machine after opening the SSH tunnel:
  ssh -L 4096:127.0.0.1:4096 JQAkten5aGApQzSPT875P62bONzMlWoH@ssh.app.daytona.io

Then run this client:
  python3 client_local.py

And configure your browser/system to use localhost:1080 as HTTP proxy.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("client")

WORKER_PORT = 4096
LOCAL_PROXY_PORT = 1080
BUFFER_SIZE = 65536
CONNECT_TIMEOUT = 10


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    logger.info("new connection from %s", peer)

    try:
        worker_reader, worker_writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", WORKER_PORT), timeout=CONNECT_TIMEOUT,
        )
    except Exception as exc:
        logger.error("cannot connect to worker on port %d: %s", WORKER_PORT, exc)
        writer.close()
        return

    async def _copy(src, dst, label):
        try:
            while True:
                data = await asyncio.wait_for(src.read(BUFFER_SIZE), timeout=300)
                if not data:
                    break
                dst.write(data)
                await dst.drain()
        except (asyncio.TimeoutError, ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception:
            pass
        finally:
            try:
                dst.close()
            except Exception:
                pass

    await asyncio.gather(
        _copy(reader, worker_writer, "client->worker"),
        _copy(worker_writer, reader, "worker->client"),
    )
    logger.info("connection closed: %s", peer)


async def main():
    server = await asyncio.start_server(handle_client, "127.0.0.1", LOCAL_PROXY_PORT)
    logger.info("=== Client proxy on localhost:%d ===", LOCAL_PROXY_PORT)
    logger.info("Configure your browser/system to use HTTP proxy 127.0.0.1:%d", LOCAL_PROXY_PORT)
    logger.info("Make sure SSH tunnel is open: ssh -L %d:127.0.0.1:%d user@ssh.app.daytona.io", WORKER_PORT, WORKER_PORT)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    async with server:
        await stop_event.wait()

    logger.info("Client stopped")


if __name__ == "__main__":
    asyncio.run(main())
