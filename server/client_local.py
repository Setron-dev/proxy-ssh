#!/usr/bin/env python3
"""
Local client — runs on your machine, forwards traffic to Daytona worker via SSH tunnel.

Usage:
  1. Open SSH tunnel (in another terminal):
     ssh -L 4096:127.0.0.1:4096 JQAkten5aGApQzSPT875P62bONzMlWoH@ssh.app.daytona.io

  2. Run this client:
     python3 client_local.py

  3. Configure browser to use localhost:1080 as HTTP proxy
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [client] %(levelname)s: %(message)s",
    stream=sys.stdout,
)

PROXY_PORT = 1080
WORKER_PORT = 4096
BUFFER = 65536


async def _copy(src, dst):
    try:
        while True:
            data = await src.read(BUFFER)
            if not data:
                break
            dst.write(data)
            await dst.drain()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    except Exception:
        pass
    finally:
        try: dst.close()
        except Exception: pass


async def handle_client(reader, writer):
    peer = writer.get_extra_info("peername")
    try:
        wr, ww = await asyncio.open_connection("127.0.0.1", WORKER_PORT)
        await asyncio.gather(
            _copy(reader, ww),
            _copy(wr, writer),
        )
    except ConnectionRefusedError:
        pass
    except Exception as exc:
        pass
    finally:
        try: writer.close()
        except Exception: pass


async def main():
    server = await asyncio.start_server(handle_client, "0.0.0.0", PROXY_PORT)
    print(f"\n=== Client proxy on 0.0.0.0:{PROXY_PORT} ===")
    print("  Configure your browser to use 0.0.0.0:{} as HTTP proxy".format(PROXY_PORT))
    print("  SSH tunnel must be open: ssh -L {}:127.0.0.1:{} user@ssh.app.daytona.io\n".format(WORKER_PORT, WORKER_PORT))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with server:
        await stop.wait()
    print("Client stopped")


if __name__ == "__main__":
    asyncio.run(main())
