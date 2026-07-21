#!/usr/bin/env python3
"""
Worker for Daytona.
Listens on a TCP port, accepts connections, relays to target websites.
Communicates with client via SSH tunnel.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("worker")

LISTEN_PORT = 4096
BUFFER_SIZE = 65536
CONNECT_TIMEOUT = 20
READ_TIMEOUT = 30


def parse_target(data: bytes) -> tuple[str, int]:
    """Parse Host header from HTTP request to determine target."""
    try:
        header_block = data.split(b"\r\n\r\n", 1)[0]
        lines = header_block.split(b"\r\n")
        for line in lines[1:]:
            lower = line.lower()
            if lower.startswith(b"host:"):
                host = line[5:].strip().decode("utf-8", errors="ignore")
                if ":" in host:
                    h, p = host.rsplit(":", 1)
                    if p.isdigit():
                        return h, int(p)
                return host, 80
    except Exception:
        pass
    return "", 0


async def relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Bidirectional relay between two streams."""
    async def _copy(src, dst, label):
        try:
            while True:
                data = await asyncio.wait_for(src.read(BUFFER_SIZE), timeout=READ_TIMEOUT)
                if not data:
                    break
                dst.write(data)
                await dst.drain()
        except (asyncio.TimeoutError, ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as exc:
            logger.debug("%s copy error: %s", label, exc)
        finally:
            try:
                dst.close()
            except Exception:
                pass

    await asyncio.gather(
        _copy(reader, writer, "c->t"),
        _copy(writer, reader, "t->c"),
    )


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    t0 = time.monotonic()

    try:
        first_chunk = await asyncio.wait_for(reader.read(BUFFER_SIZE), timeout=10)
        if not first_chunk:
            writer.close()
            return

        host, port = parse_target(first_chunk)
        if not host:
            logger.warning("cannot parse target from %s", peer)
            writer.close()
            return

        logger.info("connecting to %s:%d for %s", host, port, peer)
        target_reader, target_writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=CONNECT_TIMEOUT,
        )

        if first_chunk:
            target_writer.write(first_chunk)
            await target_writer.drain()

        await relay(reader, target_writer)

        ms = (time.monotonic() - t0) * 1000
        logger.info("done %s:%d for %s (%.0fms)", host, port, peer, ms)

    except asyncio.TimeoutError:
        logger.warning("timeout for %s", peer)
    except (ConnectionError, asyncio.IncompleteReadError) as exc:
        logger.info("connection error for %s: %s", peer, exc)
    except Exception as exc:
        logger.error("error for %s: %s", peer, exc)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def main():
    server = await asyncio.start_server(handle_client, "0.0.0.0", LISTEN_PORT)
    logger.info("=== Worker listening on port %d ===", LISTEN_PORT)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    async with server:
        await stop_event.wait()

    logger.info("Worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
