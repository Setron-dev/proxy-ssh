from __future__ import annotations

import sys
from typing import Any

websockets = None


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
