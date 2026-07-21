from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

try:
    import orjson

    def _dumps(obj: Any) -> bytes:
        return orjson.dumps(obj)

    def _loads(raw: bytes) -> dict:
        return orjson.loads(raw)
except ImportError:
    import json

    def _dumps(obj: Any) -> bytes:
        return json.dumps(obj).encode()

    def _loads(raw: bytes) -> dict:
        return json.loads(raw)


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
    error_msg: str = ""
    token: str = ""

    def to_json(self) -> bytes:
        d: dict[str, Any] = {"type": self.msg_type.value}
        if self.request_id:
            d["request_id"] = self.request_id
        if self.client_id:
            d["client_id"] = self.client_id
        if self.payload_data:
            d["data"] = self.payload_data
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
