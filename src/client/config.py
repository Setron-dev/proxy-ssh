from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..common.config_base import BaseConfigManager


@dataclass
class ClientConfig:
    server_url: str = ""
    auth_token: str = ""
    upstream_port: int = 4096
    tls_verify: bool = True
    log_level: str = "INFO"

    def to_dict(self) -> dict[str, Any]:
        return {
            "server_url": self.server_url,
            "auth_token": self.auth_token,
            "upstream_port": self.upstream_port,
            "tls_verify": self.tls_verify,
            "log_level": self.log_level,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ClientConfig:
        return cls(
            server_url=d.get("server_url", ""),
            auth_token=d.get("auth_token", ""),
            upstream_port=d.get("upstream_port", 4096),
            tls_verify=d.get("tls_verify", True),
            log_level=d.get("log_level", "INFO"),
        )


class ClientConfigManager(BaseConfigManager):
    SENSITIVE_FIELDS = ["auth_token"]

    def __init__(self, config_path: str | None = None) -> None:
        super().__init__(config_filename="client-config.json")
        if config_path:
            from pathlib import Path
            self._config_path = Path(config_path)
        self._config: ClientConfig | None = None

    def load(self) -> ClientConfig:
        if self._config:
            return self._config
        if not self._config_path.exists():
            raise FileNotFoundError(f"Client config not found: {self._config_path}")
        raw = self._load_json(self._config_path)
        self._decrypt_fields(raw, self.SENSITIVE_FIELDS)
        self._config = ClientConfig.from_dict(raw)
        return self._config

    def save(self, config: ClientConfig) -> None:
        self.ensure_dirs()
        data = config.to_dict()
        self._encrypt_fields(data, self.SENSITIVE_FIELDS)
        self._save_json(self._config_path, data)
        self._config = config
