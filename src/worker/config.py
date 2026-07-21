from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from ..common.config_base import BaseConfigManager


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
class ClientConfig:
    server_url: str = ""
    auth_token: str = ""
    upstream_port: int = 4096
    tls_verify: bool = True


@dataclass
class WorkerConfig:
    ssh: SSHConfig = field(default_factory=SSHConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    client: ClientConfig = field(default_factory=ClientConfig)
    service_port: int = 4096
    version: str = "1.0.0"
    first_run: bool = True
    log_level: str = "INFO"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> WorkerConfig:
        ssh_data = d.get("ssh", {})
        server_data = d.get("server", {})
        client_data = d.get("client", {})
        return cls(
            ssh=SSHConfig(**{k: v for k, v in ssh_data.items() if k in SSHConfig.__dataclass_fields__}),
            server=ServerConfig(**{k: v for k, v in server_data.items() if k in ServerConfig.__dataclass_fields__}),
            client=ClientConfig(**{k: v for k, v in client_data.items() if k in ClientConfig.__dataclass_fields__}),
            service_port=d.get("service_port", 4096),
            version=d.get("version", "1.0.0"),
            first_run=d.get("first_run", True),
            log_level=d.get("log_level", "INFO"),
        )


class WorkerConfigManager(BaseConfigManager):
    SENSITIVE_FIELDS = ["password", "key_passphrase", "auth_token"]

    def __init__(self) -> None:
        super().__init__(config_filename="config.json")
        self._config: WorkerConfig | None = None

    def load(self) -> WorkerConfig:
        if self._config:
            return self._config
        if not self._config_path.exists():
            raise FileNotFoundError(f"Config not found: {self._config_path}")
        raw = self._load_json(self._config_path)
        self._decrypt_fields(raw, self.SENSITIVE_FIELDS)
        self._config = WorkerConfig.from_dict(raw)
        return self._config

    def save(self, config: WorkerConfig) -> None:
        self.ensure_dirs()
        data = config.to_dict()
        self._encrypt_fields(data, self.SENSITIVE_FIELDS)
        self._save_json(self._config_path, data)
        self._config = config

    def generate_client_config(self, config: WorkerConfig) -> None:
        """Generate the client config file from worker config."""
        client_cfg = {
            "server_url": config.server.url,
            "auth_token": config.server.auth_token,
            "upstream_port": config.service_port,
            "tls_verify": config.server.tls_verify,
        }
        self._encrypt_fields(client_cfg, ["auth_token"])
        self._save_json(self.client_config_path, client_cfg)
