from __future__ import annotations

import json
import logging
import os
import platform
import stat
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from .crypto import decrypt_secret, encrypt_secret, _get_machine_key

logger = logging.getLogger("proxy-ssh.config")

APP_NAME = "proxy-ssh"

if platform.system() == "Windows":
    DEFAULT_BASE_DIR = Path(os.environ.get("APPDATA", "~")) / APP_NAME
else:
    DEFAULT_BASE_DIR = Path.home() / ".config" / APP_NAME


def setup_logging(level: str = "INFO", log_path: Path | None = None) -> None:
    import sys
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


class BaseConfigManager:
    def __init__(self, config_filename: str = "config.json") -> None:
        self._config_dir = DEFAULT_BASE_DIR
        self._config_path = DEFAULT_BASE_DIR / config_filename
        self._machine_key = _get_machine_key()

    @property
    def config_dir(self) -> Path:
        return self._config_dir

    @property
    def config_path(self) -> Path:
        return self._config_path

    @property
    def log_path(self) -> Path:
        return self._config_dir / "logs" / f"{APP_NAME}.log"

    @property
    def client_binary_path(self) -> Path:
        import sys
        if getattr(sys, "frozen", False):
            return Path(sys.executable).parent / "proxy-ssh-client"
        return Path(__file__).resolve().parent.parent.parent / "proxy-ssh-client.py"

    @property
    def client_config_path(self) -> Path:
        return self._config_dir / "client-config.json"

    def ensure_dirs(self) -> None:
        self._config_dir.mkdir(parents=True, exist_ok=True)
        (self._config_dir / "logs").mkdir(exist_ok=True)

    def exists(self) -> bool:
        return self._config_path.exists()

    def delete(self) -> None:
        if self._config_path.exists():
            self._config_path.unlink()
        if self.client_config_path.exists():
            self.client_config_path.unlink()

    def _encrypt_fields(self, data: dict[str, Any], fields: list[str]) -> dict[str, Any]:
        """Encrypt sensitive fields in nested dicts."""
        for key, val in data.items():
            if isinstance(val, dict):
                self._encrypt_fields(val, fields)
            elif isinstance(val, str) and key in fields and val:
                data[key] = encrypt_secret(val, self._machine_key)
        return data

    def _decrypt_fields(self, data: dict[str, Any], fields: list[str]) -> dict[str, Any]:
        """Decrypt sensitive fields in nested dicts."""
        for key, val in data.items():
            if isinstance(val, dict):
                self._decrypt_fields(val, fields)
            elif isinstance(val, str) and key in fields and val:
                data[key] = decrypt_secret(val, self._machine_key)
        return data

    def _save_json(self, path: Path, data: dict[str, Any]) -> None:
        self.ensure_dirs()
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)

    def _load_json(self, path: Path) -> dict[str, Any]:
        with open(path, "r") as f:
            return json.load(f)
