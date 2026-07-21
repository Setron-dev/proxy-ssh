from __future__ import annotations

import asyncio
import socket
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import WorkerConfigManager


class CheckResult:
    def __init__(self, name: str, passed: bool, message: str, fix: str = "") -> None:
        self.name = name
        self.passed = passed
        self.message = message
        self.fix = fix

    def __str__(self) -> str:
        icon = "\033[92m\u2713\033[0m" if self.passed else "\033[91m\u2717\033[0m"
        return f"  {icon} {self.name}: {self.message}"


class Doctor:
    def __init__(self, config_manager: WorkerConfigManager) -> None:
        self._cm = config_manager

    async def run(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        results.append(self._check_ssh())
        results.append(self._check_config())
        if self._cm.exists():
            results.append(self._check_key_permissions())
            results.extend(await self._check_server())
        results.append(self._check_client_binary())
        results.append(self._check_internet())
        return results

    def _check_ssh(self) -> CheckResult:
        try:
            r = subprocess.run(["ssh", "-V"], capture_output=True, text=True, timeout=5)
            version = r.stderr.strip() if r.stderr else r.stdout.strip()
            return CheckResult("SSH Client", True, version)
        except FileNotFoundError:
            return CheckResult("SSH Client", False, "not found", "apt install openssh-client / brew install openssh")
        except Exception as exc:
            return CheckResult("SSH Client", False, str(exc))

    def _check_config(self) -> CheckResult:
        if not self._cm.exists():
            return CheckResult("Configuration", False, "not found", "Run: proxy-ssh setup")
        try:
            config = self._cm.load()
            if not config.ssh.host:
                return CheckResult("Configuration", False, "SSH host not set", "Run: proxy-ssh setup")
            if not config.server.url:
                return CheckResult("Configuration", False, "server URL not set", "Run: proxy-ssh setup")
            return CheckResult("Configuration", True, f"host={config.ssh.host} user={config.ssh.username}")
        except Exception as exc:
            return CheckResult("Configuration", False, str(exc), "Run: proxy-ssh setup")

    def _check_key_permissions(self) -> CheckResult:
        try:
            config = self._cm.load()
            if config.ssh.auth_method == "key" and config.ssh.key_path:
                key = Path(config.ssh.key_path).expanduser()
                if not key.exists():
                    return CheckResult("SSH Key", False, f"not found: {key}", "Run: proxy-ssh setup")
                mode = key.stat().st_mode
                if mode & 0o077:
                    return CheckResult("SSH Key Permissions", False, f"too open: {oct(mode)}", f"chmod 600 {key}")
                return CheckResult("SSH Key", True, str(key))
            return CheckResult("SSH Key", True, "using password auth")
        except Exception as exc:
            return CheckResult("SSH Key", False, str(exc))

    async def _check_server(self) -> list[CheckResult]:
        results = []
        try:
            from urllib.parse import urlparse
            config = self._cm.load()
            parsed = urlparse(config.server.url)
            host = parsed.hostname or ""
            port = parsed.port or 443
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._check_tcp, host, port
            )
            results.append(result)
        except Exception as exc:
            results.append(CheckResult("Server", False, str(exc)))
        return results

    def _check_client_binary(self) -> CheckResult:
        binary = self._cm.client_binary_path
        if binary.exists():
            return CheckResult("Client Binary", True, str(binary))
        return CheckResult(
            "Client Binary", False, f"not found: {binary}",
            "Build with: pyinstaller --onefile --name proxy-ssh-client proxy-ssh-client.py"
        )

    @staticmethod
    def _check_tcp(host: str, port: int) -> CheckResult:
        try:
            sock = socket.create_connection((host, port), timeout=10)
            sock.close()
            return CheckResult("Server Reachable", True, f"{host}:{port}")
        except socket.timeout:
            return CheckResult("Server Reachable", False, f"timeout {host}:{port}", "Check server address/firewall")
        except Exception as exc:
            return CheckResult("Server Reachable", False, str(exc))

    @staticmethod
    def _check_internet() -> CheckResult:
        try:
            sock = socket.create_connection(("1.1.1.1", 53), timeout=5)
            sock.close()
            return CheckResult("Internet", True, "reachable")
        except Exception:
            return CheckResult("Internet", False, "no connection", "Check network")
