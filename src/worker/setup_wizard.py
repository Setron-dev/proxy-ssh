from __future__ import annotations

import asyncio
import getpass
import subprocess
from pathlib import Path

from ..common.config_base import DEFAULT_BASE_DIR
from .config import SSHConfig, ServerConfig, WorkerConfig, WorkerConfigManager


class SetupWizard:
    def __init__(self, config_manager: WorkerConfigManager) -> None:
        self._cm = config_manager

    async def run(self, reset: bool = False) -> WorkerConfig:
        print()
        print("  \033[1m\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\033[0m")
        print("  \033[1m\u2551        proxy-ssh - Setup             \u2551\033[0m")
        print("  \033[1m\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\033[0m")
        print()
        if self._cm.exists() and not reset:
            print("  Configuration already exists.")
            print("  Run 'proxy-ssh setup --reset' to reconfigure.\n")
            return self._cm.load()
        if reset:
            print("  \033[93mResetting configuration...\033[0m\n")
            self._cm.delete()

        ssh = self._collect_ssh()
        server = self._collect_server()
        service_port = ssh.local_forward_port

        config = WorkerConfig(
            ssh=ssh, server=server, service_port=service_port,
            client=ClientConfig(
                server_url=server.url,
                auth_token=server.auth_token,
                upstream_port=service_port,
                tls_verify=server.tls_verify,
            ),
            first_run=False,
        )

        print("\n  \033[93mVerifying SSH connection...\033[0m")
        ok, msg = await self._verify_ssh(ssh)
        if ok:
            print(f"  \033[92m\u2713 {msg}\033[0m")
        else:
            print(f"  \033[91m\u2717 {msg}\033[0m")
            resp = input("  Continue anyway? [y/N]: ").strip().lower()
            if resp != "y":
                print("  Setup cancelled.")
                return self._cm.load()

        self._cm.save(config)
        self._cm.generate_client_config(config)
        print(f"\n  \033[92m\u2713 Configuration saved!\033[0m")
        print(f"  Worker: {self._cm.config_path}")
        print(f"  Client: {self._cm.client_config_path}")
        print("\n  Next step: proxy-ssh start\n")
        return config

    def _collect_ssh(self) -> SSHConfig:
        print("  \033[1mSSH Configuration\033[0m")
        print("  " + "\u2500" * 36 + "\n")
        host = input("  SSH Host: ").strip()
        while not host:
            print("  \033[91mHost cannot be empty.\033[0m")
            host = input("  SSH Host: ").strip()
        port_str = input("  SSH Port [22]: ").strip()
        port = int(port_str) if port_str.isdigit() else 22
        username = input("  SSH Username: ").strip()
        while not username:
            print("  \033[91mUsername cannot be empty.\033[0m")
            username = input("  SSH Username: ").strip()
        print("\n  Authentication:")
        print("    1) Private Key")
        print("    2) Password\n")
        while True:
            choice = input("  Select [1]: ").strip() or "1"
            if choice in ("1", "2"):
                break
            print("  \033[91mInvalid choice.\033[0m")
        auth_method = "key" if choice == "1" else "password"
        key_path, key_passphrase, password = "", "", ""
        if auth_method == "key":
            default_key = str(Path.home() / ".ssh" / "id_rsa")
            key_path = input(f"  Key Path [{default_key}]: ").strip() or default_key
            expanded = Path(key_path).expanduser()
            if expanded.exists():
                needs_pass = self._key_needs_passphrase(str(expanded))
                if needs_pass:
                    print("  \033[93mKey is encrypted.\033[0m")
                    key_passphrase = getpass("  Key Passphrase: ")
                else:
                    print("  \033[92mKey detected, no passphrase required.\033[0m")
            else:
                print(f"  \033[93mKey not found: {expanded}\033[0m")
        else:
            password = getpass("  SSH Password: ")
        port_str = input("\n  Service Port [4096]: ").strip()
        fwd = int(port_str) if port_str.isdigit() else 4096
        print()
        return SSHConfig(
            host=host, port=port, username=username, auth_method=auth_method,
            key_path=key_path, key_passphrase=key_passphrase, password=password,
            local_forward_port=fwd, remote_forward_port=fwd,
        )

    def _collect_server(self) -> ServerConfig:
        print("  \033[1mServer Configuration\033[0m")
        print("  " + "\u2500" * 36 + "\n")
        url = input("  Server URL (wss://...): ").strip()
        while not url:
            print("  \033[91mURL cannot be empty.\033[0m")
            url = input("  Server URL (wss://...): ").strip()
        token = input("  Auth Token: ").strip()
        while not token:
            print("  \033[91mToken cannot be empty.\033[0m")
            token = input("  Auth Token: ").strip()
        tls = input("  Verify TLS? [Y/n]: ").strip().lower() != "n"
        print()
        return ServerConfig(url=url, auth_token=token, tls_verify=tls)

    @staticmethod
    def _key_needs_passphrase(key_path: str) -> bool:
        """Check if an SSH key requires a passphrase by trying to read it."""
        import subprocess
        try:
            r = subprocess.run(
                ["ssh-keygen", "-y", "-f", key_path, "-P", ""],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode == 0:
                return False
            output = (r.stderr or r.stdout).lower()
            if "passphrase" in output or "password" in output:
                return True
            return False
        except FileNotFoundError:
            return False
        except Exception:
            return False

    @staticmethod
    async def _verify_ssh(ssh: SSHConfig) -> tuple[bool, str]:
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
               "-o", "StrictHostKeyChecking=accept-new", "-p", str(ssh.port)]
        if ssh.auth_method == "key" and ssh.key_path:
            cmd.extend(["-i", ssh.key_path])
        cmd.extend([f"{ssh.username}@{ssh.host}", "echo", "proxy-ssh-ok"])
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
            if proc.returncode == 0:
                return True, f"connected to {ssh.host}"
            err = stderr.decode().strip()
            if "Permission denied" in err:
                return False, "authentication failed"
            return False, f"failed: {err[:100]}"
        except asyncio.TimeoutError:
            return False, "connection timeout"
        except FileNotFoundError:
            return False, "ssh not found"
        except Exception as exc:
            return False, str(exc)
