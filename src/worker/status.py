from __future__ import annotations

import logging
import time
from pathlib import Path

from .ssh_manager import SSHTunnelInfo, TunnelStatus

logger = logging.getLogger("proxy-ssh.status")


def format_uptime(seconds: float) -> str:
    if seconds <= 0:
        return "N/A"
    days = int(seconds // 86400)
    hours = int((seconds % 86400) // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def print_status(
    ssh_info: SSHTunnelInfo,
    server_connected: bool,
    client_running: bool = False,
    client_restarts: int = 0,
    server_error: str = "",
    server_reconnects: int = 0,
) -> None:
    colors = {
        "connected": "\033[92m", "connecting": "\033[93m",
        "reconnecting": "\033[93m", "disconnected": "\033[91m", "failed": "\033[91m",
    }
    R = "\033[0m"
    sc = colors.get(ssh_info.status.value, "")
    srv_c = "\033[92m" if server_connected else "\033[91m"
    cli_c = "\033[92m" if client_running else "\033[91m"
    print()
    print("  \033[1mproxy-ssh Status\033[0m")
    print("  " + "\u2500" * 40)
    print()
    print("  \033[1mSSH Tunnel\033[0m")
    print(f"  Status:     {sc}{ssh_info.status.value.upper()}{R}")
    print(f"  PID:        {ssh_info.pid or 'N/A'}")
    print(f"  Uptime:     {format_uptime(ssh_info.uptime)}")
    print(f"  Reconnects: {ssh_info.reconnect_count}")
    if ssh_info.last_error:
        print(f"  Last Error: {ssh_info.last_error[:80]}")
    print()
    print("  \033[1mServer\033[0m")
    print(f"  Status:     {srv_c}{'CONNECTED' if server_connected else 'DISCONNECTED'}{R}")
    print(f"  Reconnects: {server_reconnects}")
    if server_error:
        print(f"  Last Error: {server_error[:80]}")
    print()
    print("  \033[1mClient\033[0m")
    print(f"  Status:     {cli_c}{'RUNNING' if client_running else 'STOPPED'}{R}")
    print(f"  Restarts:   {client_restarts}")
    print()
