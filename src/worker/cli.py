from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import time

from ..common.config_base import setup_logging
from .config import WorkerConfigManager
from .doctor import Doctor
from .setup_wizard import SetupWizard
from .status import print_status, format_uptime
from .worker import RelayWorker


__version__ = "1.0.0"


def cmd_setup(args: argparse.Namespace) -> None:
    cm = WorkerConfigManager()
    cm.ensure_dirs()
    setup_logging("INFO")
    asyncio.run(SetupWizard(cm).run(reset=args.reset))


def cmd_start(_args: argparse.Namespace) -> None:
    cm = WorkerConfigManager()
    if not cm.exists():
        print("\033[91m  No configuration found. Run 'proxy-ssh setup' first.\033[0m\n")
        sys.exit(1)
    running, pid = cm.check_process_running()
    if running:
        print(f"\033[93m  Already running (PID {pid}).\033[0m\n")
        sys.exit(1)
    config = cm.load()
    setup_logging(config.log_level, cm.log_path)
    print("\033[92m  Starting proxy-ssh...\033[0m")
    worker = RelayWorker(config, cm)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown():
        loop.create_task(worker.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)
    try:
        loop.run_until_complete(worker.run())
    except KeyboardInterrupt:
        print("\n\033[93m  Shutting down...\033[0m")
        loop.run_until_complete(worker.stop())
    finally:
        loop.close()


def cmd_stop(_args: argparse.Namespace) -> None:
    cm = WorkerConfigManager()
    running, pid = cm.check_process_running()
    if not running:
        print("\033[93m  No proxy-ssh running.\033[0m")
        return
    print(f"\033[93m  Stopping (PID {pid})...\033[0m")
    try:
        os_kill = __import__("os").kill
        os_kill(pid, signal.SIGTERM)
        time.sleep(2)
        try:
            os_kill(pid, 0)
            os_kill(pid, signal.SIGKILL)
            print("\033[92m  Force killed.\033[0m")
        except ProcessLookupError:
            print("\033[92m  Stopped.\033[0m")
    except PermissionError:
        print(f"\033[91m  Permission denied for PID {pid}.\033[0m")
    except ProcessLookupError:
        print("\033[92m  Already stopped.\033[0m")
    cm.remove_pid()


def cmd_restart(args: argparse.Namespace) -> None:
    cmd_stop(args)
    time.sleep(1)
    cmd_start(args)


def cmd_status(_args: argparse.Namespace) -> None:
    cm = WorkerConfigManager()
    running, pid = cm.check_process_running()
    if not running:
        print("\n  \033[1mproxy-ssh Status\033[0m")
        print("  " + "\u2500" * 40)
        print("\n  \033[91m  NOT RUNNING\033[0m\n")
        print("  Start with: proxy-ssh start\n")
        return
    print(f"\n  \033[92m  Running (PID {pid})\033[0m")
    try:
        from .ssh_manager import SSHTunnelInfo
        print_status(SSHTunnelInfo(), False)
    except Exception:
        pass


def cmd_logs(args: argparse.Namespace) -> None:
    cm = WorkerConfigManager()
    log_path = cm.log_path
    if not log_path.exists():
        print("\033[93m  No logs found.\033[0m")
        return
    if args.follow:
        print(f"\033[90m  Following {log_path} (Ctrl+C to stop)\033[0m\n")
        try:
            with open(log_path, "r") as f:
                f.seek(0, 2)
                while True:
                    line = f.readline()
                    if line:
                        print(line.rstrip())
                    else:
                        time.sleep(0.1)
        except KeyboardInterrupt:
            print()
    else:
        with open(log_path, "r") as f:
            lines = f.readlines()
        for line in lines[-50:]:
            print(line.rstrip())


def cmd_doctor(_args: argparse.Namespace) -> None:
    cm = WorkerConfigManager()
    setup_logging("WARNING")
    print("\n  \033[1mproxy-ssh Doctor\033[0m")
    print("  " + "\u2500" * 40 + "\n")
    results = asyncio.run(Doctor(cm).run())
    failed = sum(1 for r in results if not r.passed)
    for r in results:
        print(r)
    print()
    if failed == 0:
        print("  \033[92mAll checks passed!\033[0m")
    else:
        print(f"  \033[91m{failed} issue(s) found.\033[0m\n")
        print("  \033[1mFixes:\033[0m")
        for r in results:
            if not r.passed and r.fix:
                print(f"    - {r.fix}")
    print()


def cmd_config(_args: argparse.Namespace) -> None:
    cm = WorkerConfigManager()
    if not cm.exists():
        print("\033[91m  No configuration. Run 'proxy-ssh setup' first.\033[0m\n")
        sys.exit(1)
    config = cm.load()
    print("\n  \033[1mConfiguration\033[0m")
    print("  " + "\u2500" * 40 + "\n")
    print(f"  SSH Host:       {config.ssh.host}:{config.ssh.port}")
    print(f"  SSH User:       {config.ssh.username}")
    print(f"  Auth Method:    {config.ssh.auth_method}")
    print(f"  Key Path:       {config.ssh.key_path or 'N/A'}")
    print(f"  Service Port:   {config.service_port}")
    print(f"  Server URL:     {config.server.url}")
    print(f"  TLS Verify:     {config.server.tls_verify}")
    print(f"  Log Level:      {config.log_level}")
    print(f"  Config:         {cm.config_path}")
    print(f"  Logs:           {cm.log_path}\n")


def cmd_reset(_args: argparse.Namespace) -> None:
    cm = WorkerConfigManager()
    if not cm.exists():
        print("\033[93m  No configuration to reset.\033[0m")
        return
    resp = input("  \033[93mDelete all configuration? [y/N]: \033[0m")
    if resp.strip().lower() != "y":
        print("  Cancelled.")
        return
    cmd_stop(_args)
    cm.delete()
    print("  \033[92mConfiguration deleted.\033[0m")
    print("  Run 'proxy-ssh setup' to reconfigure.")


def cmd_version(_args: argparse.Namespace) -> None:
    print(f"\n  proxy-ssh v{__version__}\n")


def cmd_update(_args: argparse.Namespace) -> None:
    print("\033[93m  Update not yet implemented.\033[0m\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="proxy-ssh", description="proxy-ssh - SSH Tunnel + WebSocket Relay"
    )
    sub = parser.add_subparsers(dest="command", help="Commands")

    p = sub.add_parser("setup", help="Configure proxy-ssh")
    p.add_argument("--reset", action="store_true", help="Reset configuration")

    sub.add_parser("start", help="Start proxy-ssh")
    sub.add_parser("stop", help="Stop proxy-ssh")
    sub.add_parser("restart", help="Restart proxy-ssh")
    sub.add_parser("status", help="Show status")

    p = sub.add_parser("logs", help="View logs")
    p.add_argument("-f", "--follow", action="store_true", help="Follow logs")

    sub.add_parser("doctor", help="Run diagnostics")
    sub.add_parser("config", help="Show configuration")
    sub.add_parser("reset", help="Delete configuration")
    sub.add_parser("update", help="Update proxy-ssh")
    sub.add_parser("version", help="Show version")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)

    cmds = {
        "setup": cmd_setup, "start": cmd_start, "stop": cmd_stop,
        "restart": cmd_restart, "status": cmd_status, "logs": cmd_logs,
        "doctor": cmd_doctor, "config": cmd_config, "reset": cmd_reset,
        "update": cmd_update, "version": cmd_version,
    }
    handler = cmds.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
