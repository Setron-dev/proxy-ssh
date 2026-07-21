# proxy-ssh

Modular SSH tunnel + WebSocket relay system.

## Architecture

```
Client ──TCP──> Server ◀──WebSocket──> proxy-ssh-client (managed automatically)
                                         │
                                         └── SSH tunnel ──> Remote machine
```

**proxy-ssh** (Worker) - Main program the user interacts with.
**proxy-ssh-client** (Client) - Internal component, managed automatically by the Worker.

The user only runs `proxy-ssh`. The Worker handles everything else.

## Install

### From release
Download binaries from [Releases](https://github.com/Setron-dev/proxy-ssh/releases).

### From source
```bash
pip install websockets orjson
python proxy-ssh.py --help
```

## Quick start

```bash
python proxy-ssh.py setup    # Configure (SSH + Server)
python proxy-ssh.py start    # Start (Worker + Client)
python proxy-ssh.py status   # Check status
python proxy-ssh.py logs -f  # Tail logs
python proxy-ssh.py stop     # Stop everything
```

## Commands

| Command     | Description                          |
|-------------|--------------------------------------|
| `setup`     | Interactive configuration wizard     |
| `start`     | Start worker + client                |
| `stop`      | Stop worker + client                 |
| `restart`   | Restart everything                   |
| `status`    | Show SSH, server, client status      |
| `logs`      | Tail worker logs                     |
| `doctor`    | Run diagnostics                      |
| `config`    | Show configuration                   |
| `reset`     | Remove all config and state          |
| `version`   | Show version                         |

## Setup

`proxy-ssh setup` collects:
- SSH Host, Port, Username
- Authentication (Private Key or Password)
- Service Port (shared between worker and client)
- Server URL and Auth Token

The Worker automatically:
1. Saves all settings
2. Generates client config
3. Manages the client process (start/stop/restart)

## Project structure

```
proxy-ssh/
├── proxy-ssh.py              # Worker entry point
├── proxy-ssh-client.py       # Client entry point
├── src/
│   ├── common/               # Shared: protocol, crypto, config
│   ├── worker/               # Worker: CLI, SSH, setup, doctor
│   └── client/               # Client: WebSocket connector, TCP relay
├── server/                   # Relay server (runs on VPS)
└── .github/workflows/        # Build both binaries
```

## License

MIT
