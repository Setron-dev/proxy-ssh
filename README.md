# proxy-ssh

Transparent SSH tunnel + WebSocket relay system.  
User runs **one program** (`proxy-ssh`). Worker manages everything including the client.

## How It Works

```
User's Machine                    VPS (Server)                   Remote Machine
┌──────────────┐                ┌──────────────┐               ┌──────────────┐
│  proxy-ssh   │──WebSocket───▶│ Relay Server │               │              │
│  (Worker)    │                │              │               │              │
│              │                │  TCP:3128    │◀──Clients────▶│              │
│  └─Client    │──SSH Tunnel──────────────────────────────────▶│  SSH Server  │
│    (auto)    │                │  WS:8443     │               │              │
└──────────────┘                └──────────────┘               └──────────────┘
```

1. Worker creates SSH tunnel to remote machine
2. Worker spawns Client (automatic, user never sees it)
3. Client connects to Relay Server via WebSocket
4. External clients connect to Server's TCP port
5. Traffic flows: Client → Server → Client → SSH Tunnel → Remote
6. All data is relayed transparently (HTTP, HTTPS, WebSocket, raw TCP)

## Install

### Download binary
```bash
# Linux
wget https://github.com/Setron-dev/proxy-ssh/releases/latest/download/proxy-ssh-linux-amd64
chmod +x proxy-ssh-linux-amd64
./proxy-ssh-linux-amd64 --help
```

### From source
```bash
git clone https://github.com/Setron-dev/proxy-ssh.git
cd proxy-ssh
pip install websockets orjson
python proxy-ssh.py --help
```

## Quick Start

```bash
# Step 1: Configure (one time only)
python proxy-ssh.py setup

# Step 2: Start
python proxy-ssh.py start

# Step 3: Check
python proxy-ssh.py status
python proxy-ssh.py logs -f

# Step 4: Stop
python proxy-ssh.py stop
```

## Commands

| Command     | Description                                    |
|-------------|------------------------------------------------|
| `setup`     | Interactive wizard (SSH + Server config)        |
| `start`     | Start worker + auto-start client               |
| `stop`      | Stop worker + auto-stop client                 |
| `restart`   | Restart everything                             |
| `status`    | Show SSH tunnel, server, client status         |
| `logs`      | View logs (`-f` to follow)                     |
| `doctor`    | Run diagnostics (checks SSH, config, network)  |
| `config`    | Show current configuration                     |
| `reset`     | Delete all config and state                    |
| `version`   | Show version                                   |

## Setup Wizard

When you run `proxy-ssh setup`, it asks:

```
SSH Configuration
  SSH Host: myserver.com
  SSH Port [22]:
  SSH Username: root

  Authentication:
    1) Private Key
    2) Password
  Select [1]:

  Key detected, no passphrase required.   ← auto-detected

  Service Port [4096]: 8080

Server Configuration
  Server URL (wss://...): wss://myvps.com:8443/relay
  Auth Token: my-secret-token
  Verify TLS? [Y/n]:
```

**Key passphrase detection** works like OpenSSH:
- If key has no passphrase → used directly, no prompt
- If key is encrypted → passphrase requested automatically
- No manual "Key has passphrase?" question

The Worker then:
1. Saves config to `~/.config/proxy-ssh/config.json`
2. Generates client config to `~/.config/proxy-ssh/client-config.json`
3. Starts Client as background process automatically

## Server Setup

Run on your VPS:

```bash
cd server/
pip install websockets orjson

# Edit config
vim config.json

# Run
python relay_server.py
```

### Server Config (`server/config.json`)
```json
{
  "client_listener": { "host": "0.0.0.0", "port": 3128 },
  "worker_ws": {
    "host": "0.0.0.0", "port": 8443,
    "path": "/relay",
    "auth_token": "YOUR_SECRET_TOKEN"
  },
  "tls": {
    "enabled": true,
    "certfile": "/etc/letsencrypt/live/yourdomain.com/fullchain.pem",
    "keyfile": "/etc/letsencrypt/live/yourdomain.com/privkey.pem"
  },
  "timeouts": { "client_idle": 300, "worker_response": 60 }
}
```

## Client Management

The user **never** manages the Client directly:

| User Action | Worker Does |
|---|---|
| `proxy-ssh start` | Starts SSH tunnel + spawns Client |
| `proxy-ssh stop` | Kills SSH tunnel + kills Client |
| `proxy-ssh restart` | Restarts everything |
| Client crashes | Worker auto-restarts Client |
| Config changed | Worker regenerates Client config + restarts Client |

## Project Structure

```
proxy-ssh/
├── proxy-ssh.py                # Worker entry point
├── proxy-ssh-client.py         # Client entry point (internal)
├── src/
│   ├── common/                 # Shared code
│   │   ├── protocol.py         #   WebSocket protocol messages
│   │   ├── json_lib.py         #   JSON serialization (orjson/json)
│   │   ├── crypto.py           #   Config encryption at rest
│   │   └── config_base.py      #   Base config manager
│   ├── worker/                 # Worker package
│   │   ├── cli.py              #   CLI entry point + commands
│   │   ├── config.py           #   WorkerConfig + ClientConfig
│   │   ├── ssh_manager.py      #   SSH tunnel lifecycle
│   │   ├── process_manager.py  #   Client subprocess management
│   │   ├── setup_wizard.py     #   Interactive setup
│   │   ├── doctor.py           #   Diagnostics
│   │   ├── status.py           #   Status display
│   │   └── worker.py           #   Main worker orchestrator
│   └── client/                 # Client package
│       ├── __main__.py         #   Client entry point
│       ├── config.py           #   Client config reader
│       ├── connector.py        #   WebSocket server connector
│       └── relay.py            #   Transparent TCP relay
├── server/                     # Relay server (runs on VPS)
│   ├── relay_server.py         #   Server entry point
│   ├── worker_bridge.py        #   WebSocket handler for workers
│   ├── client_listener.py      #   TCP listener for external clients
│   ├── connection_manager.py   #   Client connection tracking
│   ├── protocol.py             #   Protocol messages
│   └── config.json             #   Server config
├── tests/                      # Integration tests
│   └── integration_test.py     #   20-site protocol test suite
└── .github/workflows/          # CI/CD
    └── build.yml               #   Build + Deploy + Test
```

## Supported Protocols

All traffic is relayed transparently. Tested with:

- HTTP (GET, POST, headers, cookies, gzip, redirects)
- Raw TCP (echo, daytime, custom)
- Any TCP protocol (TLS, SSH, custom)

## License

MIT
