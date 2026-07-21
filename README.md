# proxy-ssh

Transparent SSH tunnel + WebSocket relay system.
User runs **one program** (`proxy-ssh`). Worker manages everything including the client.

## How It Works

```
User's Machine                    VPS (Server)                   Remote Machine
+--------------+                +--------------+               +--------------+
|  proxy-ssh   |--WebSocket--->| Relay Server |               |              |
|  (Worker)    |                |              |               |              |
|              |                |  TCP:3128    |<--Clients---->|              |
|  +--Client   |--SSH Tunnel------------------------------>|  SSH Server  |
|    (auto)    |                |  WS:8443     |               |              |
+--------------+                +--------------+               +--------------+
```

1. Worker creates SSH tunnel to remote machine
2. Worker spawns Client (automatic, user never sees it)
3. Client connects to Relay Server via WebSocket
4. External clients connect to Server's TCP port
5. Traffic flows: Client -> Server -> Client -> SSH Tunnel -> Remote
6. All data is relayed transparently (HTTP, HTTPS, WebSocket, raw TCP)

## Install

### Linux
```bash
wget -O proxy-ssh https://github.com/Setron-dev/proxy-ssh/releases/latest/download/proxy-ssh-linux-amd64
wget -O proxy-ssh-client https://github.com/Setron-dev/proxy-ssh/releases/latest/download/proxy-ssh-client-linux-amd64
chmod +x proxy-ssh proxy-ssh-client
sudo mv proxy-ssh proxy-ssh-client /usr/local/bin/
```

### macOS
```bash
curl -Lo proxy-ssh https://github.com/Setron-dev/proxy-ssh/releases/latest/download/proxy-ssh-macos-arm64
curl -Lo proxy-ssh-client https://github.com/Setron-dev/proxy-ssh/releases/latest/download/proxy-ssh-client-macos-arm64
chmod +x proxy-ssh proxy-ssh-client
sudo mv proxy-ssh proxy-ssh-client /usr/local/bin/
```

### Windows
Download `.exe` files from [Releases](https://github.com/Setron-dev/proxy-ssh/releases/latest) and add to PATH.

## Quick Start

```bash
proxy-ssh setup    # Configure (one time)
proxy-ssh start    # Start worker + client
proxy-ssh status   # Check status
proxy-ssh logs -f  # Tail logs
proxy-ssh stop     # Stop
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

  Key detected, no passphrase required.   <- auto-detected

  Service Port [4096]: 8080

Server Configuration
  Server URL (wss://...): wss://myvps.com:8443/relay
  Auth Token: my-secret-token
  Verify TLS? [Y/n]:
```

**Key passphrase detection** works like OpenSSH:
- If key has no passphrase -> used directly, no prompt
- If key is encrypted -> passphrase requested automatically

The Worker then:
1. Saves config to `~/.config/proxy-ssh/config.json`
2. Generates client config to `~/.config/proxy-ssh/client-config.json`
3. Starts Client as background process automatically

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
+-- proxy-ssh.py                # Worker entry point
+-- proxy-ssh-client.py         # Client entry point (internal)
+-- src/
|   +-- common/                 # Shared code
|   +-- worker/                 # Worker: CLI, SSH, setup, doctor
|   +-- client/                 # Client: WebSocket connector, TCP relay
+-- server/                     # Relay server (runs on VPS)
+-- .github/workflows/          # CI/CD
    +-- build.yml               # Build + Release
```

## License

MIT
