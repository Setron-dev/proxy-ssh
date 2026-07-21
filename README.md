# proxy-ssh

Transparent TCP relay over SSH tunnel with WebSocket control channel.

Single-file Python program. No protocol parsing. No modification of data. Works with HTTP, HTTPS, WebSocket, and any TCP protocol.

## Install

```bash
pip install websockets orjson
```

## Usage

```bash
python proxy-ssh.py setup      # Configure SSH + Server
python proxy-ssh.py start      # Start the service
python proxy-ssh.py stop       # Stop the service
python proxy-ssh.py restart    # Restart the service
python proxy-ssh.py status     # Show status
python proxy-ssh.py logs       # View logs
python proxy-ssh.py logs -f    # Follow logs in real-time
python proxy-ssh.py doctor     # Run diagnostics
python proxy-ssh.py config     # Show configuration
python proxy-ssh.py reset      # Delete configuration
python proxy-ssh.py version    # Show version
```

## How It Works

```
[Client] ──TCP──▶ [proxy-ssh (worker)] ◀══WebSocket══▶ [Relay Server] ◀──TCP──▶ [Remote Client]
```

1. **Client** connects to the worker's TCP port (default: 4096)
2. **Worker** creates a unique `connection_id` for each TCP connection
3. All raw bytes are forwarded to the **Relay Server** via a persistent WebSocket connection
4. The server routes responses back using the same `connection_id`
5. Full duplex. No protocol parsing. No data modification.

## Building Executables

### Using PyInstaller

```bash
pip install pyinstaller
pyinstaller --onefile --name proxy-ssh proxy-ssh.py
```

Output: `dist/proxy-ssh`

### Using GitHub Actions

Push a tag to trigger automatic builds:

```bash
git tag v1.0.0
git push origin v1.0.0
```

Binaries for Linux, macOS, and Windows will be available in GitHub Releases.

## Configuration

Config is stored at:
- **Linux**: `~/.config/proxy-ssh/config.json`
- **macOS**: `~/.config/proxy-ssh/config.json`
- **Windows**: `%APPDATA%/proxy-ssh/config.json`

All sensitive data (passwords, tokens) is encrypted at rest using machine-derived keys.

## Requirements

- Python 3.10+
- OpenSSH client (`ssh` command)
- Network access to the relay server

## License

MIT
