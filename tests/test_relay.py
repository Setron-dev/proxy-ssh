from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock

import pytest


class TestProtocol:
    def test_message_to_json_and_back(self):
        from relay_worker.protocol import RelayMessage, MessageType

        msg = RelayMessage.request(
            request_id="abc123",
            client_id="client-1",
            protocol="http",
            method="GET",
            path="/test",
            headers={"Host": "example.com"},
            data="hello",
        )
        raw = msg.to_json()
        restored = RelayMessage.from_json(raw)

        assert restored.type == MessageType.REQUEST
        assert restored.request_id == "abc123"
        assert restored.client_id == "client-1"
        assert restored.protocol == "http"
        assert restored.method == "GET"
        assert restored.path == "/test"
        assert restored.headers["Host"] == "example.com"
        assert restored.data == "hello"

    def test_data_message(self):
        from relay_worker.protocol import RelayMessage, MessageType

        msg = RelayMessage.data_msg("req1", "base64data")
        raw = msg.to_json()
        restored = RelayMessage.from_json(raw)

        assert restored.type == MessageType.DATA
        assert restored.request_id == "req1"
        assert restored.data == "base64data"

    def test_close_message(self):
        from relay_worker.protocol import RelayMessage, MessageType

        msg = RelayMessage.close("req1")
        raw = msg.to_json()
        restored = RelayMessage.from_json(raw)

        assert restored.type == MessageType.CLOSE
        assert restored.request_id == "req1"

    def test_auth_message(self):
        from relay_worker.protocol import RelayMessage, MessageType

        msg = RelayMessage.auth("mytoken")
        raw = msg.to_json()
        restored = RelayMessage.from_json(raw)

        assert restored.type == MessageType.AUTH
        assert restored.token == "mytoken"

    def test_new_request_id_unique(self):
        from relay_worker.protocol import new_request_id

        ids = {new_request_id() for _ in range(100)}
        assert len(ids) == 100

    def test_error_message(self):
        from relay_worker.protocol import RelayMessage, MessageType

        msg = RelayMessage.error("req1", "something went wrong")
        raw = msg.to_json()
        restored = RelayMessage.from_json(raw)

        assert restored.type == MessageType.ERROR
        assert restored.error == "something went wrong"


class TestConfig:
    def test_config_roundtrip(self):
        from relay_worker.config import ConfigManager, WorkerConfig, SSHConfig, ServerConfig

        with tempfile.TemporaryDirectory() as tmpdir:
            cm = ConfigManager()
            cm._config_dir = Path(tmpdir)
            cm._config_path = Path(tmpdir) / "config.json"
            cm._machine_key = "test-key-for-encryption"

            config = WorkerConfig(
                ssh=SSHConfig(
                    host="test.example.com",
                    port=2222,
                    username="testuser",
                    auth_method="key",
                    key_path="/home/test/.ssh/id_rsa",
                    password="",
                    local_forward_port=4096,
                    remote_forward_port=4096,
                ),
                server=ServerConfig(
                    url="wss://server.example.com:8443/relay",
                    auth_token="secret-token",
                    tls_verify=True,
                ),
            )

            cm.save(config)
            assert cm.exists()

            loaded = cm.load()
            assert loaded.ssh.host == "test.example.com"
            assert loaded.ssh.port == 2222
            assert loaded.ssh.username == "testuser"
            assert loaded.server.url == "wss://server.example.com:8443/relay"

    def test_config_encrypts_secrets(self):
        from relay_worker.config import ConfigManager, WorkerConfig, SSHConfig, ServerConfig

        with tempfile.TemporaryDirectory() as tmpdir:
            cm = ConfigManager()
            cm._config_dir = Path(tmpdir)
            cm._config_path = Path(tmpdir) / "config.json"
            cm._machine_key = "test-key"

            config = WorkerConfig(
                ssh=SSHConfig(
                    host="host",
                    password="mypassword",
                ),
                server=ServerConfig(
                    auth_token="mytoken",
                ),
            )

            cm.save(config)

            with open(cm._config_path) as f:
                raw = json.load(f)

            assert raw["ssh"]["password"] != "mypassword"
            assert raw["server"]["auth_token"] != "mytoken"

            loaded = cm.load()
            assert loaded.ssh.password == "mypassword"
            assert loaded.server.auth_token == "mytoken"

    def test_config_delete(self):
        from relay_worker.config import ConfigManager

        with tempfile.TemporaryDirectory() as tmpdir:
            cm = ConfigManager()
            cm._config_dir = Path(tmpdir)
            cm._config_path = Path(tmpdir) / "config.json"

            cm._config_path.write_text("{}")
            assert cm.exists()

            cm.delete()
            assert not cm.exists()


class TestDoctor:
    def test_check_ssh_found(self):
        from relay_worker.doctor import Doctor

        with tempfile.TemporaryDirectory() as tmpdir:
            cm = ConfigManager()
            cm._config_dir = Path(tmpdir)
            cm._config_path = Path(tmpdir) / "config.json"

            doctor = Doctor(cm)
            result = doctor._check_ssh()
            assert result.passed is True

    def test_check_config_missing(self):
        from relay_worker.doctor import Doctor

        with tempfile.TemporaryDirectory() as tmpdir:
            cm = ConfigManager()
            cm._config_dir = Path(tmpdir)
            cm._config_path = Path(tmpdir) / "config.json"

            doctor = Doctor(cm)
            result = doctor._check_config()
            assert result.passed is False
            assert "setup" in result.fix.lower()


class TestStatusCollector:
    def test_uptime_format(self):
        from relay_worker.status import StatusCollector

        assert StatusCollector._format_uptime(0) == "N/A"
        assert StatusCollector._format_uptime(61) == "1m 1s"
        assert StatusCollector._format_uptime(3661) == "1h 1m 1s"
        assert StatusCollector._format_uptime(90061) == "1d 1h 1m 1s"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
