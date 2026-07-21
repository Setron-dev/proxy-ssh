from __future__ import annotations

import getpass
import hashlib
import os
import platform
import secrets
from pathlib import Path


def _get_machine_key() -> str:
    system = platform.system()
    if system == "Linux":
        machine_id = ""
        try:
            machine_id = Path("/etc/machine-id").read_text().strip()
        except Exception:
            pass
        user = getpass.getuser()
        return hashlib.sha256(f"{machine_id}:{user}".encode()).hexdigest()
    elif system == "Darwin":
        serial = ""
        try:
            serial = os.popen(
                "ioreg -rd1 -c IOPlatformExpertDevice 2>/dev/null "
                "| grep IOPlatformSerialNumber | awk '{print $NF}' | tr -d '\"'"
            ).read().strip()
        except Exception:
            pass
        user = getpass.getuser()
        return hashlib.sha256(f"{serial}:{user}".encode()).hexdigest()
    else:
        return hashlib.sha256(f"{platform.node()}:{getpass.getuser()}".encode()).hexdigest()


def _xor_encrypt(data: bytes, key: bytes) -> bytes:
    return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))


def _derive_key(password: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)


def encrypt_secret(plaintext: str, machine_key: str) -> str:
    salt = secrets.token_bytes(16)
    key = _derive_key(machine_key, salt)
    iv = secrets.token_bytes(16)
    encrypted = _xor_encrypt(plaintext.encode("utf-8"), key)
    return (salt + iv + encrypted).hex()


def decrypt_secret(ciphertext: str, machine_key: str) -> str:
    try:
        raw = bytes.fromhex(ciphertext)
        salt = raw[:16]
        _iv = raw[16:32]
        encrypted = raw[32:]
        key = _derive_key(machine_key, salt)
        return _xor_encrypt(encrypted, key).decode("utf-8")
    except Exception:
        return ""
