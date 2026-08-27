"""Authenticated encryption for deliberately exported configuration secrets."""

import base64
import json
import os


_AAD = b"MCCxBot encrypted config v1"
_MAX_ENCRYPTED_BACKUP_BYTES = 1024 * 1024


def _derive_key(passphrase: str, salt: bytes) -> bytes:
    if len(passphrase) < 16:
        raise ValueError("CONFIG_EXPORT_PASSPHRASE must contain at least 16 characters")
    try:
        from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
    except ImportError as exc:
        raise RuntimeError(
            "Encrypted exports require the pinned cryptography dependency"
        ) from exc
    return Scrypt(salt=salt, length=32, n=2**14, r=8, p=1).derive(
        passphrase.encode("utf-8")
    )


def encrypt_config_export(config: dict, passphrase: str) -> bytes:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    salt = os.urandom(16)
    nonce = os.urandom(12)
    key = _derive_key(passphrase, salt)
    plaintext = json.dumps(
        config, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, _AAD)
    envelope = {
        "format": "mccxbot-config-aes256gcm-v1",
        "kdf": "scrypt-n16384-r8-p1",
        "salt": base64.b64encode(salt).decode("ascii"),
        "nonce": base64.b64encode(nonce).decode("ascii"),
        "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
    }
    return json.dumps(envelope, separators=(",", ":")).encode("utf-8")


def decrypt_config_export(encrypted: bytes, passphrase: str) -> dict:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if len(encrypted) > _MAX_ENCRYPTED_BACKUP_BYTES:
        raise ValueError("encrypted backup exceeds the 1 MiB safety limit")
    envelope = json.loads(encrypted)
    if type(envelope) is not dict:
        raise ValueError("encrypted backup root must be an object")
    if envelope.get("format") != "mccxbot-config-aes256gcm-v1":
        raise ValueError("unsupported encrypted backup format")
    try:
        salt = base64.b64decode(envelope["salt"], validate=True)
        nonce = base64.b64decode(envelope["nonce"], validate=True)
        ciphertext = base64.b64decode(envelope["ciphertext"], validate=True)
    except (KeyError, ValueError) as exc:
        raise ValueError("invalid encrypted backup envelope") from exc
    if len(salt) != 16 or len(nonce) != 12:
        raise ValueError("invalid encrypted backup parameters")
    plaintext = AESGCM(_derive_key(passphrase, salt)).decrypt(
        nonce, ciphertext, _AAD
    )
    config = json.loads(plaintext)
    if type(config) is not dict:
        raise ValueError("decrypted config root must be an object")
    return config
