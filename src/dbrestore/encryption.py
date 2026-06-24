"""AES-256-GCM encryption for backup artifacts.

File layout: MAGIC | version | 16-byte salt | 12-byte nonce | ciphertext+GCM tag.
The passphrase is stretched into a 32-byte key with scrypt; a random salt and
nonce per file mean the same passphrase yields different ciphertext each run.
"""

from __future__ import annotations

import os
import struct
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

from dbrestore.errors import ArtifactError

MAGIC = b"DBRE"
FORMAT_VERSION = 1
SALT_SIZE = 16
NONCE_SIZE = 12
TAG_SIZE = 16
HEADER_SIZE = len(MAGIC) + 1 + SALT_SIZE + NONCE_SIZE
ENCRYPTED_EXTENSION = ".enc"

_SCRYPT_N = 2**17
_SCRYPT_R = 8
_SCRYPT_P = 1
_KEY_LENGTH = 32


def derive_key(passphrase: str, salt: bytes) -> bytes:
    kdf = Scrypt(salt=salt, length=_KEY_LENGTH, n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P)
    return kdf.derive(passphrase.encode("utf-8"))


def encrypt_file(source: Path, destination: Path, passphrase: str) -> Path:
    salt = os.urandom(SALT_SIZE)
    nonce = os.urandom(NONCE_SIZE)
    key = derive_key(passphrase, salt)

    plaintext = source.read_bytes()
    aesgcm = AESGCM(key)
    ciphertext = aesgcm.encrypt(nonce, plaintext, None)

    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as out:
        out.write(MAGIC)
        out.write(struct.pack("B", FORMAT_VERSION))
        out.write(salt)
        out.write(nonce)
        out.write(ciphertext)  # includes appended GCM tag
    return destination


def decrypt_file(source: Path, destination: Path, passphrase: str) -> Path:
    data = source.read_bytes()
    if len(data) < HEADER_SIZE + TAG_SIZE:
        raise ArtifactError(f"Encrypted file too small or corrupt: {source}")

    magic = data[: len(MAGIC)]
    if magic != MAGIC:
        raise ArtifactError(f"Not a dbrestore-encrypted file (bad magic): {source}")

    version = struct.unpack("B", data[len(MAGIC) : len(MAGIC) + 1])[0]
    if version != FORMAT_VERSION:
        raise ArtifactError(f"Unsupported encryption format version {version}: {source}")

    offset = len(MAGIC) + 1
    salt = data[offset : offset + SALT_SIZE]
    offset += SALT_SIZE
    nonce = data[offset : offset + NONCE_SIZE]
    offset += NONCE_SIZE
    ciphertext_with_tag = data[offset:]

    key = derive_key(passphrase, salt)
    aesgcm = AESGCM(key)
    try:
        plaintext = aesgcm.decrypt(nonce, ciphertext_with_tag, None)
    except Exception as exc:
        raise ArtifactError("Decryption failed — wrong passphrase or corrupt file") from exc

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(plaintext)
    return destination


def is_encrypted(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < HEADER_SIZE:
        return False
    with path.open("rb") as f:
        return f.read(len(MAGIC)) == MAGIC
