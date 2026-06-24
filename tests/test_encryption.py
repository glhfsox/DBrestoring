from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from dbrestore.encryption import (
    ENCRYPTED_EXTENSION,
    MAGIC,
    decrypt_file,
    encrypt_file,
    is_encrypted,
)
from dbrestore.errors import ArtifactError
from dbrestore.operations import run_backup, run_restore

PASSPHRASE = "test-passphrase-42"


class TestEncryptionModule:
    def test_encrypt_decrypt_roundtrip(self, tmp_path: Path) -> None:
        original = tmp_path / "data.sql"
        original.write_bytes(b"CREATE TABLE t (id INT);\nINSERT INTO t VALUES (1);\n")

        encrypted = tmp_path / f"data.sql{ENCRYPTED_EXTENSION}"
        encrypt_file(original, encrypted, PASSPHRASE)

        assert encrypted.exists()
        assert encrypted.read_bytes()[:4] == MAGIC
        assert encrypted.read_bytes() != original.read_bytes()

        decrypted = tmp_path / "data_restored.sql"
        decrypt_file(encrypted, decrypted, PASSPHRASE)

        assert decrypted.read_bytes() == original.read_bytes()

    def test_decrypt_wrong_passphrase_raises(self, tmp_path: Path) -> None:
        original = tmp_path / "secret.bin"
        original.write_bytes(b"sensitive data here")

        encrypted = tmp_path / f"secret.bin{ENCRYPTED_EXTENSION}"
        encrypt_file(original, encrypted, PASSPHRASE)

        with pytest.raises(ArtifactError, match="wrong passphrase or corrupt"):
            decrypt_file(encrypted, tmp_path / "out.bin", "wrong-passphrase")

    def test_decrypt_corrupt_file_raises(self, tmp_path: Path) -> None:
        corrupt = tmp_path / "corrupt.enc"
        # Valid header (magic + version + 16-byte salt + 12-byte nonce) + fake ciphertext+tag
        corrupt.write_bytes(MAGIC + b"\x01" + b"\x00" * 28 + b"\xff" * 32)

        with pytest.raises(ArtifactError, match="wrong passphrase or corrupt"):
            decrypt_file(corrupt, tmp_path / "out.bin", PASSPHRASE)

    def test_decrypt_bad_magic_raises(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.enc"
        bad.write_bytes(b"NOPE" + b"\x00" * 60)

        with pytest.raises(ArtifactError, match="bad magic"):
            decrypt_file(bad, tmp_path / "out.bin", PASSPHRASE)

    def test_decrypt_too_small_raises(self, tmp_path: Path) -> None:
        tiny = tmp_path / "tiny.enc"
        tiny.write_bytes(b"DBRE\x01short")

        with pytest.raises(ArtifactError, match="too small or corrupt"):
            decrypt_file(tiny, tmp_path / "out.bin", PASSPHRASE)

    def test_is_encrypted_positive(self, tmp_path: Path) -> None:
        original = tmp_path / "f.bin"
        original.write_bytes(b"hello world")
        encrypted = tmp_path / "f.bin.enc"
        encrypt_file(original, encrypted, PASSPHRASE)

        assert is_encrypted(encrypted) is True

    def test_is_encrypted_negative(self, tmp_path: Path) -> None:
        plain = tmp_path / "plain.sql"
        plain.write_bytes(b"SELECT 1;")

        assert is_encrypted(plain) is False

    def test_is_encrypted_missing_file(self, tmp_path: Path) -> None:
        assert is_encrypted(tmp_path / "nonexistent.enc") is False


def _make_sqlite_config(tmp_path: Path, *, passphrase: str | None = None) -> tuple[Path, Path]:
    source = tmp_path / "source.sqlite3"
    restored = tmp_path / "restored.sqlite3"
    config_path = tmp_path / "dbrestore.yaml"

    with sqlite3.connect(source) as conn:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO items (name) VALUES ('encrypted-widget')")
        conn.commit()

    encryption_block = ""
    if passphrase:
        encryption_block = f"""
  encryption:
    passphrase: "{passphrase}"
"""

    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
{encryption_block}
profiles:
  source:
    db_type: sqlite
    database: {source}
  target:
    db_type: sqlite
    database: {restored}
""".strip(),
        encoding="utf-8",
    )
    return config_path, restored


def test_encrypted_backup_restore_via_cli_passphrase(tmp_path: Path) -> None:
    config_path, restored = _make_sqlite_config(tmp_path)

    backup_result = run_backup(
        profile_name="source",
        config_path=config_path,
        passphrase=PASSPHRASE,
    )

    artifact = Path(backup_result["artifact_path"])
    assert artifact.name.endswith(ENCRYPTED_EXTENSION)
    assert backup_result["metadata"]["encryption"] == "aes-256-gcm"

    run_restore(
        profile_name="target",
        config_path=config_path,
        input_path=artifact,
        passphrase=PASSPHRASE,
    )

    with sqlite3.connect(restored) as conn:
        row = conn.execute("SELECT name FROM items").fetchone()
    assert row == ("encrypted-widget",)


def test_encrypted_backup_restore_via_config_passphrase(tmp_path: Path) -> None:
    config_path, restored = _make_sqlite_config(tmp_path, passphrase=PASSPHRASE)

    backup_result = run_backup(
        profile_name="source",
        config_path=config_path,
    )

    artifact = Path(backup_result["artifact_path"])
    assert artifact.name.endswith(ENCRYPTED_EXTENSION)

    run_restore(
        profile_name="target",
        config_path=config_path,
        input_path=artifact,
    )

    with sqlite3.connect(restored) as conn:
        row = conn.execute("SELECT name FROM items").fetchone()
    assert row == ("encrypted-widget",)


def test_restore_encrypted_without_passphrase_raises(tmp_path: Path) -> None:
    config_path, _ = _make_sqlite_config(tmp_path)

    backup_result = run_backup(
        profile_name="source",
        config_path=config_path,
        passphrase=PASSPHRASE,
    )

    artifact = Path(backup_result["artifact_path"])

    with pytest.raises(ArtifactError, match="no passphrase was provided"):
        run_restore(
            profile_name="target",
            config_path=config_path,
            input_path=artifact,
        )


def test_restore_encrypted_with_wrong_passphrase_raises(tmp_path: Path) -> None:
    config_path, _ = _make_sqlite_config(tmp_path)

    backup_result = run_backup(
        profile_name="source",
        config_path=config_path,
        passphrase=PASSPHRASE,
    )

    artifact = Path(backup_result["artifact_path"])

    with pytest.raises(ArtifactError, match="wrong passphrase or corrupt"):
        run_restore(
            profile_name="target",
            config_path=config_path,
            input_path=artifact,
            passphrase="completely-wrong",
        )
