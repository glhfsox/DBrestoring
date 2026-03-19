from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from dbrestore.adapters import get_adapter
from dbrestore.errors import PreflightError
from dbrestore.operations import run_backup, run_restore


def test_sqlite_backup_restore_integration(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    restored = tmp_path / "restored.sqlite3"
    config_path = tmp_path / "dbrestore.yaml"

    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
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

    backup_result = run_backup(profile_name="source", config_path=config_path)
    run_restore(
        profile_name="target",
        config_path=config_path,
        input_path=Path(backup_result["artifact_path"]),
    )

    with sqlite3.connect(restored) as connection:
        row = connection.execute("SELECT name FROM items").fetchone()

    assert row == ("widget",)


def test_restore_precheck_stops_restore_before_adapter_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.sqlite3"
    restored = tmp_path / "restored.sqlite3"
    config_path = tmp_path / "dbrestore.yaml"

    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
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

    backup_result = run_backup(profile_name="source", config_path=config_path)
    adapter = get_adapter("sqlite")
    restore_called = {"value": False}

    def fake_validate_restore_target(profile: object) -> None:
        raise PreflightError("target database is not writable")

    def fake_restore(profile: object, source_path: Path, redactor: object) -> None:
        restore_called["value"] = True

    monkeypatch.setattr(adapter, "validate_restore_target", fake_validate_restore_target)
    monkeypatch.setattr(adapter, "restore", fake_restore)

    with pytest.raises(PreflightError, match="target database is not writable"):
        run_restore(
            profile_name="target",
            config_path=config_path,
            input_path=Path(backup_result["artifact_path"]),
        )

    assert restore_called["value"] is False
