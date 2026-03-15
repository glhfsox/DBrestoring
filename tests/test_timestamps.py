from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dbrestore.logging import RunLogger
from dbrestore.operations import run_backup
from dbrestore.utils import format_storage_timestamp, format_timestamp, parse_timestamp


def test_format_timestamp_uses_second_precision_and_timezone() -> None:
    value = datetime(2026, 3, 15, 18, 31, 38, 987654, tzinfo=timezone(timedelta(hours=1)))

    assert format_timestamp(value) == "18:31:38 2026-03-15"
    assert format_storage_timestamp(value) == "20260315T183138"


def test_parse_timestamp_accepts_z_suffix() -> None:
    parsed = parse_timestamp("2026-03-15T17:31:38Z")

    assert parsed.utcoffset() == timedelta(0)
    assert parsed.microsecond == 0


def test_parse_timestamp_accepts_simple_local_format() -> None:
    parsed = parse_timestamp("18:31:38 2026-03-15")

    assert parsed.tzinfo is not None
    assert parsed.strftime("%H:%M:%S %Y-%m-%d") == "18:31:38 2026-03-15"


def test_run_logger_writes_local_second_precision_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_now = datetime(2026, 3, 15, 18, 31, 38, 654321, tzinfo=timezone(timedelta(hours=1)))
    monkeypatch.setattr("dbrestore.logging.current_time", lambda: fixed_now)

    log_file = tmp_path / "runs.jsonl"
    logger = RunLogger(log_file)
    logger.log_event("backup.completed", {"profile": "pg"})

    record = json.loads(log_file.read_text(encoding="utf-8").strip())
    assert record["timestamp"] == "18:31:38 2026-03-15"


def test_backup_manifest_uses_local_second_precision_timestamps(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
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
  sqlite_local:
    db_type: sqlite
    database: {source}
""".strip(),
        encoding="utf-8",
    )

    result = run_backup(profile_name="sqlite_local", config_path=config_path)
    manifest_path = Path(result["manifest_path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    for key in ("started_at", "finished_at"):
        assert "." not in manifest[key]
        assert parse_timestamp(manifest[key]).tzinfo is not None
        assert len(manifest[key].split()) == 2
