from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from dbrestore import notifications as notifications_module
from dbrestore.operations import run_backup


def test_backup_success_sends_slack_notification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = _create_sqlite_source(tmp_path)
    config_path = _write_notification_config(tmp_path, database_path)
    sent: list[tuple[str, str]] = []

    monkeypatch.setenv("SLACK_WEBHOOK", "https://hooks.slack.test/services/T000/B000/XXXX")
    monkeypatch.setattr(
        notifications_module,
        "send_slack_webhook",
        lambda webhook_url, message: sent.append((webhook_url, message)),
    )

    run_backup(profile_name="sqlite_local", config_path=config_path)

    assert len(sent) == 1
    assert sent[0][0] == "https://hooks.slack.test/services/T000/B000/XXXX"
    assert "[dbrestore] Backup completed" in sent[0][1]
    assert "Profile: sqlite_local" in sent[0][1]


def test_backup_success_does_not_fail_when_slack_notification_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = _create_sqlite_source(tmp_path)
    config_path = _write_notification_config(tmp_path, database_path)

    monkeypatch.setenv("SLACK_WEBHOOK", "https://hooks.slack.test/services/T000/B000/XXXX")

    def fail_send(webhook_url: str, message: str) -> None:
        raise notifications_module.NotificationDeliveryError("network down")

    monkeypatch.setattr(notifications_module, "send_slack_webhook", fail_send)

    result = run_backup(profile_name="sqlite_local", config_path=config_path)

    assert result["profile"] == "sqlite_local"
    log_file = tmp_path / "logs" / "runs.jsonl"
    events = [
        json.loads(line)["event"] for line in log_file.read_text(encoding="utf-8").splitlines()
    ]
    assert "backup.completed" in events
    assert "notification.failed" in events


def test_notification_events_can_filter_out_backup_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = _create_sqlite_source(tmp_path)
    config_path = _write_notification_config(tmp_path, database_path, events=["backup.failed"])
    sent: list[tuple[str, str]] = []

    monkeypatch.setenv("SLACK_WEBHOOK", "https://hooks.slack.test/services/T000/B000/XXXX")
    monkeypatch.setattr(
        notifications_module,
        "send_slack_webhook",
        lambda webhook_url, message: sent.append((webhook_url, message)),
    )

    run_backup(profile_name="sqlite_local", config_path=config_path)

    assert sent == []


def _create_sqlite_source(tmp_path: Path) -> Path:
    database_path = tmp_path / "source.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()
    return database_path


def _write_notification_config(
    tmp_path: Path, database_path: Path, events: list[str] | None = None
) -> Path:
    config_path = tmp_path / "dbrestore.yaml"
    events_block = ""
    if events is not None:
        serialized_events = "\n".join(f"        - {event}" for event in events)
        events_block = f"\n      events:\n{serialized_events}"

    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
  notifications:
    slack:
      webhook_url: ${{SLACK_WEBHOOK}}{events_block}
profiles:
  sqlite_local:
    db_type: sqlite
    database: {database_path}
""".strip(),
        encoding="utf-8",
    )
    return config_path
