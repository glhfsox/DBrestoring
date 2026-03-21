from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from dbrestore.adapters import get_adapter
from dbrestore.config import load_config, write_raw_config
from dbrestore.errors import ConfigError
from dbrestore.gui import _dialog_geometry
from dbrestore.operations import (
    list_backup_history,
    list_run_log_events,
    run_backup,
    run_scheduled_cycle,
    run_test_connection_with_config,
    validate_profile_config,
)


def test_write_raw_config_round_trips_profile(tmp_path: Path) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    raw_config = {
        "version": 1,
        "defaults": {
            "output_dir": "./backups",
            "log_dir": "./logs",
            "compression": "gzip",
        },
        "profiles": {
            "sqlite_local": {
                "db_type": "sqlite",
                "database": "./data/app.sqlite3",
                "schedule": {
                    "preset": "daily",
                    "persistent": True,
                },
            }
        },
    }

    write_raw_config(config_path, raw_config)
    loaded = load_config(config_path, require_env=False)

    assert loaded.get_profile("sqlite_local").db_type == "sqlite"
    schedule = loaded.get_profile("sqlite_local").schedule
    assert schedule is not None
    assert getattr(schedule, "preset", None) == "daily"


def test_history_helpers_list_backups_and_logs(tmp_path: Path) -> None:
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

    run_backup(profile_name="sqlite_local", config_path=config_path)

    history = list_backup_history(config_path=config_path, profile_name="sqlite_local")
    events = list_run_log_events(config_path=config_path, profile_name="sqlite_local")

    assert len(history) == 1
    assert history[0]["profile"] == "sqlite_local"
    assert history[0]["artifact_path"]
    assert any(event["event"] == "backup.completed" for event in events)


def test_validate_profile_config_checks_selected_profile_runtime_config(tmp_path: Path) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
profiles:
  sqlite_local:
    db_type: sqlite
    database: ./data/app.sqlite3
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path, require_env=False)
    result = validate_profile_config(config, "sqlite_local")

    assert result["status"] == "ok"
    assert result["profile"] == "sqlite_local"


def test_test_connection_with_config_uses_selected_profile_runtime_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
profiles:
  postgres_local:
    db_type: postgres
    host: localhost
    username: app
    password: secret
    database: app_db
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path, require_env=False)
    seen: dict[str, str | None] = {}
    adapter = get_adapter("postgres")

    def fake_test_connection(profile: Any) -> None:
        seen["database"] = profile.database
        seen["password"] = profile.password_value

    monkeypatch.setattr(adapter, "test_connection", fake_test_connection)

    result = run_test_connection_with_config(config, "postgres_local")

    assert result["status"] == "ok"
    assert seen == {
        "database": "app_db",
        "password": "secret",
    }


def test_validate_profile_config_reports_missing_required_tool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
profiles:
  postgres_local:
    db_type: postgres
    host: localhost
    username: app
    password: secret
    database: app_db
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path, require_env=False)
    monkeypatch.setattr("dbrestore.operations.shutil.which", lambda tool: None)

    with pytest.raises(ConfigError, match="Required tool not found on PATH: pg_dump"):
        validate_profile_config(config, "postgres_local")


def test_dialog_geometry_clamps_to_screen() -> None:
    class FakeRoot:
        def update_idletasks(self) -> None:
            return None

        def winfo_screenwidth(self) -> int:
            return 800

        def winfo_screenheight(self) -> int:
            return 600

        def winfo_width(self) -> int:
            return 500

        def winfo_height(self) -> int:
            return 400

        def winfo_rootx(self) -> int:
            return 100

        def winfo_rooty(self) -> int:
            return 50

    width, height, x_pos, y_pos = _dialog_geometry(FakeRoot(), width=1200, height=900)

    assert width == 720
    assert height == 520
    assert x_pos >= 20
    assert y_pos >= 20


def test_run_backup_emits_progress_updates(tmp_path: Path) -> None:
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

    progress_events: list[dict[str, Any]] = []
    run_backup(
        profile_name="sqlite_local",
        config_path=config_path,
        progress=progress_events.append,
    )

    assert progress_events
    assert progress_events[0]["message"] == "Loading profile 'sqlite_local'"
    assert any(event["mode"] == "auto" for event in progress_events)
    assert any("target_percent" in event for event in progress_events if event["mode"] == "auto")
    assert progress_events[-1]["percent"] == 100.0


def test_run_scheduled_cycle_emits_progress_updates(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    verification_target = tmp_path / "verification.sqlite3"
    config_path = tmp_path / "dbrestore.yaml"

    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    with sqlite3.connect(verification_target) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
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
    verification:
      target_profile: verification_target
  verification_target:
    db_type: sqlite
    database: {verification_target}
""".strip(),
        encoding="utf-8",
    )

    progress_events: list[dict[str, Any]] = []
    run_scheduled_cycle(
        profile_name="source",
        config_path=config_path,
        progress=progress_events.append,
    )

    assert progress_events
    assert any("Backup:" in event["message"] for event in progress_events)
    assert any("Verify:" in event["message"] for event in progress_events)
    assert progress_events[-1]["message"] == "Scheduled cycle completed"
    assert progress_events[-1]["percent"] == 100.0
