from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dbrestore import scheduler as scheduler_module
from dbrestore.cli import app

runner = CliRunner()


def test_validate_config_command_succeeds(tmp_path: Path) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
profiles:
  sqlite_local:
    db_type: sqlite
    database: ./data/app.sqlite3
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["validate-config", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "Configuration is valid" in result.stdout


def test_validate_config_command_uses_default_yaml_filename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate-config"])

    assert result.exit_code == 0
    assert "Configuration is valid" in result.stdout


def test_backup_command_creates_manifest_and_artifact(tmp_path: Path) -> None:
    database_path = tmp_path / "data.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
  compression: gzip
profiles:
  sqlite_local:
    db_type: sqlite
    database: {database_path}
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app, ["backup", "--profile", "sqlite_local", "--config", str(config_path)]
    )

    assert result.exit_code == 0
    assert "Backup completed" in result.stdout

    backup_root = tmp_path / "backups" / "sqlite_local"
    run_dirs = list(backup_root.iterdir())
    assert len(run_dirs) == 1
    manifest_path = run_dirs[0] / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["compression"] == "gzip"
    assert manifest["profile"] == "sqlite_local"
    assert Path(manifest["artifact_path"]).exists()


def test_restore_command_uses_backup_directory_input(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    restored = tmp_path / "restored.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    config_path = tmp_path / "dbrestore.yaml"
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

  sqlite_restore:
    db_type: sqlite
    database: {restored}
""".strip(),
        encoding="utf-8",
    )

    backup_result = runner.invoke(
        app, ["backup", "--profile", "sqlite_local", "--config", str(config_path)]
    )
    assert backup_result.exit_code == 0

    run_dir = next((tmp_path / "backups" / "sqlite_local").iterdir())
    restore_result = runner.invoke(
        app,
        [
            "restore",
            "--profile",
            "sqlite_restore",
            "--config",
            str(config_path),
            "--input",
            str(run_dir),
        ],
    )

    assert restore_result.exit_code == 0
    with sqlite3.connect(restored) as connection:
        row = connection.execute("SELECT name FROM items").fetchone()
    assert row == ("widget",)


def test_restore_command_rejects_selective_restore_for_sqlite(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    restored = tmp_path / "restored.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    config_path = tmp_path / "dbrestore.yaml"
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

  sqlite_restore:
    db_type: sqlite
    database: {restored}
""".strip(),
        encoding="utf-8",
    )

    backup_result = runner.invoke(
        app, ["backup", "--profile", "sqlite_local", "--config", str(config_path)]
    )
    assert backup_result.exit_code == 0
    run_dir = next((tmp_path / "backups" / "sqlite_local").iterdir())

    restore_result = runner.invoke(
        app,
        [
            "restore",
            "--profile",
            "sqlite_restore",
            "--config",
            str(config_path),
            "--input",
            str(run_dir),
            "--table",
            "items",
        ],
    )

    assert restore_result.exit_code == 1
    assert "Selective restore is not supported for db_type 'sqlite'" in restore_result.stderr


def test_backup_retention_keeps_last_two_runs(tmp_path: Path) -> None:
    database_path = tmp_path / "data.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
profiles:
  sqlite_local:
    db_type: sqlite
    database: {database_path}
    compression: false
    retention:
      keep_last: 2
""".strip(),
        encoding="utf-8",
    )

    for _ in range(3):
        result = runner.invoke(
            app, ["backup", "--profile", "sqlite_local", "--config", str(config_path)]
        )
        assert result.exit_code == 0

    run_dirs = sorted((tmp_path / "backups" / "sqlite_local").iterdir())
    assert len(run_dirs) == 2
    for run_dir in run_dirs:
        assert (run_dir / "manifest.json").exists()


def test_verify_latest_command_restores_into_target_profile(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    verification_target = tmp_path / "verification.sqlite3"

    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    with sqlite3.connect(verification_target) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('stale')")
        connection.commit()

    config_path = tmp_path / "dbrestore.yaml"
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
  verification_target:
    db_type: sqlite
    database: {verification_target}
""".strip(),
        encoding="utf-8",
    )

    backup_result = runner.invoke(
        app, ["backup", "--profile", "source", "--config", str(config_path)]
    )
    assert backup_result.exit_code == 0

    verify_result = runner.invoke(
        app,
        [
            "verify-latest",
            "--profile",
            "source",
            "--target-profile",
            "verification_target",
            "--config",
            str(config_path),
        ],
    )

    assert verify_result.exit_code == 0
    assert "Verification succeeded" in verify_result.stdout

    with sqlite3.connect(verification_target) as connection:
        row = connection.execute("SELECT name FROM items").fetchone()
    assert row == ("widget",)

    log_file = tmp_path / "logs" / "runs.jsonl"
    events = [json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines()]
    assert any(event["event"] == "verification.completed" for event in events)


def test_run_scheduled_command_runs_backup_and_verification_from_profile_config(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.sqlite3"
    verification_target = tmp_path / "verification.sqlite3"

    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    with sqlite3.connect(verification_target) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('stale')")
        connection.commit()

    config_path = tmp_path / "dbrestore.yaml"
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

    result = runner.invoke(
        app, ["run-scheduled", "--profile", "source", "--config", str(config_path)]
    )

    assert result.exit_code == 0
    assert "verification=verified" in result.stdout

    with sqlite3.connect(verification_target) as connection:
        row = connection.execute("SELECT name FROM items").fetchone()
    assert row == ("widget",)

    log_file = tmp_path / "logs" / "runs.jsonl"
    events = [json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines()]
    assert any(event["event"] == "scheduled_cycle.completed" for event in events)
    assert any(event["event"] == "verification.completed" for event in events)


def test_schedule_install_command_writes_units_and_env_template(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "data.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
profiles:
  postgres_local:
    db_type: postgres
    host: localhost
    username: postgres
    password: ${PGPASSWORD}
    database: app_db
    schedule:
      preset: hourly
""".strip(),
        encoding="utf-8",
    )

    systemctl_calls: list[list[str]] = []

    def fake_run_systemctl(args: list[str], check: bool = True) -> object:
        systemctl_calls.append(args)

        class Result:
            returncode = 0
            stdout = "ok\n"
            stderr = ""

        return Result()

    monkeypatch.setattr(scheduler_module, "_run_systemctl", fake_run_systemctl)
    monkeypatch.setattr(scheduler_module.os, "geteuid", lambda: 1000)

    unit_dir = tmp_path / "systemd"
    env_dir = tmp_path / "env"
    result = runner.invoke(
        app,
        [
            "schedule",
            "install",
            "--profile",
            "postgres_local",
            "--config",
            str(config_path),
            "--unit-dir",
            str(unit_dir),
            "--env-dir",
            str(env_dir),
        ],
    )

    assert result.exit_code == 0
    assert "Installed dbrestore-backup-postgres_local.timer" in result.stdout
    service_unit = (unit_dir / "dbrestore-backup-postgres_local.service").read_text(
        encoding="utf-8"
    )
    timer_unit = (unit_dir / "dbrestore-backup-postgres_local.timer").read_text(encoding="utf-8")
    env_template = (env_dir / "postgres_local.env").read_text(encoding="utf-8")
    assert "ExecStart=" in service_unit
    assert "EnvironmentFile=" in service_unit
    assert "OnCalendar=hourly" in timer_unit
    assert "Persistent=true" in timer_unit
    assert "PGPASSWORD=" in env_template
    assert systemctl_calls[0] == ["daemon-reload"]
    assert systemctl_calls[1] == ["enable", "--now", "dbrestore-backup-postgres_local.timer"]


def test_schedule_status_command_reports_states(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
profiles:
  sqlite_local:
    db_type: sqlite
    database: ./data/app.sqlite3
    schedule:
      preset: weekly
      persistent: true
""".strip(),
        encoding="utf-8",
    )

    unit_dir = tmp_path / "systemd"
    env_dir = tmp_path / "env"
    unit_dir.mkdir()
    (unit_dir / "dbrestore-backup-sqlite_local.service").write_text("service", encoding="utf-8")
    (unit_dir / "dbrestore-backup-sqlite_local.timer").write_text("timer", encoding="utf-8")

    def fake_state(args: list[str], check: bool = True) -> object:
        state_map: dict[tuple[str, ...], str] = {
            ("is-enabled", "dbrestore-backup-sqlite_local.timer"): "enabled\n",
            ("is-active", "dbrestore-backup-sqlite_local.timer"): "active\n",
            ("is-active", "dbrestore-backup-sqlite_local.service"): "inactive\n",
            (
                "show",
                "dbrestore-backup-sqlite_local.timer",
                "--property=NextElapseUSecRealtime",
                "--value",
            ): "Sat 2026-03-21 12:00:00 CET\n",
            (
                "show",
                "dbrestore-backup-sqlite_local.timer",
                "--property=LastTriggerUSec",
                "--value",
            ): "Sat 2026-03-21 11:00:00 CET\n",
        }

        class Result:
            def __init__(self, stdout: str) -> None:
                self.returncode = 0
                self.stdout = stdout
                self.stderr = ""

        return Result(state_map.get(tuple(args), "unknown\n"))

    monkeypatch.setattr(scheduler_module, "_run_systemctl", fake_state)

    result = runner.invoke(
        app,
        [
            "schedule",
            "status",
            "--profile",
            "sqlite_local",
            "--config",
            str(config_path),
            "--unit-dir",
            str(unit_dir),
            "--env-dir",
            str(env_dir),
        ],
    )

    assert result.exit_code == 0
    assert "Timer: dbrestore-backup-sqlite_local.timer (enabled, active)" in result.stdout
    assert "OnCalendar: weekly" in result.stdout
    assert "Next run: Sat 2026-03-21 12:00:00 CET" in result.stdout
    assert "Last trigger: Sat 2026-03-21 11:00:00 CET" in result.stdout


def test_status_command_reports_readiness_dashboard_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.sqlite3"
    verification_target = tmp_path / "verification.sqlite3"

    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    with sqlite3.connect(verification_target) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("CREATE TABLE IF NOT EXISTS items_shadow (id INTEGER PRIMARY KEY)")
        connection.commit()

    config_path = tmp_path / "dbrestore.yaml"
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
    schedule:
      preset: daily
    verification:
      target_profile: verification_target
  verification_target:
    db_type: sqlite
    database: {verification_target}
""".strip(),
        encoding="utf-8",
    )

    runner.invoke(app, ["backup", "--profile", "source", "--config", str(config_path)])
    runner.invoke(
        app,
        [
            "verify-latest",
            "--profile",
            "source",
            "--target-profile",
            "verification_target",
            "--config",
            str(config_path),
        ],
    )

    unit_dir = tmp_path / "systemd"
    env_dir = tmp_path / "env"
    unit_dir.mkdir()
    (unit_dir / "dbrestore-backup-source.service").write_text("service", encoding="utf-8")
    (unit_dir / "dbrestore-backup-source.timer").write_text("timer", encoding="utf-8")

    def fake_state(args: list[str], check: bool = True) -> object:
        state_map: dict[tuple[str, ...], str] = {
            ("is-enabled", "dbrestore-backup-source.timer"): "enabled\n",
            ("is-active", "dbrestore-backup-source.timer"): "active\n",
            ("is-active", "dbrestore-backup-source.service"): "inactive\n",
            (
                "show",
                "dbrestore-backup-source.timer",
                "--property=NextElapseUSecRealtime",
                "--value",
            ): "Sun 2026-03-22 12:00:00 CET\n",
        }

        class Result:
            def __init__(self, stdout: str) -> None:
                self.returncode = 0
                self.stdout = stdout
                self.stderr = ""

        return Result(state_map.get(tuple(args), "unknown\n"))

    monkeypatch.setattr(scheduler_module, "_run_systemctl", fake_state)

    result = runner.invoke(
        app,
        [
            "status",
            "--profile",
            "source",
            "--config",
            str(config_path),
            "--unit-dir",
            str(unit_dir),
            "--env-dir",
            str(env_dir),
        ],
    )

    assert result.exit_code == 0
    assert "Last Backup:" in result.stdout
    assert "Last Verification: ok" in result.stdout
    assert "Next Run: Sun 2026-03-22 12:00:00 CET" in result.stdout
    assert "Verification Target: verification_target (scheduled=True)" in result.stdout


def test_preflight_command_reports_checks(tmp_path: Path) -> None:
    database_path = tmp_path / "data.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.commit()

    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: ./backups
profiles:
  sqlite_local:
    db_type: sqlite
    database: {database_path}
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "preflight",
            "--profile",
            "sqlite_local",
            "--config",
            str(config_path),
            "--no-connection",
        ],
    )

    assert result.exit_code == 0
    assert "Preflight: warning" in result.stdout
    assert "- environment: ok -" in result.stdout
    assert "- verification: warning -" in result.stdout


def test_schedule_show_and_save_env_commands(
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
    username: postgres
    password: ${PGPASSWORD}
    database: app_db
    schedule:
      preset: hourly
""".strip(),
        encoding="utf-8",
    )

    env_dir = tmp_path / "env"
    show_result = runner.invoke(
        app,
        [
            "schedule",
            "show-env",
            "--profile",
            "postgres_local",
            "--config",
            str(config_path),
            "--env-dir",
            str(env_dir),
        ],
    )

    assert show_result.exit_code == 0
    assert "PGPASSWORD=" in show_result.stdout

    payload_path = tmp_path / "schedule.env"
    payload_path.write_text("PGPASSWORD=secret\n", encoding="utf-8")
    save_result = runner.invoke(
        app,
        [
            "schedule",
            "save-env",
            "--profile",
            "postgres_local",
            "--config",
            str(config_path),
            "--env-dir",
            str(env_dir),
            "--env-file",
            str(payload_path),
        ],
    )

    assert save_result.exit_code == 0
    assert "Saved env file:" in save_result.stdout
    assert (env_dir / "postgres_local.env").read_text(encoding="utf-8") == "PGPASSWORD=secret\n"


def test_schedule_show_env_reports_permission_error(
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
    username: postgres
    password: ${PGPASSWORD}
    database: app_db
    schedule:
      preset: hourly
""".strip(),
        encoding="utf-8",
    )

    env_dir = tmp_path / "env"
    env_dir.mkdir()
    env_path = env_dir / "postgres_local.env"
    env_path.write_text("PGPASSWORD=secret\n", encoding="utf-8")
    original_read_text = Path.read_text

    def fake_read_text(self: Path, encoding: str | None = None, errors: str | None = None) -> str:
        if self == env_path:
            raise PermissionError("permission denied")
        return original_read_text(self, encoding=encoding, errors=errors)

    monkeypatch.setattr(Path, "read_text", fake_read_text)

    result = runner.invoke(
        app,
        [
            "schedule",
            "show-env",
            "--profile",
            "postgres_local",
            "--config",
            str(config_path),
            "--env-dir",
            str(env_dir),
        ],
    )

    assert result.exit_code == 1
    assert "Unable to read env file" in result.stderr


def test_schedule_remove_command_deletes_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    unit_dir = tmp_path / "systemd"
    env_dir = tmp_path / "env"
    unit_dir.mkdir()
    env_dir.mkdir()
    (unit_dir / "dbrestore-backup-sqlite_local.service").write_text("service", encoding="utf-8")
    (unit_dir / "dbrestore-backup-sqlite_local.timer").write_text("timer", encoding="utf-8")
    (env_dir / "sqlite_local.env").write_text("SQLITE_TOKEN=\n", encoding="utf-8")

    systemctl_calls: list[list[str]] = []

    def fake_run_systemctl(args: list[str], check: bool = True) -> object:
        systemctl_calls.append(args)

        class Result:
            returncode = 0
            stdout = "ok\n"
            stderr = ""

        return Result()

    monkeypatch.setattr(scheduler_module, "_run_systemctl", fake_run_systemctl)

    result = runner.invoke(
        app,
        [
            "schedule",
            "remove",
            "--profile",
            "sqlite_local",
            "--unit-dir",
            str(unit_dir),
            "--env-dir",
            str(env_dir),
            "--delete-env-file",
        ],
    )

    assert result.exit_code == 0
    assert "Removed schedule for profile 'sqlite_local'" in result.stdout
    assert not (unit_dir / "dbrestore-backup-sqlite_local.service").exists()
    assert not (unit_dir / "dbrestore-backup-sqlite_local.timer").exists()
    assert not (env_dir / "sqlite_local.env").exists()
    assert systemctl_calls[0] == ["disable", "--now", "dbrestore-backup-sqlite_local.timer"]
