from __future__ import annotations

from pathlib import Path

import pytest

from dbrestore.config import collect_profile_env_vars, load_config
from dbrestore.errors import ConfigError
from dbrestore.operations import run_validate_config
from dbrestore.utils import Redactor, expand_env_placeholders


def test_expand_env_placeholders_reports_missing() -> None:
    expanded, missing = expand_env_placeholders({"password": "${DB_PASSWORD}", "host": "localhost"}, environ={})
    assert expanded["password"] == "${DB_PASSWORD}"
    assert missing == {"DB_PASSWORD"}


def test_load_config_expands_env_vars(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PASSWORD", "topsecret")
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
profiles:
  pg:
    db_type: postgres
    host: localhost
    username: app
    password: ${DB_PASSWORD}
    database: app_db
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.get_profile("pg").password_value == "topsecret"
    assert config.output_dir_for(config.get_profile("pg")) == (tmp_path / "backups").resolve()


def test_load_config_rejects_missing_env_var(tmp_path: Path) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
profiles:
  pg:
    db_type: postgres
    username: app
    password: ${MISSING_SECRET}
    database: app_db
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="Missing environment variables"):
        load_config(config_path)


def test_load_config_rejects_duplicate_profile_keys(tmp_path: Path) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
profiles:
  postgres_local:
    db_type: postgres
    username: app
    database: app_db
  postgres_local:
    db_type: postgres
    username: app
    database: verify_db
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="duplicate key 'postgres_local'"):
        load_config(config_path)


def test_redactor_masks_secret_values() -> None:
    redactor = Redactor({"topsecret"})

    sanitized = redactor.sanitize_text("password=topsecret mongodb://user:topsecret@localhost")

    assert "topsecret" not in sanitized
    assert "***" in sanitized


def test_load_config_falls_back_to_legacy_yml_filename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "dbrestore.yml"
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
    config = load_config(require_env=False)

    assert config.source_path == config_path.resolve()


def test_load_config_parses_schedule_and_retention(tmp_path: Path) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
  retention:
    keep_last: 7
profiles:
  sqlite_local:
    db_type: sqlite
    database: ./data/app.sqlite3
    schedule:
      preset: daily
      persistent: true
    retention:
      keep_last: 2
      max_age_days: 14
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)
    profile = config.get_profile("sqlite_local")
    retention = config.retention_for(profile)

    assert profile.schedule is not None
    assert profile.schedule.on_calendar == "daily"
    assert profile.schedule.persistent is True
    assert retention is not None
    assert retention.keep_last == 2
    assert retention.max_age_days == 14


def test_load_config_accepts_legacy_on_calendar_field(tmp_path: Path) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
profiles:
  sqlite_local:
    db_type: sqlite
    database: ./data/app.sqlite3
    schedule:
      on_calendar: hourly
      persistent: true
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)
    profile = config.get_profile("sqlite_local")

    assert profile.schedule is not None
    assert profile.schedule.preset == "hourly"
    assert profile.schedule.on_calendar == "hourly"


def test_collect_profile_env_vars_reads_profile_and_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
defaults:
  output_dir: ${BACKUP_ROOT}/data
profiles:
  postgres_local:
    db_type: postgres
    username: postgres
    password: ${PGPASSWORD}
    database: app_db
    schedule:
      preset: hourly
""".strip(),
        encoding="utf-8",
    )

    env_vars = collect_profile_env_vars(config_path, "postgres_local")

    assert env_vars == ["BACKUP_ROOT", "PGPASSWORD"]


def test_load_config_parses_default_slack_notifications(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SLACK_WEBHOOK", "https://hooks.slack.test/services/T000/B000/XXXX")
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
defaults:
  notifications:
    slack:
      webhook_url: ${SLACK_WEBHOOK}
profiles:
  sqlite_local:
    db_type: sqlite
    database: ./data/app.sqlite3
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)
    notifications = config.notifications_for(config.get_profile("sqlite_local"))

    assert notifications is not None
    assert notifications.slack is not None
    assert notifications.slack.webhook_url_value == "https://hooks.slack.test/services/T000/B000/XXXX"
    assert "backup.completed" in notifications.slack.events


def test_collect_profile_env_vars_includes_s3_storage_envs(tmp_path: Path) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
storage:
  type: s3
  bucket: my-backups
  access_key_id: ${AWS_ACCESS_KEY_ID}
  secret_access_key: ${AWS_SECRET_ACCESS_KEY}
profiles:
  sqlite_local:
    db_type: sqlite
    database: ./data/app.sqlite3
""".strip(),
        encoding="utf-8",
    )

    env_vars = collect_profile_env_vars(config_path, "sqlite_local")

    assert env_vars == ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]


def test_validate_config_reports_invalid_output_dir(tmp_path: Path) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
defaults:
  output_dir: ~./backups
  log_dir: ./logs
profiles:
  sqlite_local:
    db_type: sqlite
    database: ./data/app.sqlite3
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="Invalid output_dir"):
        run_validate_config(config_path)
