from __future__ import annotations

import os
import plistlib
from pathlib import Path

import pytest
from typer.testing import CliRunner

import dbrestore.cli as cli_module
from dbrestore import scheduler as scheduler_module
from dbrestore.cli import app

runner = CliRunner()


def test_schedule_install_command_writes_launchd_plist_on_macos(
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

    launchctl_calls: list[list[str]] = []

    def fake_run_launchctl(args: list[str], check: bool = True) -> object:
        launchctl_calls.append(args)

        class Result:
            returncode = 0
            stdout = "ok\n"
            stderr = ""

        return Result()

    monkeypatch.setattr(scheduler_module.sys, "platform", "darwin")
    monkeypatch.setattr(scheduler_module, "_run_launchctl", fake_run_launchctl)
    monkeypatch.setattr(
        scheduler_module, "_resolve_run_identity", lambda *_args: ("alice", "staff")
    )
    monkeypatch.setattr(scheduler_module.os, "getuid", lambda: 501)

    unit_dir = tmp_path / "LaunchAgents"
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
    assert "Installed schedule for profile 'postgres_local' using Launchd" in result.stdout
    assert "Launchd label: io.dbrestore.backup.postgres_local" in result.stdout

    plist_path = unit_dir / "io.dbrestore.backup.postgres_local.plist"
    plist_payload = plistlib.loads(plist_path.read_bytes())
    assert plist_payload["Label"] == "io.dbrestore.backup.postgres_local"
    assert plist_payload["StartCalendarInterval"] == {"Minute": 0}
    assert plist_payload["ProgramArguments"][-2:] == [
        "--env-file",
        str(env_dir / "postgres_local.env"),
    ]
    assert "UserName" not in plist_payload
    assert "GroupName" not in plist_payload

    assert (env_dir / "postgres_local.env").read_text(encoding="utf-8").endswith("PGPASSWORD=\n")
    assert launchctl_calls[0] == ["bootout", "gui/501", str(plist_path)]
    assert launchctl_calls[1] == ["bootstrap", "gui/501", str(plist_path)]


def test_schedule_status_command_reports_launchd_state(
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
    schedule:
      preset: weekly
      persistent: true
""".strip(),
        encoding="utf-8",
    )

    unit_dir = tmp_path / "LaunchAgents"
    unit_dir.mkdir()
    plist_path = unit_dir / "io.dbrestore.backup.sqlite_local.plist"
    plist_path.write_text("<plist />", encoding="utf-8")

    def fake_run_launchctl(args: list[str], check: bool = True) -> object:
        lookup = {
            (
                "print",
                "gui/501/io.dbrestore.backup.sqlite_local",
            ): "\n".join(
                [
                    "gui/501/io.dbrestore.backup.sqlite_local = {",
                    "    state = waiting",
                    "    runs = 4",
                    "    last exit code = 0",
                    "}",
                ]
            ),
            (
                "print-disabled",
                "gui/501",
            ): '\t"io.dbrestore.backup.sqlite_local" => enabled\n',
        }

        class Result:
            def __init__(self, stdout: str, returncode: int = 0) -> None:
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = ""

        text = lookup.get(tuple(args))  # type: ignore
        if text is None:
            return Result("", returncode=1)
        return Result(text)

    monkeypatch.setattr(scheduler_module.sys, "platform", "darwin")
    monkeypatch.setattr(scheduler_module, "_run_launchctl", fake_run_launchctl)
    monkeypatch.setattr(scheduler_module.os, "getuid", lambda: 501)

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
            str(tmp_path / "env"),
        ],
    )

    assert result.exit_code == 0
    assert "Backend: Launchd" in result.stdout
    assert "Job: io.dbrestore.backup.sqlite_local (loaded=True, state=waiting)" in result.stdout
    assert f"Definition: {plist_path}" in result.stdout
    assert "Domain: gui/501" in result.stdout
    assert "Enabled: True" in result.stdout
    assert "Last exit code: 0" in result.stdout
    assert "OnCalendar: weekly" in result.stdout


def test_run_scheduled_command_loads_env_file_before_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_path = tmp_path / "schedule.env"
    env_path.write_text("PGPASSWORD=secret\n", encoding="utf-8")
    monkeypatch.delenv("PGPASSWORD", raising=False)

    def fake_run_scheduled_cycle(
        profile_name: str, config_path: Path, console: object = None
    ) -> dict[str, str]:
        assert profile_name == "demo"
        assert os.environ["PGPASSWORD"] == "secret"
        return {"verification_status": "skipped"}

    monkeypatch.setattr(cli_module, "run_scheduled_cycle", fake_run_scheduled_cycle)

    result = runner.invoke(
        app,
        [
            "run-scheduled",
            "--profile",
            "demo",
            "--config",
            str(tmp_path / "dbrestore.yaml"),
            "--env-file",
            str(env_path),
        ],
    )

    assert result.exit_code == 0
    assert "Scheduled cycle completed for profile 'demo' (verification=skipped)" in result.stdout
