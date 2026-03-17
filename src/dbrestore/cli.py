"""This module exposes the command-line interface and maps commands to operations.
It stays intentionally thin: parse user input, call the right service function, print a clean result.
All actual backup, restore, verification, and scheduling rules live deeper in the app.
That keeps CLI behavior and GUI behavior consistent because both hit the same core logic."""

from __future__ import annotations

from pathlib import Path

import typer

from dbrestore.config import DEFAULT_CONFIG_PATH
from dbrestore.errors import DBRestoreError
from dbrestore.operations import run_backup, run_restore, run_test_connection, run_validate_config, run_verify_latest_backup
from dbrestore.scheduler import DEFAULT_ENV_DIR, DEFAULT_SYSTEMD_UNIT_DIR, install_schedule, remove_schedule, schedule_status

app = typer.Typer(help="Back up and restore supported databases.", no_args_is_help=True)
schedule_app = typer.Typer(help="Manage systemd-based backup schedules.")


def _handle_error(exc: DBRestoreError) -> None:
    typer.echo(str(exc), err=True)
    raise typer.Exit(code=1) from exc


@app.command("backup")
def backup_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."),
    output_dir: Path | None = typer.Option(None, "--output-dir", help="Override the configured output directory."),
    no_compress: bool = typer.Option(False, "--no-compress", help="Disable gzip compression for this backup."),
) -> None:
    try:
        run_backup(
            profile_name=profile,
            config_path=config,
            output_dir_override=output_dir,
            no_compress=no_compress,
            console=typer.echo,
        )
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("restore")
def restore_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    input_path: Path = typer.Option(..., "--input", help="Backup artifact path or run directory."),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."),
    table: list[str] | None = typer.Option(
        None,
        "--table",
        help="PostgreSQL table to restore. Repeat this option for multiple tables.",
    ),
    collection: list[str] | None = typer.Option(
        None,
        "--collection",
        help="MongoDB collection to restore. Repeat this option for multiple collections.",
    ),
) -> None:
    try:
        run_restore(
            profile_name=profile,
            input_path=input_path,
            config_path=config,
            tables=table,
            collections=collection,
            console=typer.echo,
        )
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("test-connection")
def test_connection_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."),
) -> None:
    try:
        result = run_test_connection(profile_name=profile, config_path=config)
        typer.echo(f"Connection succeeded for profile '{result['profile']}'")
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("validate-config")
def validate_config_command(
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."),
) -> None:
    try:
        result = run_validate_config(config_path=config)
        typer.echo(f"Configuration is valid for {len(result['profiles'])} profile(s)")
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("gui")
def gui_command(
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."),
) -> None:
    try:
        from dbrestore.gui import launch_gui

        launch_gui(config_path=config)
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("verify-latest")
def verify_latest_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Source backup profile from the YAML config."),
    target_profile: str = typer.Option(..., "--target-profile", "-t", help="Separate restore target profile used for verification."),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."),
) -> None:
    try:
        result = run_verify_latest_backup(
            source_profile_name=profile,
            target_profile_name=target_profile,
            config_path=config,
            console=typer.echo,
        )
        typer.echo(
            f"Verification succeeded for backup '{result['run_id']}' from '{profile}' into '{target_profile}'"
        )
    except DBRestoreError as exc:
        _handle_error(exc)


@schedule_app.command("install")
def schedule_install_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."),
    unit_dir: Path = typer.Option(DEFAULT_SYSTEMD_UNIT_DIR, "--unit-dir", help="Systemd unit directory."),
    env_dir: Path = typer.Option(DEFAULT_ENV_DIR, "--env-dir", help="Directory for per-profile env files."),
    force: bool = typer.Option(False, "--force", help="Overwrite existing unit files."),
    enable_timer: bool = typer.Option(True, "--enable/--no-enable", help="Enable and start the timer after install."),
    run_as_user: str | None = typer.Option(None, "--run-as-user", help="Linux user account the backup service should run as."),
    run_as_group: str | None = typer.Option(None, "--run-as-group", help="Linux group the backup service should run as."),
) -> None:
    try:
        result = install_schedule(
            profile_name=profile,
            config_path=config,
            unit_dir=unit_dir,
            env_dir=env_dir,
            force=force,
            enable_timer=enable_timer,
            run_as_user=run_as_user,
            run_as_group=run_as_group,
        )
        typer.echo(f"Installed {result['timer_name']} for profile '{profile}'")
        typer.echo(f"Service unit: {result['service_path']}")
        typer.echo(f"Timer unit: {result['timer_path']}")
        if result["env_file_path"]:
            typer.echo(f"Env file: {result['env_file_path']}")
            if result["env_template_created"]:
                typer.echo("Created env template file. Fill in the missing values before relying on the timer.")
    except DBRestoreError as exc:
        _handle_error(exc)


@schedule_app.command("status")
def schedule_status_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."),
    unit_dir: Path = typer.Option(DEFAULT_SYSTEMD_UNIT_DIR, "--unit-dir", help="Systemd unit directory."),
    env_dir: Path = typer.Option(DEFAULT_ENV_DIR, "--env-dir", help="Directory for per-profile env files."),
) -> None:
    try:
        result = schedule_status(profile_name=profile, config_path=config, unit_dir=unit_dir, env_dir=env_dir)
        typer.echo(f"Profile: {result['profile']}")
        typer.echo(f"Timer: {result['timer_name']} ({result['timer_enabled']}, {result['timer_active']})")
        typer.echo(f"Service: {result['service_name']} ({result['service_active']})")
        typer.echo(f"OnCalendar: {result['on_calendar']}")
        typer.echo(f"Persistent: {result['persistent']}")
        if result["env_file_path"]:
            typer.echo(f"Env file: {result['env_file_path']} (exists={result['env_file_exists']})")
    except DBRestoreError as exc:
        _handle_error(exc)


@schedule_app.command("remove")
def schedule_remove_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    unit_dir: Path = typer.Option(DEFAULT_SYSTEMD_UNIT_DIR, "--unit-dir", help="Systemd unit directory."),
    env_dir: Path = typer.Option(DEFAULT_ENV_DIR, "--env-dir", help="Directory for per-profile env files."),
    delete_env_file: bool = typer.Option(False, "--delete-env-file", help="Delete the per-profile env file as part of removal."),
) -> None:
    try:
        result = remove_schedule(profile_name=profile, unit_dir=unit_dir, env_dir=env_dir, delete_env_file=delete_env_file)
        typer.echo(f"Removed schedule for profile '{profile}'")
        if result["removed_files"]:
            typer.echo(f"Removed {len(result['removed_files'])} file(s)")
    except DBRestoreError as exc:
        _handle_error(exc)


app.add_typer(schedule_app, name="schedule")


def main() -> None:
    app()
