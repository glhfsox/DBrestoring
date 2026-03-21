"""This module exposes the command-line interface and maps commands to operations.
It stays intentionally thin: parse user input, call the right service function, print a clean result.
All actual backup, restore, verification, and scheduling rules live deeper in the app.
That keeps CLI behavior and GUI behavior consistent because both hit the same core logic."""

from __future__ import annotations

from pathlib import Path

import typer

from dbrestore.config import DEFAULT_CONFIG_PATH
from dbrestore.errors import DBRestoreError
from dbrestore.operations import (
    collect_profile_status,
    run_backup,
    run_profile_preflight,
    run_restore,
    run_scheduled_cycle,
    run_test_connection,
    run_validate_config,
    run_verify_latest_backup,
)
from dbrestore.scheduler import (
    DEFAULT_ENV_DIR,
    DEFAULT_SYSTEMD_UNIT_DIR,
    install_schedule,
    load_schedule_env_file,
    remove_schedule,
    save_schedule_env_file,
    schedule_status,
)

app = typer.Typer(help="Back up and restore supported databases.", no_args_is_help=True)
schedule_app = typer.Typer(help="Manage systemd-based backup schedules.")


def _handle_error(exc: DBRestoreError) -> None:
    typer.echo(str(exc), err=True)
    raise typer.Exit(code=1) from exc


@app.command("backup")
def backup_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
    output_dir: Path | None = typer.Option(
        None, "--output-dir", help="Override the configured output directory."
    ),
    no_compress: bool = typer.Option(
        False, "--no-compress", help="Disable gzip compression for this backup."
    ),
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


@app.command("run-scheduled")
def run_scheduled_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
) -> None:
    try:
        result = run_scheduled_cycle(profile_name=profile, config_path=config, console=typer.echo)
        typer.echo(
            f"Scheduled cycle completed for profile '{profile}' (verification={result['verification_status']})"
        )
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("restore")
def restore_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    input_path: Path = typer.Option(..., "--input", help="Backup artifact path or run directory."),
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
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
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
) -> None:
    try:
        result = run_test_connection(profile_name=profile, config_path=config)
        typer.echo(f"Connection succeeded for profile '{result['profile']}'")
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("validate-config")
def validate_config_command(
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
) -> None:
    try:
        result = run_validate_config(config_path=config)
        typer.echo(f"Configuration is valid for {len(result['profiles'])} profile(s)")
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("preflight")
def preflight_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
    unit_dir: Path = typer.Option(
        DEFAULT_SYSTEMD_UNIT_DIR, "--unit-dir", help="Systemd unit directory."
    ),
    env_dir: Path = typer.Option(
        DEFAULT_ENV_DIR, "--env-dir", help="Directory for per-profile env files."
    ),
    include_connection: bool = typer.Option(
        True, "--include-connection/--no-connection", help="Run a live DB connection test."
    ),
) -> None:
    try:
        result = run_profile_preflight(
            profile_name=profile,
            config_path=config,
            unit_dir=unit_dir,
            env_dir=env_dir,
            include_connection=include_connection,
        )
        typer.echo(f"Profile: {result['profile']}")
        typer.echo(f"Preflight: {result['status']}")
        for check in result["checks"]:
            typer.echo(f"- {check['name']}: {check['status']} - {check['message']}")
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("status")
def status_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
    unit_dir: Path = typer.Option(
        DEFAULT_SYSTEMD_UNIT_DIR, "--unit-dir", help="Systemd unit directory."
    ),
    env_dir: Path = typer.Option(
        DEFAULT_ENV_DIR, "--env-dir", help="Directory for per-profile env files."
    ),
) -> None:
    try:
        result = collect_profile_status(
            profile_name=profile,
            config_path=config,
            unit_dir=unit_dir,
            env_dir=env_dir,
        )
        typer.echo(f"Profile: {result['profile']}")
        typer.echo(f"DB Type: {result['db_type']}")
        typer.echo(f"Storage: {result['storage']['target']}")
        typer.echo(f"Storage Health: {result['storage']['health'].get('status', 'unknown')}")
        if result["last_backup"] is not None:
            typer.echo(
                f"Last Backup: {result['last_backup'].get('finished_at')} ({result['last_backup'].get('run_id')})"
            )
        else:
            typer.echo("Last Backup: none")
        if result["last_verification"] is not None:
            payload = result["last_verification"]["payload"]
            typer.echo(
                f"Last Verification: {result['last_verification']['status']} at {result['last_verification']['timestamp']} into {payload.get('target_profile')}"
            )
        else:
            typer.echo("Last Verification: none")
        schedule = result["schedule"]
        typer.echo(f"Schedule: {schedule.get('message', 'not configured')}")
        if schedule.get("next_run"):
            typer.echo(f"Next Run: {schedule['next_run']}")
        typer.echo(
            "Retention: "
            f"configured={result['retention']['configured']}, "
            f"total_runs={result['retention']['total_runs']}, "
            f"pending_delete={result['retention']['pending_delete_count']}"
        )
        verification = result["verification"]
        if verification["configured"]:
            typer.echo(
                f"Verification Target: {verification['target_profile']} "
                f"(scheduled={verification['scheduled_after_backup']})"
            )
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("gui")
def gui_command(
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
) -> None:
    try:
        from dbrestore.gui import launch_gui

        launch_gui(config_path=config)
    except DBRestoreError as exc:
        _handle_error(exc)


@app.command("verify-latest")
def verify_latest_command(
    profile: str = typer.Option(
        ..., "--profile", "-p", help="Source backup profile from the YAML config."
    ),
    target_profile: str | None = typer.Option(
        None,
        "--target-profile",
        "-t",
        help="Separate restore target profile used for verification. Defaults to verification.target_profile from config when omitted.",
    ),
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
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
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
    unit_dir: Path = typer.Option(
        DEFAULT_SYSTEMD_UNIT_DIR, "--unit-dir", help="Systemd unit directory."
    ),
    env_dir: Path = typer.Option(
        DEFAULT_ENV_DIR, "--env-dir", help="Directory for per-profile env files."
    ),
    force: bool = typer.Option(False, "--force", help="Overwrite existing unit files."),
    enable_timer: bool = typer.Option(
        True, "--enable/--no-enable", help="Enable and start the timer after install."
    ),
    run_as_user: str | None = typer.Option(
        None, "--run-as-user", help="Linux user account the backup service should run as."
    ),
    run_as_group: str | None = typer.Option(
        None, "--run-as-group", help="Linux group the backup service should run as."
    ),
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
                typer.echo(
                    "Created env template file. Fill in the missing values before relying on the timer."
                )
    except DBRestoreError as exc:
        _handle_error(exc)


@schedule_app.command("status")
def schedule_status_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
    unit_dir: Path = typer.Option(
        DEFAULT_SYSTEMD_UNIT_DIR, "--unit-dir", help="Systemd unit directory."
    ),
    env_dir: Path = typer.Option(
        DEFAULT_ENV_DIR, "--env-dir", help="Directory for per-profile env files."
    ),
) -> None:
    try:
        result = schedule_status(
            profile_name=profile, config_path=config, unit_dir=unit_dir, env_dir=env_dir
        )
        typer.echo(f"Profile: {result['profile']}")
        typer.echo(
            f"Timer: {result['timer_name']} ({result['timer_enabled']}, {result['timer_active']})"
        )
        typer.echo(f"Service: {result['service_name']} ({result['service_active']})")
        typer.echo(f"OnCalendar: {result['on_calendar']}")
        typer.echo(f"Persistent: {result['persistent']}")
        if result["verification_target_profile"]:
            typer.echo(f"Verification target: {result['verification_target_profile']}")
        if result["next_run"]:
            typer.echo(f"Next run: {result['next_run']}")
        if result["last_trigger"]:
            typer.echo(f"Last trigger: {result['last_trigger']}")
        if result["env_file_path"]:
            typer.echo(f"Env file: {result['env_file_path']} (exists={result['env_file_exists']})")
            typer.echo(
                f"Env values set: {result['env_values_set_count']}/{len(result['env_vars'])}"
            )
            if result["env_vars_missing"]:
                typer.echo(f"Missing env values: {', '.join(result['env_vars_missing'])}")
    except DBRestoreError as exc:
        _handle_error(exc)


@schedule_app.command("show-env")
def schedule_show_env_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
    env_dir: Path = typer.Option(
        DEFAULT_ENV_DIR, "--env-dir", help="Directory for per-profile env files."
    ),
) -> None:
    try:
        result = load_schedule_env_file(profile_name=profile, config_path=config, env_dir=env_dir)
        typer.echo(f"Env file: {result['env_file_path']} (exists={result['exists']})")
        typer.echo(result["text"])
    except DBRestoreError as exc:
        _handle_error(exc)


@schedule_app.command("save-env")
def schedule_save_env_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config", "-c", help="Path to YAML configuration."
    ),
    env_dir: Path = typer.Option(
        DEFAULT_ENV_DIR, "--env-dir", help="Directory for per-profile env files."
    ),
    env_file: Path = typer.Option(
        ...,
        "--env-file",
        help="Path to a text file whose contents should become the schedule env file.",
    ),
) -> None:
    try:
        result = save_schedule_env_file(
            profile_name=profile,
            contents=env_file.read_text(encoding="utf-8"),
            config_path=config,
            env_dir=env_dir,
        )
        typer.echo(f"Saved env file: {result['env_file_path']}")
        if result["missing_vars"]:
            typer.echo(f"Unset values: {', '.join(result['missing_vars'])}")
    except DBRestoreError as exc:
        _handle_error(exc)


@schedule_app.command("remove")
def schedule_remove_command(
    profile: str = typer.Option(..., "--profile", "-p", help="Profile name from the YAML config."),
    unit_dir: Path = typer.Option(
        DEFAULT_SYSTEMD_UNIT_DIR, "--unit-dir", help="Systemd unit directory."
    ),
    env_dir: Path = typer.Option(
        DEFAULT_ENV_DIR, "--env-dir", help="Directory for per-profile env files."
    ),
    delete_env_file: bool = typer.Option(
        False, "--delete-env-file", help="Delete the per-profile env file as part of removal."
    ),
) -> None:
    try:
        result = remove_schedule(
            profile_name=profile,
            unit_dir=unit_dir,
            env_dir=env_dir,
            delete_env_file=delete_env_file,
        )
        typer.echo(f"Removed schedule for profile '{profile}'")
        if result["removed_files"]:
            typer.echo(f"Removed {len(result['removed_files'])} file(s)")
    except DBRestoreError as exc:
        _handle_error(exc)


app.add_typer(schedule_app, name="schedule")


def main() -> None:
    app()
