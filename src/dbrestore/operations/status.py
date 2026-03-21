"""Readiness, health, and preflight views over the application's operational state."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dbrestore.config import DEFAULT_CONFIG_PATH, collect_profile_env_vars, load_config
from dbrestore.errors import DBRestoreError
from dbrestore.scheduler import DEFAULT_ENV_DIR, DEFAULT_SYSTEMD_UNIT_DIR, schedule_status
from dbrestore.storage import get_storage_backend

from .backup_restore import run_test_connection_with_config, validate_profile_config
from .common import build_redactor
from .history import list_backup_history, summarize_latest_event
from .retention import summarize_retention_policy
from .verification import configured_verification_target, resolve_verification_target


def collect_profile_status(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    *,
    unit_dir: Path = DEFAULT_SYSTEMD_UNIT_DIR,
    env_dir: Path = DEFAULT_ENV_DIR,
) -> dict[str, Any]:
    config = load_config(config_path, require_env=False)
    profile = config.get_profile(profile_name)
    output_dir = config.output_dir_for(profile)
    storage = get_storage_backend(config)
    backups = list_backup_history(config_path=config_path, profile_name=profile_name, limit=1)
    last_backup = backups[0] if backups else None
    last_backup_event = summarize_latest_event(
        config_path=config_path,
        profile_name=profile_name,
        completed_event="backup.completed",
        failed_event="backup.failed",
    )
    last_verification = summarize_latest_event(
        config_path=config_path,
        profile_name=profile_name,
        completed_event="verification.completed",
        failed_event="verification.failed",
    )
    retention = summarize_retention_policy(
        config,
        profile_name,
        profile,
        output_dir,
        storage_backend=storage,
    )
    storage_health = _safe_storage_health(config, profile_name, output_dir)
    schedule = _safe_schedule_status(
        profile_name=profile_name,
        config_path=config_path,
        profile_has_schedule=profile.schedule is not None,
        unit_dir=unit_dir,
        env_dir=env_dir,
    )
    verification_target = configured_verification_target(config, profile_name)
    return {
        "profile": profile_name,
        "db_type": profile.db_type,
        "storage": {
            "type": config.storage.type,
            "target": _storage_target(config, profile_name, output_dir),
            "health": storage_health,
        },
        "last_backup": last_backup,
        "last_backup_event": last_backup_event,
        "last_verification": last_verification,
        "verification": {
            "configured": verification_target is not None,
            "target_profile": verification_target,
            "scheduled_after_backup": bool(
                profile.verification and profile.verification.schedule_after_backup
            ),
        },
        "schedule": schedule,
        "retention": retention,
    }


def run_profile_preflight(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    *,
    unit_dir: Path = DEFAULT_SYSTEMD_UNIT_DIR,
    env_dir: Path = DEFAULT_ENV_DIR,
    include_connection: bool = True,
) -> dict[str, Any]:
    config = load_config(config_path, require_env=False)
    profile = config.get_profile(profile_name)
    checks: list[dict[str, Any]] = []
    status = "ok"

    def add_check(name: str, check_status: str, message: str, **extra: Any) -> None:
        nonlocal status
        if check_status == "error":
            status = "error"
        elif check_status == "warning" and status == "ok":
            status = "warning"
        checks.append({"name": name, "status": check_status, "message": message, **extra})

    env_vars = collect_profile_env_vars(config_path, profile_name)
    missing_env = [name for name in env_vars if os.environ.get(name) in {None, ""}]
    if missing_env:
        add_check(
            "environment",
            "error",
            f"Missing required environment variables: {', '.join(missing_env)}",
            missing=missing_env,
        )
    elif env_vars:
        add_check(
            "environment",
            "ok",
            f"Environment variables present: {', '.join(env_vars)}",
            variables=env_vars,
        )
    else:
        add_check("environment", "ok", "No environment variables required")

    try:
        result = validate_profile_config(config, profile_name)
        add_check("config", "ok", f"Profile '{result['profile']}' is valid")
    except DBRestoreError as exc:
        add_check("config", "error", str(exc))

    if include_connection and not missing_env:
        try:
            result = run_test_connection_with_config(config, profile_name)
            add_check(
                "connection",
                "ok",
                f"Connection succeeded for profile '{result['profile']}'",
            )
        except DBRestoreError as exc:
            add_check("connection", "error", str(exc))
    elif include_connection:
        add_check(
            "connection",
            "warning",
            "Skipped connection test because required environment variables are missing",
        )

    try:
        health = get_storage_backend(config).health_check(
            profile_name, config.output_dir_for(profile)
        )
        add_check(
            "storage", "ok", health.get("message", "Storage health check succeeded"), health=health
        )
    except DBRestoreError as exc:
        add_check("storage", "error", str(exc))

    if profile.verification is not None:
        try:
            target = resolve_verification_target(config, profile_name, None)
            add_check(
                "verification",
                "ok",
                f"Verification target is '{target}'",
                target_profile=target,
            )
        except DBRestoreError as exc:
            add_check("verification", "error", str(exc))
    else:
        add_check(
            "verification",
            "warning",
            "No verification.target_profile configured; scheduled confidence loop is disabled",
        )

    if profile.schedule is not None:
        schedule = _safe_schedule_status(
            profile_name=profile_name,
            config_path=config_path,
            profile_has_schedule=True,
            unit_dir=unit_dir,
            env_dir=env_dir,
        )
        schedule_state = schedule.get("status", "warning")
        message = schedule.get("message") or f"Schedule preset: {profile.schedule.on_calendar}"
        add_check("schedule", schedule_state, message, schedule=schedule)
    else:
        add_check("schedule", "warning", "No schedule configured for this profile")

    return {
        "profile": profile_name,
        "db_type": profile.db_type,
        "status": status,
        "checks": checks,
    }


def _safe_storage_health(config: Any, profile_name: str, output_dir: Path) -> dict[str, Any]:
    try:
        return get_storage_backend(config).health_check(profile_name, output_dir)
    except DBRestoreError as exc:
        profile = config.get_profile(profile_name)
        redactor = build_redactor(profile)
        return {
            "status": "error",
            "message": redactor.sanitize_text(exc),
            "target": _storage_target(config, profile_name, output_dir),
        }


def _safe_schedule_status(
    *,
    profile_name: str,
    config_path: Path,
    profile_has_schedule: bool,
    unit_dir: Path,
    env_dir: Path,
) -> dict[str, Any]:
    if not profile_has_schedule:
        return {
            "status": "warning",
            "message": "No schedule configured",
            "configured": False,
        }

    try:
        result = schedule_status(
            profile_name=profile_name,
            config_path=config_path,
            unit_dir=unit_dir,
            env_dir=env_dir,
        )
    except DBRestoreError as exc:
        return {
            "status": "error",
            "message": str(exc),
            "configured": True,
        }

    if not result.get("service_exists") or not result.get("timer_exists"):
        result["status"] = "warning"
        result["message"] = "Schedule is configured in YAML but units are not installed"
        return result

    result["status"] = "ok"
    result["message"] = (
        f"Timer {result['timer_name']} is {result['timer_enabled']}/{result['timer_active']}"
    )
    return result


def _storage_target(config: Any, profile_name: str, output_dir: Path) -> str:
    if config.storage.type == "s3":
        prefix = "/".join(
            part.strip("/") for part in (config.storage.prefix, profile_name) if part.strip("/")
        )
        return f"s3://{config.storage.bucket}/{prefix}"
    return str(output_dir / profile_name)
