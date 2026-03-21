"""Verification flows that restore the latest backup into a disposable target."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from dbrestore.config import DEFAULT_CONFIG_PATH, AppConfig, load_config
from dbrestore.errors import ConfigError
from dbrestore.logging import RunLogger
from dbrestore.notifications import notify_event
from dbrestore.storage import get_storage_backend
from dbrestore.utils import current_time, format_timestamp

from .common import (
    ProgressCallback,
    build_redactor,
    duration_ms,
    emit_progress,
    scaled_progress,
    wrap_error,
)
from .history import get_latest_backup_run


def configured_verification_target(config: AppConfig, source_profile_name: str) -> str | None:
    profile = config.get_profile(source_profile_name)
    if profile.verification is None:
        return None
    return profile.verification.target_profile


def resolve_verification_target(
    config: AppConfig,
    source_profile_name: str,
    target_profile_name: str | None,
) -> str:
    resolved_target = target_profile_name or configured_verification_target(
        config, source_profile_name
    )
    if not resolved_target:
        raise ConfigError(
            f"Profile '{source_profile_name}' does not define verification.target_profile and no --target-profile was provided"
        )

    source_profile = config.get_profile(source_profile_name)
    target_profile = config.get_profile(resolved_target)
    if source_profile_name == resolved_target:
        raise ConfigError(
            "Verification target profile must be different from the backup source profile"
        )
    if source_profile.db_type != target_profile.db_type:
        raise ConfigError(
            f"Verification requires matching db_type values. Source is '{source_profile.db_type}', "
            f"target is '{target_profile.db_type}'"
        )
    return resolved_target


def run_verify_latest_backup(
    source_profile_name: str,
    target_profile_name: str | None = None,
    config_path: Path = DEFAULT_CONFIG_PATH,
    console: Callable[[str], None] | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    from .backup_restore import run_restore, run_test_connection_with_config

    emit_progress(progress, message=f"Loading verification for '{source_profile_name}'", percent=5)
    config = load_config(config_path)
    resolved_target = resolve_verification_target(config, source_profile_name, target_profile_name)
    source_profile = config.get_profile(source_profile_name)
    target_profile = config.get_profile(resolved_target)

    logger = RunLogger(config.log_file_path(), console=console)
    redactor = build_redactor(source_profile, target_profile)
    redactor.add(config.storage.secret_access_key_value, config.storage.session_token_value)
    notification_settings = config.notifications_for(source_profile)
    if notification_settings is not None and notification_settings.slack is not None:
        redactor.add(notification_settings.slack.webhook_url_value)
    started_at = current_time()
    latest_backup: dict[str, Any] | None = None
    storage = get_storage_backend(config)

    try:
        emit_progress(progress, message="Resolving latest backup", percent=15)
        latest_backup = get_latest_backup_run(config, source_profile_name, storage_backend=storage)
        artifact_path = Path(latest_backup["run_dir"])
        logger.print(
            f"Verifying latest backup from '{source_profile_name}' into '{resolved_target}'"
        )
        logger.log_event(
            "verification.started",
            {
                "profile": source_profile_name,
                "profiles": [source_profile_name, resolved_target],
                "target_profile": resolved_target,
                "run_id": latest_backup.get("run_id"),
                "artifact_path": latest_backup.get("artifact_path"),
            },
        )
        restore_result = run_restore(
            profile_name=resolved_target,
            input_path=artifact_path,
            config_path=config_path,
            console=console,
            notify=False,
            progress=scaled_progress(progress, start=25, end=80, prefix="Restore"),
        )
        emit_progress(
            progress,
            message="Testing verification target",
            percent=88,
            target_percent=97,
            mode="auto",
        )
        connection_result = run_test_connection_with_config(config, resolved_target)
        finished_at = current_time()
        result = {
            "profile": source_profile_name,
            "profiles": [source_profile_name, resolved_target],
            "target_profile": resolved_target,
            "db_type": source_profile.db_type,
            "run_id": latest_backup.get("run_id"),
            "artifact_path": latest_backup.get("artifact_path"),
            "started_at": format_timestamp(started_at),
            "finished_at": format_timestamp(finished_at),
            "duration_ms": duration_ms(started_at, finished_at),
            "restore_status": restore_result.get("status"),
            "connection_status": connection_result.get("status"),
            "status": "verified",
        }
        logger.log_event("verification.completed", result)
        notify_event(notification_settings, "verification.completed", result, logger, redactor)
        logger.print(
            f"Verification completed for backup '{latest_backup.get('run_id')}' into '{resolved_target}'"
        )
        emit_progress(progress, message="Verification completed", percent=100)
        return result
    except Exception as exc:
        message = redactor.sanitize_text(exc)
        payload = {
            "profile": source_profile_name,
            "profiles": [source_profile_name, resolved_target],
            "target_profile": resolved_target,
            "run_id": latest_backup.get("run_id") if latest_backup else None,
            "artifact_path": latest_backup.get("artifact_path") if latest_backup else None,
            "error": message,
        }
        logger.log_event("verification.failed", payload)
        notify_event(notification_settings, "verification.failed", payload, logger, redactor)
        raise wrap_error(message, exc) from exc
