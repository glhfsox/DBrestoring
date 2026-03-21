"""Primary backup, restore, validation, and scheduled-cycle orchestration."""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Callable
from contextlib import ExitStack
from pathlib import Path
from typing import Any

from dbrestore.adapters import get_adapter
from dbrestore.config import DEFAULT_CONFIG_PATH, AppConfig, load_config
from dbrestore.errors import ArtifactError, ConfigError
from dbrestore.logging import RunLogger
from dbrestore.models import BackupManifest
from dbrestore.notifications import notify_event
from dbrestore.storage import StorageBackend, get_storage_backend
from dbrestore.utils import (
    current_time,
    format_timestamp,
    gunzip_decompress,
    gzip_compress,
)

from .common import (
    ProgressCallback,
    build_redactor,
    collect_profile_validation_issues,
    duration_ms,
    emit_progress,
    resolve_restore_selection,
    scaled_progress,
    validate_backup_preflight,
    validate_restore_preflight,
    wrap_error,
)
from .retention import apply_retention_policy
from .verification import configured_verification_target, resolve_verification_target


def run_backup(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    output_dir_override: Path | None = None,
    no_compress: bool = False,
    storage_backend: StorageBackend | None = None,
    console: Callable[[str], None] | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    emit_progress(progress, message=f"Loading profile '{profile_name}'", percent=5)
    config = load_config(config_path)
    profile = config.get_profile(profile_name)
    adapter = get_adapter(profile.db_type)
    redactor = build_redactor(profile)
    redactor.add(config.storage.secret_access_key_value, config.storage.session_token_value)
    notification_settings = config.notifications_for(profile)
    if notification_settings is not None and notification_settings.slack is not None:
        redactor.add(notification_settings.slack.webhook_url_value)
    log_file = config.log_file_path()
    logger = RunLogger(log_file, console=console)
    started_at = current_time()
    output_dir = config.output_dir_for(profile, override=output_dir_override)
    compression_enabled = config.compression_enabled_for(profile, cli_disable=no_compress)
    storage = storage_backend or get_storage_backend(config)

    emit_progress(progress, message="Running backup preflight checks", percent=12)
    validate_backup_preflight(output_dir, adapter.required_tools())
    emit_progress(progress, message="Preparing backup workspace", percent=20)
    prepared = storage.prepare_backup_paths(
        profile_name, output_dir, started_at, adapter.artifact_extension()
    )

    logger.print(f"Starting backup for profile '{profile_name}'")
    logger.log_event(
        "backup.started",
        {
            "run_id": prepared.run_id,
            "profile": profile_name,
            "db_type": profile.db_type,
            "output_dir": str(output_dir),
            "compression": "gzip" if compression_enabled else "none",
        },
    )

    try:
        emit_progress(
            progress,
            message="Creating backup artifact",
            percent=35,
            target_percent=68,
            mode="auto",
        )
        metadata = adapter.backup(profile, prepared.artifact_path, redactor)
        artifact_path = prepared.artifact_path
        emit_progress(progress, message="Processing backup artifact", percent=70)
        if compression_enabled:
            emit_progress(
                progress,
                message="Compressing backup artifact",
                percent=78,
                target_percent=88,
                mode="auto",
            )
            artifact_path = gzip_compress(prepared.artifact_path)
            prepared.artifact_path.unlink()

        finished_at = current_time()
        manifest = BackupManifest(
            run_id=prepared.run_id,
            profile=profile_name,
            db_type=profile.db_type,
            backup_type="full",
            started_at=format_timestamp(started_at),
            finished_at=format_timestamp(finished_at),
            duration_ms=duration_ms(started_at, finished_at),
            artifact_path=str(artifact_path),
            compression="gzip" if compression_enabled else "none",
            source=profile.public_source_metadata(),
            metadata=metadata,
        )
        stored_run = storage.finalize_backup(
            profile_name=profile_name,
            prepared=prepared,
            manifest=manifest,
            artifact_path=artifact_path,
        )
        emit_progress(progress, message="Applying retention policy", percent=90)
        retention = apply_retention_policy(
            config,
            profile_name,
            profile,
            output_dir,
            logger,
            storage_backend=storage,
        )
        result = stored_run.manifest | {
            "manifest_path": stored_run.manifest_path,
            "retention_deleted_count": retention["deleted_count"],
        }
        logger.log_event("backup.completed", result)
        notify_event(notification_settings, "backup.completed", result, logger, redactor)
        logger.print(f"Backup completed: {stored_run.artifact_path}")
        emit_progress(progress, message="Backup completed", percent=100)
        return result
    except Exception as exc:
        message = redactor.sanitize_text(exc)
        payload = {
            "run_id": prepared.run_id,
            "profile": profile_name,
            "db_type": profile.db_type,
            "error": message,
        }
        logger.log_event("backup.failed", payload)
        notify_event(notification_settings, "backup.failed", payload, logger, redactor)
        raise wrap_error(message, exc) from exc


def run_restore(
    profile_name: str,
    input_path: Path,
    config_path: Path = DEFAULT_CONFIG_PATH,
    console: Callable[[str], None] | None = None,
    tables: list[str] | None = None,
    collections: list[str] | None = None,
    notify: bool = True,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    emit_progress(progress, message=f"Loading restore profile '{profile_name}'", percent=5)
    config = load_config(config_path)
    profile = config.get_profile(profile_name)
    adapter = get_adapter(profile.db_type)
    storage = get_storage_backend(config)
    redactor = build_redactor(profile)
    redactor.add(config.storage.secret_access_key_value, config.storage.session_token_value)
    notification_settings = config.notifications_for(profile) if notify else None
    if notification_settings is not None and notification_settings.slack is not None:
        redactor.add(notification_settings.slack.webhook_url_value)
    logger = RunLogger(config.log_file_path(), console=console)
    started_at = current_time()
    selection = resolve_restore_selection(
        adapter=adapter,
        profile=profile,
        tables=tables,
        collections=collections,
    )
    emit_progress(progress, message="Resolving backup input", percent=15)
    resolved = storage.resolve_restore_input(input_path)
    resolved_artifact = resolved.artifact_path
    manifest = resolved.manifest

    if manifest is not None and manifest.get("db_type") not in {None, profile.db_type}:
        raise ArtifactError(
            f"Artifact DB type '{manifest['db_type']}' does not match profile DB type '{profile.db_type}'"
        )

    logger.print(f"Starting restore for profile '{profile_name}'")
    logger.log_event(
        "restore.started",
        {
            "profile": profile_name,
            "db_type": profile.db_type,
            "artifact_path": str(resolved_artifact),
            "restore_selection": selection or [],
            "restore_selection_kind": adapter.restore_filter_kind(),
        },
    )

    try:
        emit_progress(progress, message="Running restore preflight checks", percent=25)
        validate_restore_preflight(adapter.required_tools())
        adapter.validate_restore_target(profile)
        with ExitStack() as stack:
            if resolved.cleanup_dir is not None:
                stack.callback(shutil.rmtree, resolved.cleanup_dir, ignore_errors=True)
            source_path = resolved_artifact
            if resolved_artifact.suffix == ".gz":
                emit_progress(
                    progress,
                    message="Decompressing backup artifact",
                    percent=35,
                    target_percent=45,
                    mode="auto",
                )
                temp_dir = Path(stack.enter_context(tempfile.TemporaryDirectory()))
                restored_name = resolved_artifact.stem
                source_path = gunzip_decompress(resolved_artifact, temp_dir / restored_name)
            emit_progress(
                progress,
                message="Restoring database",
                percent=50,
                target_percent=92,
                mode="auto",
            )
            adapter.restore(profile, source_path, redactor, selection=selection)

        finished_at = current_time()
        result = {
            "profile": profile_name,
            "db_type": profile.db_type,
            "artifact_path": str(resolved_artifact),
            "restore_selection": selection or [],
            "restore_selection_kind": adapter.restore_filter_kind(),
            "started_at": format_timestamp(started_at),
            "finished_at": format_timestamp(finished_at),
            "duration_ms": duration_ms(started_at, finished_at),
            "status": "success",
        }
        logger.log_event("restore.completed", result)
        logger.print(f"Restore completed from: {resolved_artifact}")
        emit_progress(progress, message="Restore completed", percent=100)
        return result
    except Exception as exc:
        message = redactor.sanitize_text(exc)
        payload = {
            "profile": profile_name,
            "db_type": profile.db_type,
            "artifact_path": str(resolved_artifact),
            "restore_selection": selection or [],
            "restore_selection_kind": adapter.restore_filter_kind(),
            "error": message,
        }
        logger.log_event("restore.failed", payload)
        notify_event(notification_settings, "restore.failed", payload, logger, redactor)
        raise wrap_error(message, exc) from exc


def run_test_connection(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> dict[str, Any]:
    config = load_config(config_path)
    return run_test_connection_with_config(config, profile_name)


def run_test_connection_with_config(config: AppConfig, profile_name: str) -> dict[str, Any]:
    profile = config.get_profile(profile_name)
    adapter = get_adapter(profile.db_type)
    redactor = build_redactor(profile)

    try:
        adapter.test_connection(profile)
    except Exception as exc:
        raise wrap_error(redactor.sanitize_text(exc), exc) from exc

    return {
        "profile": profile_name,
        "db_type": profile.db_type,
        "status": "ok",
    }


def run_validate_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    config = load_config(config_path)
    issues: list[str] = []

    for profile_name, profile in config.profiles.items():
        issues.extend(collect_profile_validation_issues(config, profile_name, profile))

    if issues:
        joined = "\n".join(issues)
        raise ConfigError(f"Configuration validation failed:\n{joined}")

    return {
        "status": "ok",
        "profiles": sorted(config.profiles.keys()),
        "config_path": str(config.source_path or config_path),
    }


def validate_profile_config(config: AppConfig, profile_name: str) -> dict[str, Any]:
    profile = config.get_profile(profile_name)
    issues = collect_profile_validation_issues(config, profile_name, profile)
    if issues:
        joined = "\n".join(issues)
        raise ConfigError(f"Profile validation failed:\n{joined}")

    return {
        "status": "ok",
        "profile": profile_name,
        "db_type": profile.db_type,
        "config_path": str(config.source_path or DEFAULT_CONFIG_PATH),
    }


def run_scheduled_cycle(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    console: Callable[[str], None] | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    from .verification import run_verify_latest_backup

    emit_progress(progress, message=f"Loading scheduled cycle for '{profile_name}'", percent=5)
    config = load_config(config_path)
    profile = config.get_profile(profile_name)
    configured_target = configured_verification_target(config, profile_name)
    redactor = build_redactor(profile)
    logger = RunLogger(config.log_file_path(), console=console)
    started_at = current_time()
    verification_target = configured_target
    logger.log_event(
        "scheduled_cycle.started",
        {
            "profile": profile_name,
            "target_profile": configured_target,
            "verification_enabled": bool(
                configured_target
                and profile.verification
                and profile.verification.schedule_after_backup
            ),
        },
    )

    try:
        if configured_target:
            redactor.add(config.get_profile(configured_target).password_value)
        if (
            configured_target
            and profile.verification
            and profile.verification.schedule_after_backup
        ):
            verification_target = resolve_verification_target(config, profile_name, None)
        emit_progress(progress, message="Starting scheduled backup", percent=10)
        backup_result = run_backup(
            profile_name=profile_name,
            config_path=config_path,
            console=console,
            progress=scaled_progress(progress, start=10, end=65, prefix="Backup"),
        )
        verification_result = None
        if (
            verification_target
            and profile.verification
            and profile.verification.schedule_after_backup
        ):
            emit_progress(progress, message="Starting scheduled verification", percent=70)
            verification_result = run_verify_latest_backup(
                source_profile_name=profile_name,
                target_profile_name=verification_target,
                config_path=config_path,
                console=console,
                progress=scaled_progress(progress, start=70, end=95, prefix="Verify"),
            )
        else:
            emit_progress(progress, message="Skipping verification step", percent=90)
        finished_at = current_time()
        result = {
            "profile": profile_name,
            "run_id": backup_result.get("run_id"),
            "artifact_path": backup_result.get("artifact_path"),
            "target_profile": verification_target,
            "verification_enabled": bool(
                verification_target
                and profile.verification
                and profile.verification.schedule_after_backup
            ),
            "verification_status": verification_result.get("status")
            if verification_result is not None
            else "skipped",
            "started_at": format_timestamp(started_at),
            "finished_at": format_timestamp(finished_at),
            "duration_ms": duration_ms(started_at, finished_at),
            "status": "success",
        }
        logger.log_event("scheduled_cycle.completed", result)
        emit_progress(progress, message="Scheduled cycle completed", percent=100)
        return result
    except Exception as exc:
        message = redactor.sanitize_text(exc)
        logger.log_event(
            "scheduled_cycle.failed",
            {
                "profile": profile_name,
                "target_profile": verification_target,
                "error": message,
            },
        )
        raise wrap_error(message, exc) from exc
