"""This is the orchestration layer of the application.
It ties together config loading, adapter selection, storage, logging, retention, notifications, and verification into one coherent flow.
CLI and GUI mainly delegate here, so this file defines how a backup or restore run behaves end to end.
If you want the quickest mental model of the system, understand this module before the lower-level pieces."""

from __future__ import annotations

import json
import shutil
import tempfile
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from dbrestore.adapters import get_adapter
from dbrestore.adapters.base import DatabaseAdapter
from dbrestore.config import AppConfig, DEFAULT_CONFIG_PATH, ProfileModel, load_config
from dbrestore.errors import ArtifactError, ConfigError, DBRestoreError, PreflightError
from dbrestore.logging import RunLogger
from dbrestore.models import BackupManifest
from dbrestore.notifications import notify_event
from dbrestore.storage import StorageBackend, get_storage_backend
from dbrestore.utils import (
    Redactor,
    current_time,
    ensure_directory,
    format_timestamp,
    gunzip_decompress,
    gzip_compress,
    parse_timestamp,
    validate_writable_path,
)


def build_redactor(*profiles: ProfileModel) -> Redactor:
    redactor = Redactor()
    for profile in profiles:
        redactor.add(profile.password_value)
    return redactor


def run_backup(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    output_dir_override: Path | None = None,
    no_compress: bool = False,
    storage_backend: StorageBackend | None = None,
    console: Callable[[str], None] | None = None,
) -> dict[str, Any]:
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

    _validate_backup_preflight(output_dir, adapter.required_tools())
    prepared = storage.prepare_backup_paths(profile_name, output_dir, started_at, adapter.artifact_extension())

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
        metadata = adapter.backup(profile, prepared.artifact_path, redactor)
        artifact_path = prepared.artifact_path
        if compression_enabled:
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
            duration_ms=_duration_ms(started_at, finished_at),
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
        raise _wrap_error(message, exc) from exc


def run_restore(
    profile_name: str,
    input_path: Path,
    config_path: Path = DEFAULT_CONFIG_PATH,
    console: Callable[[str], None] | None = None,
    tables: list[str] | None = None,
    collections: list[str] | None = None,
    notify: bool = True,
) -> dict[str, Any]:
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
    selection = _resolve_restore_selection(
        adapter=adapter,
        profile=profile,
        tables=tables,
        collections=collections,
    )
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
        _validate_restore_preflight(adapter.required_tools())
        adapter.validate_restore_target(profile)
        with ExitStack() as stack:
            if resolved.cleanup_dir is not None:
                stack.callback(shutil.rmtree, resolved.cleanup_dir, ignore_errors=True)
            source_path = resolved_artifact
            if resolved_artifact.suffix == ".gz":
                temp_dir = Path(stack.enter_context(tempfile.TemporaryDirectory()))
                restored_name = resolved_artifact.stem
                source_path = gunzip_decompress(resolved_artifact, temp_dir / restored_name)
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
            "duration_ms": _duration_ms(started_at, finished_at),
            "status": "success",
        }
        logger.log_event("restore.completed", result)
        logger.print(f"Restore completed from: {resolved_artifact}")
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
        raise _wrap_error(message, exc) from exc


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
        raise _wrap_error(redactor.sanitize_text(exc), exc) from exc

    return {
        "profile": profile_name,
        "db_type": profile.db_type,
        "status": "ok",
    }


def run_validate_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    config = load_config(config_path)
    issues: list[str] = []

    for profile_name, profile in config.profiles.items():
        issues.extend(_collect_profile_validation_issues(config, profile_name, profile))

    if issues:
        joined = "\n".join(issues)
        raise ConfigError(f"Configuration validation failed:\n{joined}")

    return {
        "status": "ok",
        "profiles": sorted(config.profiles.keys()),
        "config_path": str(config.source_path or config_path),
    }


def run_verify_latest_backup(
    source_profile_name: str,
    target_profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    console: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    config = load_config(config_path)
    source_profile = config.get_profile(source_profile_name)
    target_profile = config.get_profile(target_profile_name)
    if source_profile_name == target_profile_name:
        raise ConfigError("Verification target profile must be different from the backup source profile")
    if source_profile.db_type != target_profile.db_type:
        raise ConfigError(
            f"Verification requires matching db_type values. Source is '{source_profile.db_type}', "
            f"target is '{target_profile.db_type}'"
        )

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
        latest_backup = get_latest_backup_run(config, source_profile_name, storage_backend=storage)
        artifact_path = Path(latest_backup["run_dir"])
        logger.print(
            f"Verifying latest backup from '{source_profile_name}' into '{target_profile_name}'"
        )
        logger.log_event(
            "verification.started",
            {
                "profile": source_profile_name,
                "profiles": [source_profile_name, target_profile_name],
                "target_profile": target_profile_name,
                "run_id": latest_backup.get("run_id"),
                "artifact_path": latest_backup.get("artifact_path"),
            },
        )
        restore_result = run_restore(
            profile_name=target_profile_name,
            input_path=artifact_path,
            config_path=config_path,
            console=console,
            notify=False,
        )
        connection_result = run_test_connection_with_config(config, target_profile_name)
        finished_at = current_time()
        result = {
            "profile": source_profile_name,
            "profiles": [source_profile_name, target_profile_name],
            "target_profile": target_profile_name,
            "db_type": source_profile.db_type,
            "run_id": latest_backup.get("run_id"),
            "artifact_path": latest_backup.get("artifact_path"),
            "started_at": format_timestamp(started_at),
            "finished_at": format_timestamp(finished_at),
            "duration_ms": _duration_ms(started_at, finished_at),
            "restore_status": restore_result.get("status"),
            "connection_status": connection_result.get("status"),
            "status": "verified",
        }
        logger.log_event("verification.completed", result)
        notify_event(notification_settings, "verification.completed", result, logger, redactor)
        logger.print(
            f"Verification completed for backup '{latest_backup.get('run_id')}' into '{target_profile_name}'"
        )
        return result
    except Exception as exc:
        message = redactor.sanitize_text(exc)
        payload = {
            "profile": source_profile_name,
            "profiles": [source_profile_name, target_profile_name],
            "target_profile": target_profile_name,
            "run_id": latest_backup.get("run_id") if latest_backup else None,
            "artifact_path": latest_backup.get("artifact_path") if latest_backup else None,
            "error": message,
        }
        logger.log_event("verification.failed", payload)
        notify_event(notification_settings, "verification.failed", payload, logger, redactor)
        raise _wrap_error(message, exc) from exc


def validate_profile_config(config: AppConfig, profile_name: str) -> dict[str, Any]:
    profile = config.get_profile(profile_name)
    issues = _collect_profile_validation_issues(config, profile_name, profile)
    if issues:
        joined = "\n".join(issues)
        raise ConfigError(f"Profile validation failed:\n{joined}")

    return {
        "status": "ok",
        "profile": profile_name,
        "db_type": profile.db_type,
        "config_path": str(config.source_path or DEFAULT_CONFIG_PATH),
    }


def get_latest_backup_run(
    config: AppConfig,
    profile_name: str,
    *,
    storage_backend: StorageBackend | None = None,
) -> dict[str, Any]:
    profile = config.get_profile(profile_name)
    output_dir = config.output_dir_for(profile)
    storage = storage_backend or get_storage_backend(config)
    latest = storage.latest_backup_run(profile_name, output_dir)
    manifest = latest.manifest
    return {
        "profile": profile_name,
        "run_dir": str(latest.run_dir),
        "run_id": manifest.get("run_id"),
        "db_type": manifest.get("db_type"),
        "artifact_path": manifest.get("artifact_path"),
        "finished_at": manifest.get("finished_at"),
    }


def _collect_profile_validation_issues(
    config: AppConfig,
    profile_name: str,
    profile: ProfileModel,
) -> list[str]:
    adapter = get_adapter(profile.db_type)
    output_dir = config.output_dir_for(profile)
    issues: list[str] = []
    try:
        validate_writable_path(output_dir)
    except ValueError as exc:
        issues.append(f"[{profile_name}] {exc}")

    for tool in adapter.required_tools():
        if shutil.which(tool) is None:
            issues.append(f"[{profile_name}] Required tool not found on PATH: {tool}")
    return issues


def _resolve_restore_selection(
    *,
    adapter: DatabaseAdapter,
    profile: ProfileModel,
    tables: list[str] | None,
    collections: list[str] | None,
) -> list[str] | None:
    requested_tables = [item.strip() for item in (tables or []) if item.strip()]
    requested_collections = [item.strip() for item in (collections or []) if item.strip()]
    if requested_tables and requested_collections:
        raise ConfigError("Use either --table or --collection for selective restore, not both.")

    kind = adapter.restore_filter_kind()
    if not requested_tables and not requested_collections:
        return None

    if kind == "table":
        if requested_collections:
            raise ConfigError(f"Profile '{profile.db_type}' restore uses --table, not --collection.")
        return adapter.normalize_restore_selection(profile, requested_tables)

    if kind == "collection":
        if requested_tables:
            raise ConfigError(f"Profile '{profile.db_type}' restore uses --collection, not --table.")
        return adapter.normalize_restore_selection(profile, requested_collections)

    raise ConfigError(
        f"Selective restore is not supported for db_type '{profile.db_type}' with the current backup format."
    )


def _validate_backup_preflight(output_dir: Path, required_tools: list[str]) -> None:
    try:
        validate_writable_path(output_dir)
    except ValueError as exc:
        raise PreflightError(str(exc)) from exc
    ensure_directory(output_dir)
    _ensure_tools_available(required_tools)


def _validate_restore_preflight(required_tools: list[str]) -> None:
    _ensure_tools_available(required_tools)


def _ensure_tools_available(required_tools: list[str]) -> None:
    missing = [tool for tool in required_tools if shutil.which(tool) is None]
    if missing:
        raise PreflightError(f"Required tool(s) not found on PATH: {', '.join(missing)}")


def _duration_ms(started_at: datetime, finished_at: datetime) -> int:
    return int((finished_at - started_at).total_seconds() * 1000)


def _wrap_error(message: str, original: Exception) -> DBRestoreError:
    if isinstance(original, DBRestoreError):
        original.args = (message,)
        return original
    return DBRestoreError(message)


def apply_retention_policy(
    config: AppConfig,
    profile_name: str,
    profile: ProfileModel,
    output_dir: Path,
    logger: RunLogger,
    *,
    storage_backend: StorageBackend | None = None,
) -> dict[str, Any]:
    storage = storage_backend or get_storage_backend(config)
    policy = config.retention_for(profile)
    if policy is None:
        return {"deleted_count": 0, "deleted_runs": []}

    runs = storage.list_backup_runs(profile_name, output_dir)
    if not runs:
        return {"deleted_count": 0, "deleted_runs": []}

    deletion_reasons: dict[str, dict[str, Any]] = {}
    if policy.keep_last is not None:
        for run in runs[policy.keep_last :]:
            entry = deletion_reasons.setdefault(run.run_dir, {"run": run, "reasons": set()})
            entry["reasons"].add("keep_last")

    if policy.max_age_days is not None:
        cutoff = current_time() - timedelta(days=policy.max_age_days)
        for run in runs:
            if run.finished_at < cutoff.astimezone(timezone.utc):
                entry = deletion_reasons.setdefault(run.run_dir, {"run": run, "reasons": set()})
                entry["reasons"].add("max_age_days")

    ordered_entries = [
        entry
        for _, entry in sorted(
            deletion_reasons.items(),
            key=lambda item: Path(item[0]).name,
        )
    ]
    deleted_runs = storage.delete_backup_runs([entry["run"] for entry in ordered_entries])
    for entry, deleted_run in zip(ordered_entries, deleted_runs, strict=True):
        logger.log_event(
            "retention.deleted",
            {
                "profile": profile_name,
                "run_dir": deleted_run,
                "reasons": sorted(entry["reasons"]),
            },
        )

    if deleted_runs:
        logger.print(f"Retention removed {len(deleted_runs)} old backup run(s) for profile '{profile_name}'")

    return {
        "deleted_count": len(deleted_runs),
        "deleted_runs": deleted_runs,
    }


def list_backup_history(
    config_path: Path = DEFAULT_CONFIG_PATH,
    profile_name: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    config = load_config(config_path, require_env=False)
    storage = get_storage_backend(config)
    profile_names = [profile_name] if profile_name else sorted(config.profiles.keys())
    history: list[dict[str, Any]] = []

    for current_profile in profile_names:
        profile = config.get_profile(current_profile)
        output_dir = config.output_dir_for(profile)
        for run in storage.list_backup_runs(current_profile, output_dir):
            manifest = run.manifest
            history.append(
                {
                    "profile": current_profile,
                    "run_dir": str(run.run_dir),
                    "run_id": manifest.get("run_id"),
                    "db_type": manifest.get("db_type"),
                    "backup_type": manifest.get("backup_type"),
                    "artifact_path": manifest.get("artifact_path"),
                    "manifest_path": str(run.manifest_path),
                    "compression": manifest.get("compression"),
                    "started_at": manifest.get("started_at"),
                    "finished_at": manifest.get("finished_at"),
                    "duration_ms": manifest.get("duration_ms"),
                }
            )

    history.sort(key=_history_sort_key, reverse=True)
    if limit is not None:
        return history[:limit]
    return history


def list_run_log_events(
    config_path: Path = DEFAULT_CONFIG_PATH,
    profile_name: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    config = load_config(config_path, require_env=False)
    log_file = config.log_file_path()
    if not log_file.exists():
        return []

    events: list[dict[str, Any]] = []
    with log_file.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            payload = event.get("payload", {})
            if profile_name is not None:
                payload_profile = payload.get("profile")
                payload_profiles = payload.get("profiles", [])
                if payload_profile != profile_name and profile_name not in payload_profiles:
                    continue
            events.append(event)

    events.sort(key=_event_sort_key, reverse=True)
    return events[:limit]


def _history_sort_key(item: dict[str, Any]) -> datetime:
    finished_at = item.get("finished_at")
    if not finished_at:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return parse_timestamp(str(finished_at)).astimezone(timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _event_sort_key(item: dict[str, Any]) -> datetime:
    timestamp = item.get("timestamp")
    if not timestamp:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return parse_timestamp(str(timestamp)).astimezone(timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
