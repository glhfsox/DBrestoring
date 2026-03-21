"""Shared helpers used across backup, verification, history, and health services."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from dbrestore.adapters import get_adapter
from dbrestore.adapters.base import DatabaseAdapter
from dbrestore.config import AppConfig, ProfileModel
from dbrestore.errors import ConfigError, DBRestoreError, PreflightError
from dbrestore.utils import Redactor, ensure_directory, validate_writable_path

type ProgressCallback = Callable[[dict[str, Any]], None]


def build_redactor(*profiles: ProfileModel) -> Redactor:
    redactor = Redactor()
    for profile in profiles:
        redactor.add(profile.password_value)
    return redactor


def missing_required_tools(required_tools: list[str]) -> list[str]:
    return [tool for tool in required_tools if shutil.which(tool) is None]


def ensure_tools_available(required_tools: list[str]) -> None:
    missing = missing_required_tools(required_tools)
    if missing:
        raise PreflightError(f"Required tool(s) not found on PATH: {', '.join(missing)}")


def validate_backup_preflight(output_dir: Path, required_tools: list[str]) -> None:
    try:
        validate_writable_path(output_dir)
    except ValueError as exc:
        raise PreflightError(str(exc)) from exc
    ensure_directory(output_dir)
    ensure_tools_available(required_tools)


def validate_restore_preflight(required_tools: list[str]) -> None:
    ensure_tools_available(required_tools)


def resolve_restore_selection(
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
            raise ConfigError(
                f"Profile '{profile.db_type}' restore uses --table, not --collection."
            )
        return adapter.normalize_restore_selection(profile, requested_tables)

    if kind == "collection":
        if requested_tables:
            raise ConfigError(
                f"Profile '{profile.db_type}' restore uses --collection, not --table."
            )
        return adapter.normalize_restore_selection(profile, requested_collections)

    raise ConfigError(
        f"Selective restore is not supported for db_type '{profile.db_type}' with the current backup format."
    )


def collect_profile_validation_issues(
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


def duration_ms(started_at: datetime, finished_at: datetime) -> int:
    return int((finished_at - started_at).total_seconds() * 1000)


def wrap_error(message: str, original: Exception) -> DBRestoreError:
    if isinstance(original, DBRestoreError):
        original.args = (message,)
        return original
    return DBRestoreError(message)


def emit_progress(
    progress: ProgressCallback | None,
    *,
    message: str,
    percent: float | None = None,
    target_percent: float | None = None,
    mode: str = "determinate",
) -> None:
    if progress is None:
        return
    payload: dict[str, Any] = {
        "message": message,
        "mode": mode,
    }
    if percent is not None:
        payload["percent"] = max(0.0, min(100.0, float(percent)))
    if target_percent is not None:
        payload["target_percent"] = max(0.0, min(100.0, float(target_percent)))
    progress(payload)


def scaled_progress(
    progress: ProgressCallback | None,
    *,
    start: float,
    end: float,
    prefix: str | None = None,
) -> ProgressCallback | None:
    if progress is None:
        return None

    def nested(payload: dict[str, Any]) -> None:
        message = str(payload.get("message", "")).strip()
        if prefix:
            message = f"{prefix}: {message}" if message else prefix

        mode = str(payload.get("mode", "determinate"))
        if "percent" in payload:
            child_percent = max(0.0, min(100.0, float(payload["percent"])))
            percent = start + ((end - start) * (child_percent / 100.0))
        else:
            percent = start if mode == "determinate" else None

        if "target_percent" in payload:
            child_target = max(0.0, min(100.0, float(payload["target_percent"])))
            target_percent = start + ((end - start) * (child_target / 100.0))
        else:
            target_percent = None

        emit_progress(
            progress,
            message=message,
            percent=percent,
            target_percent=target_percent,
            mode=mode,
        )

    return nested
