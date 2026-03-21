"""History and run-log queries used by the CLI and GUI."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dbrestore.config import DEFAULT_CONFIG_PATH, AppConfig, load_config
from dbrestore.storage import StorageBackend, get_storage_backend
from dbrestore.utils import parse_timestamp


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


def latest_log_event(
    config_path: Path = DEFAULT_CONFIG_PATH,
    *,
    profile_name: str,
    event_names: set[str],
    limit: int = 400,
) -> dict[str, Any] | None:
    for event in list_run_log_events(
        config_path=config_path, profile_name=profile_name, limit=limit
    ):
        if event.get("event") in event_names:
            return event
    return None


def summarize_latest_event(
    config_path: Path = DEFAULT_CONFIG_PATH,
    *,
    profile_name: str,
    completed_event: str,
    failed_event: str,
    limit: int = 400,
) -> dict[str, Any] | None:
    event = latest_log_event(
        config_path=config_path,
        profile_name=profile_name,
        event_names={completed_event, failed_event},
        limit=limit,
    )
    if event is None:
        return None

    payload = event.get("payload", {})
    return {
        "event": event.get("event"),
        "timestamp": event.get("timestamp"),
        "status": "ok" if event.get("event") == completed_event else "error",
        "payload": payload,
    }


def _history_sort_key(item: dict[str, Any]) -> datetime:
    finished_at = item.get("finished_at")
    if not finished_at:
        return datetime.min.replace(tzinfo=UTC)
    try:
        return parse_timestamp(str(finished_at)).astimezone(UTC)
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)


def _event_sort_key(item: dict[str, Any]) -> datetime:
    timestamp = item.get("timestamp")
    if not timestamp:
        return datetime.min.replace(tzinfo=UTC)
    try:
        return parse_timestamp(str(timestamp)).astimezone(UTC)
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
