"""Retention helpers for backup histories."""

from __future__ import annotations

from datetime import UTC, timedelta
from pathlib import Path
from typing import Any

from dbrestore.config import AppConfig, ProfileModel, RetentionModel
from dbrestore.logging import RunLogger
from dbrestore.storage import BackupRunRecord, StorageBackend, get_storage_backend
from dbrestore.utils import current_time


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

    ordered_entries = _retention_deletion_entries(policy, runs)
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
        logger.print(
            f"Retention removed {len(deleted_runs)} old backup run(s) for profile '{profile_name}'"
        )

    return {
        "deleted_count": len(deleted_runs),
        "deleted_runs": deleted_runs,
    }


def summarize_retention_policy(
    config: AppConfig,
    profile_name: str,
    profile: ProfileModel,
    output_dir: Path,
    *,
    storage_backend: StorageBackend | None = None,
) -> dict[str, Any]:
    storage = storage_backend or get_storage_backend(config)
    policy = config.retention_for(profile)
    runs = storage.list_backup_runs(profile_name, output_dir)
    if policy is None:
        return {
            "configured": False,
            "keep_last": None,
            "max_age_days": None,
            "total_runs": len(runs),
            "pending_delete_count": 0,
            "pending_delete_runs": [],
        }

    ordered_entries = _retention_deletion_entries(policy, runs)
    return {
        "configured": True,
        "keep_last": policy.keep_last,
        "max_age_days": policy.max_age_days,
        "total_runs": len(runs),
        "pending_delete_count": len(ordered_entries),
        "pending_delete_runs": [entry["run"].run_dir for entry in ordered_entries],
    }


def _retention_deletion_entries(
    policy: RetentionModel,
    runs: list[BackupRunRecord],
) -> list[dict[str, Any]]:
    deletion_reasons: dict[str, dict[str, Any]] = {}
    if policy.keep_last is not None:
        for run in runs[policy.keep_last :]:
            entry = deletion_reasons.setdefault(run.run_dir, {"run": run, "reasons": set()})
            entry["reasons"].add("keep_last")

    if policy.max_age_days is not None:
        cutoff = current_time() - timedelta(days=policy.max_age_days)
        for run in runs:
            if run.finished_at < cutoff.astimezone(UTC):
                entry = deletion_reasons.setdefault(run.run_dir, {"run": run, "reasons": set()})
                entry["reasons"].add("max_age_days")

    return [
        entry
        for _, entry in sorted(
            deletion_reasons.items(),
            key=lambda item: Path(item[0]).name,
        )
    ]
