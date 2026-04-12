"""This module stores small shared data models used across the app.
These models describe artifacts like backup manifests in a stable, serializable shape.
They sit below operations and storage, giving both sides a common language for metadata.
If a run produces a file plus structured details about it, that shape is usually defined here."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from dbrestore.utils import json_safe


@dataclass(frozen=True)
class BackupManifest:
    run_id: str
    profile: str
    db_type: str
    backup_type: str
    started_at: str
    finished_at: str
    duration_ms: int
    artifact_path: str
    compression: str
    source: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)
    parent_run_id: str | None = None
    chain: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return json_safe(asdict(self))
