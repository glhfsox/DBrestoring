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

    def to_dict(self) -> dict[str, Any]:
        return json_safe(asdict(self))

