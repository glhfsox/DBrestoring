"""Small GUI-only helpers and shared presentation constants."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from dbrestore.config import DB_TYPE_ALIASES
from dbrestore.errors import DBRestoreError
from dbrestore.utils import format_display_timestamp, parse_timestamp


def default_raw_config() -> dict[str, Any]:
    return {
        "version": 1,
        "defaults": {
            "output_dir": "./backups",
            "log_dir": "./logs",
            "compression": "gzip",
        },
        "profiles": {},
    }


def stringify_optional(value: Any) -> str:
    return "" if value is None else str(value)


def collect_retention_block(*, keep_last: str, max_age_days: str) -> dict[str, int]:
    retention: dict[str, int] = {}
    if keep_last:
        retention["keep_last"] = int(keep_last)
    if max_age_days:
        retention["max_age_days"] = int(max_age_days)
    return retention


def pretty_timestamp(value: Any) -> str:
    if not value:
        return ""
    try:
        return format_display_timestamp(parse_timestamp(str(value)))
    except ValueError:
        return str(value)


def profile_compression_label(value: Any) -> str:
    if value is True:
        return "gzip"
    if value is False:
        return "none"
    return "inherit"


def restore_option_label(record: dict[str, Any]) -> str:
    timestamp = pretty_timestamp(record.get("finished_at")) or "Unknown time"
    run_id = record.get("run_id") or "unknown-run"
    artifact_name = Path(record.get("artifact_path") or "").name or "artifact"
    return f"{timestamp} | {run_id} | {artifact_name}"


def normalize_db_type_label(value: str) -> str:
    return DB_TYPE_ALIASES.get(value.strip().lower(), value.strip().lower())


def set_widget_state(widget: Any, enabled: bool) -> None:
    if enabled:
        widget.state(["!disabled"])
    else:
        widget.state(["disabled"])


def open_path_in_file_manager(target: Path) -> None:
    opener = _file_manager_opener()
    if opener is None:
        raise DBRestoreError("No supported file opener was found for this operating system.")
    try:
        subprocess.Popen([opener, str(target)])
    except OSError as exc:
        raise DBRestoreError(f"Unable to open '{target}': {exc}") from exc


def _file_manager_opener() -> str | None:
    if sys.platform == "darwin":
        return shutil.which("open")
    if os.name == "nt":
        return shutil.which("explorer")
    return shutil.which("xdg-open")


PALETTE = {
    "canvas": "#F3F0EA",
    "card": "#FCFBF8",
    "field": "#F0ECE5",
    "field_alt": "#E7E1D7",
    "accent": "#176B87",
    "accent_dark": "#135468",
    "accent_soft": "#D8EBF2",
    "danger": "#B33939",
    "ink": "#1F2A2E",
    "muted": "#5D676A",
    "border": "#D6D0C7",
}
