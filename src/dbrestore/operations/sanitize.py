"""Pull a database snapshot and apply PII masking to produce a sanitized copy."""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from dbrestore.adapters import get_adapter
from dbrestore.config import DEFAULT_CONFIG_PATH, load_config
from dbrestore.errors import ConfigError
from dbrestore.logging import RunLogger
from dbrestore.masking import MaskRule, apply_masking_sqlite, derive_key
from dbrestore.utils import ensure_directory

from .common import build_redactor, wrap_error


def run_sanitize(
    profile_name: str,
    output_path: Path,
    config_path: Path = DEFAULT_CONFIG_PATH,
    target_profile: str | None = None,
    console: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    config = load_config(config_path)
    profile = config.get_profile(profile_name)
    adapter = get_adapter(profile.db_type)
    redactor = build_redactor(profile)
    logger = RunLogger(config.log_file_path(), console=console)

    masking = config.masking_for(profile)
    if masking is None or not masking.rules:
        raise ConfigError(f"No masking rules configured for profile '{profile_name}'.")
    if profile.db_type != "sqlite":
        raise ConfigError(
            "sanitize currently supports sqlite profiles. Masking for "
            "Postgres/MySQL (restore into a scratch target, mask, re-dump) is next."
        )

    rules = [MaskRule(r.table, r.column, r.strategy, r.value) for r in masking.rules]
    salt = masking.salt.get_secret_value().encode("utf-8") if masking.salt else os.urandom(32)
    if masking.salt is not None:
        redactor.add(masking.salt.get_secret_value())
    key = derive_key(salt)

    try:
        logger.print(f"Pulling snapshot for profile '{profile_name}'")
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = Path(tmp) / f"{profile_name}{adapter.artifact_extension()}"
            adapter.backup(profile, snapshot, redactor)

            logger.print(f"Applying {len(rules)} masking rule(s)")
            with sqlite3.connect(snapshot) as conn:
                counts = apply_masking_sqlite(conn, rules, key)

            ensure_directory(output_path.parent)
            shutil.copy(snapshot, output_path)

        total = sum(counts.values())
        logger.print(f"Sanitized copy written to {output_path} ({total} values masked)")

        if target_profile is not None:
            target = config.get_profile(target_profile)
            target_adapter = get_adapter(target.db_type)
            target_adapter.validate_restore_target(target)
            target_adapter.restore(target, output_path, build_redactor(target))
            logger.print(f"Restored sanitized copy into profile '{target_profile}'")

        return {
            "profile": profile_name,
            "output_path": str(output_path),
            "masked_counts": counts,
            "total_masked": total,
            "target_profile": target_profile,
        }
    except ConfigError:
        raise
    except Exception as exc:
        raise wrap_error(redactor.sanitize_text(exc), exc) from exc
