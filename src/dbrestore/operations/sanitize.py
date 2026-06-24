"""Pull a database snapshot and apply PII masking to produce a sanitized copy.

SQLite is masked directly on the dumped file. Postgres/MySQL can't be masked as a
dump, so they go through a scratch target: backup source -> restore into the
scratch DB -> mask it in place via SQL -> optionally dump the sanitized result.
The source database is never modified.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from dbrestore.adapters import get_adapter
from dbrestore.config import DEFAULT_CONFIG_PATH, ProfileModel, load_config
from dbrestore.errors import ConfigError
from dbrestore.logging import RunLogger
from dbrestore.masking import (
    MYSQL,
    POSTGRES,
    Dialect,
    MaskRule,
    apply_masking,
    apply_masking_sqlite,
    derive_key,
)
from dbrestore.utils import ensure_directory

from .common import build_redactor, wrap_error

_SCRATCH_ENGINES = ("postgres", "mysql", "mariadb")


def _connect(profile: ProfileModel) -> tuple[Any, Dialect]:
    if profile.db_type == "postgres":
        import psycopg

        conn = psycopg.connect(
            host=profile.effective_host,
            port=profile.effective_port or 5432,
            user=profile.username,
            password=profile.password_value or "",
            dbname=profile.database,
            connect_timeout=10,
        )
        return conn, POSTGRES
    if profile.db_type in ("mysql", "mariadb"):
        import pymysql

        conn = pymysql.connect(
            host=profile.effective_host,
            port=profile.effective_port or 3306,
            user=profile.username,
            password=profile.password_value or "",
            database=profile.database,
            connect_timeout=10,
        )
        return conn, MYSQL
    raise ConfigError(f"Masking over a live connection is not supported for {profile.db_type!r}.")


def run_sanitize(
    profile_name: str,
    output_path: Path | None = None,
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

    rules = [MaskRule(r.table, r.column, r.strategy, r.value) for r in masking.rules]
    salt = masking.salt.get_secret_value().encode("utf-8") if masking.salt else os.urandom(32)
    if masking.salt is not None:
        redactor.add(masking.salt.get_secret_value())
    key = derive_key(salt)

    # Fall back to the destination configured in the masking block, so a scheduled
    # `dbrestore sanitize --profile X` works with no extra flags.
    if output_path is None and masking.output:
        output_path = Path(masking.output)
    if target_profile is None and masking.target_profile:
        target_profile = masking.target_profile

    try:
        if profile.db_type == "sqlite":
            counts = _sanitize_sqlite(
                config,
                profile,
                profile_name,
                adapter,
                rules,
                key,
                output_path,
                target_profile,
                redactor,
                logger,
            )
        elif profile.db_type in _SCRATCH_ENGINES:
            counts = _sanitize_via_scratch(
                config,
                profile,
                profile_name,
                adapter,
                rules,
                key,
                output_path,
                target_profile,
                redactor,
                logger,
            )
        else:
            raise ConfigError(f"Masking is not supported for db_type {profile.db_type!r}.")

        total = sum(counts.values())
        logger.print(f"Sanitize complete ({total} values masked)")
        return {
            "profile": profile_name,
            "db_type": profile.db_type,
            "output_path": str(output_path) if output_path else None,
            "target_profile": target_profile,
            "masked_counts": counts,
            "total_masked": total,
        }
    except ConfigError:
        raise
    except Exception as exc:
        raise wrap_error(redactor.sanitize_text(exc), exc) from exc


def _sanitize_sqlite(
    config,
    profile,
    profile_name,
    adapter,
    rules,
    key,
    output_path,
    target_profile,
    redactor,
    logger,
) -> dict[str, int]:
    if output_path is None and target_profile is None:
        raise ConfigError("Provide --output and/or --target-profile for a sqlite sanitize.")

    logger.print(f"Pulling snapshot for profile '{profile_name}'")
    with tempfile.TemporaryDirectory() as tmp:
        snapshot = Path(tmp) / f"{profile_name}{adapter.artifact_extension()}"
        adapter.backup(profile, snapshot, redactor)

        logger.print(f"Applying {len(rules)} masking rule(s)")
        with sqlite3.connect(snapshot) as conn:
            counts = apply_masking_sqlite(conn, rules, key)

        if output_path is not None:
            ensure_directory(output_path.parent)
            shutil.copy(snapshot, output_path)
            logger.print(f"Sanitized copy written to {output_path}")
        if target_profile is not None:
            target = config.get_profile(target_profile)
            target_adapter = get_adapter(target.db_type)
            target_adapter.validate_restore_target(target)
            target_adapter.restore(target, snapshot, build_redactor(target))
            logger.print(f"Restored sanitized copy into profile '{target_profile}'")
    return counts


def _sanitize_via_scratch(
    config,
    profile,
    profile_name,
    adapter,
    rules,
    key,
    output_path,
    target_profile,
    redactor,
    logger,
) -> dict[str, int]:
    if target_profile is None:
        raise ConfigError(
            f"Masking {profile.db_type} requires --target-profile: a scratch database to "
            "restore into and mask (the source is never touched)."
        )
    target = config.get_profile(target_profile)
    if target.db_type != profile.db_type:
        raise ConfigError(
            f"target profile '{target_profile}' is {target.db_type}, but source is "
            f"{profile.db_type}; they must be the same engine."
        )
    target_adapter = get_adapter(target.db_type)
    target_adapter.validate_restore_target(target)

    with tempfile.TemporaryDirectory() as tmp:
        dump = Path(tmp) / f"{profile_name}{adapter.artifact_extension()}"
        logger.print(f"Pulling snapshot for profile '{profile_name}'")
        adapter.backup(profile, dump, redactor)

        logger.print(f"Restoring into scratch target '{target_profile}'")
        target_adapter.restore(target, dump, build_redactor(target))

        logger.print(f"Applying {len(rules)} masking rule(s) on '{target_profile}'")
        conn, dialect = _connect(target)
        try:
            counts = apply_masking(conn, rules, key, dialect)
        finally:
            conn.close()

        if output_path is not None:
            ensure_directory(output_path.parent)
            logger.print(f"Dumping sanitized result to {output_path}")
            target_adapter.backup(target, output_path, build_redactor(target))
    return counts
