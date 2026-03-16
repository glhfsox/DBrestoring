from __future__ import annotations

import sqlite3
from pathlib import Path
from uuid import uuid4

from dbrestore.adapters.base import DatabaseAdapter
from dbrestore.config import ProfileModel
from dbrestore.errors import DatabaseConnectionError, PreflightError
from dbrestore.utils import Redactor, ensure_directory


class SQLiteAdapter(DatabaseAdapter):
    @property
    def db_type(self) -> str:
        return "sqlite"

    def required_tools(self) -> list[str]:
        return []

    def artifact_extension(self) -> str:
        return ".sqlite"

    def test_connection(self, profile: ProfileModel) -> None:
        path = profile.resolved_database_path()
        if not path.exists():
            raise DatabaseConnectionError(f"SQLite database file not found: {path}")
        try:
            connection = sqlite3.connect(path)
            connection.execute("SELECT 1")
            connection.close()
        except Exception as exc:
            raise DatabaseConnectionError(f"SQLite connection failed: {exc}") from exc

    def validate_restore_target(self, profile: ProfileModel) -> None:
        target_path = profile.resolved_database_path()
        table_name = f"__dbrestore_preflight_{uuid4().hex[:12]}"
        try:
            ensure_directory(target_path.parent)
            with sqlite3.connect(target_path) as connection:
                connection.execute(f"CREATE TABLE {table_name} (id INTEGER)")
                connection.execute(f"DROP TABLE {table_name}")
        except Exception as exc:
            raise PreflightError(
                f"SQLite restore pre-check failed for '{target_path}': "
                "ensure the target database file and parent directory are writable."
            ) from exc

    def backup(self, profile: ProfileModel, destination: Path, redactor: Redactor) -> dict[str, str]:
        source_path = profile.resolved_database_path()
        if not source_path.exists():
            raise DatabaseConnectionError(f"SQLite database file not found: {source_path}")

        ensure_directory(destination.parent)
        with sqlite3.connect(source_path) as source_conn, sqlite3.connect(destination) as destination_conn:
            source_conn.backup(destination_conn)
        return {"format": "sqlite_backup"}

    def restore(
        self,
        profile: ProfileModel,
        source: Path,
        redactor: Redactor,
        selection: list[str] | None = None,
    ) -> None:
        del redactor, selection
        target_path = profile.resolved_database_path()
        ensure_directory(target_path.parent)
        with sqlite3.connect(source) as source_conn, sqlite3.connect(target_path) as target_conn:
            source_conn.backup(target_conn)
