from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pymysql

from dbrestore.adapters.base import CommandSpec, ExternalToolAdapter
from dbrestore.errors import DatabaseConnectionError, PreflightError


class MySQLAdapter(ExternalToolAdapter):
    def __init__(self, db_type: str = "mysql") -> None:
        self._db_type = db_type

    @property
    def db_type(self) -> str:
        return self._db_type

    def required_tools(self) -> list[str]:
        return ["mysqldump", "mysql"]

    def artifact_extension(self) -> str:
        return ".sql"

    def test_connection(self, profile: object) -> None:
        try:
            connection = pymysql.connect(
                host=profile.effective_host,
                port=profile.effective_port,
                user=profile.username,
                password=profile.password_value,
                database=profile.database,
                connect_timeout=5,
            )
            connection.close()
        except Exception as exc:
            label = "MariaDB" if self._db_type == "mariadb" else "MySQL"
            raise DatabaseConnectionError(f"{label} connection failed: {exc}") from exc

    def validate_restore_target(self, profile: object) -> None:
        label = "MariaDB" if self._db_type == "mariadb" else "MySQL"
        table_name = f"__dbrestore_preflight_{uuid4().hex[:12]}"
        try:
            connection = pymysql.connect(
                host=profile.effective_host,
                port=profile.effective_port,
                user=profile.username,
                password=profile.password_value,
                database=profile.database,
                connect_timeout=5,
            )
        except Exception as exc:
            raise PreflightError(
                f"{label} restore pre-check failed for database '{profile.database}': {exc}"
            ) from exc

        try:
            cursor = connection.cursor()
            try:
                cursor.execute(f"CREATE TABLE `{table_name}` (id INT)")
                cursor.execute(f"DROP TABLE `{table_name}`")
                connection.commit()
            finally:
                cursor.close()
        except Exception as exc:
            raise PreflightError(
                f"{label} restore pre-check failed for database '{profile.database}': "
                f"user '{profile.username}' cannot create and drop tables in the target database. "
                "Grant CREATE and DROP on the restore target before restoring."
            ) from exc
        finally:
            connection.close()

    def build_backup_command(self, profile: object, destination: Path) -> CommandSpec:
        return CommandSpec(
            args=[
                "mysqldump",
                "--single-transaction",
                "--host",
                profile.effective_host,
                "--port",
                str(profile.effective_port),
                "--user",
                profile.username,
                "--result-file",
                str(destination),
                profile.database,
            ],
            env={"MYSQL_PWD": profile.password_value or ""},
        )

    def build_restore_command(self, profile: object, source: Path) -> CommandSpec:
        return CommandSpec(
            args=[
                "mysql",
                "--host",
                profile.effective_host,
                "--port",
                str(profile.effective_port),
                "--user",
                profile.username,
                profile.database,
            ],
            env={"MYSQL_PWD": profile.password_value or ""},
            stdin_path=source,
        )

    def backup(self, profile: object, destination: Path, redactor: object) -> dict[str, str]:
        super().backup(profile, destination, redactor)
        return {"format": "sql"}
