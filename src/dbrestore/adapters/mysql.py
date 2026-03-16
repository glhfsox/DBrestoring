from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pymysql

from dbrestore.adapters.base import CommandSpec, ExternalToolAdapter
from dbrestore.config import ProfileModel
from dbrestore.errors import DatabaseConnectionError, PreflightError
from dbrestore.utils import Redactor


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

    def test_connection(self, profile: ProfileModel) -> None:
        port = self._connection_port(profile)
        username = self._connection_user(profile)
        password = self._connection_password(profile)
        try:
            connection = pymysql.connect(
                host=profile.effective_host,
                port=port,
                user=username,
                password=password,
                database=profile.database,
                connect_timeout=5,
            )
            connection.close()
        except Exception as exc:
            label = "MariaDB" if self._db_type == "mariadb" else "MySQL"
            raise DatabaseConnectionError(f"{label} connection failed: {exc}") from exc

    def validate_restore_target(self, profile: ProfileModel) -> None:
        label = "MariaDB" if self._db_type == "mariadb" else "MySQL"
        table_name = f"__dbrestore_preflight_{uuid4().hex[:12]}"
        port = self._connection_port(profile)
        username = self._connection_user(profile)
        password = self._connection_password(profile)
        try:
            connection = pymysql.connect(
                host=profile.effective_host,
                port=port,
                user=username,
                password=password,
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
                f"user '{username}' cannot create and drop tables in the target database. "
                "Grant CREATE and DROP on the restore target before restoring."
            ) from exc
        finally:
            connection.close()

    def build_backup_command(self, profile: ProfileModel, destination: Path) -> CommandSpec:
        port = self._connection_port(profile)
        username = self._connection_user(profile)
        password = self._connection_password(profile)
        return CommandSpec(
            args=[
                "mysqldump",
                "--single-transaction",
                "--host",
                profile.effective_host,
                "--port",
                str(port),
                "--user",
                username,
                "--result-file",
                str(destination),
                profile.database,
            ],
            env={"MYSQL_PWD": password},
        )

    def build_restore_command(
        self,
        profile: ProfileModel,
        source: Path,
        selection: list[str] | None = None,
    ) -> CommandSpec:
        del selection
        port = self._connection_port(profile)
        username = self._connection_user(profile)
        password = self._connection_password(profile)
        return CommandSpec(
            args=[
                "mysql",
                "--host",
                profile.effective_host,
                "--port",
                str(port),
                "--user",
                username,
                profile.database,
            ],
            env={"MYSQL_PWD": password},
            stdin_path=source,
        )

    def backup(self, profile: ProfileModel, destination: Path, redactor: Redactor) -> dict[str, str]:
        super().backup(profile, destination, redactor)
        return {"format": "sql"}

    def _connection_port(self, profile: ProfileModel) -> int:
        port = profile.effective_port
        if port is None:
            raise ValueError(f"{self._db_type} profiles require a port")
        return port

    def _connection_user(self, profile: ProfileModel) -> str:
        username = profile.username
        if username is None:
            raise ValueError(f"{self._db_type} profiles require a username")
        return username

    def _connection_password(self, profile: ProfileModel) -> str:
        return profile.password_value or ""
