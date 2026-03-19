from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql

from dbrestore.adapters.base import CommandSpec, ExternalToolAdapter
from dbrestore.config import ProfileModel
from dbrestore.errors import DatabaseConnectionError, PreflightError
from dbrestore.utils import Redactor


class PostgresAdapter(ExternalToolAdapter):
    @property
    def db_type(self) -> str:
        return "postgres"

    def required_tools(self) -> list[str]:
        return ["pg_dump", "pg_restore"]

    def restore_filter_kind(self) -> str | None:
        return "table"

    def artifact_extension(self) -> str:
        return ".dump"

    def test_connection(self, profile: ProfileModel) -> None:
        port = self._connection_port(profile)
        username = self._connection_user(profile)
        password = self._connection_password(profile)
        try:
            with psycopg.connect(
                host=profile.effective_host,
                port=port,
                user=username,
                password=password,
                dbname=profile.database,
                connect_timeout=5,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
        except Exception as exc:
            raise DatabaseConnectionError(f"PostgreSQL connection failed: {exc}") from exc

    def validate_restore_target(self, profile: ProfileModel) -> None:
        table_name = f"__dbrestore_preflight_{uuid4().hex[:12]}"
        port = self._connection_port(profile)
        username = self._connection_user(profile)
        password = self._connection_password(profile)
        try:
            with psycopg.connect(
                host=profile.effective_host,
                port=port,
                user=username,
                password=password,
                dbname=profile.database,
                connect_timeout=5,
            ) as connection:
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            sql.SQL("CREATE TABLE public.{} (id integer)").format(
                                sql.Identifier(table_name)
                            )
                        )
                    connection.rollback()
                except Exception as exc:
                    raise PreflightError(
                        f"PostgreSQL restore pre-check failed for database '{profile.database}': "
                        f"user '{username}' cannot create tables in schema public. "
                        "Fix the target database before restoring, for example with: "
                        f"ALTER SCHEMA public OWNER TO {username}; "
                        f"GRANT USAGE, CREATE ON SCHEMA public TO {username};"
                    ) from exc
        except PreflightError:
            raise
        except Exception as exc:
            raise PreflightError(
                f"PostgreSQL restore pre-check failed for database '{profile.database}': {exc}"
            ) from exc

    def build_backup_command(self, profile: ProfileModel, destination: Path) -> CommandSpec:
        port = self._connection_port(profile)
        username = self._connection_user(profile)
        password = self._connection_password(profile)
        return CommandSpec(
            args=[
                "pg_dump",
                "--format=custom",
                "--file",
                str(destination),
                "--host",
                profile.effective_host,
                "--port",
                str(port),
                "--username",
                username,
                profile.database,
            ],
            env={"PGPASSWORD": password},
        )

    def build_restore_command(
        self,
        profile: ProfileModel,
        source: Path,
        selection: list[str] | None = None,
    ) -> CommandSpec:
        port = self._connection_port(profile)
        username = self._connection_user(profile)
        password = self._connection_password(profile)
        args: list[str] = [
            "pg_restore",
            "--clean",
            "--if-exists",
            "--no-owner",
        ]
        for item in selection or []:
            args.extend(["--table", item])
        args.extend(
            [
                "--host",
                profile.effective_host,
                "--port",
                str(port),
                "--username",
                username,
                "--dbname",
                profile.database,
                str(source),
            ]
        )
        return CommandSpec(
            args=args,
            env={"PGPASSWORD": password},
        )

    def backup(
        self, profile: ProfileModel, destination: Path, redactor: Redactor
    ) -> dict[str, str]:
        super().backup(profile, destination, redactor)
        return {"format": "pg_dump_custom"}

    def _connection_port(self, profile: ProfileModel) -> int:
        port = profile.effective_port
        if port is None:
            raise ValueError("postgres profiles require a port")
        return port

    def _connection_user(self, profile: ProfileModel) -> str:
        username = profile.username
        if username is None:
            raise ValueError("postgres profiles require a username")
        return username

    def _connection_password(self, profile: ProfileModel) -> str:
        return profile.password_value or ""
