from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import psycopg

from dbrestore.adapters.base import CommandSpec, ExternalToolAdapter
from dbrestore.errors import DatabaseConnectionError, PreflightError


class PostgresAdapter(ExternalToolAdapter):
    @property
    def db_type(self) -> str:
        return "postgres"

    def required_tools(self) -> list[str]:
        return ["pg_dump", "pg_restore"]

    def artifact_extension(self) -> str:
        return ".dump"

    def test_connection(self, profile: object) -> None:
        try:
            with psycopg.connect(
                host=profile.effective_host,
                port=profile.effective_port,
                user=profile.username,
                password=profile.password_value,
                dbname=profile.database,
                connect_timeout=5,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
        except Exception as exc:
            raise DatabaseConnectionError(f"PostgreSQL connection failed: {exc}") from exc

    def validate_restore_target(self, profile: object) -> None:
        table_name = f"__dbrestore_preflight_{uuid4().hex[:12]}"
        try:
            with psycopg.connect(
                host=profile.effective_host,
                port=profile.effective_port,
                user=profile.username,
                password=profile.password_value,
                dbname=profile.database,
                connect_timeout=5,
            ) as connection:
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(f"CREATE TABLE public.{table_name} (id integer)")
                    connection.rollback()
                except Exception as exc:
                    raise PreflightError(
                        f"PostgreSQL restore pre-check failed for database '{profile.database}': "
                        f"user '{profile.username}' cannot create tables in schema public. "
                        "Fix the target database before restoring, for example with: "
                        f"ALTER SCHEMA public OWNER TO {profile.username}; "
                        f"GRANT USAGE, CREATE ON SCHEMA public TO {profile.username};"
                    ) from exc
        except PreflightError:
            raise
        except Exception as exc:
            raise PreflightError(
                f"PostgreSQL restore pre-check failed for database '{profile.database}': {exc}"
            ) from exc

    def build_backup_command(self, profile: object, destination: Path) -> CommandSpec:
        return CommandSpec(
            args=[
                "pg_dump",
                "--format=custom",
                "--file",
                str(destination),
                "--host",
                profile.effective_host,
                "--port",
                str(profile.effective_port),
                "--username",
                profile.username,
                profile.database,
            ],
            env={"PGPASSWORD": profile.password_value or ""},
        )

    def build_restore_command(self, profile: object, source: Path) -> CommandSpec:
        return CommandSpec(
            args=[
                "pg_restore",
                "--clean",
                "--if-exists",
                "--no-owner",
                "--host",
                profile.effective_host,
                "--port",
                str(profile.effective_port),
                "--username",
                profile.username,
                "--dbname",
                profile.database,
                str(source),
            ],
            env={"PGPASSWORD": profile.password_value or ""},
        )

    def backup(self, profile: object, destination: Path, redactor: object) -> dict[str, str]:
        super().backup(profile, destination, redactor)
        return {"format": "pg_dump_custom"}
