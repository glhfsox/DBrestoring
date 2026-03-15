from __future__ import annotations

from pathlib import Path
from urllib.parse import quote_plus
from uuid import uuid4

from pymongo import MongoClient

from dbrestore.adapters.base import CommandSpec, ExternalToolAdapter
from dbrestore.errors import DatabaseConnectionError, PreflightError


class MongoAdapter(ExternalToolAdapter):
    @property
    def db_type(self) -> str:
        return "mongo"

    def required_tools(self) -> list[str]:
        return ["mongodump", "mongorestore"]

    def artifact_extension(self) -> str:
        return ".archive"

    def test_connection(self, profile: object) -> None:
        try:
            client = MongoClient(self._build_connection_uri(profile), serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            client.close()
        except Exception as exc:
            raise DatabaseConnectionError(f"MongoDB connection failed: {exc}") from exc

    def validate_restore_target(self, profile: object) -> None:
        collection_name = f"__dbrestore_preflight_{uuid4().hex[:12]}"
        client = None
        try:
            client = MongoClient(self._build_connection_uri(profile), serverSelectionTimeoutMS=5000)
            database = client[profile.database]
            collection = database[collection_name]
            collection.insert_one({"created_by": "dbrestore"})
            database.drop_collection(collection_name)
        except Exception as exc:
            raise PreflightError(
                f"MongoDB restore pre-check failed for database '{profile.database}': "
                "the configured user cannot create and drop collections in the target database. "
                "Grant write privileges on the restore target before restoring."
            ) from exc
        finally:
            if client is not None:
                client.close()

    def build_backup_command(self, profile: object, destination: Path) -> CommandSpec:
        return CommandSpec(
            args=[
                "mongodump",
                f"--archive={destination}",
                "--db",
                profile.database,
                "--uri",
                self._build_connection_uri(profile),
            ]
        )

    def build_restore_command(self, profile: object, source: Path) -> CommandSpec:
        return CommandSpec(
            args=[
                "mongorestore",
                "--drop",
                f"--archive={source}",
                "--nsInclude",
                f"{profile.database}.*",
                "--uri",
                self._build_connection_uri(profile),
            ]
        )

    def backup(self, profile: object, destination: Path, redactor: object) -> dict[str, str]:
        super().backup(profile, destination, redactor)
        return {"format": "mongo_archive"}

    def _build_connection_uri(self, profile: object) -> str:
        host = profile.effective_host
        port = profile.effective_port
        database = profile.database
        auth_database = profile.auth_database or database
        username = profile.username
        password = profile.password_value

        if username and password:
            return (
                f"mongodb://{quote_plus(username)}:{quote_plus(password)}@"
                f"{host}:{port}/{database}?authSource={quote_plus(auth_database)}"
            )
        return f"mongodb://{host}:{port}/{database}"
