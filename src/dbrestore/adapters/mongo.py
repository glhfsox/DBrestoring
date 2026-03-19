from __future__ import annotations

from pathlib import Path
from urllib.parse import quote_plus
from uuid import uuid4

from pymongo import MongoClient

from dbrestore.adapters.base import CommandSpec, ExternalToolAdapter
from dbrestore.config import ProfileModel
from dbrestore.errors import DatabaseConnectionError, PreflightError
from dbrestore.utils import Redactor


class MongoAdapter(ExternalToolAdapter):
    @property
    def db_type(self) -> str:
        return "mongo"

    def required_tools(self) -> list[str]:
        return ["mongodump", "mongorestore"]

    def restore_filter_kind(self) -> str | None:
        return "collection"

    def artifact_extension(self) -> str:
        return ".archive"

    def test_connection(self, profile: ProfileModel) -> None:
        try:
            client = MongoClient(self._build_connection_uri(profile), serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            client.close()
        except Exception as exc:
            raise DatabaseConnectionError(f"MongoDB connection failed: {exc}") from exc

    def validate_restore_target(self, profile: ProfileModel) -> None:
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

    def build_backup_command(self, profile: ProfileModel, destination: Path) -> CommandSpec:
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

    def normalize_restore_selection(self, profile: ProfileModel, selection: list[str]) -> list[str]:
        normalized: list[str] = []
        for item in selection:
            candidate = item.strip()
            if not candidate:
                continue
            if "." not in candidate:
                candidate = f"{profile.database}.{candidate}"
            normalized.append(candidate)
        return normalized

    def build_restore_command(
        self,
        profile: ProfileModel,
        source: Path,
        selection: list[str] | None = None,
    ) -> CommandSpec:
        args = [
            "mongorestore",
            "--drop",
            f"--archive={source}",
        ]
        ns_includes = selection or [f"{profile.database}.*"]
        for item in ns_includes:
            args.extend(["--nsInclude", item])
        args.extend(
            [
                "--uri",
                self._build_connection_uri(profile),
            ]
        )
        return CommandSpec(args=args)

    def backup(
        self, profile: ProfileModel, destination: Path, redactor: Redactor
    ) -> dict[str, str]:
        super().backup(profile, destination, redactor)
        return {"format": "mongo_archive"}

    def _build_connection_uri(self, profile: ProfileModel) -> str:
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
