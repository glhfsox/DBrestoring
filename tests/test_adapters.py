from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from dbrestore.adapters import mongo as mongo_module
from dbrestore.adapters import mysql as mysql_module
from dbrestore.adapters import postgres as postgres_module
from dbrestore.adapters.mongo import MongoAdapter
from dbrestore.adapters.mysql import MySQLAdapter
from dbrestore.adapters.postgres import PostgresAdapter
from dbrestore.adapters.sqlite import SQLiteAdapter
from dbrestore.config import ProfileModel
from dbrestore.errors import PreflightError
from dbrestore.utils import Redactor


def test_postgres_builds_expected_backup_command(tmp_path: Path) -> None:
    profile = ProfileModel(
        db_type="postgres",
        host="db.local",
        port=5432,
        username="app",
        password="secret",
        database="appdb",
    )
    adapter = PostgresAdapter()

    command = adapter.build_backup_command(profile, tmp_path / "backup.dump")

    assert command.args[:3] == ["pg_dump", "--format=custom", "--file"]
    assert command.env["PGPASSWORD"] == "secret"


def test_mysql_builds_expected_restore_command(tmp_path: Path) -> None:
    profile = ProfileModel(
        db_type="mysql",
        host="db.local",
        port=3306,
        username="app",
        password="secret",
        database="appdb",
    )
    adapter = MySQLAdapter()
    source = tmp_path / "backup.sql"

    command = adapter.build_restore_command(profile, source)

    assert command.args[0] == "mysql"
    assert command.stdin_path == source
    assert command.env["MYSQL_PWD"] == "secret"


def test_mongo_builds_archive_commands() -> None:
    profile = ProfileModel(
        db_type="mongo",
        host="mongo.local",
        port=27017,
        username="app",
        password="secret",
        database="appdb",
        auth_database="admin",
    )
    adapter = MongoAdapter()

    backup_command = adapter.build_backup_command(profile, Path("/tmp/backup.archive"))
    restore_command = adapter.build_restore_command(profile, Path("/tmp/backup.archive"))

    assert backup_command.args[0] == "mongodump"
    assert "--uri" in backup_command.args
    assert restore_command.args[0] == "mongorestore"
    assert "--nsInclude" in restore_command.args


def test_sqlite_backup_and_restore_round_trip(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    backup_path = tmp_path / "backup.sqlite"
    restored = tmp_path / "restored.sqlite3"

    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    adapter = SQLiteAdapter()
    backup_profile = ProfileModel(db_type="sqlite", database=str(source))
    restore_profile = ProfileModel(db_type="sqlite", database=str(restored))
    backup_profile.set_base_dir(tmp_path)
    restore_profile.set_base_dir(tmp_path)

    adapter.backup(backup_profile, backup_path, Redactor())
    adapter.restore(restore_profile, backup_path, Redactor())

    with sqlite3.connect(restored) as connection:
        row = connection.execute("SELECT name FROM items").fetchone()

    assert row == ("widget",)


def test_postgres_restore_precheck_reports_missing_public_schema_permissions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = ProfileModel(
        db_type="postgres",
        host="db.local",
        port=5432,
        username="app",
        password="secret",
        database="appdb",
    )
    adapter = PostgresAdapter()

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        def execute(self, query: str) -> None:
            raise RuntimeError("permission denied for schema public")

    class FakeConnection:
        def __enter__(self) -> "FakeConnection":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def rollback(self) -> None:
            return None

    monkeypatch.setattr(postgres_module.psycopg, "connect", lambda **_: FakeConnection())

    with pytest.raises(PreflightError, match="cannot create tables in schema public"):
        adapter.validate_restore_target(profile)


def test_mysql_restore_precheck_exercises_create_and_drop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = ProfileModel(
        db_type="mysql",
        host="db.local",
        port=3306,
        username="app",
        password="secret",
        database="appdb",
    )
    adapter = MySQLAdapter()
    executed: list[str] = []
    state = {"committed": False, "closed": False}

    class FakeCursor:
        def execute(self, query: str) -> None:
            executed.append(query)

        def close(self) -> None:
            return None

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            state["committed"] = True

        def close(self) -> None:
            state["closed"] = True

    monkeypatch.setattr(mysql_module.pymysql, "connect", lambda **_: FakeConnection())

    adapter.validate_restore_target(profile)

    assert len(executed) == 2
    assert executed[0].startswith("CREATE TABLE `__dbrestore_preflight_")
    assert executed[1].startswith("DROP TABLE `__dbrestore_preflight_")
    assert state == {"committed": True, "closed": True}


def test_mongo_restore_precheck_exercises_collection_write_and_drop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = ProfileModel(
        db_type="mongo",
        host="mongo.local",
        port=27017,
        username="app",
        password="secret",
        database="appdb",
        auth_database="admin",
    )
    adapter = MongoAdapter()
    observed: dict[str, object] = {}

    class FakeCollection:
        def insert_one(self, document: dict[str, str]) -> None:
            observed["inserted"] = document

    class FakeDatabase:
        def __getitem__(self, name: str) -> FakeCollection:
            observed["collection_name"] = name
            return FakeCollection()

        def drop_collection(self, name: str) -> None:
            observed["dropped"] = name

    class FakeClient:
        def __getitem__(self, name: str) -> FakeDatabase:
            observed["database_name"] = name
            return FakeDatabase()

        def close(self) -> None:
            observed["closed"] = True

    monkeypatch.setattr(mongo_module, "MongoClient", lambda *args, **kwargs: FakeClient())

    adapter.validate_restore_target(profile)

    assert observed["database_name"] == "appdb"
    assert str(observed["collection_name"]).startswith("__dbrestore_preflight_")
    assert observed["dropped"] == observed["collection_name"]
    assert observed["inserted"] == {"created_by": "dbrestore"}
    assert observed["closed"] is True


def test_sqlite_restore_precheck_validates_writable_target(tmp_path: Path) -> None:
    target = tmp_path / "restored.sqlite3"
    profile = ProfileModel(db_type="sqlite", database=str(target))
    profile.set_base_dir(tmp_path)
    adapter = SQLiteAdapter()

    adapter.validate_restore_target(profile)

    assert target.exists()
    with sqlite3.connect(target) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '__dbrestore_preflight_%'"
        ).fetchall()
    assert tables == []
