from __future__ import annotations

import io
import sqlite3
from pathlib import Path

from dbrestore.config import load_config
from dbrestore.operations import run_backup, run_restore
from dbrestore.storage import LocalStorageBackend, S3StorageBackend, get_storage_backend


def test_get_storage_backend_returns_local_backend_for_explicit_local_storage(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
storage:
  type: local
profiles:
  sqlite_local:
    db_type: sqlite
    database: ./data/app.sqlite3
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path, require_env=False)
    backend = get_storage_backend(config)

    assert isinstance(backend, LocalStorageBackend)
    assert config.storage.type == "local"


def test_get_storage_backend_returns_s3_backend_for_s3_storage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        """
version: 1
storage:
  type: s3
  bucket: my-backups
  prefix: nightly
profiles:
  sqlite_local:
    db_type: sqlite
    database: ./data/app.sqlite3
""".strip(),
        encoding="utf-8",
    )

    fake_client = FakeS3Client()
    monkeypatch.setattr("dbrestore.storage._build_s3_client", lambda storage_config: fake_client)

    config = load_config(config_path, require_env=False)
    backend = get_storage_backend(config)

    assert isinstance(backend, S3StorageBackend)
    assert config.storage.type == "s3"


def test_local_storage_backend_lists_runs_and_resolves_run_directory_input(tmp_path: Path) -> None:
    source = tmp_path / "source.sqlite3"
    config_path = tmp_path / "dbrestore.yaml"

    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
storage:
  type: local
profiles:
  sqlite_local:
    db_type: sqlite
    database: {source}
""".strip(),
        encoding="utf-8",
    )

    result = run_backup(profile_name="sqlite_local", config_path=config_path)
    config = load_config(config_path, require_env=False)
    profile = config.get_profile("sqlite_local")
    backend = get_storage_backend(config)
    output_dir = config.output_dir_for(profile)

    runs = backend.list_backup_runs("sqlite_local", output_dir)

    assert len(runs) == 1
    assert runs[0].manifest["run_id"] == result["run_id"]
    assert runs[0].artifact_path is not None
    assert Path(runs[0].artifact_path).exists()

    latest = backend.latest_backup_run("sqlite_local", output_dir)
    resolved = backend.resolve_restore_input(latest.run_dir)

    assert latest.run_dir == runs[0].run_dir
    assert str(resolved.artifact_path) == runs[0].artifact_path
    assert resolved.manifest is not None
    assert resolved.manifest["run_id"] == result["run_id"]


def test_s3_storage_backend_uploads_lists_and_restores_runs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "source.sqlite3"
    restored = tmp_path / "restored.sqlite3"
    config_path = tmp_path / "dbrestore.yaml"

    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO items (name) VALUES ('widget')")
        connection.commit()

    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: ./staging
  log_dir: ./logs
storage:
  type: s3
  bucket: my-backups
  prefix: nightly
profiles:
  source:
    db_type: sqlite
    database: {source}
  target:
    db_type: sqlite
    database: {restored}
""".strip(),
        encoding="utf-8",
    )

    fake_client = FakeS3Client()
    monkeypatch.setattr("dbrestore.storage._build_s3_client", lambda storage_config: fake_client)

    result = run_backup(profile_name="source", config_path=config_path)

    assert result["artifact_path"].startswith("s3://my-backups/nightly/source/")
    assert any(key.endswith("manifest.json") for bucket, key in fake_client.objects)

    config = load_config(config_path, require_env=False)
    profile = config.get_profile("source")
    backend = get_storage_backend(config)
    output_dir = config.output_dir_for(profile)
    runs = backend.list_backup_runs("source", output_dir)

    assert len(runs) == 1
    assert runs[0].run_dir.startswith("s3://my-backups/nightly/source/")
    assert runs[0].artifact_path is not None
    assert runs[0].artifact_path.startswith("s3://my-backups/nightly/source/")

    run_restore(profile_name="target", config_path=config_path, input_path=Path(runs[0].run_dir))

    with sqlite3.connect(restored) as connection:
        row = connection.execute("SELECT name FROM items").fetchone()

    assert row == ("widget",)


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.objects[(bucket, key)] = Path(filename).read_bytes()

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        Path(filename).write_bytes(self.objects[(bucket, key)])

    def get_object(self, Bucket: str, Key: str) -> dict[str, io.BytesIO]:
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def delete_object(self, Bucket: str, Key: str) -> None:
        self.objects.pop((Bucket, Key), None)

    def get_paginator(self, name: str) -> FakePaginator:
        assert name == "list_objects_v2"
        return FakePaginator(self.objects)


class FakePaginator:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects

    def paginate(self, Bucket: str, Prefix: str) -> list[dict[str, object]]:
        contents = [
            {"Key": key}
            for bucket, key in sorted(self.objects.keys())
            if bucket == Bucket and key.startswith(Prefix)
        ]
        return [{"Contents": contents}]
