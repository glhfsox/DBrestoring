from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from dbrestore import chunking
from dbrestore.chunking import (
    CHUNK_STORE_DIRNAME,
    CHUNKS_MANIFEST_NAME,
    ChunkStore,
    chunk_file,
    reassemble_from_chunks,
)
from dbrestore.errors import ConfigError
from dbrestore.operations import run_backup, run_restore


@pytest.fixture()
def small_chunk_size(monkeypatch: pytest.MonkeyPatch) -> int:
    size = 4096
    monkeypatch.setattr(chunking, "CHUNK_SIZE", size)
    return size


def _write_config(config_path: Path, source: Path, restored: Path, backups_dir: Path) -> None:
    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: {backups_dir}
  log_dir: {backups_dir.parent / "logs"}
  retention:
    keep_last: 10
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


def _seed_db(path: Path, rows: list[tuple[int, str]]) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("DELETE FROM items")
        connection.executemany("INSERT INTO items (id, name) VALUES (?, ?)", rows)
        connection.commit()


def _read_db(path: Path) -> list[tuple[int, str]]:
    with sqlite3.connect(path) as connection:
        return list(connection.execute("SELECT id, name FROM items ORDER BY id"))


def test_chunk_store_roundtrip(tmp_path: Path) -> None:
    source = tmp_path / "payload.bin"
    source.write_bytes(b"hello world" * 10000)
    store = ChunkStore(tmp_path / "store")

    summary = chunk_file(source, store)
    assert summary.total_bytes == source.stat().st_size
    assert summary.hashes
    assert summary.new_chunks >= 1
    assert summary.reused_chunks == 0

    again = chunk_file(source, store)
    assert again.hashes == summary.hashes
    assert again.new_chunks == 0
    assert again.reused_chunks == len(summary.hashes)

    destination = tmp_path / "restored.bin"
    reassemble_from_chunks(summary.hashes, store, destination)
    assert destination.read_bytes() == source.read_bytes()


def test_chunk_store_deletes_unreferenced(tmp_path: Path) -> None:
    store = ChunkStore(tmp_path / "store")
    source_a = tmp_path / "a.bin"
    source_b = tmp_path / "b.bin"
    source_a.write_bytes(b"alpha" * 5000)
    source_b.write_bytes(b"bravo" * 5000)

    summary_a = chunk_file(source_a, store)
    summary_b = chunk_file(source_b, store)
    before = len(store.all_hashes())
    assert before == len(set(summary_a.hashes + summary_b.hashes))

    deleted = store.delete_unreferenced(set(summary_b.hashes))
    assert deleted == len(set(summary_a.hashes) - set(summary_b.hashes))
    remaining = set(store.all_hashes())
    assert remaining == set(summary_b.hashes)


def _baseline_rows(count: int) -> list[tuple[int, str]]:
    return [(i, f"row-{i:06d}-" + "x" * 40) for i in range(1, count + 1)]


def test_sqlite_differential_roundtrip(tmp_path: Path, small_chunk_size: int) -> None:
    source = tmp_path / "source.sqlite3"
    restored = tmp_path / "restored.sqlite3"
    backups_dir = tmp_path / "backups"
    config_path = tmp_path / "dbrestore.yaml"
    _write_config(config_path, source, restored, backups_dir)

    baseline = _baseline_rows(2000)
    _seed_db(source, baseline)
    first = run_backup(profile_name="source", config_path=config_path, mode="differential")
    assert first["backup_type"] == "full"
    assert first["compression"] == "chunked"
    assert first["parent_run_id"] is None
    assert first["chain"] == []

    first_manifest_path = Path(first["manifest_path"])
    first_run_dir = first_manifest_path.parent
    chunks_manifest_path = first_run_dir / CHUNKS_MANIFEST_NAME
    assert chunks_manifest_path.exists()
    first_chunks = json.loads(chunks_manifest_path.read_text())
    assert len(first_chunks["hashes"]) > 1

    extended = baseline + [(2001, "extra-row")]
    _seed_db(source, extended)
    second = run_backup(profile_name="source", config_path=config_path, mode="differential")
    assert second["backup_type"] == "differential"
    assert second["parent_run_id"] == first["run_id"]
    assert second["chain"] == [first["run_id"]]
    assert second["metadata"]["chunks"]["reused"] >= 1

    profile_chunk_root = backups_dir / "source" / CHUNK_STORE_DIRNAME
    assert profile_chunk_root.exists()

    run_restore(
        profile_name="target",
        config_path=config_path,
        input_path=Path(second["artifact_path"]),
    )
    assert _read_db(restored) == extended

    run_restore(
        profile_name="target",
        config_path=config_path,
        input_path=Path(first["artifact_path"]),
    )
    assert _read_db(restored) == baseline


def test_sqlite_incremental_chain_and_gc(tmp_path: Path, small_chunk_size: int) -> None:
    source = tmp_path / "source.sqlite3"
    restored = tmp_path / "restored.sqlite3"
    backups_dir = tmp_path / "backups"
    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: {backups_dir}
  log_dir: {backups_dir.parent / "logs"}
  retention:
    keep_last: 2
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

    _seed_db(source, [(1, "alpha")])
    baseline = run_backup(profile_name="source", config_path=config_path, mode="incremental")
    assert baseline["backup_type"] == "full"
    assert baseline["parent_run_id"] is None

    _seed_db(source, [(1, "alpha"), (2, "beta")])
    second = run_backup(profile_name="source", config_path=config_path, mode="incremental")
    assert second["backup_type"] == "incremental"
    assert second["parent_run_id"] == baseline["run_id"]
    assert second["chain"] == [baseline["run_id"]]

    _seed_db(source, [(1, "alpha"), (2, "beta"), (3, "gamma")])
    third = run_backup(profile_name="source", config_path=config_path, mode="incremental")
    assert third["backup_type"] == "incremental"
    assert third["parent_run_id"] == second["run_id"]
    assert third["chain"] == [baseline["run_id"], second["run_id"]]

    store_root = backups_dir / "source" / CHUNK_STORE_DIRNAME
    assert store_root.exists()
    surviving_run_dirs = [
        child
        for child in (backups_dir / "source").iterdir()
        if child.is_dir() and child.name != CHUNK_STORE_DIRNAME
    ]
    assert len(surviving_run_dirs) == 2

    baseline_run_dir = Path(baseline["manifest_path"]).parent
    assert not baseline_run_dir.exists()

    referenced: set[str] = set()
    for run_dir in surviving_run_dirs:
        chunks_path = run_dir / CHUNKS_MANIFEST_NAME
        if chunks_path.exists():
            referenced.update(json.loads(chunks_path.read_text())["hashes"])
    store = ChunkStore(store_root)
    on_disk = set(store.all_hashes())
    assert on_disk == referenced

    run_restore(
        profile_name="target",
        config_path=config_path,
        input_path=Path(third["artifact_path"]),
    )
    assert _read_db(restored) == [(1, "alpha"), (2, "beta"), (3, "gamma")]


def test_chunked_mode_rejects_s3_storage(tmp_path: Path) -> None:
    from dataclasses import dataclass

    from dbrestore.storage import StorageBackend

    @dataclass
    class DummyS3(StorageBackend):
        def prepare_backup_paths(self, *args, **kwargs):  # pragma: no cover - not reached
            raise NotImplementedError

        def finalize_backup(self, *args, **kwargs):  # pragma: no cover - not reached
            raise NotImplementedError

        def resolve_restore_input(self, *args, **kwargs):  # pragma: no cover - not reached
            raise NotImplementedError

        def list_backup_runs(self, *args, **kwargs):  # pragma: no cover - not reached
            return []

        def delete_backup_runs(self, *args, **kwargs):  # pragma: no cover - not reached
            return []

        def health_check(self, *args, **kwargs):  # pragma: no cover - not reached
            return {"status": "ok"}

    source = tmp_path / "source.sqlite3"
    restored = tmp_path / "restored.sqlite3"
    backups_dir = tmp_path / "backups"
    config_path = tmp_path / "dbrestore.yaml"
    _write_config(config_path, source, restored, backups_dir)
    _seed_db(source, [(1, "alpha")])

    with pytest.raises(ConfigError, match="local storage"):
        run_backup(
            profile_name="source",
            config_path=config_path,
            storage_backend=DummyS3(),
            mode="differential",
        )
