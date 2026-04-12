"""Content-addressed chunk store used by incremental and differential backups.

Full artifacts are split into fixed-size blocks, hashed with sha256, and written
into a per-profile store keyed by the hash. Each run writes only a small
`chunks.json` manifest listing the hashes it needs; the actual bytes live once
in the shared store and are reused by every run that sees the same block.

Restore reverses the process: walk the hash list, look each block up in the
store, and stream them back into a temp artifact that the adapter can consume.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dbrestore.errors import ArtifactError

CHUNK_SIZE = 1 << 20  # 1 MiB
HASH_ALGO = "sha256"
CHUNK_STORE_DIRNAME = ".chunks"
CHUNKS_MANIFEST_NAME = "chunks.json"
CHUNKS_MANIFEST_VERSION = 1


@dataclass(frozen=True)
class ChunkSummary:
    hashes: list[str]
    new_chunks: int
    reused_chunks: int
    total_bytes: int


class ChunkStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def chunk_path(self, chunk_hash: str) -> Path:
        if len(chunk_hash) < 4:
            raise ArtifactError(f"Invalid chunk hash: {chunk_hash!r}")
        return self.root / chunk_hash[:2] / chunk_hash[2:]

    def has(self, chunk_hash: str) -> bool:
        return self.chunk_path(chunk_hash).exists()

    def put(self, chunk_hash: str, data: bytes) -> bool:
        path = self.chunk_path(chunk_hash)
        if path.exists():
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)
        return True

    def read(self, chunk_hash: str) -> bytes:
        path = self.chunk_path(chunk_hash)
        if not path.exists():
            raise ArtifactError(f"Chunk missing from store: {chunk_hash}")
        return path.read_bytes()

    def all_hashes(self) -> list[str]:
        if not self.root.exists():
            return []
        hashes: list[str] = []
        for prefix_dir in self.root.iterdir():
            if not prefix_dir.is_dir() or len(prefix_dir.name) != 2:
                continue
            for entry in prefix_dir.iterdir():
                if entry.is_file() and not entry.name.startswith("."):
                    hashes.append(prefix_dir.name + entry.name)
        return hashes

    def delete_unreferenced(self, referenced: set[str]) -> int:
        if not self.root.exists():
            return 0
        deleted = 0
        for chunk_hash in self.all_hashes():
            if chunk_hash in referenced:
                continue
            try:
                self.chunk_path(chunk_hash).unlink()
                deleted += 1
            except FileNotFoundError:
                pass
        for prefix_dir in list(self.root.iterdir()):
            if prefix_dir.is_dir() and not any(prefix_dir.iterdir()):
                prefix_dir.rmdir()
        return deleted


def chunk_file(source: Path, store: ChunkStore) -> ChunkSummary:
    hashes: list[str] = []
    new_count = 0
    reused_count = 0
    total = 0
    with source.open("rb") as handle:
        while True:
            block = handle.read(CHUNK_SIZE)
            if not block:
                break
            total += len(block)
            digest = hashlib.sha256(block).hexdigest()
            hashes.append(digest)
            if store.put(digest, block):
                new_count += 1
            else:
                reused_count += 1
    return ChunkSummary(
        hashes=hashes,
        new_chunks=new_count,
        reused_chunks=reused_count,
        total_bytes=total,
    )


def reassemble_from_chunks(hashes: Iterable[str], store: ChunkStore, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as output:
        for digest in hashes:
            path = store.chunk_path(digest)
            if not path.exists():
                raise ArtifactError(
                    f"Cannot reassemble artifact, chunk missing from store: {digest}"
                )
            with path.open("rb") as chunk_handle:
                while True:
                    buffer = chunk_handle.read(CHUNK_SIZE)
                    if not buffer:
                        break
                    output.write(buffer)
    return destination


def write_chunks_manifest(path: Path, summary: ChunkSummary) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": CHUNKS_MANIFEST_VERSION,
        "algorithm": HASH_ALGO,
        "chunk_size": CHUNK_SIZE,
        "total_bytes": summary.total_bytes,
        "hashes": list(summary.hashes),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_chunks_manifest(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactError(f"Unable to read chunks manifest: {path}") from exc
    if not isinstance(data, dict) or "hashes" not in data:
        raise ArtifactError(f"Invalid chunks manifest: {path}")
    return data


def collect_referenced_hashes(profile_dir: Path) -> set[str]:
    referenced: set[str] = set()
    if not profile_dir.exists():
        return referenced
    for run_dir in profile_dir.iterdir():
        if not run_dir.is_dir() or run_dir.name == CHUNK_STORE_DIRNAME:
            continue
        chunks_manifest = run_dir / CHUNKS_MANIFEST_NAME
        if not chunks_manifest.exists():
            continue
        try:
            data = read_chunks_manifest(chunks_manifest)
        except ArtifactError:
            continue
        referenced.update(str(h) for h in data.get("hashes", []))
    return referenced


def profile_chunk_store(profile_dir: Path) -> ChunkStore:
    return ChunkStore(profile_dir / CHUNK_STORE_DIRNAME)
