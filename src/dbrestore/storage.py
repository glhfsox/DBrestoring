"""This module abstracts where backup artifacts live after they are created.
Operations prepare a run, then storage backends handle local folders, S3 uploads, downloads for restore, and retention cleanup.
That separation lets backup logic stay the same even when the destination changes.
If artifacts are missing, misnamed, or stored remotely, this file is usually the place to inspect."""

from __future__ import annotations

import json
import shutil
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from dbrestore.errors import ArtifactError, ConfigError
from dbrestore.models import BackupManifest
from dbrestore.utils import (
    ensure_directory,
    expand_user_path,
    find_existing_parent,
    format_storage_timestamp,
    parse_timestamp,
    validate_writable_path,
)


@dataclass(frozen=True)
class PreparedBackupPaths:
    run_id: str
    run_dir: Path
    artifact_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class ResolvedRestoreInput:
    artifact_path: Path
    manifest: dict[str, Any] | None
    cleanup_dir: Path | None = None


@dataclass(frozen=True)
class BackupRunRecord:
    profile: str
    run_dir: str
    manifest_path: str
    artifact_path: str | None
    manifest: dict[str, Any]
    finished_at: datetime


class StorageBackend(ABC):
    @abstractmethod
    def prepare_backup_paths(
        self,
        profile_name: str,
        output_dir: Path,
        started_at: datetime,
        extension: str,
    ) -> PreparedBackupPaths:
        raise NotImplementedError

    @abstractmethod
    def finalize_backup(
        self,
        profile_name: str,
        prepared: PreparedBackupPaths,
        manifest: BackupManifest,
        artifact_path: Path,
    ) -> BackupRunRecord:
        raise NotImplementedError

    @abstractmethod
    def resolve_restore_input(self, input_path: Path | str) -> ResolvedRestoreInput:
        raise NotImplementedError

    @abstractmethod
    def list_backup_runs(self, profile_name: str, output_dir: Path) -> list[BackupRunRecord]:
        raise NotImplementedError

    @abstractmethod
    def delete_backup_runs(self, runs: list[BackupRunRecord]) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def health_check(self, profile_name: str, output_dir: Path) -> dict[str, Any]:
        raise NotImplementedError

    def latest_backup_run(self, profile_name: str, output_dir: Path) -> BackupRunRecord:
        runs = self.list_backup_runs(profile_name, output_dir)
        if not runs:
            raise ArtifactError(f"No backup runs found for profile '{profile_name}'")
        return runs[0]


class LocalStorageBackend(StorageBackend):
    def prepare_backup_paths(
        self,
        profile_name: str,
        output_dir: Path,
        started_at: datetime,
        extension: str,
    ) -> PreparedBackupPaths:
        timestamp = format_storage_timestamp(started_at)
        run_id = uuid4().hex[:12]
        run_dir = ensure_directory(output_dir / profile_name / f"{timestamp}_{run_id}")
        artifact_path = run_dir / f"{profile_name}_{timestamp}{extension}"
        manifest_path = run_dir / "manifest.json"
        return PreparedBackupPaths(
            run_id=run_id,
            run_dir=run_dir,
            artifact_path=artifact_path,
            manifest_path=manifest_path,
        )

    def finalize_backup(
        self,
        profile_name: str,
        prepared: PreparedBackupPaths,
        manifest: BackupManifest,
        artifact_path: Path,
    ) -> BackupRunRecord:
        finalized = replace(manifest, artifact_path=str(artifact_path))
        ensure_directory(prepared.manifest_path.parent)
        prepared.manifest_path.write_text(
            json.dumps(finalized.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return BackupRunRecord(
            profile=profile_name,
            run_dir=str(prepared.run_dir),
            manifest_path=str(prepared.manifest_path),
            artifact_path=str(artifact_path),
            manifest=finalized.to_dict(),
            finished_at=_finished_at_from_manifest(finalized.to_dict()),
        )

    def resolve_restore_input(self, input_path: Path | str) -> ResolvedRestoreInput:
        candidate = _resolve_local_input_path(input_path)
        if not candidate.exists():
            raise ArtifactError(f"Backup input not found: {candidate}")

        if candidate.is_dir():
            manifest_path = candidate / "manifest.json"
            if not manifest_path.exists():
                raise ArtifactError(f"manifest.json not found in backup directory: {candidate}")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            artifact_path = self._resolve_artifact_path(candidate, manifest)
            if artifact_path is None:
                raise ArtifactError(f"manifest.json is missing artifact_path: {candidate}")
            return ResolvedRestoreInput(artifact_path=artifact_path, manifest=manifest)

        manifest_path = candidate.parent / "manifest.json"
        manifest = None
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return ResolvedRestoreInput(artifact_path=candidate, manifest=manifest)

    def list_backup_runs(self, profile_name: str, output_dir: Path) -> list[BackupRunRecord]:
        profile_dir = output_dir / profile_name
        if not profile_dir.exists():
            return []

        runs: list[BackupRunRecord] = []
        for child in profile_dir.iterdir():
            if not child.is_dir():
                continue
            manifest_path = child / "manifest.json"
            if not manifest_path.exists():
                continue
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                finished_at = _finished_at_from_manifest(manifest)
                artifact_path = self._resolve_artifact_path(child, manifest, require_exists=False)
            except (json.JSONDecodeError, KeyError, ValueError, ArtifactError):
                continue
            runs.append(
                BackupRunRecord(
                    profile=profile_name,
                    run_dir=str(child),
                    manifest_path=str(manifest_path),
                    artifact_path=str(artifact_path) if artifact_path is not None else None,
                    manifest=manifest,
                    finished_at=finished_at,
                )
            )

        runs.sort(key=lambda item: item.finished_at, reverse=True)
        return runs

    def delete_backup_runs(self, runs: list[BackupRunRecord]) -> list[str]:
        deleted_runs: list[str] = []
        for run in runs:
            shutil.rmtree(Path(run.run_dir))
            deleted_runs.append(run.run_dir)
        return deleted_runs

    def health_check(self, profile_name: str, output_dir: Path) -> dict[str, Any]:
        target_dir = output_dir / profile_name
        validate_writable_path(output_dir)
        ensure_directory(target_dir)
        disk_root = target_dir if target_dir.exists() else find_existing_parent(target_dir)
        if disk_root is None:
            raise ArtifactError(f"No existing parent directory found for: {target_dir}")
        usage = shutil.disk_usage(disk_root)
        return {
            "status": "ok",
            "kind": "local",
            "output_dir": str(output_dir),
            "target": str(target_dir),
            "free_bytes": usage.free,
            "used_bytes": usage.used,
            "total_bytes": usage.total,
            "message": f"Local storage is writable at {target_dir}",
        }

    def _resolve_artifact_path(
        self,
        run_dir: Path,
        manifest: dict[str, Any],
        *,
        require_exists: bool = True,
    ) -> Path | None:
        artifact_value = manifest.get("artifact_path")
        if not artifact_value:
            return None
        try:
            artifact_path = expand_user_path(Path(artifact_value), field_name="artifact_path")
        except ValueError as exc:
            raise ArtifactError(str(exc)) from exc
        if not artifact_path.is_absolute():
            artifact_path = (run_dir / artifact_path).resolve()
        if require_exists and not artifact_path.exists():
            raise ArtifactError(f"Artifact referenced by manifest does not exist: {artifact_path}")
        return artifact_path


class S3StorageBackend(StorageBackend):
    def __init__(self, storage_config: Any) -> None:
        self.bucket = storage_config.bucket or ""
        self.prefix = storage_config.prefix
        self.client = _build_s3_client(storage_config)
        self.local_backend = LocalStorageBackend()

    def prepare_backup_paths(
        self,
        profile_name: str,
        output_dir: Path,
        started_at: datetime,
        extension: str,
    ) -> PreparedBackupPaths:
        return self.local_backend.prepare_backup_paths(
            profile_name, output_dir, started_at, extension
        )

    def finalize_backup(
        self,
        profile_name: str,
        prepared: PreparedBackupPaths,
        manifest: BackupManifest,
        artifact_path: Path,
    ) -> BackupRunRecord:
        run_name = prepared.run_dir.name
        artifact_key = _join_s3_key(self.prefix, profile_name, run_name, artifact_path.name)
        manifest_key = _join_s3_key(self.prefix, profile_name, run_name, "manifest.json")
        remote_artifact_uri = _build_s3_uri(self.bucket, artifact_key)
        finalized = replace(manifest, artifact_path=remote_artifact_uri)

        ensure_directory(prepared.manifest_path.parent)
        prepared.manifest_path.write_text(
            json.dumps(finalized.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        self.client.upload_file(str(artifact_path), self.bucket, artifact_key)
        self.client.upload_file(str(prepared.manifest_path), self.bucket, manifest_key)
        shutil.rmtree(prepared.run_dir, ignore_errors=True)

        return BackupRunRecord(
            profile=profile_name,
            run_dir=_build_s3_uri(self.bucket, _join_s3_key(self.prefix, profile_name, run_name)),
            manifest_path=_build_s3_uri(self.bucket, manifest_key),
            artifact_path=remote_artifact_uri,
            manifest=finalized.to_dict(),
            finished_at=_finished_at_from_manifest(finalized.to_dict()),
        )

    def resolve_restore_input(self, input_path: Path | str) -> ResolvedRestoreInput:
        raw_input = _normalize_storage_input(input_path)
        if not _is_s3_uri(raw_input):
            return self.local_backend.resolve_restore_input(input_path)

        manifest: dict[str, Any] | None = None
        artifact_uri = raw_input
        if raw_input.endswith("/manifest.json"):
            manifest = self._read_manifest(raw_input)
            artifact_uri = _extract_artifact_uri(raw_input, manifest)
        elif _looks_like_run_uri(raw_input):
            manifest_uri = f"{raw_input.rstrip('/')}/manifest.json"
            manifest = self._read_manifest(manifest_uri)
            artifact_uri = _extract_artifact_uri(manifest_uri, manifest)
        else:
            manifest_uri = f"{raw_input.rsplit('/', 1)[0]}/manifest.json"
            try:
                manifest = self._read_manifest(manifest_uri)
            except ArtifactError:
                manifest = None

        bucket, key = _parse_s3_uri(artifact_uri)
        temp_dir = Path(tempfile.mkdtemp(prefix="dbrestore-s3-"))
        local_artifact = temp_dir / Path(key).name
        self.client.download_file(bucket, key, str(local_artifact))
        return ResolvedRestoreInput(
            artifact_path=local_artifact,
            manifest=manifest,
            cleanup_dir=temp_dir,
        )

    def list_backup_runs(self, profile_name: str, output_dir: Path) -> list[BackupRunRecord]:
        del output_dir
        manifests: list[BackupRunRecord] = []
        prefix = _join_s3_key(self.prefix, profile_name)
        for key in self._list_keys(prefix):
            if not key.endswith("/manifest.json"):
                continue
            try:
                manifest_uri = _build_s3_uri(self.bucket, key)
                manifest = self._read_manifest(manifest_uri)
                run_key = key[: -len("/manifest.json")]
                manifests.append(
                    BackupRunRecord(
                        profile=profile_name,
                        run_dir=_build_s3_uri(self.bucket, run_key),
                        manifest_path=manifest_uri,
                        artifact_path=manifest.get("artifact_path"),
                        manifest=manifest,
                        finished_at=_finished_at_from_manifest(manifest),
                    )
                )
            except (ArtifactError, ValueError, KeyError):
                continue

        manifests.sort(key=lambda item: item.finished_at, reverse=True)
        return manifests

    def delete_backup_runs(self, runs: list[BackupRunRecord]) -> list[str]:
        deleted_runs: list[str] = []
        for run in runs:
            if run.artifact_path:
                bucket, key = _parse_s3_uri(run.artifact_path)
                self.client.delete_object(Bucket=bucket, Key=key)
            bucket, key = _parse_s3_uri(run.manifest_path)
            self.client.delete_object(Bucket=bucket, Key=key)
            deleted_runs.append(run.run_dir)
        return deleted_runs

    def health_check(self, profile_name: str, output_dir: Path) -> dict[str, Any]:
        staging = self.local_backend.health_check(profile_name, output_dir)
        prefix = _join_s3_key(self.prefix, profile_name)
        try:
            keys = self._list_keys(prefix)
        except Exception as exc:
            raise ArtifactError(
                f"Unable to reach S3 storage s3://{self.bucket}/{prefix.rstrip('/')}"
            ) from exc
        return {
            "status": "ok",
            "kind": "s3",
            "bucket": self.bucket,
            "prefix": prefix,
            "target": _build_s3_uri(self.bucket, prefix),
            "reachable_keys": len(keys),
            "staging": staging,
            "message": f"S3 storage is reachable at s3://{self.bucket}/{prefix}",
        }

    def _read_manifest(self, manifest_uri: str) -> dict[str, Any]:
        bucket, key = _parse_s3_uri(manifest_uri)
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
        except Exception as exc:
            raise ArtifactError(f"Unable to read S3 manifest: {manifest_uri}") from exc
        payload = response["Body"].read()
        try:
            return json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ArtifactError(f"Invalid JSON in S3 manifest: {manifest_uri}") from exc

    def _list_keys(self, prefix: str) -> list[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f"{prefix.rstrip('/')}/"):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if key:
                    keys.append(key)
        return keys


def get_storage_backend(config: object) -> StorageBackend:
    storage = getattr(config, "storage", None)
    storage_type = getattr(storage, "type", "local")
    if storage_type == "local":
        return LocalStorageBackend()
    if storage_type == "s3":
        return S3StorageBackend(storage)
    raise ConfigError(f"Unsupported storage backend: {storage_type}")


def _build_s3_client(storage_config: Any) -> Any:
    try:
        from boto3.session import Session
    except ImportError as exc:
        raise ConfigError("S3 storage requires boto3 to be installed") from exc

    session = Session(
        aws_access_key_id=storage_config.access_key_id,
        aws_secret_access_key=storage_config.secret_access_key_value,
        aws_session_token=storage_config.session_token_value,
        region_name=storage_config.region,
    )
    return session.client("s3", endpoint_url=storage_config.endpoint_url)


def _resolve_local_input_path(input_path: Path | str) -> Path:
    raw = str(input_path)
    try:
        return expand_user_path(Path(raw), field_name="input path").resolve()
    except ValueError as exc:
        raise ArtifactError(str(exc)) from exc


def _normalize_storage_input(input_path: Path | str) -> str:
    value = str(input_path).strip()
    if value.startswith("s3:/") and not value.startswith("s3://"):
        return value.replace("s3:/", "s3://", 1)
    return value


def _is_s3_uri(value: str) -> bool:
    return value.startswith("s3://")


def _looks_like_run_uri(value: str) -> bool:
    last_segment = value.rstrip("/").rsplit("/", 1)[-1]
    return "." not in last_segment


def _extract_artifact_uri(manifest_uri: str, manifest: dict[str, Any]) -> str:
    artifact_uri = manifest.get("artifact_path")
    if not artifact_uri:
        raise ArtifactError(f"manifest.json is missing artifact_path: {manifest_uri}")
    return str(artifact_uri)


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    normalized = _normalize_storage_input(uri)
    parsed = urlparse(normalized)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ArtifactError(f"Invalid S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _build_s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key.lstrip('/')}"


def _join_s3_key(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part and part.strip("/"))


def _finished_at_from_manifest(manifest: dict[str, Any]) -> datetime:
    return parse_timestamp(str(manifest["finished_at"])).astimezone(UTC)
