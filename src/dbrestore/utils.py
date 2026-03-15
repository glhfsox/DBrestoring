from __future__ import annotations

import gzip
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from typing import Any, Iterable

ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
URI_PASSWORD_PATTERNS = (
    re.compile(r"(mongodb(?:\+srv)?://[^:/@\s]+:)([^@/\s]+)(@)", re.IGNORECASE),
    re.compile(r"(postgres(?:ql)?://[^:/@\s]+:)([^@/\s]+)(@)", re.IGNORECASE),
    re.compile(r"(mysql://[^:/@\s]+:)([^@/\s]+)(@)", re.IGNORECASE),
)


def local_timezone() -> tzinfo:
    return datetime.now().astimezone().tzinfo or timezone.utc


def current_time() -> datetime:
    return datetime.now().astimezone().replace(microsecond=0)


def format_timestamp(value: datetime) -> str:
    localized = value.astimezone(local_timezone()).replace(microsecond=0)
    return localized.strftime("%H:%M:%S %Y-%m-%d")


def format_storage_timestamp(value: datetime) -> str:
    localized = value.astimezone(local_timezone()).replace(microsecond=0)
    return localized.strftime("%Y%m%dT%H%M%S")


def format_display_timestamp(value: datetime) -> str:
    return format_timestamp(value)


def parse_timestamp(value: str, *, assume_timezone: tzinfo | None = None) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = datetime.strptime(normalized, "%H:%M:%S %Y-%m-%d")

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=assume_timezone or local_timezone())
    return parsed


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def expand_user_path(path: Path, *, field_name: str = "path") -> Path:
    try:
        return path.expanduser()
    except RuntimeError as exc:
        raise ValueError(
            f"Invalid {field_name}: {path}. Use ~/dir or a regular relative/absolute path."
        ) from exc


def find_existing_parent(path: Path) -> Path | None:
    current = path
    while True:
        if current.exists():
            return current
        if current.parent == current:
            return None
        current = current.parent


def validate_writable_path(path: Path) -> None:
    path = expand_user_path(path)
    if path.exists():
        if not path.is_dir():
            raise ValueError(f"Path exists but is not a directory: {path}")
        if not os.access(path, os.W_OK):
            raise ValueError(f"Directory is not writable: {path}")
        return

    parent = find_existing_parent(path)
    if parent is None or not parent.is_dir():
        raise ValueError(f"No existing parent directory found for: {path}")
    if not os.access(parent, os.W_OK):
        raise ValueError(f"Parent directory is not writable: {parent}")


def gzip_compress(source: Path) -> Path:
    destination = source.with_name(f"{source.name}.gz")
    with source.open("rb") as input_handle, gzip.open(destination, "wb") as output_handle:
        shutil.copyfileobj(input_handle, output_handle)
    return destination


def gunzip_decompress(source: Path, destination: Path) -> Path:
    with gzip.open(source, "rb") as input_handle, destination.open("wb") as output_handle:
        shutil.copyfileobj(input_handle, output_handle)
    return destination


def expand_env_placeholders(value: Any, environ: dict[str, str] | None = None) -> tuple[Any, set[str]]:
    env = environ or dict(os.environ)
    missing: set[str] = set()

    def _expand(item: Any) -> Any:
        if isinstance(item, dict):
            return {key: _expand(inner) for key, inner in item.items()}
        if isinstance(item, list):
            return [_expand(inner) for inner in item]
        if isinstance(item, str):
            def replace(match: re.Match[str]) -> str:
                name = match.group(1)
                replacement = env.get(name)
                if replacement is None:
                    missing.add(name)
                    return match.group(0)
                return replacement

            return ENV_VAR_PATTERN.sub(replace, item)
        return item

    return _expand(value), missing


def collect_env_placeholders(value: Any) -> set[str]:
    found: set[str] = set()

    def _scan(item: Any) -> None:
        if isinstance(item, dict):
            for inner in item.values():
                _scan(inner)
            return
        if isinstance(item, list):
            for inner in item:
                _scan(inner)
            return
        if isinstance(item, str):
            found.update(match.group(1) for match in ENV_VAR_PATTERN.finditer(item))

    _scan(value)
    return found


def json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return format_timestamp(value)
    if isinstance(value, dict):
        return {key: json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [json_safe(inner) for inner in value]
    return value


@dataclass
class Redactor:
    secrets: set[str] = field(default_factory=set)

    def add(self, *values: str | None) -> None:
        for value in values:
            if value:
                self.secrets.add(value)

    def sanitize_text(self, value: Any) -> str:
        text = str(value)
        for pattern in URI_PASSWORD_PATTERNS:
            text = pattern.sub(r"\1***\3", text)
        text = re.sub(r"(--password(?:=|\s+))(\S+)", r"\1***", text)
        text = re.sub(r"(password=)([^&\s]+)", r"\1***", text, flags=re.IGNORECASE)
        for secret in sorted(self.secrets, key=len, reverse=True):
            text = text.replace(secret, "***")
        return text

    def sanitize_command(self, args: Iterable[str]) -> str:
        return self.sanitize_text(" ".join(str(arg) for arg in args))
