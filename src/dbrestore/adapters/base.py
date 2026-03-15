from __future__ import annotations

import os
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbrestore.errors import CommandExecutionError
from dbrestore.utils import Redactor


@dataclass(frozen=True)
class CommandSpec:
    args: list[str]
    env: dict[str, str] = field(default_factory=dict)
    stdin_path: Path | None = None


def run_command(spec: CommandSpec, redactor: Redactor) -> None:
    environment = os.environ.copy()
    environment.update(spec.env)
    stdin_handle = None
    try:
        if spec.stdin_path is not None:
            stdin_handle = spec.stdin_path.open("rb")
        result = subprocess.run(
            spec.args,
            check=False,
            env=environment,
            stdin=stdin_handle,
            capture_output=True,
            text=True,
        )
    finally:
        if stdin_handle is not None:
            stdin_handle.close()

    if result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        sanitized_details = redactor.sanitize_text(details)
        sanitized_command = redactor.sanitize_command(spec.args)
        raise CommandExecutionError(f"Command failed: {sanitized_command}\n{sanitized_details}")


class DatabaseAdapter(ABC):
    @property
    @abstractmethod
    def db_type(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def required_tools(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def artifact_extension(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def test_connection(self, profile: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    def backup(self, profile: Any, destination: Path, redactor: Redactor) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def restore(self, profile: Any, source: Path, redactor: Redactor) -> None:
        raise NotImplementedError

    def validate_restore_target(self, profile: Any) -> None:
        return None


class ExternalToolAdapter(DatabaseAdapter, ABC):
    @abstractmethod
    def build_backup_command(self, profile: Any, destination: Path) -> CommandSpec:
        raise NotImplementedError

    @abstractmethod
    def build_restore_command(self, profile: Any, source: Path) -> CommandSpec:
        raise NotImplementedError

    def backup(self, profile: Any, destination: Path, redactor: Redactor) -> dict[str, Any]:
        run_command(self.build_backup_command(profile, destination), redactor)
        return {}

    def restore(self, profile: Any, source: Path, redactor: Redactor) -> None:
        run_command(self.build_restore_command(profile, source), redactor)
