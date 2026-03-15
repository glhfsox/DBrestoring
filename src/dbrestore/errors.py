from __future__ import annotations


class DBRestoreError(Exception):
    """Base exception for dbrestore failures."""


class ConfigError(DBRestoreError):
    """Raised when configuration is invalid."""


class PreflightError(DBRestoreError):
    """Raised when a preflight validation fails."""


class CommandExecutionError(DBRestoreError):
    """Raised when a native database tool exits unsuccessfully."""


class ArtifactError(DBRestoreError):
    """Raised when a backup artifact cannot be resolved or read."""


class DatabaseConnectionError(DBRestoreError):
    """Raised when a database connection test fails."""


class SchedulingError(DBRestoreError):
    """Raised when schedule management or systemd integration fails."""
