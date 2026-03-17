"""This file keeps the app's shared exception types in one place.
Higher layers raise these errors instead of leaking random low-level exceptions directly to CLI or GUI users.
That makes failures easier to classify, display, and test across the whole project.
If you want clearer error handling, this hierarchy is the first thing to extend."""

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
