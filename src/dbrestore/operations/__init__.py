"""Public operations API used by the CLI, GUI, and tests."""

import shutil

from .backup_restore import (
    run_backup,
    run_restore,
    run_scheduled_cycle,
    run_test_connection,
    run_test_connection_with_config,
    run_validate_config,
    validate_profile_config,
)
from .common import build_redactor
from .history import (
    get_latest_backup_run,
    list_backup_history,
    list_run_log_events,
)
from .retention import apply_retention_policy, summarize_retention_policy
from .status import collect_profile_status, run_profile_preflight
from .verification import configured_verification_target, run_verify_latest_backup

__all__ = [
    "apply_retention_policy",
    "build_redactor",
    "collect_profile_status",
    "configured_verification_target",
    "get_latest_backup_run",
    "list_backup_history",
    "list_run_log_events",
    "shutil",
    "run_backup",
    "run_profile_preflight",
    "run_restore",
    "run_scheduled_cycle",
    "run_test_connection",
    "run_test_connection_with_config",
    "run_validate_config",
    "run_verify_latest_backup",
    "summarize_retention_policy",
    "validate_profile_config",
]
