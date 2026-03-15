from __future__ import annotations

import grp
import os
import pwd
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dbrestore.config import DEFAULT_CONFIG_PATH, collect_profile_env_vars, load_config
from dbrestore.errors import ConfigError, SchedulingError
from dbrestore.utils import ensure_directory, expand_user_path

DEFAULT_SYSTEMD_UNIT_DIR = Path("/etc/systemd/system")
DEFAULT_ENV_DIR = Path("/etc/dbrestore/env")


@dataclass(frozen=True)
class SchedulePaths:
    profile_name: str
    unit_basename: str
    service_name: str
    timer_name: str
    service_path: Path
    timer_path: Path
    env_file_path: Path | None
    env_vars: list[str]


def install_schedule(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    unit_dir: Path = DEFAULT_SYSTEMD_UNIT_DIR,
    env_dir: Path = DEFAULT_ENV_DIR,
    enable_timer: bool = True,
    force: bool = False,
    run_as_user: str | None = None,
    run_as_group: str | None = None,
) -> dict[str, Any]:
    config = load_config(config_path, require_env=False)
    profile = config.get_profile(profile_name)
    if profile.schedule is None:
        raise SchedulingError(f"Profile '{profile_name}' does not have a schedule configured")

    resolved_config_path = config.source_path or config_path.expanduser().resolve()
    resolved_unit_dir = _resolve_path(unit_dir, field_name="unit_dir")
    resolved_env_dir = _resolve_path(env_dir, field_name="env_dir")
    env_vars = collect_profile_env_vars(resolved_config_path, profile_name)
    schedule_paths = _build_schedule_paths(profile_name, resolved_unit_dir, resolved_env_dir, env_vars)
    user_name, group_name = _resolve_run_identity(run_as_user, run_as_group)

    if not force:
        existing = [
            path
            for path in (schedule_paths.service_path, schedule_paths.timer_path)
            if path.exists()
        ]
        if existing:
            joined = ", ".join(str(path) for path in existing)
            raise SchedulingError(
                f"Schedule unit file(s) already exist for profile '{profile_name}': {joined}. Use --force to overwrite."
            )

    ensure_directory(schedule_paths.service_path.parent)
    service_contents = render_service_unit(
        profile_name=profile_name,
        service_name=schedule_paths.service_name,
        config_path=resolved_config_path,
        run_as_user=user_name,
        run_as_group=group_name,
        env_file_path=schedule_paths.env_file_path if schedule_paths.env_vars else None,
    )
    timer_contents = render_timer_unit(
        profile_name=profile_name,
        timer_name=schedule_paths.timer_name,
        service_name=schedule_paths.service_name,
        on_calendar=profile.schedule.on_calendar,
        persistent=profile.schedule.persistent,
    )
    schedule_paths.service_path.write_text(service_contents, encoding="utf-8")
    schedule_paths.timer_path.write_text(timer_contents, encoding="utf-8")

    env_template_created = False
    if schedule_paths.env_vars and schedule_paths.env_file_path is not None:
        ensure_directory(schedule_paths.env_file_path.parent)
        if not schedule_paths.env_file_path.exists():
            schedule_paths.env_file_path.write_text(
                render_env_template(profile_name, schedule_paths.env_vars),
                encoding="utf-8",
            )
            os.chmod(schedule_paths.env_file_path, 0o600)
            env_template_created = True

    _run_systemctl(["daemon-reload"])
    if enable_timer:
        _run_systemctl(["enable", "--now", schedule_paths.timer_name])

    return {
        "profile": profile_name,
        "service_name": schedule_paths.service_name,
        "timer_name": schedule_paths.timer_name,
        "service_path": str(schedule_paths.service_path),
        "timer_path": str(schedule_paths.timer_path),
        "env_file_path": str(schedule_paths.env_file_path) if schedule_paths.env_vars else None,
        "env_vars": schedule_paths.env_vars,
        "env_template_created": env_template_created,
        "enabled": enable_timer,
        "run_as_user": user_name,
        "run_as_group": group_name,
        "on_calendar": profile.schedule.on_calendar,
        "persistent": profile.schedule.persistent,
    }


def remove_schedule(
    profile_name: str,
    unit_dir: Path = DEFAULT_SYSTEMD_UNIT_DIR,
    env_dir: Path = DEFAULT_ENV_DIR,
    delete_env_file: bool = False,
) -> dict[str, Any]:
    resolved_unit_dir = _resolve_path(unit_dir, field_name="unit_dir")
    resolved_env_dir = _resolve_path(env_dir, field_name="env_dir")
    schedule_paths = _build_schedule_paths(profile_name, resolved_unit_dir, resolved_env_dir, [])

    _run_systemctl(["disable", "--now", schedule_paths.timer_name], check=False)
    _run_systemctl(["reset-failed", schedule_paths.timer_name, schedule_paths.service_name], check=False)

    removed_files: list[str] = []
    for path in (schedule_paths.timer_path, schedule_paths.service_path):
        if path.exists():
            path.unlink()
            removed_files.append(str(path))

    if delete_env_file and schedule_paths.env_file_path is not None and schedule_paths.env_file_path.exists():
        schedule_paths.env_file_path.unlink()
        removed_files.append(str(schedule_paths.env_file_path))

    _run_systemctl(["daemon-reload"], check=False)

    return {
        "profile": profile_name,
        "service_name": schedule_paths.service_name,
        "timer_name": schedule_paths.timer_name,
        "removed_files": removed_files,
        "deleted_env_file": delete_env_file,
    }


def schedule_status(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    unit_dir: Path = DEFAULT_SYSTEMD_UNIT_DIR,
    env_dir: Path = DEFAULT_ENV_DIR,
) -> dict[str, Any]:
    config = load_config(config_path, require_env=False)
    profile = config.get_profile(profile_name)
    if profile.schedule is None:
        raise SchedulingError(f"Profile '{profile_name}' does not have a schedule configured")

    resolved_config_path = config.source_path or config_path.expanduser().resolve()
    resolved_unit_dir = _resolve_path(unit_dir, field_name="unit_dir")
    resolved_env_dir = _resolve_path(env_dir, field_name="env_dir")
    env_vars = collect_profile_env_vars(resolved_config_path, profile_name)
    schedule_paths = _build_schedule_paths(profile_name, resolved_unit_dir, resolved_env_dir, env_vars)

    service_exists = schedule_paths.service_path.exists()
    timer_exists = schedule_paths.timer_path.exists()
    status = {
        "profile": profile_name,
        "service_name": schedule_paths.service_name,
        "timer_name": schedule_paths.timer_name,
        "service_path": str(schedule_paths.service_path),
        "timer_path": str(schedule_paths.timer_path),
        "service_exists": service_exists,
        "timer_exists": timer_exists,
        "env_file_path": str(schedule_paths.env_file_path) if schedule_paths.env_vars else None,
        "env_file_exists": bool(schedule_paths.env_file_path and schedule_paths.env_file_path.exists()),
        "env_vars": env_vars,
        "on_calendar": profile.schedule.on_calendar,
        "persistent": profile.schedule.persistent,
        "timer_enabled": "unknown",
        "timer_active": "unknown",
        "service_active": "unknown",
    }

    if service_exists or timer_exists:
        status["timer_enabled"] = _systemctl_state(["is-enabled", schedule_paths.timer_name])
        status["timer_active"] = _systemctl_state(["is-active", schedule_paths.timer_name])
        status["service_active"] = _systemctl_state(["is-active", schedule_paths.service_name])

    return status


def render_service_unit(
    profile_name: str,
    service_name: str,
    config_path: Path,
    run_as_user: str,
    run_as_group: str,
    env_file_path: Path | None,
) -> str:
    exec_args = [
        sys.executable,
        "-m",
        "dbrestore",
        "backup",
        "--profile",
        profile_name,
        "--config",
        str(config_path),
    ]
    lines = [
        "[Unit]",
        f"Description=dbrestore backup for profile {profile_name}",
        "Wants=network-online.target",
        "After=network-online.target",
        "",
        "[Service]",
        "Type=oneshot",
        f"User={run_as_user}",
        f"Group={run_as_group}",
    ]
    if env_file_path is not None:
        lines.append(f"EnvironmentFile={env_file_path}")
    lines.append(f"ExecStart={shlex.join(exec_args)}")
    lines.append("")
    return "\n".join(lines)


def render_timer_unit(
    profile_name: str,
    timer_name: str,
    service_name: str,
    on_calendar: str,
    persistent: bool,
) -> str:
    lines = [
        "[Unit]",
        f"Description=dbrestore schedule for profile {profile_name}",
        "",
        "[Timer]",
        f"OnCalendar={on_calendar}",
        "AccuracySec=1s",
        "RandomizedDelaySec=0",
        f"Persistent={'true' if persistent else 'false'}",
        f"Unit={service_name}",
        "",
        "[Install]",
        "WantedBy=timers.target",
        "",
    ]
    return "\n".join(lines)


def render_env_template(profile_name: str, env_vars: list[str]) -> str:
    lines = [
        f"# dbrestore environment for profile {profile_name}",
        "# Fill in the values below and keep this file readable only by root.",
    ]
    for name in env_vars:
        lines.append(f"{name}=")
    lines.append("")
    return "\n".join(lines)


def _build_schedule_paths(
    profile_name: str,
    unit_dir: Path,
    env_dir: Path,
    env_vars: list[str],
) -> SchedulePaths:
    basename = _sanitize_unit_name(profile_name)
    service_name = f"dbrestore-backup-{basename}.service"
    timer_name = f"dbrestore-backup-{basename}.timer"
    env_file_path = env_dir / f"{basename}.env"
    return SchedulePaths(
        profile_name=profile_name,
        unit_basename=basename,
        service_name=service_name,
        timer_name=timer_name,
        service_path=unit_dir / service_name,
        timer_path=unit_dir / timer_name,
        env_file_path=env_file_path,
        env_vars=env_vars,
    )


def _sanitize_unit_name(profile_name: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.@-]+", "-", profile_name).strip("-")
    if not sanitized:
        raise SchedulingError(f"Unable to derive a valid systemd unit name from profile '{profile_name}'")
    return sanitized


def _resolve_run_identity(run_as_user: str | None, run_as_group: str | None) -> tuple[str, str]:
    user_name = run_as_user or _default_run_user()
    try:
        user_info = pwd.getpwnam(user_name)
    except KeyError as exc:
        raise SchedulingError(f"User '{user_name}' does not exist") from exc

    if run_as_group is None:
        group_name = grp.getgrgid(user_info.pw_gid).gr_name
    else:
        group_name = run_as_group
        try:
            grp.getgrnam(group_name)
        except KeyError as exc:
            raise SchedulingError(f"Group '{group_name}' does not exist") from exc

    return user_name, group_name


def _default_run_user() -> str:
    if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
        return os.environ["SUDO_USER"]
    return pwd.getpwuid(os.getuid()).pw_name


def _run_systemctl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    binary = shutil.which("systemctl")
    if binary is None:
        raise SchedulingError("systemctl not found on PATH")

    result = subprocess.run([binary, *args], check=False, capture_output=True, text=True)
    if check and result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        raise SchedulingError(f"systemctl {' '.join(args)} failed: {details}")
    return result


def _systemctl_state(args: list[str]) -> str:
    result = _run_systemctl(args, check=False)
    if result.returncode == 0:
        return result.stdout.strip() or "active"
    details = result.stdout.strip() or result.stderr.strip()
    return details or "unknown"


def _resolve_path(path: Path, *, field_name: str) -> Path:
    try:
        return expand_user_path(path, field_name=field_name).resolve()
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc
