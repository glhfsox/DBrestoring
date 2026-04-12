"""This module bridges the app with OS-native schedulers.
On Linux it installs systemd service and timer units.
On macOS it installs launchd property lists.
The actual backup work still happens through the normal CLI and operations flow.
So this layer is about automation plumbing, not about backup mechanics themselves."""

from __future__ import annotations

import os
import plistlib
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import grp
except ImportError:  # pragma: no cover - only hit on non-Unix platforms.
    grp = None

try:
    import pwd
except ImportError:  # pragma: no cover - only hit on non-Unix platforms.
    pwd = None

from dbrestore.config import DEFAULT_CONFIG_PATH, collect_profile_env_vars, load_config
from dbrestore.errors import ConfigError, SchedulingError
from dbrestore.utils import ensure_directory, expand_user_path

DEFAULT_SYSTEMD_UNIT_DIR = Path("/etc/systemd/system")
DEFAULT_LAUNCHD_AGENT_DIR = Path.home() / "Library" / "LaunchAgents"
DEFAULT_LAUNCHD_DAEMON_DIR = Path("/Library/LaunchDaemons")
DEFAULT_LINUX_ENV_DIR = Path("/etc/dbrestore/env")
DEFAULT_MACOS_ENV_DIR = Path.home() / "Library" / "Application Support" / "dbrestore" / "env"


@dataclass(frozen=True)
class SchedulePaths:
    profile_name: str
    unit_basename: str
    backend: str
    job_label: str
    definition_name: str
    definition_path: Path
    env_file_path: Path | None
    env_vars: list[str]
    service_name: str | None = None
    timer_name: str | None = None
    service_path: Path | None = None
    timer_path: Path | None = None


def schedule_backend() -> str:
    if sys.platform == "darwin":
        return "launchd"
    if sys.platform.startswith("linux"):
        return "systemd"
    return "unsupported"


def schedule_backend_display_name(backend: str | None = None) -> str:
    resolved = backend or schedule_backend()
    return {
        "launchd": "Launchd",
        "systemd": "Systemd",
    }.get(resolved, "Scheduler")


def default_schedule_unit_dir() -> Path:
    backend = schedule_backend()
    if backend == "launchd":
        return DEFAULT_LAUNCHD_AGENT_DIR
    if backend == "systemd":
        return DEFAULT_SYSTEMD_UNIT_DIR
    return Path("./.dbrestore/schedule")


def default_env_dir() -> Path:
    backend = schedule_backend()
    if backend == "launchd":
        return DEFAULT_MACOS_ENV_DIR
    if backend == "systemd":
        return DEFAULT_LINUX_ENV_DIR
    return Path("./.dbrestore/env")


DEFAULT_SCHEDULE_UNIT_DIR = default_schedule_unit_dir()
DEFAULT_ENV_DIR = default_env_dir()
SCHEDULE_BACKEND = schedule_backend()
SCHEDULE_BACKEND_DISPLAY_NAME = schedule_backend_display_name(SCHEDULE_BACKEND)


def install_schedule(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    unit_dir: Path = DEFAULT_SCHEDULE_UNIT_DIR,
    env_dir: Path = DEFAULT_ENV_DIR,
    enable_timer: bool = True,
    force: bool = False,
    run_as_user: str | None = None,
    run_as_group: str | None = None,
) -> dict[str, Any]:
    backend = _require_supported_schedule_backend()
    config = load_config(config_path, require_env=False)
    profile = config.get_profile(profile_name)
    if profile.schedule is None:
        raise SchedulingError(f"Profile '{profile_name}' does not have a schedule configured")

    resolved_config_path = config.source_path or config_path.expanduser().resolve()
    resolved_unit_dir = _resolve_path(unit_dir, field_name="unit_dir")
    resolved_env_dir = _resolve_path(env_dir, field_name="env_dir")
    env_vars = collect_profile_env_vars(resolved_config_path, profile_name)
    schedule_paths = _build_schedule_paths(
        profile_name,
        backend,
        resolved_unit_dir,
        resolved_env_dir,
        env_vars,
    )
    user_name, group_name = _resolve_install_identity(
        backend,
        resolved_unit_dir,
        run_as_user,
        run_as_group,
    )

    existing = _existing_schedule_definition_paths(schedule_paths)
    if existing and not force:
        joined = ", ".join(str(path) for path in existing)
        raise SchedulingError(
            f"Schedule definition file(s) already exist for profile '{profile_name}': {joined}. Use --force to overwrite."
        )

    if backend == "systemd":
        assert schedule_paths.service_path is not None
        assert schedule_paths.timer_path is not None
        assert schedule_paths.service_name is not None
        assert schedule_paths.timer_name is not None

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
    else:
        ensure_directory(schedule_paths.definition_path.parent)
        definition_contents = render_launchd_plist(
            profile_name=profile_name,
            job_label=schedule_paths.job_label,
            config_path=resolved_config_path,
            env_file_path=schedule_paths.env_file_path if schedule_paths.env_vars else None,
            on_calendar=profile.schedule.on_calendar,
            run_as_user=user_name,
            run_as_group=group_name,
            unit_dir=resolved_unit_dir,
        )
        schedule_paths.definition_path.write_text(definition_contents, encoding="utf-8")

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

    if backend == "systemd":
        assert schedule_paths.timer_name is not None
        _run_systemctl(["daemon-reload"])
        if enable_timer:
            _run_systemctl(["enable", "--now", schedule_paths.timer_name])
    else:
        domain_target = _launchd_domain_target(resolved_unit_dir)
        if enable_timer:
            _run_launchctl(
                ["bootout", domain_target, str(schedule_paths.definition_path)], check=False
            )
            _run_launchctl(["bootstrap", domain_target, str(schedule_paths.definition_path)])

    return {
        **_base_schedule_metadata(schedule_paths, resolved_unit_dir),
        "env_template_created": env_template_created,
        "enabled": enable_timer,
        "run_as_user": user_name,
        "run_as_group": group_name,
        "on_calendar": profile.schedule.on_calendar,
        "persistent": profile.schedule.persistent,
        "verification_target_profile": profile.verification.target_profile
        if profile.verification and profile.verification.schedule_after_backup
        else None,
    }


def remove_schedule(
    profile_name: str,
    unit_dir: Path = DEFAULT_SCHEDULE_UNIT_DIR,
    env_dir: Path = DEFAULT_ENV_DIR,
    delete_env_file: bool = False,
) -> dict[str, Any]:
    backend = _require_supported_schedule_backend()
    resolved_unit_dir = _resolve_path(unit_dir, field_name="unit_dir")
    resolved_env_dir = _resolve_path(env_dir, field_name="env_dir")
    schedule_paths = _build_schedule_paths(
        profile_name,
        backend,
        resolved_unit_dir,
        resolved_env_dir,
        [],
    )

    removed_files: list[str] = []
    if backend == "systemd":
        assert schedule_paths.timer_name is not None
        assert schedule_paths.service_name is not None
        assert schedule_paths.timer_path is not None
        assert schedule_paths.service_path is not None

        _run_systemctl(["disable", "--now", schedule_paths.timer_name], check=False)
        _run_systemctl(
            ["reset-failed", schedule_paths.timer_name, schedule_paths.service_name],
            check=False,
        )

        for path in (schedule_paths.timer_path, schedule_paths.service_path):
            if path.exists():
                path.unlink()
                removed_files.append(str(path))

        _run_systemctl(["daemon-reload"], check=False)
    else:
        domain_target = _launchd_domain_target(resolved_unit_dir)
        _run_launchctl(["bootout", domain_target, str(schedule_paths.definition_path)], check=False)
        if schedule_paths.definition_path.exists():
            schedule_paths.definition_path.unlink()
            removed_files.append(str(schedule_paths.definition_path))

    if (
        delete_env_file
        and schedule_paths.env_file_path is not None
        and schedule_paths.env_file_path.exists()
    ):
        schedule_paths.env_file_path.unlink()
        removed_files.append(str(schedule_paths.env_file_path))

    return {
        **_base_schedule_metadata(schedule_paths, resolved_unit_dir),
        "removed_files": removed_files,
        "deleted_env_file": delete_env_file,
    }


def schedule_status(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    unit_dir: Path = DEFAULT_SCHEDULE_UNIT_DIR,
    env_dir: Path = DEFAULT_ENV_DIR,
) -> dict[str, Any]:
    backend = _require_supported_schedule_backend()
    config = load_config(config_path, require_env=False)
    profile = config.get_profile(profile_name)
    if profile.schedule is None:
        raise SchedulingError(f"Profile '{profile_name}' does not have a schedule configured")

    resolved_config_path = config.source_path or config_path.expanduser().resolve()
    resolved_unit_dir = _resolve_path(unit_dir, field_name="unit_dir")
    resolved_env_dir = _resolve_path(env_dir, field_name="env_dir")
    env_vars = collect_profile_env_vars(resolved_config_path, profile_name)
    schedule_paths = _build_schedule_paths(
        profile_name,
        backend,
        resolved_unit_dir,
        resolved_env_dir,
        env_vars,
    )

    status = {
        **_base_schedule_metadata(schedule_paths, resolved_unit_dir),
        "configured": True,
        "installed": False,
        "env_file_exists": bool(
            schedule_paths.env_file_path and schedule_paths.env_file_path.exists()
        ),
        "on_calendar": profile.schedule.on_calendar,
        "persistent": profile.schedule.persistent,
        "next_run": None,
        "last_trigger": None,
        "verification_target_profile": profile.verification.target_profile
        if profile.verification and profile.verification.schedule_after_backup
        else None,
    }

    env_file_values = _load_env_values(
        schedule_paths.env_file_path if schedule_paths.env_vars else None
    )
    status["env_vars_missing"] = [
        name for name in env_vars if not (env_file_values.get(name) or "").strip()
    ]
    status["env_values_set_count"] = len(
        [name for name in env_vars if (env_file_values.get(name) or "").strip()]
    )

    if backend == "systemd":
        assert schedule_paths.service_path is not None
        assert schedule_paths.timer_path is not None
        assert schedule_paths.service_name is not None
        assert schedule_paths.timer_name is not None

        service_exists = schedule_paths.service_path.exists()
        timer_exists = schedule_paths.timer_path.exists()
        status.update(
            {
                "installed": service_exists and timer_exists,
                "service_exists": service_exists,
                "timer_exists": timer_exists,
                "timer_enabled": "unknown",
                "timer_active": "unknown",
                "service_active": "unknown",
            }
        )
        if service_exists or timer_exists:
            status["timer_enabled"] = _systemctl_state(["is-enabled", schedule_paths.timer_name])
            status["timer_active"] = _systemctl_state(["is-active", schedule_paths.timer_name])
            status["service_active"] = _systemctl_state(["is-active", schedule_paths.service_name])
        if timer_exists:
            status["next_run"] = _systemctl_show_property(
                schedule_paths.timer_name,
                ["NextElapseUSecRealtime", "NextElapseUSec"],
            )
            status["last_trigger"] = _systemctl_show_property(
                schedule_paths.timer_name,
                ["LastTriggerUSec", "LastTriggerUSecRealtime"],
            )
        return status

    domain_target = _launchd_domain_target(resolved_unit_dir)
    service_target = _launchd_service_target(domain_target, schedule_paths.job_label)
    definition_exists = schedule_paths.definition_path.exists()
    launchctl_result = _run_launchctl(["print", service_target], check=False)
    launchctl_output = launchctl_result.stdout or launchctl_result.stderr
    disabled = _launchd_disabled_state(domain_target, schedule_paths.job_label)

    status.update(
        {
            "installed": definition_exists,
            "definition_exists": definition_exists,
            "job_domain": domain_target,
            "job_loaded": launchctl_result.returncode == 0,
            "job_enabled": None if disabled is None else not disabled,
            "job_state": "unloaded",
            "job_pid": None,
            "job_runs": None,
            "last_exit_code": None,
        }
    )
    if launchctl_result.returncode == 0:
        pid_value = _launchctl_print_value(launchctl_output, "pid")
        status["job_state"] = _launchctl_print_value(launchctl_output, "state") or "loaded"
        status["job_pid"] = int(pid_value) if pid_value and pid_value.isdigit() else None
        status["job_runs"] = _launchctl_print_value(launchctl_output, "runs")
        status["last_exit_code"] = _launchctl_print_value(launchctl_output, "last exit code")

    return status


def load_schedule_env_file(
    profile_name: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    env_dir: Path = DEFAULT_ENV_DIR,
) -> dict[str, Any]:
    config = load_config(config_path, require_env=False)
    resolved_config_path = config.source_path or config_path.expanduser().resolve()
    env_vars = collect_profile_env_vars(resolved_config_path, profile_name)
    resolved_env_dir = _resolve_path(env_dir, field_name="env_dir")
    env_file_path = _build_env_file_path(profile_name, resolved_env_dir)
    if env_file_path.exists():
        text = _read_env_file_text(env_file_path)
    else:
        text = render_env_template(profile_name, env_vars)
    values = _parse_env_file_text(text)
    missing = [name for name in env_vars if not (values.get(name) or "").strip()]
    return {
        "profile": profile_name,
        "env_file_path": str(env_file_path),
        "exists": env_file_path.exists(),
        "env_vars": env_vars,
        "text": text,
        "missing_vars": missing,
    }


def save_schedule_env_file(
    profile_name: str,
    contents: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    env_dir: Path = DEFAULT_ENV_DIR,
) -> dict[str, Any]:
    config = load_config(config_path, require_env=False)
    resolved_config_path = config.source_path or config_path.expanduser().resolve()
    collect_profile_env_vars(resolved_config_path, profile_name)
    resolved_env_dir = _resolve_path(env_dir, field_name="env_dir")
    env_file_path = _build_env_file_path(profile_name, resolved_env_dir)
    ensure_directory(env_file_path.parent)
    normalized = contents if contents.endswith("\n") else f"{contents}\n"
    try:
        env_file_path.write_text(normalized, encoding="utf-8")
        os.chmod(env_file_path, 0o600)
    except OSError as exc:
        raise SchedulingError(f"Unable to write env file: {env_file_path}: {exc}") from exc
    return load_schedule_env_file(profile_name, config_path=config_path, env_dir=env_dir)


def load_schedule_env_vars_into_environment(env_file_path: Path) -> dict[str, str]:
    resolved_env_file = _resolve_path(env_file_path, field_name="env_file")
    values = _parse_env_file_text(_read_env_file_text(resolved_env_file))
    for name, value in values.items():
        os.environ[name] = value
    return values


def render_service_unit(
    profile_name: str,
    service_name: str,
    config_path: Path,
    run_as_user: str,
    run_as_group: str,
    env_file_path: Path | None,
) -> str:
    exec_args = _scheduled_command_args(profile_name, config_path, env_file_path)
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


def render_launchd_plist(
    profile_name: str,
    job_label: str,
    config_path: Path,
    env_file_path: Path | None,
    on_calendar: str,
    run_as_user: str,
    run_as_group: str,
    unit_dir: Path,
) -> str:
    payload: dict[str, Any] = {
        "Label": job_label,
        "ProgramArguments": _scheduled_command_args(profile_name, config_path, env_file_path),
        "WorkingDirectory": str(config_path.parent),
        "ProcessType": "Background",
        "StartCalendarInterval": _launchd_calendar_interval(on_calendar),
    }
    if _launchd_domain_target(unit_dir) == "system":
        payload["UserName"] = run_as_user
        payload["GroupName"] = run_as_group
    return plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=False).decode("utf-8")


def render_env_template(profile_name: str, env_vars: list[str]) -> str:
    lines = [
        f"# dbrestore environment for profile {profile_name}",
        "# Fill in the values below and keep this file readable only by the account running the job.",
    ]
    for name in env_vars:
        lines.append(f"{name}=")
    lines.append("")
    return "\n".join(lines)


def _base_schedule_metadata(schedule_paths: SchedulePaths, unit_dir: Path) -> dict[str, Any]:
    data: dict[str, Any] = {
        "backend": schedule_paths.backend,
        "backend_label": schedule_backend_display_name(schedule_paths.backend),
        "profile": schedule_paths.profile_name,
        "job_label": schedule_paths.job_label,
        "definition_name": schedule_paths.definition_name,
        "definition_path": str(schedule_paths.definition_path),
        "unit_dir": str(unit_dir),
        "env_file_path": str(schedule_paths.env_file_path) if schedule_paths.env_vars else None,
        "env_vars": schedule_paths.env_vars,
    }
    if schedule_paths.service_name is not None:
        data["service_name"] = schedule_paths.service_name
    if schedule_paths.timer_name is not None:
        data["timer_name"] = schedule_paths.timer_name
    if schedule_paths.service_path is not None:
        data["service_path"] = str(schedule_paths.service_path)
    if schedule_paths.timer_path is not None:
        data["timer_path"] = str(schedule_paths.timer_path)
    if schedule_paths.backend == "launchd":
        data["job_domain"] = _launchd_domain_target(unit_dir)
    return data


def _build_schedule_paths(
    profile_name: str,
    backend: str,
    unit_dir: Path,
    env_dir: Path,
    env_vars: list[str],
) -> SchedulePaths:
    basename = _sanitize_schedule_name(profile_name)
    env_file_path = env_dir / f"{basename}.env"
    if backend == "systemd":
        service_name = f"dbrestore-backup-{basename}.service"
        timer_name = f"dbrestore-backup-{basename}.timer"
        return SchedulePaths(
            profile_name=profile_name,
            unit_basename=basename,
            backend=backend,
            job_label=timer_name,
            definition_name=timer_name,
            definition_path=unit_dir / timer_name,
            env_file_path=env_file_path,
            env_vars=env_vars,
            service_name=service_name,
            timer_name=timer_name,
            service_path=unit_dir / service_name,
            timer_path=unit_dir / timer_name,
        )

    job_label = f"io.dbrestore.backup.{basename}"
    definition_name = f"{job_label}.plist"
    return SchedulePaths(
        profile_name=profile_name,
        unit_basename=basename,
        backend=backend,
        job_label=job_label,
        definition_name=definition_name,
        definition_path=unit_dir / definition_name,
        env_file_path=env_file_path,
        env_vars=env_vars,
    )


def _existing_schedule_definition_paths(schedule_paths: SchedulePaths) -> list[Path]:
    if schedule_paths.backend == "systemd":
        candidates = [schedule_paths.service_path, schedule_paths.timer_path]
    else:
        candidates = [schedule_paths.definition_path]
    return [path for path in candidates if path is not None and path.exists()]


def _sanitize_schedule_name(profile_name: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.@-]+", "-", profile_name).strip("-")
    if not sanitized:
        raise SchedulingError(
            f"Unable to derive a valid schedule name from profile '{profile_name}'"
        )
    return sanitized


def _scheduled_command_args(
    profile_name: str,
    config_path: Path,
    env_file_path: Path | None,
) -> list[str]:
    args = [
        sys.executable,
        "-m",
        "dbrestore",
        "run-scheduled",
        "--profile",
        profile_name,
        "--config",
        str(config_path),
    ]
    if env_file_path is not None:
        args.extend(["--env-file", str(env_file_path)])
    return args


def _launchd_calendar_interval(on_calendar: str) -> dict[str, int]:
    if on_calendar == "hourly":
        return {"Minute": 0}
    if on_calendar == "daily":
        return {"Hour": 0, "Minute": 0}
    if on_calendar == "weekly":
        return {"Weekday": 1, "Hour": 0, "Minute": 0}
    raise SchedulingError(
        f"Unsupported launchd schedule preset '{on_calendar}'. Supported presets: hourly, daily, weekly."
    )


def _launchd_domain_target(unit_dir: Path) -> str:
    if unit_dir == DEFAULT_LAUNCHD_DAEMON_DIR or unit_dir.is_relative_to(
        DEFAULT_LAUNCHD_DAEMON_DIR
    ):
        return "system"
    return f"gui/{os.getuid()}"


def _launchd_service_target(domain_target: str, job_label: str) -> str:
    return f"{domain_target}/{job_label}"


def _resolve_install_identity(
    backend: str,
    unit_dir: Path,
    run_as_user: str | None,
    run_as_group: str | None,
) -> tuple[str, str]:
    if backend != "launchd":
        return _resolve_run_identity(run_as_user, run_as_group)

    if _launchd_domain_target(unit_dir) == "system":
        return _resolve_run_identity(run_as_user, run_as_group)

    if run_as_user is not None or run_as_group is not None:
        raise SchedulingError(
            "run_as_user and run_as_group are only supported for launchd plists installed in /Library/LaunchDaemons"
        )
    return _resolve_run_identity(None, None)


def _resolve_run_identity(run_as_user: str | None, run_as_group: str | None) -> tuple[str, str]:
    if pwd is None or grp is None:
        raise SchedulingError("Schedule management is only supported on Unix-like systems")
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
    if pwd is None:
        raise SchedulingError("Schedule management is only supported on Unix-like systems")
    if os.geteuid() == 0 and os.environ.get("SUDO_USER"):
        return os.environ["SUDO_USER"]
    return pwd.getpwuid(os.getuid()).pw_name


def _require_supported_schedule_backend() -> str:
    backend = schedule_backend()
    if backend == "unsupported":
        raise SchedulingError(
            "Schedule management is only supported on Linux (systemd) and macOS (launchd)"
        )
    return backend


def _run_systemctl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    binary = shutil.which("systemctl")
    if binary is None:
        raise SchedulingError("systemctl not found on PATH")

    result = subprocess.run([binary, *args], check=False, capture_output=True, text=True)
    if check and result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        raise SchedulingError(f"systemctl {' '.join(args)} failed: {details}")
    return result


def _run_launchctl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    binary = shutil.which("launchctl")
    if binary is None:
        raise SchedulingError("launchctl not found on PATH")

    result = subprocess.run([binary, *args], check=False, capture_output=True, text=True)
    if check and result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        raise SchedulingError(f"launchctl {' '.join(args)} failed: {details}")
    return result


def _systemctl_state(args: list[str]) -> str:
    result = _run_systemctl(args, check=False)
    if result.returncode == 0:
        return result.stdout.strip() or "active"
    details = result.stdout.strip() or result.stderr.strip()
    return details or "unknown"


def _systemctl_show_property(unit_name: str, properties: list[str]) -> str | None:
    for property_name in properties:
        result = _run_systemctl(
            ["show", unit_name, f"--property={property_name}", "--value"],
            check=False,
        )
        value = result.stdout.strip() or result.stderr.strip()
        if result.returncode == 0 and value and value.lower() not in {"n/a", "[not set]"}:
            return value
    return None


def _launchd_disabled_state(domain_target: str, job_label: str) -> bool | None:
    result = _run_launchctl(["print-disabled", domain_target], check=False)
    if result.returncode != 0:
        return None

    pattern = rf'"{re.escape(job_label)}" => (enabled|disabled)'
    match = re.search(pattern, result.stdout)
    if match is None:
        return None
    return match.group(1) == "disabled"


def _launchctl_print_value(output: str, key: str) -> str | None:
    pattern = rf"^\s*{re.escape(key)} = (.+)$"
    match = re.search(pattern, output, flags=re.MULTILINE)
    if match is None:
        return None
    return match.group(1).strip()


def _build_env_file_path(profile_name: str, env_dir: Path) -> Path:
    return env_dir / f"{_sanitize_schedule_name(profile_name)}.env"


def _load_env_values(env_file_path: Path | None) -> dict[str, str]:
    if env_file_path is None or not env_file_path.exists():
        return {}
    return _parse_env_file_text(_read_env_file_text(env_file_path))


def _read_env_file_text(env_file_path: Path) -> str:
    try:
        return env_file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SchedulingError(f"Unable to read env file: {env_file_path}: {exc}") from exc


def _parse_env_file_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        values[name.strip()] = value.strip()
    return values


def _resolve_path(path: Path, *, field_name: str) -> Path:
    try:
        return expand_user_path(path, field_name=field_name).resolve()
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc
