"""Best-effort reporting of backup runs to a dbrestore control plane.

Reporting never raises into the backup path: a control plane that is down or
misconfigured must not fail a backup. Failures are logged and swallowed.
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from typing import Any

from dbrestore.config import ControlPlaneModel
from dbrestore.logging import RunLogger

_TIMEOUT_SECONDS = 10


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def build_payload(
    settings: ControlPlaneModel,
    run: dict[str, Any],
    status: str,
    *,
    error: str | None = None,
) -> dict[str, Any]:
    server_id = settings.server_id or socket.gethostname()
    server_name = settings.server_name or server_id
    run_id = run.get("run_id") or f"{server_id}:{run.get('profile')}:{run.get('finished_at')}"
    return {
        "server": {"id": server_id, "name": server_name},
        "run": {
            "id": str(run_id),
            "profile": run.get("profile"),
            "db_type": run.get("db_type"),
            "backup_type": run.get("backup_type") or "full",
            "status": status,
            "size_bytes": _coerce_int(run.get("size_bytes")),
            "duration_ms": _coerce_int(run.get("duration_ms")),
            "started_at": run.get("started_at"),
            "finished_at": run.get("finished_at"),
            "error": error,
        },
    }


def report_run(
    settings: ControlPlaneModel,
    run: dict[str, Any],
    status: str,
    logger: RunLogger,
    *,
    error: str | None = None,
) -> bool:
    payload = build_payload(settings, run, status, error=error)
    url = settings.url.rstrip("/") + "/api/v1/runs"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.token_value}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
            status = getattr(response, "status", None) or response.getcode()
            body = response.read(2048).decode("utf-8", "replace")
        try:
            accepted = json.loads(body).get("ok") is True
        except ValueError:
            accepted = False
        if status in (200, 201) and accepted:
            logger.print(f"Reported backup run to control plane ({url})")
            return True
        logger.print(
            f"Control plane did not accept the run (HTTP {status}). "
            "The endpoint may be behind deployment protection or auth — "
            "the API must be publicly reachable."
        )
        return False
    except (urllib.error.URLError, OSError, TimeoutError, ValueError) as exc:
        logger.print(f"Control plane report failed (non-fatal): {exc}")
        return False
