"""This module handles outbound notifications such as Slack messages.
Operations send high-level events here after important outcomes like completed or failed backups.
Delivery is intentionally separated from business logic so notifications stay optional and replaceable.
If alerting changes in the future, this file is where that integration should evolve."""

from __future__ import annotations

import json
from typing import Any
from urllib import request

from dbrestore.config import NotificationsModel
from dbrestore.logging import RunLogger
from dbrestore.utils import Redactor


class NotificationDeliveryError(Exception):
    """Raised when an outbound notification cannot be delivered."""


def notify_event(
    settings: NotificationsModel | None,
    event: str,
    payload: dict[str, Any],
    logger: RunLogger,
    redactor: Redactor,
) -> None:
    if settings is None or settings.slack is None:
        return
    if event not in settings.slack.events:
        return

    try:
        send_slack_webhook(settings.slack.webhook_url_value, build_slack_message(event, payload))
        logger.log_event(
            "notification.sent",
            {
                "channel": "slack",
                "event": event,
                "profile": payload.get("profile"),
                "target_profile": payload.get("target_profile"),
            },
        )
    except Exception as exc:
        logger.log_event(
            "notification.failed",
            {
                "channel": "slack",
                "event": event,
                "profile": payload.get("profile"),
                "target_profile": payload.get("target_profile"),
                "error": redactor.sanitize_text(exc),
            },
        )


def send_slack_webhook(webhook_url: str, message: str) -> None:
    body = json.dumps({"text": message}).encode("utf-8")
    req = request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=5) as response:
            status = response.getcode()
    except Exception as exc:
        raise NotificationDeliveryError(f"Slack notification failed: {exc}") from exc

    if status >= 400:
        raise NotificationDeliveryError(f"Slack notification failed with HTTP {status}")


def build_slack_message(event: str, payload: dict[str, Any]) -> str:
    title = {
        "backup.completed": "Backup completed",
        "backup.failed": "Backup failed",
        "restore.failed": "Restore failed",
        "verification.completed": "Verification completed",
        "verification.failed": "Verification failed",
    }.get(event, event)

    lines = [f"[dbrestore] {title}"]
    if payload.get("profile"):
        lines.append(f"Profile: {payload['profile']}")
    if payload.get("target_profile"):
        lines.append(f"Target profile: {payload['target_profile']}")
    if payload.get("db_type"):
        lines.append(f"DB type: {payload['db_type']}")
    if payload.get("run_id"):
        lines.append(f"Run ID: {payload['run_id']}")
    if payload.get("status"):
        lines.append(f"Status: {payload['status']}")
    if payload.get("artifact_path"):
        lines.append(f"Artifact: {payload['artifact_path']}")
    if payload.get("error"):
        lines.append(f"Error: {_clip(str(payload['error']))}")
    return "\n".join(lines)


def _clip(value: str, limit: int = 400) -> str:
    if len(value) <= limit:
        return value
    return f"{value[: limit - 3]}..."
