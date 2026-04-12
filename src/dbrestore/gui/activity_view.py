"""Recent activity/log view with event-type filters and payload inspection."""

from __future__ import annotations

import json
from typing import Any

from dbrestore.operations import list_run_log_events

from .base import GUIBoundMixin
from .helpers import PALETTE, pretty_timestamp

EVENT_CATEGORIES = (
    "All",
    "backup",
    "restore",
    "verification",
    "scheduled_cycle",
    "retention",
    "notification",
)
EVENT_STATUSES = ("All", "started", "completed", "failed", "other")


def _event_category(event_name: str) -> str:
    if not event_name or "." not in event_name:
        return ""
    return event_name.split(".", 1)[0]


def _event_status(event_name: str) -> str:
    if not event_name or "." not in event_name:
        return "other"
    suffix = event_name.split(".", 1)[1]
    if suffix in ("started", "completed", "failed"):
        return suffix
    return "other"


def _format_duration_ms(duration_ms: Any) -> str:
    try:
        ms = float(duration_ms)
    except (TypeError, ValueError):
        return ""
    if ms >= 1000:
        return f"{ms / 1000:.1f}s"
    return f"{int(ms)}ms"


def summarize_activity_event(event_name: str, payload: dict[str, Any]) -> str:
    """Return a short, single-line description of an event for the activity table."""
    if event_name == "backup.started":
        db_type = payload.get("db_type") or ""
        compression = payload.get("compression") or ""
        parts = [p for p in (db_type, compression) if p]
        return " / ".join(parts) or "starting"
    if event_name == "backup.completed":
        duration = _format_duration_ms(payload.get("duration_ms"))
        compression = payload.get("compression") or ""
        deleted = payload.get("retention_deleted_count")
        parts = [p for p in (compression, duration) if p]
        if deleted:
            parts.append(f"pruned {deleted}")
        return ", ".join(parts) or "ok"
    if event_name == "backup.failed":
        return str(payload.get("error") or "failed")
    if event_name == "restore.started":
        selection = payload.get("restore_selection") or []
        kind = payload.get("restore_selection_kind") or "items"
        if selection:
            return f"{len(selection)} {kind}"
        return "full restore"
    if event_name == "restore.completed":
        return "ok"
    if event_name == "restore.failed":
        return str(payload.get("error") or "failed")
    if event_name == "verification.started":
        return f"target: {payload.get('target_profile') or 'n/a'}"
    if event_name == "verification.completed":
        return f"verified via {payload.get('target_profile') or 'n/a'}"
    if event_name == "verification.failed":
        return str(payload.get("error") or "failed")
    if event_name == "scheduled_cycle.started":
        return "cycle started"
    if event_name == "scheduled_cycle.completed":
        verification_status = payload.get("verification_status")
        return f"verification: {verification_status}" if verification_status else "ok"
    if event_name == "scheduled_cycle.failed":
        return str(payload.get("error") or "failed")
    if event_name == "retention.deleted":
        count = payload.get("deleted_count") or 0
        return f"deleted {count} run(s)"
    if event_name == "notification.sent":
        channel = payload.get("channel") or "n/a"
        return f"{channel}: {payload.get('event') or ''}".rstrip(": ")
    if event_name == "notification.failed":
        return str(payload.get("error") or "failed")
    return ""


class ActivityViewMixin(GUIBoundMixin):
    def _build_activity_tab(self, parent: Any) -> None:
        card = self.ttk.Frame(parent, style="Card.TFrame", padding=18)
        card.pack(fill="both", expand=True)

        header = self.ttk.Frame(card, style="Card.TFrame")
        header.pack(fill="x")
        self.ttk.Label(header, text="Recent Activity", style="CardTitle.TLabel").pack(side="left")
        self.ttk.Button(
            header, text="Refresh", style="Quiet.TButton", command=self.refresh_logs
        ).pack(side="right")

        filters = self.ttk.Frame(card, style="Card.TFrame")
        filters.pack(fill="x", pady=(10, 0))

        self.ttk.Label(filters, text="Category:", style="CardText.TLabel").pack(side="left")
        self.activity_category_var = self.tk.StringVar(value="All")
        category_combo = self.ttk.Combobox(
            filters,
            values=EVENT_CATEGORIES,
            textvariable=self.activity_category_var,
            state="readonly",
            width=16,
        )
        category_combo.pack(side="left", padx=(6, 18))
        category_combo.bind("<<ComboboxSelected>>", lambda _event: self._apply_activity_filters())

        self.ttk.Label(filters, text="Status:", style="CardText.TLabel").pack(side="left")
        self.activity_status_var = self.tk.StringVar(value="All")
        status_combo = self.ttk.Combobox(
            filters,
            values=EVENT_STATUSES,
            textvariable=self.activity_status_var,
            state="readonly",
            width=14,
        )
        status_combo.pack(side="left", padx=(6, 18))
        status_combo.bind("<<ComboboxSelected>>", lambda _event: self._apply_activity_filters())

        self.activity_count_var = self.tk.StringVar(value="0 events")
        self.ttk.Label(
            filters,
            textvariable=self.activity_count_var,
            style="CardText.TLabel",
        ).pack(side="right")

        columns = ("timestamp", "event", "profile", "summary")
        self.activity_tree = self.ttk.Treeview(
            card,
            columns=columns,
            show="headings",
            style="History.Treeview",
            selectmode="browse",
            height=14,
        )
        self.activity_tree.heading("timestamp", text="Time")
        self.activity_tree.heading("event", text="Event")
        self.activity_tree.heading("profile", text="Profile")
        self.activity_tree.heading("summary", text="Summary")
        self.activity_tree.column("timestamp", width=180, anchor="w")
        self.activity_tree.column("event", width=200, anchor="w")
        self.activity_tree.column("profile", width=160, anchor="w")
        self.activity_tree.column("summary", width=440, anchor="w")
        self.activity_tree.pack(fill="both", expand=True, pady=(12, 0))

        self.activity_tree.tag_configure("success", foreground="#0A6B47")
        self.activity_tree.tag_configure("failure", foreground=PALETTE["danger"])
        self.activity_tree.tag_configure("started", foreground=PALETTE["accent"])

        self.activity_tree.bind("<<TreeviewSelect>>", self._on_activity_row_selected)

        detail_card = self.ttk.Frame(card, style="Card.TFrame", padding=(0, 14, 0, 0))
        detail_card.pack(fill="x")
        self.ttk.Label(detail_card, text="Event Payload", style="CardTitle.TLabel").pack(anchor="w")
        detail_body = self.ttk.Frame(detail_card, style="Card.TFrame")
        detail_body.pack(fill="x", pady=(8, 0))
        self.activity_detail_text = self.tk.Text(
            detail_body,
            height=9,
            bg=PALETTE["field"],
            fg=PALETTE["ink"],
            highlightthickness=0,
            bd=0,
            relief="flat",
            wrap="word",
            font=("Cantarell", 10),
        )
        detail_scroll = self.ttk.Scrollbar(
            detail_body, orient="vertical", command=self.activity_detail_text.yview
        )
        self.activity_detail_text.configure(yscrollcommand=detail_scroll.set)
        self.activity_detail_text.pack(side="left", fill="both", expand=True)
        detail_scroll.pack(side="right", fill="y")

        self._activity_events: list[dict[str, Any]] = []

    def refresh_logs(self) -> None:
        profile_name = self.profile_name_var.get().strip() or None
        self._activity_events = list_run_log_events(
            config_path=self.config_path, profile_name=profile_name, limit=300
        )
        self._apply_activity_filters()

    def _apply_activity_filters(self) -> None:
        category = self.activity_category_var.get()
        status = self.activity_status_var.get()

        for item in self.activity_tree.get_children():
            self.activity_tree.delete(item)

        shown = 0
        for index, event in enumerate(self._activity_events):
            event_name = event.get("event") or ""
            if category != "All" and _event_category(event_name) != category:
                continue
            if status != "All" and _event_status(event_name) != status:
                continue

            payload = event.get("payload") or {}
            row_tag = self._activity_row_tag(event_name)
            self.activity_tree.insert(
                "",
                "end",
                iid=str(index),
                values=(
                    pretty_timestamp(event.get("timestamp")),
                    event_name,
                    payload.get("profile") or "",
                    summarize_activity_event(event_name, payload),
                ),
                tags=(row_tag,) if row_tag else (),
            )
            shown += 1

        total = len(self._activity_events)
        if shown == total:
            self.activity_count_var.set(f"{total} events")
        else:
            self.activity_count_var.set(f"{shown} of {total} events")

        self.activity_detail_text.delete("1.0", "end")

    def _activity_row_tag(self, event_name: str) -> str:
        status = _event_status(event_name)
        if status == "failed":
            return "failure"
        if status == "started":
            return "started"
        if status == "completed":
            return "success"
        if event_name in ("retention.deleted", "notification.sent"):
            return "success"
        if event_name == "notification.failed":
            return "failure"
        return ""

    def _on_activity_row_selected(self, _event: Any) -> None:
        selection = self.activity_tree.selection()
        if not selection:
            return
        try:
            index = int(selection[0])
        except ValueError:
            return
        if not 0 <= index < len(self._activity_events):
            return

        event = self._activity_events[index]
        self.activity_detail_text.delete("1.0", "end")
        self.activity_detail_text.insert(
            "1.0", json.dumps(event, indent=2, ensure_ascii=False, default=str)
        )
