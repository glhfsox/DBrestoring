"""Recent activity/log view."""

from __future__ import annotations

from typing import Any

from dbrestore.operations import list_run_log_events

from .base import GUIBoundMixin
from .helpers import PALETTE


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
        self.activity_text = self.tk.Text(
            card,
            bg=PALETTE["card"],
            fg=PALETTE["ink"],
            highlightthickness=0,
            bd=0,
            relief="flat",
            wrap="word",
            font=("Cantarell", 10),
        )
        activity_scroll = self.ttk.Scrollbar(
            card, orient="vertical", command=self.activity_text.yview
        )
        self.activity_text.configure(yscrollcommand=activity_scroll.set)
        self.activity_text.pack(side="left", fill="both", expand=True, pady=(14, 0))
        activity_scroll.pack(side="right", fill="y", pady=(14, 0))

    def refresh_logs(self) -> None:
        profile_name = self.profile_name_var.get().strip() or None
        events = list_run_log_events(
            config_path=self.config_path, profile_name=profile_name, limit=150
        )
        self.activity_text.delete("1.0", "end")
        for event in events:
            payload = event.get("payload", {})
            line = f"{event.get('timestamp', '')}  {event.get('event', '')}\n{payload}\n\n"
            self.activity_text.insert("end", line)
        self.activity_text.see("1.0")
