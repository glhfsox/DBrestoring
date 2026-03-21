"""Backup history view and related actions."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

from dbrestore.operations import list_backup_history

from .base import GUIBoundMixin
from .helpers import pretty_timestamp, restore_option_label, set_widget_state


class BackupsViewMixin(GUIBoundMixin):
    def _build_backups_tab(self, parent: Any) -> None:
        card = self.ttk.Frame(parent, style="Card.TFrame", padding=18)
        card.pack(fill="both", expand=True)
        header = self.ttk.Frame(card, style="Card.TFrame")
        header.pack(fill="x")
        self.ttk.Label(header, text="Backup History", style="CardTitle.TLabel").pack(side="left")
        self.ttk.Button(
            header, text="Refresh", style="Quiet.TButton", command=self.refresh_backups
        ).pack(side="right")

        columns = ("finished_at", "run_id", "compression", "artifact_path")
        self.backup_tree = self.ttk.Treeview(
            card, columns=columns, show="headings", style="History.Treeview", selectmode="browse"
        )
        self.backup_tree.heading("finished_at", text="Finished")
        self.backup_tree.heading("run_id", text="Run ID")
        self.backup_tree.heading("compression", text="Compression")
        self.backup_tree.heading("artifact_path", text="Artifact")
        self.backup_tree.column("finished_at", width=180, anchor="w")
        self.backup_tree.column("run_id", width=140, anchor="w")
        self.backup_tree.column("compression", width=110, anchor="center")
        self.backup_tree.column("artifact_path", width=620, anchor="w")
        self.backup_tree.pack(fill="both", expand=True, pady=(14, 0))

        actions = self.ttk.Frame(card, style="Card.TFrame")
        actions.pack(fill="x", pady=(12, 0))
        self.ttk.Button(
            actions,
            text="Open Folder",
            style="Quiet.TButton",
            command=self.open_selected_backup_folder,
        ).pack(side="left")
        self.ttk.Button(
            actions,
            text="Restore Selected Into Current Profile",
            style="Danger.TButton",
            command=self.restore_selected_backup,
        ).pack(side="left", padx=(8, 0))

    def refresh_backups(self) -> None:
        profile_name = self.profile_name_var.get().strip() or None
        self.backup_rows = list_backup_history(
            config_path=self.config_path, profile_name=profile_name, limit=200
        )
        for item in self.backup_tree.get_children():
            self.backup_tree.delete(item)
        for index, row in enumerate(self.backup_rows):
            self.backup_tree.insert(
                "",
                "end",
                iid=str(index),
                values=(
                    pretty_timestamp(row.get("finished_at")),
                    row.get("run_id") or "",
                    row.get("compression") or "",
                    row.get("artifact_path") or "",
                ),
            )
        self._refresh_restore_choices()

    def open_selected_backup_folder(self) -> None:
        selection = self.backup_tree.selection()
        if not selection:
            self._show_error("Select a backup first.")
            return
        record = self.backup_rows[int(selection[0])]
        if str(record["run_dir"]).startswith("s3://"):
            self._show_error(
                "This backup is stored remotely in S3 and does not have a local folder to open."
            )
            return
        target = Path(record["run_dir"])
        opener = shutil.which("xdg-open")
        if opener is None:
            self._show_error("xdg-open is not available on this system.")
            return
        subprocess.Popen([opener, str(target)])

    def restore_selected_backup(self) -> None:
        if not self._save_if_needed():
            return
        selection = self.backup_tree.selection()
        if not selection:
            self._show_error("Select a backup first.")
            return
        record = self.backup_rows[int(selection[0])]
        self._confirm_and_restore_record(record)

    def _refresh_restore_choices(self) -> None:
        options = [restore_option_label(row) for row in self.backup_rows]
        self.profile_restore_combo.configure(values=options)
        current = self.restore_choice_var.get()
        if options:
            if current not in self._restore_choice_map():
                self.restore_choice_var.set(options[0])
            set_widget_state(self.profile_restore_combo, True)
            set_widget_state(self.restore_button, True)
            return

        self.restore_choice_var.set("")
        set_widget_state(self.profile_restore_combo, False)
        set_widget_state(self.restore_button, False)

    def _restore_choice_map(self) -> dict[str, dict[str, Any]]:
        return {restore_option_label(row): row for row in self.backup_rows}
