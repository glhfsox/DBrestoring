"""Operational dashboard, scheduling controls, env editor, and preflight view."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dbrestore.errors import DBRestoreError
from dbrestore.operations import collect_profile_status, run_profile_preflight, run_scheduled_cycle
from dbrestore.scheduler import (
    DEFAULT_ENV_DIR,
    DEFAULT_SYSTEMD_UNIT_DIR,
    install_schedule,
    load_schedule_env_file,
    remove_schedule,
    save_schedule_env_file,
)

from .base import GUIBoundMixin
from .helpers import PALETTE, pretty_timestamp


class OperationsViewMixin(GUIBoundMixin):
    def _build_operations_tab(self, parent: Any) -> None:
        top = self.ttk.Frame(parent, style="App.TFrame")
        top.pack(fill="both", expand=True)

        left = self.ttk.Frame(top, style="Card.TFrame", padding=18)
        left.pack(side="left", fill="both", expand=True, padx=(0, 12))
        right = self.ttk.Frame(top, style="App.TFrame")
        right.pack(side="left", fill="both", expand=True)

        dashboard = self.ttk.Frame(left, style="Card.TFrame")
        dashboard.pack(fill="x")
        self.ttk.Label(dashboard, text="Readiness Dashboard", style="CardTitle.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w"
        )
        self.ttk.Label(
            dashboard,
            text="The latest backup, verification, retention, schedule, and storage signals for the selected profile.",
            style="CardText.TLabel",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 14))
        self._add_dashboard_row(dashboard, 2, "Last Backup", self.dashboard_last_backup_var)
        self._add_dashboard_row(
            dashboard, 3, "Last Verification", self.dashboard_last_verification_var
        )
        self._add_dashboard_row(dashboard, 4, "Next Run", self.dashboard_next_run_var)
        self._add_dashboard_row(dashboard, 5, "Storage Target", self.dashboard_storage_target_var)
        self._add_dashboard_row(dashboard, 6, "Storage Health", self.dashboard_storage_health_var)
        self._add_dashboard_row(dashboard, 7, "Retention", self.dashboard_retention_var)
        self._add_dashboard_row(dashboard, 8, "Schedule", self.dashboard_schedule_var)
        dashboard.columnconfigure(1, weight=1)

        dashboard_actions = self.ttk.Frame(left, style="Card.TFrame")
        dashboard_actions.pack(fill="x", pady=(14, 0))
        dashboard_actions.columnconfigure((0, 1, 2), weight=1)
        self.ttk.Button(
            dashboard_actions,
            text="Refresh Readiness",
            style="Quiet.TButton",
            command=self.refresh_operations_view,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.ttk.Button(
            dashboard_actions,
            text="Run Preflight",
            style="Quiet.TButton",
            command=self.run_preflight_action,
        ).grid(row=0, column=1, sticky="ew", padx=(0, 8))
        self.ttk.Button(
            dashboard_actions,
            text="Run Scheduled Cycle",
            style="Accent.TButton",
            command=self.run_scheduled_cycle_action,
        ).grid(row=0, column=2, sticky="ew")
        self.ttk.Checkbutton(
            left,
            text="Include live connection test in preflight",
            variable=self.preflight_include_connection_var,
        ).pack(anchor="w", pady=(10, 0))

        output_card = self.ttk.Frame(left, style="Card.TFrame", padding=(0, 14, 0, 0))
        output_card.pack(fill="both", expand=True)
        self.ttk.Label(output_card, text="Operational Output", style="CardTitle.TLabel").pack(
            anchor="w"
        )
        self.operations_text = self.tk.Text(
            output_card,
            height=14,
            bg=PALETTE["card"],
            fg=PALETTE["ink"],
            highlightthickness=0,
            bd=0,
            relief="flat",
            wrap="word",
            font=("Cantarell", 10),
        )
        output_scroll = self.ttk.Scrollbar(
            output_card, orient="vertical", command=self.operations_text.yview
        )
        self.operations_text.configure(yscrollcommand=output_scroll.set)
        self.operations_text.pack(side="left", fill="both", expand=True, pady=(10, 0))
        output_scroll.pack(side="right", fill="y", pady=(10, 0))

        schedule_card = self.ttk.Frame(right, style="Card.TFrame", padding=18)
        schedule_card.pack(fill="x")
        self.ttk.Label(schedule_card, text="Systemd Schedule", style="CardTitle.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w"
        )
        self.ttk.Label(
            schedule_card,
            text="Manage unit installation and status. These actions need filesystem/systemctl access from the GUI process.",
            style="CardText.TLabel",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 14))
        self._add_labeled_entry(schedule_card, 2, "Unit Directory", self.schedule_unit_dir_var)
        self._add_labeled_entry(schedule_card, 3, "Env Directory", self.schedule_env_dir_var)
        self.ttk.Label(
            schedule_card,
            textvariable=self.schedule_status_detail_var,
            style="CardText.TLabel",
            wraplength=420,
            justify="left",
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(0, 8))
        schedule_actions = self.ttk.Frame(schedule_card, style="Card.TFrame")
        schedule_actions.grid(row=5, column=0, columnspan=2, sticky="ew")
        schedule_actions.columnconfigure((0, 1, 2), weight=1)
        self.ttk.Button(
            schedule_actions,
            text="Install / Update",
            style="Accent.TButton",
            command=self.install_schedule_action,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.ttk.Button(
            schedule_actions,
            text="Refresh Status",
            style="Quiet.TButton",
            command=self.refresh_operations_view,
        ).grid(row=0, column=1, sticky="ew", padx=(0, 8))
        self.ttk.Button(
            schedule_actions,
            text="Remove",
            style="Danger.TButton",
            command=self.remove_schedule_action,
        ).grid(row=0, column=2, sticky="ew")
        schedule_card.columnconfigure(1, weight=1)

        env_card = self.ttk.Frame(right, style="Card.TFrame", padding=18)
        env_card.pack(fill="both", expand=True, pady=(12, 0))
        self.ttk.Label(env_card, text="Schedule Env File", style="CardTitle.TLabel").pack(
            anchor="w"
        )
        self.ttk.Label(
            env_card,
            textvariable=self.env_file_path_var,
            style="CardText.TLabel",
            wraplength=420,
            justify="left",
        ).pack(anchor="w", pady=(4, 10))
        self.env_editor_text = self.tk.Text(
            env_card,
            height=14,
            bg=PALETTE["card"],
            fg=PALETTE["ink"],
            highlightthickness=0,
            bd=0,
            relief="flat",
            wrap="none",
            font=("Cantarell", 10),
        )
        env_scroll = self.ttk.Scrollbar(
            env_card, orient="vertical", command=self.env_editor_text.yview
        )
        self.env_editor_text.configure(yscrollcommand=env_scroll.set)
        self.env_editor_text.pack(side="left", fill="both", expand=True)
        env_scroll.pack(side="right", fill="y")
        env_actions = self.ttk.Frame(right, style="Card.TFrame")
        env_actions.pack(fill="x", pady=(12, 0))
        env_actions.columnconfigure((0, 1), weight=1)
        self.ttk.Button(
            env_actions,
            text="Load Env Template",
            style="Quiet.TButton",
            command=self.load_env_file_action,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.ttk.Button(
            env_actions,
            text="Save Env File",
            style="Accent.TButton",
            command=self.save_env_file_action,
        ).grid(row=0, column=1, sticky="ew")

    def _add_dashboard_row(self, parent: Any, row: int, label: str, variable: Any) -> None:
        self.ttk.Label(parent, text=label).grid(row=row, column=0, sticky="nw", pady=(0, 10))
        self.ttk.Label(
            parent,
            textvariable=variable,
            style="CardText.TLabel",
            wraplength=500,
            justify="left",
        ).grid(row=row, column=1, sticky="nw", pady=(0, 10), padx=(12, 0))

    def refresh_operations_view(self) -> None:
        profile_name = self.profile_name_var.get().strip()
        if not profile_name or profile_name not in self._profile_names():
            self._set_empty_operations_dashboard()
            return

        try:
            status = collect_profile_status(
                profile_name=profile_name,
                config_path=self.config_path,
                unit_dir=self._schedule_unit_dir(),
                env_dir=self._schedule_env_dir(),
            )
            self._render_status_dashboard(status)
        except DBRestoreError as exc:
            self.dashboard_schedule_var.set(str(exc))
            self.dashboard_storage_health_var.set("Unavailable")

        try:
            env_data = load_schedule_env_file(
                profile_name=profile_name,
                config_path=self.config_path,
                env_dir=self._schedule_env_dir(),
            )
            self._populate_env_editor(env_data, append_output=False)
        except DBRestoreError:
            self.env_file_path_var.set("Schedule env file unavailable for this profile.")

    def install_schedule_action(self) -> None:
        if not self._save_if_needed():
            return
        profile_name = self.profile_name_var.get().strip()
        self._run_async(
            f"Installing schedule for '{profile_name}'",
            lambda _progress: install_schedule(
                profile_name=profile_name,
                config_path=self.config_path,
                unit_dir=self._schedule_unit_dir(),
                env_dir=self._schedule_env_dir(),
                force=True,
            ),
            callback=self._handle_schedule_install_completed,
        )

    def remove_schedule_action(self) -> None:
        if not self._save_if_needed():
            return
        profile_name = self.profile_name_var.get().strip()
        self._run_async(
            f"Removing schedule for '{profile_name}'",
            lambda _progress: remove_schedule(
                profile_name=profile_name,
                unit_dir=self._schedule_unit_dir(),
                env_dir=self._schedule_env_dir(),
            ),
            callback=self._handle_schedule_remove_completed,
        )

    def load_env_file_action(self) -> None:
        profile_name = self.profile_name_var.get().strip()
        if not profile_name or profile_name not in self._profile_names():
            self._show_error("Select and save a profile first.")
            return
        try:
            env_data = load_schedule_env_file(
                profile_name=profile_name,
                config_path=self.config_path,
                env_dir=self._schedule_env_dir(),
            )
        except DBRestoreError as exc:
            self._show_error(str(exc))
            return
        self._populate_env_editor(env_data, append_output=True)

    def save_env_file_action(self) -> None:
        profile_name = self.profile_name_var.get().strip()
        if not profile_name or profile_name not in self._profile_names():
            self._show_error("Select and save a profile first.")
            return
        contents = self.env_editor_text.get("1.0", "end")
        try:
            env_data = save_schedule_env_file(
                profile_name=profile_name,
                contents=contents,
                config_path=self.config_path,
                env_dir=self._schedule_env_dir(),
            )
        except DBRestoreError as exc:
            self._show_error(str(exc))
            return
        self._populate_env_editor(env_data, append_output=True)
        self._report_success(f"Saved env file for '{profile_name}'", show_dialog=False)

    def run_preflight_action(self) -> None:
        if not self._save_if_needed():
            return
        profile_name = self.profile_name_var.get().strip()
        self._run_async(
            f"Running preflight for '{profile_name}'",
            lambda _progress: run_profile_preflight(
                profile_name=profile_name,
                config_path=self.config_path,
                unit_dir=self._schedule_unit_dir(),
                env_dir=self._schedule_env_dir(),
                include_connection=bool(self.preflight_include_connection_var.get()),
            ),
            callback=self._handle_preflight_completed,
        )

    def run_scheduled_cycle_action(self) -> None:
        if not self._save_if_needed():
            return
        profile_name = self.profile_name_var.get().strip()
        self._run_async(
            f"Running scheduled cycle for '{profile_name}'",
            lambda progress: run_scheduled_cycle(
                profile_name=profile_name,
                config_path=self.config_path,
                progress=progress,
            ),
            callback=self._handle_scheduled_cycle_completed,
            show_overlay=True,
        )

    def _handle_schedule_install_completed(self, result: dict[str, Any]) -> None:
        self.refresh_operations_view()
        self._append_operations_output(
            f"Installed schedule {result['timer_name']} for '{result['profile']}'"
        )
        self._report_success(f"Installed schedule for '{result['profile']}'", show_dialog=False)

    def _handle_schedule_remove_completed(self, result: dict[str, Any]) -> None:
        self.refresh_operations_view()
        self._append_operations_output(f"Removed schedule for '{result['profile']}'")
        self._report_success(f"Removed schedule for '{result['profile']}'", show_dialog=False)

    def _handle_preflight_completed(self, result: dict[str, Any]) -> None:
        self.refresh_operations_view()
        lines = [f"Preflight for {result['profile']}: {result['status']}"]
        for check in result["checks"]:
            lines.append(f"- {check['name']}: {check['status']} - {check['message']}")
        self._append_operations_output("\n".join(lines))
        self._show_info("\n".join(lines))

    def _handle_scheduled_cycle_completed(self, result: dict[str, Any]) -> None:
        self.refresh_views()
        self._append_operations_output(
            "Scheduled cycle completed "
            f"for {result['profile']} with verification={result['verification_status']}"
        )
        self._report_success(
            f"Scheduled cycle completed for '{result['profile']}'",
            show_dialog=True,
        )

    def _populate_env_editor(self, env_data: dict[str, Any], *, append_output: bool) -> None:
        self.env_file_path_var.set(
            f"{env_data['env_file_path']} | missing values: {', '.join(env_data['missing_vars']) or 'none'}"
        )
        self.env_editor_text.delete("1.0", "end")
        self.env_editor_text.insert("1.0", env_data["text"])
        if append_output:
            self._append_operations_output(
                f"Loaded env file template for '{env_data['profile']}' from {env_data['env_file_path']}"
            )

    def _render_status_dashboard(self, status: dict[str, Any]) -> None:
        last_backup = status["last_backup"]
        if last_backup is not None:
            self.dashboard_last_backup_var.set(
                f"{pretty_timestamp(last_backup.get('finished_at'))} | run {last_backup.get('run_id')}"
            )
        else:
            self.dashboard_last_backup_var.set("No completed backup recorded yet")

        last_verification = status["last_verification"]
        if last_verification is not None:
            payload = last_verification.get("payload", {})
            self.dashboard_last_verification_var.set(
                f"{last_verification['status']} at {last_verification['timestamp']} into "
                f"{payload.get('target_profile') or 'n/a'}"
            )
        elif status["verification"]["configured"]:
            self.dashboard_last_verification_var.set(
                "Verification is configured but has not run yet"
            )
        else:
            self.dashboard_last_verification_var.set("Verification not configured")

        schedule = status["schedule"]
        self.dashboard_next_run_var.set(schedule.get("next_run") or "Not scheduled")
        self.dashboard_schedule_var.set(schedule.get("message") or "No schedule information")
        self.schedule_status_detail_var.set(
            f"Next run: {schedule.get('next_run') or 'n/a'} | "
            f"Last trigger: {schedule.get('last_trigger') or 'n/a'}"
        )

        storage = status["storage"]
        self.dashboard_storage_target_var.set(storage["target"])
        self.dashboard_storage_health_var.set(storage["health"].get("message", "Unknown"))

        retention = status["retention"]
        if retention["configured"]:
            self.dashboard_retention_var.set(
                "keep_last="
                f"{retention['keep_last']}, max_age_days={retention['max_age_days']}, "
                f"total_runs={retention['total_runs']}, pending_delete={retention['pending_delete_count']}"
            )
        else:
            self.dashboard_retention_var.set(
                f"No retention policy configured (total runs: {retention['total_runs']})"
            )

    def _set_empty_operations_dashboard(self) -> None:
        self.dashboard_last_backup_var.set("Select a saved profile to see readiness data")
        self.dashboard_last_verification_var.set("Select a saved profile to see readiness data")
        self.dashboard_next_run_var.set("Select a saved profile to see readiness data")
        self.dashboard_storage_target_var.set("Select a saved profile to see readiness data")
        self.dashboard_storage_health_var.set("Select a saved profile to see readiness data")
        self.dashboard_retention_var.set("Select a saved profile to see readiness data")
        self.dashboard_schedule_var.set("Select a saved profile to see readiness data")
        self.schedule_status_detail_var.set("No schedule status loaded")
        self.env_file_path_var.set("No env file loaded")
        if hasattr(self, "env_editor_text"):
            self.env_editor_text.delete("1.0", "end")

    def _append_operations_output(self, message: str) -> None:
        self.operations_text.insert("end", f"{message}\n\n")
        self.operations_text.see("end")

    def _schedule_unit_dir(self) -> Path:
        return Path(self.schedule_unit_dir_var.get().strip() or str(DEFAULT_SYSTEMD_UNIT_DIR))

    def _schedule_env_dir(self) -> Path:
        return Path(self.schedule_env_dir_var.get().strip() or str(DEFAULT_ENV_DIR))
