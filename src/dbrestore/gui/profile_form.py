"""Profile editor and primary backup/restore controls."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

from dbrestore.config import read_raw_config, validate_raw_config_data, write_raw_config
from dbrestore.errors import DBRestoreError
from dbrestore.operations import (
    run_backup,
    run_restore,
    run_test_connection_with_config,
    run_verify_latest_backup,
    validate_profile_config,
)

from .base import GUIBoundMixin
from .helpers import (
    collect_retention_block,
    normalize_db_type_label,
    profile_compression_label,
    set_widget_state,
    stringify_optional,
)


class ProfileFormMixin(GUIBoundMixin):
    def _build_profile_tab(self, parent: Any) -> None:
        left = self.ttk.Frame(parent, style="Card.TFrame", padding=18)
        left.pack(side="left", fill="both", expand=True, padx=(0, 12))
        right = self.ttk.Frame(parent, style="Card.TFrame", padding=18)
        right.pack(side="left", fill="both", expand=True)

        self.ttk.Label(left, text="Workspace Defaults", style="CardTitle.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w"
        )
        self.ttk.Label(
            left,
            text="These settings apply unless a profile overrides them.",
            style="CardText.TLabel",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 14))

        self._add_labeled_entry(left, 2, "Output Directory", self.defaults_output_dir_var)
        self._add_labeled_entry(left, 3, "Log Directory", self.defaults_log_dir_var)
        self._add_labeled_combo(
            left, 4, "Compression", self.defaults_compression_var, ["gzip", "none"]
        )
        self._add_labeled_entry(
            left, 5, "Retention Keep Last", self.defaults_retention_keep_last_var
        )
        self._add_labeled_entry(
            left, 6, "Retention Max Age Days", self.defaults_retention_max_age_var
        )

        self.ttk.Label(right, text="Profile", style="CardTitle.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w"
        )
        self.ttk.Label(
            right,
            text="Edit one database profile and run actions against it.",
            style="CardText.TLabel",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 14))

        self._add_labeled_entry(right, 2, "Profile Name", self.profile_name_var)
        self._add_labeled_combo(
            right,
            3,
            "DB Type",
            self.db_type_var,
            ["postgres", "mysql", "mariadb", "mongo", "sqlite"],
            callback=self._sync_db_type_state,
        )
        self.host_entry = self._add_labeled_entry(right, 4, "Host", self.host_var)
        self.port_entry = self._add_labeled_entry(right, 5, "Port", self.port_var)
        self.username_entry = self._add_labeled_entry(right, 6, "Username", self.username_var)
        self.password_entry = self._add_labeled_entry(
            right, 7, "Password / Env Ref", self.password_var, show="*"
        )
        self._add_labeled_entry(right, 8, "Database", self.database_var)
        self.auth_database_entry = self._add_labeled_entry(
            right, 9, "Auth Database", self.auth_database_var
        )
        self._add_labeled_entry(right, 10, "Output Directory Override", self.profile_output_dir_var)
        self._add_labeled_combo(
            right,
            11,
            "Compression Override",
            self.profile_compression_var,
            ["inherit", "gzip", "none"],
        )
        self._add_labeled_combo(
            right,
            12,
            "Schedule Preset",
            self.schedule_preset_var,
            ["", "hourly", "daily", "weekly"],
        )
        schedule_label = self.ttk.Label(
            right,
            text="Persistent catch-up is enabled when schedule is set.",
            style="CardText.TLabel",
        )
        schedule_label.grid(row=13, column=0, sticky="w", pady=(4, 0))
        self.ttk.Checkbutton(
            right, text="Persistent Catch-Up", variable=self.schedule_persistent_var
        ).grid(row=13, column=1, sticky="w", pady=(4, 0))
        self._add_labeled_entry(right, 14, "Retention Keep Last", self.retention_keep_last_var)
        self._add_labeled_entry(right, 15, "Retention Max Age Days", self.retention_max_age_var)

        action_bar = self.ttk.Frame(right, style="Card.TFrame")
        action_bar.grid(row=16, column=0, columnspan=2, sticky="ew", pady=(18, 0))
        action_bar.columnconfigure((0, 1, 2), weight=1)
        self.ttk.Button(
            action_bar, text="Save Profile", style="Accent.TButton", command=self.save_profile
        ).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.ttk.Button(
            action_bar,
            text="Validate Profile",
            style="Quiet.TButton",
            command=self.validate_profile,
        ).grid(row=0, column=1, sticky="ew", padx=(0, 8))
        self.ttk.Button(
            action_bar, text="Test Connection", style="Quiet.TButton", command=self.test_connection
        ).grid(row=0, column=2, sticky="ew")

        actions_card = self.ttk.Frame(right, style="Card.TFrame")
        actions_card.grid(row=17, column=0, columnspan=2, sticky="ew", pady=(14, 0))
        self.ttk.Label(actions_card, text="Backup And Restore", style="CardTitle.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w"
        )
        self.ttk.Label(
            actions_card,
            text="Run a backup now, restore a backup, or wire a verification target for the confidence loop.",
            style="CardText.TLabel",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 12))
        self.profile_restore_combo = self._add_labeled_combo(
            actions_card,
            2,
            "Restore Source",
            self.restore_choice_var,
            [],
        )
        self.ttk.Label(actions_card, textvariable=self.restore_filter_label_var).grid(
            row=3,
            column=0,
            sticky="w",
            pady=(0, 10),
            padx=(0, 12),
        )
        self.restore_filter_entry = self.ttk.Entry(
            actions_card, textvariable=self.restore_filter_var
        )
        self.restore_filter_entry.grid(row=3, column=1, sticky="ew", pady=(0, 10))
        self.ttk.Label(
            actions_card,
            textvariable=self.restore_filter_hint_var,
            style="CardText.TLabel",
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(0, 8))
        self.verify_target_combo = self._add_labeled_combo(
            actions_card,
            5,
            "Verify Into Profile",
            self.verify_target_profile_var,
            [],
        )
        self.ttk.Label(
            actions_card,
            textvariable=self.verify_hint_var,
            style="CardText.TLabel",
        ).grid(row=6, column=0, columnspan=2, sticky="w", pady=(4, 0))
        self.ttk.Checkbutton(
            actions_card,
            text="Run verification after scheduled backups",
            variable=self.verify_schedule_after_backup_var,
        ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(8, 0))

        action_bar_2 = self.ttk.Frame(actions_card, style="Card.TFrame")
        action_bar_2.grid(row=8, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        action_bar_2.columnconfigure((0, 1), weight=1)
        self.ttk.Button(
            action_bar_2, text="Run Backup", style="Accent.TButton", command=self.run_backup_action
        ).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.restore_button = self.ttk.Button(
            action_bar_2,
            text="Restore Selected Backup",
            style="Danger.TButton",
            command=self.restore_profile_backup,
        )
        self.restore_button.grid(row=0, column=1, sticky="ew")
        action_bar_3 = self.ttk.Frame(actions_card, style="Card.TFrame")
        action_bar_3.grid(row=9, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        action_bar_3.columnconfigure((0, 1), weight=1)
        self.verify_button = self.ttk.Button(
            action_bar_3,
            text="Verify Latest Backup",
            style="Quiet.TButton",
            command=self.verify_latest_backup_action,
        )
        self.verify_button.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.ttk.Button(
            action_bar_3, text="Refresh Lists", style="Quiet.TButton", command=self.refresh_views
        ).grid(row=0, column=1, sticky="ew")

        for frame in (left, right):
            frame.columnconfigure(1, weight=1)
        actions_card.columnconfigure(1, weight=1)

    def _add_labeled_entry(
        self, parent: Any, row: int, label: str, variable: Any, show: str | None = None
    ) -> Any:
        self.ttk.Label(parent, text=label).grid(
            row=row, column=0, sticky="w", pady=(0, 10), padx=(0, 12)
        )
        entry = self.ttk.Entry(parent, textvariable=variable, show=show or "")
        entry.grid(row=row, column=1, sticky="ew", pady=(0, 10))
        return entry

    def _add_labeled_combo(
        self,
        parent: Any,
        row: int,
        label: str,
        variable: Any,
        values: list[str],
        callback: Any | None = None,
    ) -> Any:
        self.ttk.Label(parent, text=label).grid(
            row=row, column=0, sticky="w", pady=(0, 10), padx=(0, 12)
        )
        combo = self.ttk.Combobox(parent, textvariable=variable, values=values, state="readonly")
        combo.grid(row=row, column=1, sticky="ew", pady=(0, 10))
        if callback is not None:
            combo.bind("<<ComboboxSelected>>", callback)
        return combo

    def reload_config(self, *, select_first: bool) -> None:
        previous_selection = self.selected_profile_name
        if self.config_path.exists():
            _, self.raw_config = read_raw_config(self.config_path)
        else:
            self.raw_config = self._default_raw_config()
        self._populate_defaults_from_raw()
        self._refresh_profile_list(previous_selection if not select_first else None)
        if select_first and self.profile_listbox.size() > 0:
            self.profile_listbox.selection_set(0)
            self._on_profile_selected()
        elif previous_selection and previous_selection in self._profile_names():
            index = self._profile_names().index(previous_selection)
            self.profile_listbox.selection_clear(0, "end")
            self.profile_listbox.selection_set(index)
            self._on_profile_selected()
        else:
            self.prepare_new_profile()
        self.refresh_views()
        self._append_status(f"Loaded config from {self.config_path}")

    def prepare_new_profile(self) -> None:
        self.selected_profile_name = None
        self.profile_listbox.selection_clear(0, "end")
        self.profile_name_var.set("")
        self.db_type_var.set("postgres")
        self.host_var.set("")
        self.port_var.set("")
        self.username_var.set("")
        self.password_var.set("")
        self.database_var.set("")
        self.auth_database_var.set("")
        self.profile_output_dir_var.set("")
        self.profile_compression_var.set("inherit")
        self.schedule_preset_var.set("")
        self.schedule_persistent_var.set(True)
        self.retention_keep_last_var.set("")
        self.retention_max_age_var.set("")
        self.restore_choice_var.set("")
        self.verify_target_profile_var.set("")
        self.verify_schedule_after_backup_var.set(True)
        self._refresh_verification_targets()
        self._sync_db_type_state()
        self.status_var.set("Editing new profile")

    def save_profile(self) -> bool:
        profile_name = self.profile_name_var.get().strip()
        if not profile_name:
            self._show_error("Profile name is required.")
            return False

        try:
            candidate = self._build_candidate_config(profile_name)
            validate_raw_config_data(candidate, source_path=self.config_path, require_env=False)
        except (DBRestoreError, ValueError) as exc:
            self._show_error(str(exc))
            return False

        write_raw_config(self.config_path, candidate)
        self.raw_config = candidate
        self.selected_profile_name = profile_name
        self._refresh_profile_list(profile_name)
        self._refresh_verification_targets()
        self._append_status(f"Saved profile '{profile_name}'")
        self.status_var.set(f"Saved profile '{profile_name}'")
        return True

    def delete_profile(self) -> None:
        from tkinter import messagebox

        profile_name = self.profile_name_var.get().strip()
        if not profile_name:
            self._show_error("No profile selected.")
            return
        if profile_name not in self.raw_config.get("profiles", {}):
            self.prepare_new_profile()
            return
        if not messagebox.askyesno(
            "Delete Profile", f"Delete profile '{profile_name}' from the config?"
        ):
            return

        candidate = dict(self.raw_config)
        profiles = dict(candidate.get("profiles", {}))
        profiles.pop(profile_name, None)
        candidate["profiles"] = profiles
        write_raw_config(self.config_path, candidate)
        self.raw_config = candidate
        self.prepare_new_profile()
        self._refresh_profile_list(None)
        self._refresh_verification_targets()
        self.refresh_views()
        self._append_status(f"Deleted profile '{profile_name}'")

    def validate_profile(self) -> None:
        try:
            profile_name, config = self._build_selected_profile_runtime_config(
                require_env=True,
                include_output_default=True,
            )
        except (DBRestoreError, ValueError) as exc:
            self._show_error(str(exc))
            return
        self._run_async(
            f"Validating profile '{profile_name}'",
            lambda _progress: validate_profile_config(config, profile_name),
            callback=lambda result: self._report_success(
                f"Profile '{result['profile']}' is valid",
                show_dialog=True,
            ),
        )

    def test_connection(self) -> None:
        try:
            profile_name, config = self._build_selected_profile_runtime_config(
                require_env=True,
                include_output_default=False,
            )
        except (DBRestoreError, ValueError) as exc:
            self._show_error(str(exc))
            return
        self._run_async(
            f"Testing connection for '{profile_name}'",
            lambda _progress: run_test_connection_with_config(config, profile_name),
            callback=lambda result: self._report_success(
                f"Connection succeeded for profile '{result['profile']}'",
                show_dialog=True,
            ),
        )

    def run_backup_action(self) -> None:
        if not self._save_if_needed():
            return
        profile_name = self.profile_name_var.get().strip()
        self._run_async(
            f"Running backup for '{profile_name}'",
            lambda progress: run_backup(
                profile_name=profile_name,
                config_path=self.config_path,
                progress=progress,
            ),
            callback=self._handle_backup_completed,
            show_overlay=True,
        )

    def restore_profile_backup(self) -> None:
        if not self._save_if_needed():
            return
        choice = self.restore_choice_var.get().strip()
        if not choice:
            self._show_error("Select a backup to restore.")
            return
        try:
            record = self._restore_choice_map()[choice]
        except KeyError:
            self._show_error("Select a valid backup to restore.")
            return
        self._confirm_and_restore_record(record)

    def verify_latest_backup_action(self) -> None:
        if not self._save_if_needed():
            return
        source_profile = self.profile_name_var.get().strip()
        target_profile = self.verify_target_profile_var.get().strip()
        if not target_profile or target_profile not in self._profile_names():
            self._show_error("Select a verification target profile.")
            return
        self._run_async(
            f"Verifying latest backup from '{source_profile}' into '{target_profile}'",
            lambda progress: run_verify_latest_backup(
                source_profile_name=source_profile,
                target_profile_name=target_profile,
                config_path=self.config_path,
                progress=progress,
            ),
            callback=self._handle_verification_completed,
            show_overlay=True,
        )

    def _confirm_and_restore_record(self, record: dict[str, Any]) -> None:
        from tkinter import messagebox

        profile_name = self.profile_name_var.get().strip()
        prompt = f"Restore backup '{record['run_id']}' into profile '{profile_name}'?"
        if not messagebox.askyesno("Restore Backup", prompt):
            return
        self._run_async(
            f"Restoring '{record['run_id']}' into '{profile_name}'",
            lambda progress: run_restore(
                profile_name=profile_name,
                input_path=Path(record["run_dir"]),
                config_path=self.config_path,
                tables=self._restore_filter_values()
                if normalize_db_type_label(self.db_type_var.get().strip()) == "postgres"
                else None,
                collections=self._restore_filter_values()
                if normalize_db_type_label(self.db_type_var.get().strip()) == "mongo"
                else None,
                progress=progress,
            ),
            callback=self._handle_restore_completed,
            show_overlay=True,
        )

    def refresh_views(self) -> None:
        self.refresh_backups()
        self.refresh_logs()
        if hasattr(self, "refresh_operations_view"):
            self.refresh_operations_view()

    def open_config_file(self) -> None:
        opener = shutil.which("xdg-open")
        if opener is None:
            self._show_error("xdg-open is not available on this system.")
            return
        subprocess.Popen([opener, str(self.config_path)])

    def _profile_names(self) -> list[str]:
        return sorted(self.raw_config.get("profiles", {}).keys())

    def _refresh_profile_list(self, selected: str | None) -> None:
        names = self._profile_names()
        self.profile_listbox.delete(0, "end")
        for name in names:
            self.profile_listbox.insert("end", name)
        if selected and selected in names:
            index = names.index(selected)
            self.profile_listbox.selection_set(index)

    def _populate_defaults_from_raw(self) -> None:
        defaults = self.raw_config.get("defaults", {})
        self.defaults_output_dir_var.set(defaults.get("output_dir", "./backups"))
        self.defaults_log_dir_var.set(defaults.get("log_dir", "./logs"))
        self.defaults_compression_var.set(defaults.get("compression", "gzip"))
        retention = defaults.get("retention", {})
        self.defaults_retention_keep_last_var.set(stringify_optional(retention.get("keep_last")))
        self.defaults_retention_max_age_var.set(stringify_optional(retention.get("max_age_days")))

    def _on_profile_selected(self, _event: Any | None = None) -> None:
        selection = self.profile_listbox.curselection()
        if not selection:
            return
        profile_name = self.profile_listbox.get(selection[0])
        profile = self.raw_config.get("profiles", {}).get(profile_name, {})
        self.selected_profile_name = profile_name
        self.profile_name_var.set(profile_name)
        self.db_type_var.set(profile.get("db_type", "postgres"))
        self.host_var.set(profile.get("host", ""))
        self.port_var.set(stringify_optional(profile.get("port")))
        self.username_var.set(profile.get("username", ""))
        self.password_var.set(profile.get("password", ""))
        self.database_var.set(profile.get("database", ""))
        self.auth_database_var.set(profile.get("auth_database", ""))
        self.profile_output_dir_var.set(profile.get("output_dir", ""))
        self.profile_compression_var.set(profile_compression_label(profile.get("compression")))
        schedule = profile.get("schedule", {})
        self.schedule_preset_var.set(schedule.get("preset") or schedule.get("on_calendar") or "")
        self.schedule_persistent_var.set(bool(schedule.get("persistent", True)))
        retention = profile.get("retention", {})
        self.retention_keep_last_var.set(stringify_optional(retention.get("keep_last")))
        self.retention_max_age_var.set(stringify_optional(retention.get("max_age_days")))
        verification = profile.get("verification", {})
        self.verify_target_profile_var.set(verification.get("target_profile", ""))
        self.verify_schedule_after_backup_var.set(
            bool(verification.get("schedule_after_backup", True))
        )
        self.restore_filter_var.set("")
        self._sync_db_type_state()
        self.refresh_views()
        self.status_var.set(f"Selected profile '{profile_name}'")
        self._refresh_verification_targets()

    def _sync_db_type_state(self, _event: Any | None = None) -> None:
        db_type = self.db_type_var.get()
        sqlite_only = db_type == "sqlite"
        mongo = db_type == "mongo"
        set_widget_state(self.host_entry, not sqlite_only)
        set_widget_state(self.port_entry, not sqlite_only)
        set_widget_state(self.username_entry, not sqlite_only)
        set_widget_state(self.password_entry, not sqlite_only)
        set_widget_state(self.auth_database_entry, mongo)
        self._refresh_restore_filter_state(db_type)

    def _build_candidate_config(self, profile_name: str) -> dict[str, Any]:
        candidate = {
            "version": 1,
            "defaults": self._collect_defaults_data(),
            "storage": dict(self.raw_config.get("storage", {})),
            "profiles": dict(self.raw_config.get("profiles", {})),
        }

        if self.selected_profile_name and self.selected_profile_name != profile_name:
            candidate["profiles"].pop(self.selected_profile_name, None)

        candidate["profiles"][profile_name] = self._collect_profile_data()
        return candidate

    def _build_selected_profile_runtime_config(
        self,
        *,
        require_env: bool,
        include_output_default: bool,
    ) -> tuple[str, Any]:
        profile_name = self.profile_name_var.get().strip()
        if not profile_name:
            raise DBRestoreError("Profile name is required.")

        raw_config: dict[str, Any] = {
            "version": 1,
            "storage": dict(self.raw_config.get("storage", {})),
            "profiles": {
                profile_name: self._collect_profile_data(),
            },
        }
        if include_output_default and not self.profile_output_dir_var.get().strip():
            raw_config["defaults"] = {
                "output_dir": self.defaults_output_dir_var.get().strip() or "./backups",
            }

        config = validate_raw_config_data(
            raw_config,
            source_path=self.config_path,
            require_env=require_env,
        )
        return profile_name, config

    def _collect_defaults_data(self) -> dict[str, Any]:
        defaults: dict[str, Any] = {
            "output_dir": self.defaults_output_dir_var.get().strip() or "./backups",
            "log_dir": self.defaults_log_dir_var.get().strip() or "./logs",
            "compression": self.defaults_compression_var.get() or "gzip",
        }
        retention = collect_retention_block(
            keep_last=self.defaults_retention_keep_last_var.get().strip(),
            max_age_days=self.defaults_retention_max_age_var.get().strip(),
        )
        if retention:
            defaults["retention"] = retention
        return defaults

    def _collect_profile_data(self) -> dict[str, Any]:
        db_type = self.db_type_var.get().strip() or "postgres"
        profile: dict[str, Any] = {
            "db_type": db_type,
            "database": self.database_var.get().strip(),
        }
        if db_type != "sqlite":
            if self.host_var.get().strip():
                profile["host"] = self.host_var.get().strip()
            if self.port_var.get().strip():
                profile["port"] = int(self.port_var.get().strip())
            if self.username_var.get().strip():
                profile["username"] = self.username_var.get().strip()
            if self.password_var.get().strip():
                profile["password"] = self.password_var.get().strip()
        if db_type == "mongo" and self.auth_database_var.get().strip():
            profile["auth_database"] = self.auth_database_var.get().strip()
        if self.profile_output_dir_var.get().strip():
            profile["output_dir"] = self.profile_output_dir_var.get().strip()

        compression_override = self.profile_compression_var.get().strip()
        if compression_override == "gzip":
            profile["compression"] = True
        elif compression_override == "none":
            profile["compression"] = False

        schedule_preset = self.schedule_preset_var.get().strip()
        if schedule_preset:
            profile["schedule"] = {
                "preset": schedule_preset,
                "persistent": bool(self.schedule_persistent_var.get()),
            }

        retention = collect_retention_block(
            keep_last=self.retention_keep_last_var.get().strip(),
            max_age_days=self.retention_max_age_var.get().strip(),
        )
        if retention:
            profile["retention"] = retention

        verification_target = self.verify_target_profile_var.get().strip()
        if (
            verification_target
            and verification_target in self._profile_names()
            and verification_target != self.profile_name_var.get().strip()
        ):
            profile["verification"] = {
                "target_profile": verification_target,
                "schedule_after_backup": bool(self.verify_schedule_after_backup_var.get()),
            }

        return profile

    def _save_if_needed(self) -> bool:
        return self.save_profile()

    def _handle_backup_completed(self, result: dict[str, Any]) -> None:
        self.refresh_views()
        self._report_success(
            f"Backup completed for '{result['profile']}'",
            show_dialog=True,
        )

    def _handle_restore_completed(self, result: dict[str, Any]) -> None:
        self.refresh_views()
        self._report_success(
            f"Restore completed for '{result['profile']}'",
            show_dialog=True,
        )

    def _handle_verification_completed(self, result: dict[str, Any]) -> None:
        self.refresh_views()
        self._report_success(
            f"Verification completed for '{result['run_id']}' into '{result['target_profile']}'",
            show_dialog=True,
        )

    def _refresh_verification_targets(self) -> None:
        current_profile = self.profile_name_var.get().strip()
        current_db_type = normalize_db_type_label(self.db_type_var.get().strip())
        options = [
            name
            for name in self._profile_names()
            if name != current_profile
            and normalize_db_type_label(
                self.raw_config.get("profiles", {}).get(name, {}).get("db_type", "")
            )
            == current_db_type
        ]
        if not hasattr(self, "verify_target_combo"):
            return
        self.verify_target_combo.configure(values=options)
        current_target = self.verify_target_profile_var.get()
        if options:
            if current_target not in options:
                self.verify_target_profile_var.set(options[0])
            self.verify_hint_var.set(
                "Use a separate disposable profile as the verification target and persist it here for scheduled confidence checks."
            )
            set_widget_state(self.verify_target_combo, True)
            set_widget_state(self.verify_button, True)
            return

        if current_profile:
            self.verify_target_profile_var.set("Create another matching profile first")
        else:
            self.verify_target_profile_var.set("")
        self.verify_hint_var.set("Verification requires a different profile with the same DB type.")
        set_widget_state(self.verify_target_combo, False)
        set_widget_state(self.verify_button, False)

    def _refresh_restore_filter_state(self, db_type: str) -> None:
        normalized = normalize_db_type_label(db_type)
        if normalized == "postgres":
            self.restore_filter_label_var.set("Restore Tables")
            self.restore_filter_hint_var.set(
                "Optional. Enter comma-separated table names, for example public.items, public.orders."
            )
            set_widget_state(self.restore_filter_entry, True)
            return
        if normalized == "mongo":
            self.restore_filter_label_var.set("Restore Collections")
            self.restore_filter_hint_var.set(
                "Optional. Enter comma-separated collection names. Plain names are prefixed with the database automatically."
            )
            set_widget_state(self.restore_filter_entry, True)
            return
        self.restore_filter_label_var.set("Selective Restore Filter")
        self.restore_filter_hint_var.set(
            "Selective restore is not supported for this database type with the current backup format."
        )
        self.restore_filter_var.set("")
        set_widget_state(self.restore_filter_entry, False)

    def _restore_filter_values(self) -> list[str] | None:
        values = [item.strip() for item in self.restore_filter_var.get().split(",") if item.strip()]
        return values or None
