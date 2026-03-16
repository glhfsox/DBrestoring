from __future__ import annotations

import argparse
import queue
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Any, Callable

from dbrestore.config import DB_TYPE_ALIASES, DEFAULT_CONFIG_PATH, read_raw_config, validate_raw_config_data, write_raw_config
from dbrestore.errors import DBRestoreError
from dbrestore.operations import (
    list_backup_history,
    list_run_log_events,
    run_test_connection_with_config,
    run_backup,
    run_restore,
    run_verify_latest_backup,
    validate_profile_config,
)
from dbrestore.utils import format_display_timestamp, parse_timestamp


def launch_gui(config_path: Path = DEFAULT_CONFIG_PATH) -> None:
    try:
        import tkinter as tk
        from tkinter import ttk
    except ImportError as exc:
        raise DBRestoreError("Tkinter is not available. Install python3-tk to use the GUI.") from exc

    root = tk.Tk()
    app = DBRestoreGUI(root=root, config_path=config_path)
    app.run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the dbrestore desktop GUI.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to YAML configuration.")
    args = parser.parse_args()
    launch_gui(Path(args.config))


class DBRestoreGUI:
    def __init__(self, root: Any, config_path: Path) -> None:
        import tkinter as tk
        from tkinter import ttk

        self.tk = tk
        self.ttk = ttk
        self.root = root
        self.config_path = Path(config_path).expanduser().resolve()
        self.raw_config = _default_raw_config()
        self.selected_profile_name: str | None = None
        self.backup_rows: list[dict[str, Any]] = []
        self.event_queue: queue.Queue[tuple[str, str, Any, Callable[[Any], None] | None]] = queue.Queue()
        self.busy = False

        self.root.title("dbrestore")
        self.root.geometry("1280x840")
        self.root.minsize(1120, 720)
        self.root.configure(bg=PALETTE["canvas"])

        self._init_variables()
        self._configure_styles()
        self._build_layout()
        self.reload_config(select_first=True)
        self._poll_events()

    def run(self) -> None:
        self.root.mainloop()

    def _init_variables(self) -> None:
        self.profile_name_var = self.tk.StringVar()
        self.db_type_var = self.tk.StringVar(value="postgres")
        self.host_var = self.tk.StringVar()
        self.port_var = self.tk.StringVar()
        self.username_var = self.tk.StringVar()
        self.password_var = self.tk.StringVar()
        self.database_var = self.tk.StringVar()
        self.auth_database_var = self.tk.StringVar()
        self.profile_output_dir_var = self.tk.StringVar()
        self.profile_compression_var = self.tk.StringVar(value="inherit")
        self.schedule_preset_var = self.tk.StringVar()
        self.schedule_persistent_var = self.tk.BooleanVar(value=True)
        self.retention_keep_last_var = self.tk.StringVar()
        self.retention_max_age_var = self.tk.StringVar()

        self.defaults_output_dir_var = self.tk.StringVar()
        self.defaults_log_dir_var = self.tk.StringVar()
        self.defaults_compression_var = self.tk.StringVar(value="gzip")
        self.defaults_retention_keep_last_var = self.tk.StringVar()
        self.defaults_retention_max_age_var = self.tk.StringVar()
        self.restore_choice_var = self.tk.StringVar()
        self.restore_filter_var = self.tk.StringVar()
        self.restore_filter_label_var = self.tk.StringVar(value="Selective Restore Filter")
        self.restore_filter_hint_var = self.tk.StringVar(value="")
        self.verify_target_profile_var = self.tk.StringVar()
        self.verify_hint_var = self.tk.StringVar(
            value="Use a separate disposable profile as the verification target."
        )

        self.status_var = self.tk.StringVar(value="Ready")

    def _configure_styles(self) -> None:
        style = self.ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except self.tk.TclError:
            pass

        style.configure(".", background=PALETTE["canvas"], foreground=PALETTE["ink"], font=("Cantarell", 10))
        style.configure("App.TFrame", background=PALETTE["canvas"])
        style.configure("Card.TFrame", background=PALETTE["card"], relief="flat")
        style.configure("Header.TLabel", background=PALETTE["canvas"], foreground=PALETTE["ink"], font=("Cantarell", 23, "bold"))
        style.configure("Subtle.TLabel", background=PALETTE["canvas"], foreground=PALETTE["muted"], font=("Cantarell", 10))
        style.configure("CardTitle.TLabel", background=PALETTE["card"], foreground=PALETTE["ink"], font=("Cantarell", 12, "bold"))
        style.configure("CardText.TLabel", background=PALETTE["card"], foreground=PALETTE["muted"], font=("Cantarell", 10))
        style.configure("TLabel", background=PALETTE["card"], foreground=PALETTE["ink"])
        style.configure("TEntry", fieldbackground=PALETTE["field"], foreground=PALETTE["ink"], bordercolor=PALETTE["border"], lightcolor=PALETTE["field"], darkcolor=PALETTE["field"], padding=6)
        style.map("TEntry", fieldbackground=[("disabled", PALETTE["canvas"])])
        style.configure("TCombobox", fieldbackground=PALETTE["field"], background=PALETTE["field"], foreground=PALETTE["ink"], bordercolor=PALETTE["border"], arrowsize=14, padding=4)
        style.configure("Accent.TButton", background=PALETTE["accent"], foreground="white", padding=(10, 7), borderwidth=0)
        style.map("Accent.TButton", background=[("active", PALETTE["accent_dark"])])
        style.configure("Quiet.TButton", background=PALETTE["field"], foreground=PALETTE["ink"], padding=(10, 7), borderwidth=0)
        style.map("Quiet.TButton", background=[("active", PALETTE["field_alt"])])
        style.configure("Danger.TButton", background=PALETTE["danger"], foreground="white", padding=(10, 7), borderwidth=0)
        style.map("Danger.TButton", background=[("active", "#9B2C2C")])
        style.configure("TCheckbutton", background=PALETTE["card"], foreground=PALETTE["ink"])
        style.configure("Side.TLabelframe", background=PALETTE["card"], foreground=PALETTE["ink"])
        style.configure("Side.TLabelframe.Label", background=PALETTE["card"], foreground=PALETTE["muted"], font=("Cantarell", 9, "bold"))
        style.configure("Main.TNotebook", background=PALETTE["canvas"], borderwidth=0)
        style.configure("Main.TNotebook.Tab", padding=(12, 8), background=PALETTE["field"], foreground=PALETTE["muted"])
        style.map("Main.TNotebook.Tab", background=[("selected", PALETTE["card"])], foreground=[("selected", PALETTE["ink"])])
        style.configure("History.Treeview", background=PALETTE["card"], fieldbackground=PALETTE["card"], foreground=PALETTE["ink"], rowheight=28, borderwidth=0)
        style.configure("History.Treeview.Heading", background=PALETTE["field"], foreground=PALETTE["muted"], font=("Cantarell", 10, "bold"), padding=(6, 6))
        style.map("History.Treeview", background=[("selected", PALETTE["accent_soft"])], foreground=[("selected", PALETTE["ink"])])

    def _build_layout(self) -> None:
        header = self.ttk.Frame(self.root, style="App.TFrame", padding=(22, 18, 22, 12))
        header.pack(fill="x")
        self.ttk.Label(header, text="dbrestore", style="Header.TLabel").grid(row=0, column=0, sticky="w")
        self.ttk.Label(header, text=f"Config: {self.config_path}", style="Subtle.TLabel").grid(row=1, column=0, sticky="w", pady=(4, 0))
        button_bar = self.ttk.Frame(header, style="App.TFrame")
        button_bar.grid(row=0, column=1, rowspan=2, sticky="e")
        self.ttk.Button(button_bar, text="Reload", style="Quiet.TButton", command=lambda: self.reload_config(select_first=False)).pack(side="left", padx=(0, 8))
        self.ttk.Button(button_bar, text="Open Config", style="Quiet.TButton", command=self.open_config_file).pack(side="left")
        header.columnconfigure(0, weight=1)

        content = self.ttk.Frame(self.root, style="App.TFrame", padding=(22, 0, 22, 22))
        content.pack(fill="both", expand=True)

        sidebar = self.ttk.Frame(content, style="Card.TFrame", padding=16)
        sidebar.pack(side="left", fill="y", padx=(0, 16))
        self.ttk.Label(sidebar, text="Profiles", style="CardTitle.TLabel").pack(anchor="w")
        self.ttk.Label(sidebar, text="Select one profile to edit or run.", style="CardText.TLabel").pack(anchor="w", pady=(4, 12))

        list_frame = self.ttk.Frame(sidebar, style="Card.TFrame")
        list_frame.pack(fill="both", expand=True)
        self.profile_listbox = self.tk.Listbox(
            list_frame,
            bg=PALETTE["field"],
            fg=PALETTE["ink"],
            selectbackground=PALETTE["accent"],
            selectforeground="white",
            highlightthickness=0,
            bd=0,
            relief="flat",
            font=("Cantarell", 11),
            activestyle="none",
        )
        profile_scroll = self.ttk.Scrollbar(list_frame, orient="vertical", command=self.profile_listbox.yview)
        self.profile_listbox.configure(yscrollcommand=profile_scroll.set)
        self.profile_listbox.pack(side="left", fill="both", expand=True)
        profile_scroll.pack(side="right", fill="y")
        self.profile_listbox.bind("<<ListboxSelect>>", self._on_profile_selected)

        sidebar_actions = self.ttk.Frame(sidebar, style="Card.TFrame")
        sidebar_actions.pack(fill="x", pady=(12, 0))
        self.ttk.Button(sidebar_actions, text="New Profile", style="Quiet.TButton", command=self.prepare_new_profile).pack(fill="x")
        self.ttk.Button(sidebar_actions, text="Delete Profile", style="Danger.TButton", command=self.delete_profile).pack(fill="x", pady=(8, 0))

        notes = self.ttk.LabelFrame(sidebar, text="Notes", style="Side.TLabelframe", padding=12)
        notes.pack(fill="x", pady=(16, 0))
        note_text = (
            "Password accepts either plaintext or an env reference like ${PGPASSWORD}.\n"
            "Validate/Test use the current form values. Export env refs before launching the GUI.\n"
            "Schedule installation still uses the CLI because system-wide units need elevated privileges."
        )
        self.ttk.Label(notes, text=note_text, style="CardText.TLabel", wraplength=240, justify="left").pack(anchor="w")

        main = self.ttk.Frame(content, style="App.TFrame")
        main.pack(side="left", fill="both", expand=True)

        self.notebook = self.ttk.Notebook(main, style="Main.TNotebook")
        self.notebook.pack(fill="both", expand=True)

        profile_tab = self.ttk.Frame(self.notebook, style="App.TFrame", padding=(0, 8, 0, 0))
        backups_tab = self.ttk.Frame(self.notebook, style="App.TFrame", padding=(0, 8, 0, 0))
        activity_tab = self.ttk.Frame(self.notebook, style="App.TFrame", padding=(0, 8, 0, 0))
        self.notebook.add(profile_tab, text="Profile")
        self.notebook.add(backups_tab, text="Backups")
        self.notebook.add(activity_tab, text="Activity")

        self.profile_tab = profile_tab
        self.profile_scroll_canvas, profile_content = self._build_scrollable_tab(profile_tab)
        self._build_profile_tab(profile_content)
        self._build_backups_tab(backups_tab)
        self._build_activity_tab(activity_tab)
        self._bind_global_scroll_handlers()

        status_frame = self.ttk.Frame(self.root, style="Card.TFrame", padding=(22, 12, 22, 18))
        status_frame.pack(fill="x")
        self.ttk.Label(status_frame, textvariable=self.status_var, style="CardTitle.TLabel").pack(anchor="w")
        self.status_text = self.tk.Text(
            status_frame,
            height=8,
            bg=PALETTE["card"],
            fg=PALETTE["ink"],
            highlightthickness=0,
            bd=0,
            relief="flat",
            wrap="word",
            font=("Cantarell", 10),
        )
        status_scroll = self.ttk.Scrollbar(status_frame, orient="vertical", command=self.status_text.yview)
        self.status_text.configure(yscrollcommand=status_scroll.set)
        self.status_text.pack(side="left", fill="both", expand=True, pady=(8, 0))
        status_scroll.pack(side="right", fill="y", pady=(8, 0))

    def _build_scrollable_tab(self, parent: Any) -> tuple[Any, Any]:
        canvas = self.tk.Canvas(
            parent,
            bg=PALETTE["canvas"],
            highlightthickness=0,
            bd=0,
            relief="flat",
        )
        scrollbar = self.ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        content = self.ttk.Frame(canvas, style="App.TFrame")
        window_id = canvas.create_window((0, 0), window=content, anchor="nw")

        def sync_scroll_region(_event: Any | None = None) -> None:
            canvas.configure(scrollregion=canvas.bbox("all"))

        def sync_content_width(event: Any) -> None:
            canvas.itemconfigure(window_id, width=event.width)

        content.bind("<Configure>", sync_scroll_region)
        canvas.bind("<Configure>", sync_content_width)
        return canvas, content

    def _bind_global_scroll_handlers(self) -> None:
        self.root.bind_all("<MouseWheel>", self._on_global_mousewheel, add="+")
        self.root.bind_all("<Button-4>", self._on_global_mousewheel, add="+")
        self.root.bind_all("<Button-5>", self._on_global_mousewheel, add="+")

    def _build_profile_tab(self, parent: Any) -> None:
        left = self.ttk.Frame(parent, style="Card.TFrame", padding=18)
        left.pack(side="left", fill="both", expand=True, padx=(0, 12))
        right = self.ttk.Frame(parent, style="Card.TFrame", padding=18)
        right.pack(side="left", fill="both", expand=True)

        self.ttk.Label(left, text="Workspace Defaults", style="CardTitle.TLabel").grid(row=0, column=0, columnspan=2, sticky="w")
        self.ttk.Label(left, text="These settings apply unless a profile overrides them.", style="CardText.TLabel").grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 14))

        self._add_labeled_entry(left, 2, "Output Directory", self.defaults_output_dir_var)
        self._add_labeled_entry(left, 3, "Log Directory", self.defaults_log_dir_var)
        self._add_labeled_combo(left, 4, "Compression", self.defaults_compression_var, ["gzip", "none"])
        self._add_labeled_entry(left, 5, "Retention Keep Last", self.defaults_retention_keep_last_var)
        self._add_labeled_entry(left, 6, "Retention Max Age Days", self.defaults_retention_max_age_var)

        self.ttk.Label(right, text="Profile", style="CardTitle.TLabel").grid(row=0, column=0, columnspan=2, sticky="w")
        self.ttk.Label(right, text="Edit one database profile and run actions against it.", style="CardText.TLabel").grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 14))

        self._add_labeled_entry(right, 2, "Profile Name", self.profile_name_var)
        self._add_labeled_combo(right, 3, "DB Type", self.db_type_var, ["postgres", "mysql", "mariadb", "mongo", "sqlite"], callback=self._sync_db_type_state)
        self.host_entry = self._add_labeled_entry(right, 4, "Host", self.host_var)
        self.port_entry = self._add_labeled_entry(right, 5, "Port", self.port_var)
        self.username_entry = self._add_labeled_entry(right, 6, "Username", self.username_var)
        self.password_entry = self._add_labeled_entry(right, 7, "Password / Env Ref", self.password_var, show="*")
        self._add_labeled_entry(right, 8, "Database", self.database_var)
        self.auth_database_entry = self._add_labeled_entry(right, 9, "Auth Database", self.auth_database_var)
        self._add_labeled_entry(right, 10, "Output Directory Override", self.profile_output_dir_var)
        self._add_labeled_combo(right, 11, "Compression Override", self.profile_compression_var, ["inherit", "gzip", "none"])
        self._add_labeled_combo(right, 12, "Schedule Preset", self.schedule_preset_var, ["", "hourly", "daily", "weekly"])
        schedule_label = self.ttk.Label(right, text="Persistent catch-up is enabled when schedule is set.", style="CardText.TLabel")
        schedule_label.grid(row=13, column=0, sticky="w", pady=(4, 0))
        self.ttk.Checkbutton(right, text="Persistent Catch-Up", variable=self.schedule_persistent_var).grid(row=13, column=1, sticky="w", pady=(4, 0))
        self._add_labeled_entry(right, 14, "Retention Keep Last", self.retention_keep_last_var)
        self._add_labeled_entry(right, 15, "Retention Max Age Days", self.retention_max_age_var)

        action_bar = self.ttk.Frame(right, style="Card.TFrame")
        action_bar.grid(row=16, column=0, columnspan=2, sticky="ew", pady=(18, 0))
        action_bar.columnconfigure((0, 1, 2), weight=1)
        self.ttk.Button(action_bar, text="Save Profile", style="Accent.TButton", command=self.save_profile).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.ttk.Button(action_bar, text="Validate Profile", style="Quiet.TButton", command=self.validate_profile).grid(row=0, column=1, sticky="ew", padx=(0, 8))
        self.ttk.Button(action_bar, text="Test Connection", style="Quiet.TButton", command=self.test_connection).grid(row=0, column=2, sticky="ew")

        actions_card = self.ttk.Frame(right, style="Card.TFrame")
        actions_card.grid(row=17, column=0, columnspan=2, sticky="ew", pady=(14, 0))
        self.ttk.Label(actions_card, text="Backup And Restore", style="CardTitle.TLabel").grid(row=0, column=0, columnspan=2, sticky="w")
        self.ttk.Label(
            actions_card,
            text="Run a backup now or restore one of this profile's recent backups.",
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
        self.restore_filter_entry = self.ttk.Entry(actions_card, textvariable=self.restore_filter_var)
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

        action_bar_2 = self.ttk.Frame(actions_card, style="Card.TFrame")
        action_bar_2.grid(row=7, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        action_bar_2.columnconfigure((0, 1), weight=1)
        self.ttk.Button(action_bar_2, text="Run Backup", style="Accent.TButton", command=self.run_backup_action).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.restore_button = self.ttk.Button(
            action_bar_2,
            text="Restore Selected Backup",
            style="Danger.TButton",
            command=self.restore_profile_backup,
        )
        self.restore_button.grid(row=0, column=1, sticky="ew")
        action_bar_3 = self.ttk.Frame(actions_card, style="Card.TFrame")
        action_bar_3.grid(row=8, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        action_bar_3.columnconfigure((0, 1), weight=1)
        self.verify_button = self.ttk.Button(
            action_bar_3,
            text="Verify Latest Backup",
            style="Quiet.TButton",
            command=self.verify_latest_backup_action,
        )
        self.verify_button.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self.ttk.Button(action_bar_3, text="Refresh Lists", style="Quiet.TButton", command=self.refresh_views).grid(row=0, column=1, sticky="ew")

        for frame in (left, right):
            frame.columnconfigure(1, weight=1)
        actions_card.columnconfigure(1, weight=1)

    def _build_backups_tab(self, parent: Any) -> None:
        card = self.ttk.Frame(parent, style="Card.TFrame", padding=18)
        card.pack(fill="both", expand=True)
        header = self.ttk.Frame(card, style="Card.TFrame")
        header.pack(fill="x")
        self.ttk.Label(header, text="Backup History", style="CardTitle.TLabel").pack(side="left")
        self.ttk.Button(header, text="Refresh", style="Quiet.TButton", command=self.refresh_backups).pack(side="right")

        columns = ("finished_at", "run_id", "compression", "artifact_path")
        self.backup_tree = self.ttk.Treeview(card, columns=columns, show="headings", style="History.Treeview", selectmode="browse")
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
        self.ttk.Button(actions, text="Open Folder", style="Quiet.TButton", command=self.open_selected_backup_folder).pack(side="left")
        self.ttk.Button(actions, text="Restore Selected Into Current Profile", style="Danger.TButton", command=self.restore_selected_backup).pack(side="left", padx=(8, 0))

    def _build_activity_tab(self, parent: Any) -> None:
        card = self.ttk.Frame(parent, style="Card.TFrame", padding=18)
        card.pack(fill="both", expand=True)
        header = self.ttk.Frame(card, style="Card.TFrame")
        header.pack(fill="x")
        self.ttk.Label(header, text="Recent Activity", style="CardTitle.TLabel").pack(side="left")
        self.ttk.Button(header, text="Refresh", style="Quiet.TButton", command=self.refresh_logs).pack(side="right")
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
        activity_scroll = self.ttk.Scrollbar(card, orient="vertical", command=self.activity_text.yview)
        self.activity_text.configure(yscrollcommand=activity_scroll.set)
        self.activity_text.pack(side="left", fill="both", expand=True, pady=(14, 0))
        activity_scroll.pack(side="right", fill="y", pady=(14, 0))

    def _add_labeled_entry(self, parent: Any, row: int, label: str, variable: Any, show: str | None = None) -> Any:
        self.ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=(0, 10), padx=(0, 12))
        entry = self.ttk.Entry(parent, textvariable=variable, show=show or "")
        entry.grid(row=row, column=1, sticky="ew", pady=(0, 10))
        return entry

    def _add_labeled_combo(self, parent: Any, row: int, label: str, variable: Any, values: list[str], callback: Callable[[Any], None] | None = None) -> Any:
        self.ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=(0, 10), padx=(0, 12))
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
            self.raw_config = _default_raw_config()
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
        if not messagebox.askyesno("Delete Profile", f"Delete profile '{profile_name}' from the config?"):
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
            lambda: validate_profile_config(config, profile_name),
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
            lambda: run_test_connection_with_config(config, profile_name),
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
            lambda: run_backup(profile_name=profile_name, config_path=self.config_path),
            callback=self._handle_backup_completed,
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
        if not target_profile:
            self._show_error("Select a verification target profile.")
            return
        self._run_async(
            f"Verifying latest backup from '{source_profile}' into '{target_profile}'",
            lambda: run_verify_latest_backup(
                source_profile_name=source_profile,
                target_profile_name=target_profile,
                config_path=self.config_path,
            ),
            callback=self._handle_verification_completed,
        )

    def restore_selected_backup(self) -> None:
        if not self._save_if_needed():
            return
        selection = self.backup_tree.selection()
        if not selection:
            self._show_error("Select a backup first.")
            return
        record = self.backup_rows[int(selection[0])]
        self._confirm_and_restore_record(record)

    def _confirm_and_restore_record(self, record: dict[str, Any]) -> None:
        from tkinter import messagebox

        profile_name = self.profile_name_var.get().strip()
        prompt = f"Restore backup '{record['run_id']}' into profile '{profile_name}'?"
        if not messagebox.askyesno("Restore Backup", prompt):
            return
        self._run_async(
            f"Restoring '{record['run_id']}' into '{profile_name}'",
            lambda: run_restore(
                profile_name=profile_name,
                input_path=Path(record["run_dir"]),
                config_path=self.config_path,
                tables=self._restore_filter_values() if _normalize_db_type_label(self.db_type_var.get().strip()) == "postgres" else None,
                collections=self._restore_filter_values() if _normalize_db_type_label(self.db_type_var.get().strip()) == "mongo" else None,
            ),
            callback=self._handle_restore_completed,
        )

    def refresh_views(self) -> None:
        self.refresh_backups()
        self.refresh_logs()

    def refresh_backups(self) -> None:
        profile_name = self.profile_name_var.get().strip() or None
        self.backup_rows = list_backup_history(config_path=self.config_path, profile_name=profile_name, limit=200)
        for item in self.backup_tree.get_children():
            self.backup_tree.delete(item)
        for index, row in enumerate(self.backup_rows):
            self.backup_tree.insert(
                "",
                "end",
                iid=str(index),
                values=(
                    _pretty_timestamp(row.get("finished_at")),
                    row.get("run_id") or "",
                    row.get("compression") or "",
                    row.get("artifact_path") or "",
                ),
            )
        self._refresh_restore_choices()

    def refresh_logs(self) -> None:
        profile_name = self.profile_name_var.get().strip() or None
        events = list_run_log_events(config_path=self.config_path, profile_name=profile_name, limit=150)
        self.activity_text.delete("1.0", "end")
        for event in events:
            payload = event.get("payload", {})
            line = f"{event.get('timestamp', '')}  {event.get('event', '')}\n{payload}\n\n"
            self.activity_text.insert("end", line)
        self.activity_text.see("1.0")

    def open_selected_backup_folder(self) -> None:
        selection = self.backup_tree.selection()
        if not selection:
            self._show_error("Select a backup first.")
            return
        record = self.backup_rows[int(selection[0])]
        if str(record["run_dir"]).startswith("s3://"):
            self._show_error("This backup is stored remotely in S3 and does not have a local folder to open.")
            return
        target = Path(record["run_dir"])
        opener = shutil.which("xdg-open")
        if opener is None:
            self._show_error("xdg-open is not available on this system.")
            return
        subprocess.Popen([opener, str(target)])

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
        self.defaults_retention_keep_last_var.set(_stringify_optional(retention.get("keep_last")))
        self.defaults_retention_max_age_var.set(_stringify_optional(retention.get("max_age_days")))

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
        self.port_var.set(_stringify_optional(profile.get("port")))
        self.username_var.set(profile.get("username", ""))
        self.password_var.set(profile.get("password", ""))
        self.database_var.set(profile.get("database", ""))
        self.auth_database_var.set(profile.get("auth_database", ""))
        self.profile_output_dir_var.set(profile.get("output_dir", ""))
        self.profile_compression_var.set(_profile_compression_label(profile.get("compression")))
        schedule = profile.get("schedule", {})
        self.schedule_preset_var.set(schedule.get("preset") or schedule.get("on_calendar") or "")
        self.schedule_persistent_var.set(bool(schedule.get("persistent", True)))
        retention = profile.get("retention", {})
        self.retention_keep_last_var.set(_stringify_optional(retention.get("keep_last")))
        self.retention_max_age_var.set(_stringify_optional(retention.get("max_age_days")))
        self.restore_filter_var.set("")
        self._sync_db_type_state()
        self.refresh_views()
        self.status_var.set(f"Selected profile '{profile_name}'")
        self._refresh_verification_targets()

    def _sync_db_type_state(self, _event: Any | None = None) -> None:
        db_type = self.db_type_var.get()
        sqlite_only = db_type == "sqlite"
        mongo = db_type == "mongo"
        _set_widget_state(self.host_entry, not sqlite_only)
        _set_widget_state(self.port_entry, not sqlite_only)
        _set_widget_state(self.username_entry, not sqlite_only)
        _set_widget_state(self.password_entry, not sqlite_only)
        _set_widget_state(self.auth_database_entry, mongo)
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
        retention = _collect_retention_block(
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

        retention = _collect_retention_block(
            keep_last=self.retention_keep_last_var.get().strip(),
            max_age_days=self.retention_max_age_var.get().strip(),
        )
        if retention:
            profile["retention"] = retention

        return profile

    def _save_if_needed(self) -> bool:
        return self.save_profile()

    def _run_async(
        self,
        label: str,
        action: Callable[[], Any],
        *,
        callback: Callable[[Any], None] | None = None,
    ) -> None:
        if self.busy:
            self._show_error("Another operation is still running.")
            return

        self.busy = True
        self.status_var.set(label)
        self._append_status(label)
        if hasattr(self.root, "update_idletasks"):
            self.root.update_idletasks()

        def worker() -> None:
            try:
                result = action()
            except Exception as exc:
                self.event_queue.put(("error", label, exc, None))
                return
            self.event_queue.put(("success", label, result, callback))

        threading.Thread(target=worker, daemon=True).start()

    def _poll_events(self) -> None:
        while True:
            try:
                outcome, label, payload, callback = self.event_queue.get_nowait()
            except queue.Empty:
                break

            self.busy = False
            if outcome == "error":
                self.status_var.set(f"{label} failed")
                self._append_status(f"{label} failed: {payload}")
                self._show_error(str(payload))
                continue

            if callback is not None:
                callback(payload)
                continue

            self.status_var.set(f"{label} finished")
            self._append_status(f"{label} finished")

        self.root.after(150, self._poll_events)

    def _append_status(self, message: str) -> None:
        self.status_text.insert("end", f"{message}\n")
        self.status_text.see("end")

    def _report_success(self, message: str, *, show_dialog: bool = False) -> None:
        self.status_var.set(message)
        self._append_status(message)
        if show_dialog:
            self._show_info(message)

    def _show_error(self, message: str) -> None:
        self._show_message_dialog("dbrestore error", message, accent=PALETTE["danger"])

    def _show_info(self, message: str) -> None:
        self._show_message_dialog("dbrestore", message, accent=PALETTE["accent"])

    def _show_message_dialog(self, title: str, message: str, *, accent: str) -> None:
        dialog = self.tk.Toplevel(self.root)
        dialog.title(title)
        dialog.transient(self.root)
        dialog.configure(bg=PALETTE["canvas"])
        dialog.minsize(520, 320)

        width, height, x_pos, y_pos = _dialog_geometry(self.root, width=760, height=560)
        dialog.geometry(f"{width}x{height}+{x_pos}+{y_pos}")

        shell = self.ttk.Frame(dialog, style="Card.TFrame", padding=18)
        shell.pack(fill="both", expand=True)

        header = self.ttk.Frame(shell, style="Card.TFrame")
        header.pack(fill="x")
        self.tk.Label(
            header,
            text=title,
            bg=PALETTE["card"],
            fg=accent,
            font=("Cantarell", 13, "bold"),
            anchor="w",
        ).pack(side="left", fill="x", expand=True)

        body = self.ttk.Frame(shell, style="Card.TFrame")
        body.pack(fill="both", expand=True, pady=(14, 0))
        text = self.tk.Text(
            body,
            wrap="word",
            bg=PALETTE["card"],
            fg=PALETTE["ink"],
            insertbackground=PALETTE["ink"],
            highlightthickness=0,
            bd=0,
            relief="flat",
            font=("Cantarell", 11),
            padx=4,
            pady=4,
        )
        scrollbar = self.ttk.Scrollbar(body, orient="vertical", command=text.yview)
        text.configure(yscrollcommand=scrollbar.set)
        text.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        text.insert("1.0", message)
        text.configure(state="disabled")

        actions = self.ttk.Frame(shell, style="Card.TFrame")
        actions.pack(fill="x", pady=(14, 0))
        self.ttk.Button(actions, text="Close", style="Accent.TButton", command=dialog.destroy).pack(side="right")

        dialog.bind("<Escape>", lambda _event: dialog.destroy())
        dialog.protocol("WM_DELETE_WINDOW", dialog.destroy)
        dialog.grab_set()
        text.focus_set()
        dialog.wait_window()

    def _on_global_mousewheel(self, event: Any) -> str | None:
        if self.notebook.select() != str(self.profile_tab):
            return None
        widget = getattr(event, "widget", None)
        if widget is None or not self._is_widget_descendant(widget, self.profile_tab):
            return None

        if getattr(event, "num", None) == 4:
            delta = -1
        elif getattr(event, "num", None) == 5:
            delta = 1
        else:
            event_delta = getattr(event, "delta", 0)
            if event_delta == 0:
                return None
            delta = -1 if event_delta > 0 else 1

        self.profile_scroll_canvas.yview_scroll(delta, "units")
        return "break"

    def _is_widget_descendant(self, widget: Any, ancestor: Any) -> bool:
        current = widget
        while current is not None:
            if current == ancestor:
                return True
            current = getattr(current, "master", None)
        return False

    def _refresh_restore_choices(self) -> None:
        options = [_restore_option_label(row) for row in self.backup_rows]
        self.profile_restore_combo.configure(values=options)
        current = self.restore_choice_var.get()
        if options:
            if current not in self._restore_choice_map():
                self.restore_choice_var.set(options[0])
            _set_widget_state(self.profile_restore_combo, True)
            _set_widget_state(self.restore_button, True)
            return

        self.restore_choice_var.set("")
        _set_widget_state(self.profile_restore_combo, False)
        _set_widget_state(self.restore_button, False)

    def _restore_choice_map(self) -> dict[str, dict[str, Any]]:
        return {
            _restore_option_label(row): row
            for row in self.backup_rows
        }

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
        current_db_type = _normalize_db_type_label(self.db_type_var.get().strip())
        options = [
            name for name in self._profile_names()
            if name != current_profile
            and _normalize_db_type_label(
                self.raw_config.get("profiles", {}).get(name, {}).get("db_type", "")
            ) == current_db_type
        ]
        if not hasattr(self, "verify_target_combo"):
            return
        self.verify_target_combo.configure(values=options)
        current_target = self.verify_target_profile_var.get()
        if options:
            if current_target not in options:
                self.verify_target_profile_var.set(options[0])
            self.verify_hint_var.set("Use a separate disposable profile as the verification target.")
            _set_widget_state(self.verify_target_combo, True)
            _set_widget_state(self.verify_button, True)
            return

        if current_profile:
            self.verify_target_profile_var.set("Create another matching profile first")
        else:
            self.verify_target_profile_var.set("")
        self.verify_hint_var.set(
            "Verification requires a different profile with the same DB type."
        )
        _set_widget_state(self.verify_target_combo, False)
        _set_widget_state(self.verify_button, False)

    def _refresh_restore_filter_state(self, db_type: str) -> None:
        normalized = _normalize_db_type_label(db_type)
        if normalized == "postgres":
            self.restore_filter_label_var.set("Restore Tables")
            self.restore_filter_hint_var.set(
                "Optional. Enter comma-separated table names, for example public.items, public.orders."
            )
            _set_widget_state(self.restore_filter_entry, True)
            return
        if normalized == "mongo":
            self.restore_filter_label_var.set("Restore Collections")
            self.restore_filter_hint_var.set(
                "Optional. Enter comma-separated collection names. Plain names are prefixed with the database automatically."
            )
            _set_widget_state(self.restore_filter_entry, True)
            return
        self.restore_filter_label_var.set("Selective Restore Filter")
        self.restore_filter_hint_var.set(
            "Selective restore is not supported for this database type with the current backup format."
        )
        self.restore_filter_var.set("")
        _set_widget_state(self.restore_filter_entry, False)

    def _restore_filter_values(self) -> list[str] | None:
        values = [item.strip() for item in self.restore_filter_var.get().split(",") if item.strip()]
        return values or None


def _default_raw_config() -> dict[str, Any]:
    return {
        "version": 1,
        "defaults": {
            "output_dir": "./backups",
            "log_dir": "./logs",
            "compression": "gzip",
        },
        "profiles": {},
    }


def _stringify_optional(value: Any) -> str:
    return "" if value is None else str(value)


def _collect_retention_block(*, keep_last: str, max_age_days: str) -> dict[str, int]:
    retention: dict[str, int] = {}
    if keep_last:
        retention["keep_last"] = int(keep_last)
    if max_age_days:
        retention["max_age_days"] = int(max_age_days)
    return retention


def _pretty_timestamp(value: Any) -> str:
    if not value:
        return ""
    try:
        return format_display_timestamp(parse_timestamp(str(value)))
    except ValueError:
        return str(value)


def _profile_compression_label(value: Any) -> str:
    if value is True:
        return "gzip"
    if value is False:
        return "none"
    return "inherit"


def _restore_option_label(record: dict[str, Any]) -> str:
    timestamp = _pretty_timestamp(record.get("finished_at")) or "Unknown time"
    run_id = record.get("run_id") or "unknown-run"
    artifact_name = Path(record.get("artifact_path") or "").name or "artifact"
    return f"{timestamp} | {run_id} | {artifact_name}"


def _normalize_db_type_label(value: str) -> str:
    return DB_TYPE_ALIASES.get(value.strip().lower(), value.strip().lower())


def _dialog_geometry(root: Any, *, width: int, height: int) -> tuple[int, int, int, int]:
    if hasattr(root, "update_idletasks"):
        root.update_idletasks()

    screen_width = int(root.winfo_screenwidth())
    screen_height = int(root.winfo_screenheight())
    dialog_width = min(width, max(420, screen_width - 80))
    dialog_height = min(height, max(260, screen_height - 80))

    root_width = max(int(root.winfo_width()), 1)
    root_height = max(int(root.winfo_height()), 1)
    root_x = int(root.winfo_rootx())
    root_y = int(root.winfo_rooty())

    x_pos = max(20, root_x + (root_width - dialog_width) // 2)
    y_pos = max(20, root_y + (root_height - dialog_height) // 2)
    x_pos = min(x_pos, screen_width - dialog_width - 20)
    y_pos = min(y_pos, screen_height - dialog_height - 20)
    return dialog_width, dialog_height, x_pos, y_pos


def _set_widget_state(widget: Any, enabled: bool) -> None:
    if enabled:
        widget.state(["!disabled"])
    else:
        widget.state(["disabled"])


PALETTE = {
    "canvas": "#F3F0EA",
    "card": "#FCFBF8",
    "field": "#F0ECE5",
    "field_alt": "#E7E1D7",
    "accent": "#176B87",
    "accent_dark": "#135468",
    "accent_soft": "#D8EBF2",
    "danger": "#B33939",
    "ink": "#1F2A2E",
    "muted": "#5D676A",
    "border": "#D6D0C7",
}
