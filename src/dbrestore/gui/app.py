"""Main Tk application shell for dbrestore."""

from __future__ import annotations

import argparse
import queue
from pathlib import Path
from typing import Any

from dbrestore.config import DEFAULT_CONFIG_PATH
from dbrestore.errors import DBRestoreError
from dbrestore.scheduler import DEFAULT_ENV_DIR, DEFAULT_SYSTEMD_UNIT_DIR

from .activity_view import ActivityViewMixin
from .background_tasks import BackgroundTaskRunnerMixin
from .backups_view import BackupsViewMixin
from .dialogs import DialogHelpersMixin
from .helpers import PALETTE, default_raw_config
from .operations_view import OperationsViewMixin
from .profile_form import ProfileFormMixin


def launch_gui(config_path: Path = DEFAULT_CONFIG_PATH) -> None:
    try:
        import tkinter as tk
    except ImportError as exc:
        raise DBRestoreError(
            "Tkinter is not available. Install python3-tk to use the GUI."
        ) from exc

    root = tk.Tk()
    app = DBRestoreGUI(root=root, config_path=config_path)
    app.run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the dbrestore desktop GUI.")
    parser.add_argument(
        "--config", default=str(DEFAULT_CONFIG_PATH), help="Path to YAML configuration."
    )
    args = parser.parse_args()
    launch_gui(Path(args.config))


class DBRestoreGUI(
    ProfileFormMixin,
    BackupsViewMixin,
    ActivityViewMixin,
    OperationsViewMixin,
    BackgroundTaskRunnerMixin,
    DialogHelpersMixin,
):
    def __init__(self, root: Any, config_path: Path) -> None:
        import tkinter as tk
        from tkinter import ttk

        self.tk = tk
        self.ttk = ttk
        self.root = root
        self.config_path = Path(config_path).expanduser().resolve()
        self.raw_config = self._default_raw_config()
        self.selected_profile_name: str | None = None
        self.backup_rows: list[dict[str, Any]] = []
        self.event_queue: queue.Queue[tuple[str, str, Any, Any]] = queue.Queue()
        self.busy = False
        self._progress_mode = "determinate"
        self._auto_progress_job: str | None = None
        self._overlay_hide_job: str | None = None

        self.root.title("dbrestore")
        self.root.geometry("1360x900")
        self.root.minsize(1180, 760)
        self.root.configure(bg=PALETTE["canvas"])

        self._init_variables()
        self._configure_styles()
        self._build_layout()
        self.reload_config(select_first=True)
        self._poll_events()

    def run(self) -> None:
        self.root.mainloop()

    def _default_raw_config(self) -> dict[str, Any]:
        return default_raw_config()

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
        self.verify_schedule_after_backup_var = self.tk.BooleanVar(value=True)
        self.verify_hint_var = self.tk.StringVar(
            value="Use a separate disposable profile as the verification target."
        )

        self.schedule_unit_dir_var = self.tk.StringVar(value=str(DEFAULT_SYSTEMD_UNIT_DIR))
        self.schedule_env_dir_var = self.tk.StringVar(value=str(DEFAULT_ENV_DIR))
        self.schedule_status_detail_var = self.tk.StringVar(value="No schedule status loaded")
        self.preflight_include_connection_var = self.tk.BooleanVar(value=True)
        self.env_file_path_var = self.tk.StringVar(value="No env file loaded")
        self.dashboard_last_backup_var = self.tk.StringVar(value="No data yet")
        self.dashboard_last_verification_var = self.tk.StringVar(value="No data yet")
        self.dashboard_next_run_var = self.tk.StringVar(value="No data yet")
        self.dashboard_storage_target_var = self.tk.StringVar(value="No data yet")
        self.dashboard_storage_health_var = self.tk.StringVar(value="No data yet")
        self.dashboard_retention_var = self.tk.StringVar(value="No data yet")
        self.dashboard_schedule_var = self.tk.StringVar(value="No data yet")

        self.status_var = self.tk.StringVar(value="Ready")
        self.progress_message_var = self.tk.StringVar(value="Waiting for the next operation")
        self.progress_percent_var = self.tk.StringVar(value="")
        self.progress_value_var = self.tk.DoubleVar(value=0.0)

    def _configure_styles(self) -> None:
        style = self.ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except self.tk.TclError:
            pass

        style.configure(
            ".", background=PALETTE["canvas"], foreground=PALETTE["ink"], font=("Cantarell", 10)
        )
        style.configure("App.TFrame", background=PALETTE["canvas"])
        style.configure("Card.TFrame", background=PALETTE["card"], relief="flat")
        style.configure(
            "Header.TLabel",
            background=PALETTE["canvas"],
            foreground=PALETTE["ink"],
            font=("Cantarell", 23, "bold"),
        )
        style.configure(
            "Subtle.TLabel",
            background=PALETTE["canvas"],
            foreground=PALETTE["muted"],
            font=("Cantarell", 10),
        )
        style.configure(
            "CardTitle.TLabel",
            background=PALETTE["card"],
            foreground=PALETTE["ink"],
            font=("Cantarell", 12, "bold"),
        )
        style.configure(
            "CardText.TLabel",
            background=PALETTE["card"],
            foreground=PALETTE["muted"],
            font=("Cantarell", 10),
        )
        style.configure("TLabel", background=PALETTE["card"], foreground=PALETTE["ink"])
        style.configure(
            "TEntry",
            fieldbackground=PALETTE["field"],
            foreground=PALETTE["ink"],
            bordercolor=PALETTE["border"],
            lightcolor=PALETTE["field"],
            darkcolor=PALETTE["field"],
            padding=6,
        )
        style.map("TEntry", fieldbackground=[("disabled", PALETTE["canvas"])])
        style.configure(
            "TCombobox",
            fieldbackground=PALETTE["field"],
            background=PALETTE["field"],
            foreground=PALETTE["ink"],
            bordercolor=PALETTE["border"],
            arrowsize=14,
            padding=4,
        )
        style.configure(
            "Accent.TButton",
            background=PALETTE["accent"],
            foreground="white",
            padding=(10, 7),
            borderwidth=0,
        )
        style.map("Accent.TButton", background=[("active", PALETTE["accent_dark"])])
        style.configure(
            "Quiet.TButton",
            background=PALETTE["field"],
            foreground=PALETTE["ink"],
            padding=(10, 7),
            borderwidth=0,
        )
        style.map("Quiet.TButton", background=[("active", PALETTE["field_alt"])])
        style.configure(
            "Danger.TButton",
            background=PALETTE["danger"],
            foreground="white",
            padding=(10, 7),
            borderwidth=0,
        )
        style.map("Danger.TButton", background=[("active", "#9B2C2C")])
        style.configure("TCheckbutton", background=PALETTE["card"], foreground=PALETTE["ink"])
        style.configure(
            "Operation.Horizontal.TProgressbar",
            troughcolor=PALETTE["field"],
            background=PALETTE["accent"],
            bordercolor=PALETTE["field"],
            lightcolor=PALETTE["accent"],
            darkcolor=PALETTE["accent_dark"],
            thickness=14,
        )
        style.configure("Side.TLabelframe", background=PALETTE["card"], foreground=PALETTE["ink"])
        style.configure(
            "Side.TLabelframe.Label",
            background=PALETTE["card"],
            foreground=PALETTE["muted"],
            font=("Cantarell", 9, "bold"),
        )
        style.configure("Main.TNotebook", background=PALETTE["canvas"], borderwidth=0)
        style.configure(
            "Main.TNotebook.Tab",
            padding=(12, 8),
            background=PALETTE["field"],
            foreground=PALETTE["muted"],
        )
        style.map(
            "Main.TNotebook.Tab",
            background=[("selected", PALETTE["card"])],
            foreground=[("selected", PALETTE["ink"])],
        )
        style.configure(
            "History.Treeview",
            background=PALETTE["card"],
            fieldbackground=PALETTE["card"],
            foreground=PALETTE["ink"],
            rowheight=28,
            borderwidth=0,
        )
        style.configure(
            "History.Treeview.Heading",
            background=PALETTE["field"],
            foreground=PALETTE["muted"],
            font=("Cantarell", 10, "bold"),
            padding=(6, 6),
        )
        style.map(
            "History.Treeview",
            background=[("selected", PALETTE["accent_soft"])],
            foreground=[("selected", PALETTE["ink"])],
        )

    def _build_layout(self) -> None:
        header = self.ttk.Frame(self.root, style="App.TFrame", padding=(22, 18, 22, 12))
        header.pack(fill="x")
        self.ttk.Label(header, text="dbrestore", style="Header.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        self.ttk.Label(header, text=f"Config: {self.config_path}", style="Subtle.TLabel").grid(
            row=1, column=0, sticky="w", pady=(4, 0)
        )
        button_bar = self.ttk.Frame(header, style="App.TFrame")
        button_bar.grid(row=0, column=1, rowspan=2, sticky="e")
        self.ttk.Button(
            button_bar,
            text="Reload",
            style="Quiet.TButton",
            command=lambda: self.reload_config(select_first=False),
        ).pack(side="left", padx=(0, 8))
        self.ttk.Button(
            button_bar, text="Open Config", style="Quiet.TButton", command=self.open_config_file
        ).pack(side="left")
        header.columnconfigure(0, weight=1)

        content = self.ttk.Frame(self.root, style="App.TFrame", padding=(22, 0, 22, 22))
        content.pack(fill="both", expand=True)

        sidebar = self.ttk.Frame(content, style="Card.TFrame", padding=16)
        sidebar.pack(side="left", fill="y", padx=(0, 16))
        self.ttk.Label(sidebar, text="Profiles", style="CardTitle.TLabel").pack(anchor="w")
        self.ttk.Label(
            sidebar, text="Select one profile to edit or run.", style="CardText.TLabel"
        ).pack(anchor="w", pady=(4, 12))

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
        profile_scroll = self.ttk.Scrollbar(
            list_frame, orient="vertical", command=self.profile_listbox.yview
        )
        self.profile_listbox.configure(yscrollcommand=profile_scroll.set)
        self.profile_listbox.pack(side="left", fill="both", expand=True)
        profile_scroll.pack(side="right", fill="y")
        self.profile_listbox.bind("<<ListboxSelect>>", self._on_profile_selected)

        sidebar_actions = self.ttk.Frame(sidebar, style="Card.TFrame")
        sidebar_actions.pack(fill="x", pady=(12, 0))
        self.ttk.Button(
            sidebar_actions,
            text="New Profile",
            style="Quiet.TButton",
            command=self.prepare_new_profile,
        ).pack(fill="x")
        self.ttk.Button(
            sidebar_actions,
            text="Delete Profile",
            style="Danger.TButton",
            command=self.delete_profile,
        ).pack(fill="x", pady=(8, 0))

        notes = self.ttk.LabelFrame(sidebar, text="Notes", style="Side.TLabelframe", padding=12)
        notes.pack(fill="x", pady=(16, 0))
        note_text = (
            "Password accepts either plaintext or an env reference like ${PGPASSWORD}.\n"
            "Validate/Test use the current form values.\n"
            "The Operations tab manages systemd units, env files, preflight, and readiness data."
        )
        self.ttk.Label(
            notes, text=note_text, style="CardText.TLabel", wraplength=240, justify="left"
        ).pack(anchor="w")

        main = self.ttk.Frame(content, style="App.TFrame")
        main.pack(side="left", fill="both", expand=True)

        self.notebook = self.ttk.Notebook(main, style="Main.TNotebook")
        self.notebook.pack(fill="both", expand=True)

        profile_tab = self.ttk.Frame(self.notebook, style="App.TFrame", padding=(0, 8, 0, 0))
        backups_tab = self.ttk.Frame(self.notebook, style="App.TFrame", padding=(0, 8, 0, 0))
        activity_tab = self.ttk.Frame(self.notebook, style="App.TFrame", padding=(0, 8, 0, 0))
        operations_tab = self.ttk.Frame(self.notebook, style="App.TFrame", padding=(0, 8, 0, 0))
        self.notebook.add(profile_tab, text="Profile")
        self.notebook.add(backups_tab, text="Backups")
        self.notebook.add(activity_tab, text="Activity")
        self.notebook.add(operations_tab, text="Operations")

        self.profile_tab = profile_tab
        self.profile_scroll_canvas, profile_content = self._build_scrollable_tab(profile_tab)
        self._build_profile_tab(profile_content)
        self._build_backups_tab(backups_tab)
        self._build_activity_tab(activity_tab)
        self._build_operations_tab(operations_tab)
        self._bind_global_scroll_handlers()

        status_frame = self.ttk.Frame(self.root, style="Card.TFrame", padding=(22, 12, 22, 18))
        status_frame.pack(fill="x")
        self.ttk.Label(status_frame, textvariable=self.status_var, style="CardTitle.TLabel").pack(
            anchor="w"
        )
        progress_frame = self.ttk.Frame(status_frame, style="Card.TFrame")
        progress_frame.pack(fill="x", pady=(8, 0))
        self.ttk.Label(
            progress_frame,
            text="Operation Progress",
            style="CardTitle.TLabel",
        ).grid(row=0, column=0, sticky="w")
        self.ttk.Label(
            progress_frame,
            textvariable=self.progress_message_var,
            style="CardText.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(4, 0))
        self.ttk.Label(
            progress_frame,
            textvariable=self.progress_percent_var,
            style="CardText.TLabel",
        ).grid(row=1, column=1, sticky="e", pady=(4, 0))
        self.progress_bar = self.ttk.Progressbar(
            progress_frame,
            variable=self.progress_value_var,
            maximum=100,
            mode="determinate",
            style="Operation.Horizontal.TProgressbar",
        )
        self.progress_bar.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        progress_frame.columnconfigure(0, weight=1)
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
        status_scroll = self.ttk.Scrollbar(
            status_frame, orient="vertical", command=self.status_text.yview
        )
        self.status_text.configure(yscrollcommand=status_scroll.set)
        self.status_text.pack(side="left", fill="both", expand=True, pady=(8, 0))
        status_scroll.pack(side="right", fill="y", pady=(8, 0))

        self.progress_overlay = self.tk.Frame(self.root, bg="#DDD4C7", cursor="watch")
        self.progress_overlay.bind("<Button-1>", lambda _event: "break")
        self.progress_overlay_card = self.ttk.Frame(
            self.progress_overlay, style="Card.TFrame", padding=24
        )
        self.progress_overlay_card.place(relx=0.5, rely=0.5, anchor="center", width=520, height=240)
        self.ttk.Label(
            self.progress_overlay_card,
            text="Operation In Progress",
            style="CardTitle.TLabel",
        ).pack()
        self.tk.Label(
            self.progress_overlay_card,
            textvariable=self.progress_percent_var,
            bg=PALETTE["card"],
            fg=PALETTE["accent"],
            font=("Cantarell", 28, "bold"),
        ).pack(pady=(10, 0))
        self.ttk.Label(
            self.progress_overlay_card,
            textvariable=self.progress_message_var,
            style="CardText.TLabel",
            wraplength=360,
            justify="center",
        ).pack(pady=(10, 0))
        self.overlay_progress_bar = self.ttk.Progressbar(
            self.progress_overlay_card,
            variable=self.progress_value_var,
            maximum=100,
            mode="determinate",
            style="Operation.Horizontal.TProgressbar",
        )
        self.overlay_progress_bar.pack(fill="x", pady=(18, 0))
        self.progress_overlay.place_forget()

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

    def _append_status(self, message: str) -> None:
        self.status_text.insert("end", f"{message}\n")
        self.status_text.see("end")

    def _report_success(self, message: str, *, show_dialog: bool = False) -> None:
        self.status_var.set(message)
        self._append_status(message)
        if show_dialog:
            self._show_info(message)

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

    def _show_progress_overlay(self) -> None:
        if self._overlay_hide_job is not None:
            self.root.after_cancel(self._overlay_hide_job)
            self._overlay_hide_job = None
        self.progress_overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
        self.progress_overlay.lift()
        self.root.update_idletasks()

    def _hide_progress_overlay(self, *, delay_ms: int = 0) -> None:
        if self._overlay_hide_job is not None:
            self.root.after_cancel(self._overlay_hide_job)
            self._overlay_hide_job = None

        if delay_ms > 0:
            self._overlay_hide_job = self.root.after(delay_ms, self._hide_progress_overlay_now)
            return

        self._hide_progress_overlay_now()

    def _hide_progress_overlay_now(self) -> None:
        if self._overlay_hide_job is not None:
            self._overlay_hide_job = None
        self.progress_overlay.place_forget()
