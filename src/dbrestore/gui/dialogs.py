"""Dialog helpers used across the GUI package."""

from __future__ import annotations

from typing import Any

from .base import GUIBoundMixin
from .helpers import PALETTE


class DialogHelpersMixin(GUIBoundMixin):
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
        self.ttk.Button(actions, text="Close", style="Accent.TButton", command=dialog.destroy).pack(
            side="right"
        )

        dialog.bind("<Escape>", lambda _event: dialog.destroy())
        dialog.protocol("WM_DELETE_WINDOW", dialog.destroy)
        dialog.grab_set()
        text.focus_set()
        dialog.wait_window()


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
