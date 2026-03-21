"""Background task runner used by the Tk GUI."""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from typing import Any

from .base import GUIBoundMixin


class BackgroundTaskRunnerMixin(GUIBoundMixin):
    def _run_async(
        self,
        label: str,
        action: Callable[[Callable[[dict[str, Any]], None]], Any],
        *,
        callback: Callable[[Any], None] | None = None,
        show_overlay: bool = False,
    ) -> None:
        if self.busy:
            self._show_error("Another operation is still running.")
            return

        self.busy = True
        self.status_var.set(label)
        self._append_status(label)
        self._reset_progress_ui()
        self._apply_progress_update(
            {
                "message": label,
                "mode": "auto",
                "percent": 2.0,
                "target_percent": 8.0,
            }
        )
        if show_overlay:
            self._show_progress_overlay()
        else:
            self._hide_progress_overlay()
        if hasattr(self.root, "update_idletasks"):
            self.root.update_idletasks()

        def report_progress(payload: dict[str, Any]) -> None:
            self.event_queue.put(("progress", label, payload, None))

        def worker() -> None:
            try:
                result = action(report_progress)
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

            if outcome == "progress":
                self._apply_progress_update(payload)
                continue

            self.busy = False
            if outcome == "error":
                self._finish_progress_ui(success=False, message=f"{label} failed")
                self.status_var.set(f"{label} failed")
                self._append_status(f"{label} failed: {payload}")
                self._show_error(str(payload))
                continue

            if callback is not None:
                self._finish_progress_ui(success=True, message=f"{label} finished")
                callback(payload)
                continue

            self._finish_progress_ui(success=True, message=f"{label} finished")
            self.status_var.set(f"{label} finished")
            self._append_status(f"{label} finished")

        self.root.after(150, self._poll_events)

    def _reset_progress_ui(self) -> None:
        self._progress_mode = "determinate"
        self._cancel_auto_progress()
        self._set_progress_mode("determinate")
        self.progress_value_var.set(0.0)
        self.progress_message_var.set("Waiting for the next operation")
        self.progress_percent_var.set("")

    def _apply_progress_update(self, payload: dict[str, Any]) -> None:
        message = str(payload.get("message", "")).strip() or "Working..."
        mode = "auto" if str(payload.get("mode", "determinate")) == "auto" else "determinate"
        percent = payload.get("percent")
        target_percent = payload.get("target_percent")

        if mode != "auto":
            self._cancel_auto_progress()

        if mode != getattr(self, "_progress_mode", "determinate"):
            self._set_progress_mode(mode)
            self._progress_mode = mode

        if mode == "determinate":
            if percent is not None:
                self._set_progress_value(float(percent))
            elif not self.progress_percent_var.get():
                self.progress_percent_var.set("")
        else:
            current_value = float(self.progress_value_var.get())
            if percent is not None:
                current_value = max(current_value, float(percent))
            self._set_progress_value(current_value)
            ceiling = float(target_percent) if target_percent is not None else current_value
            self._start_auto_progress(ceiling)

        self.progress_message_var.set(message)
        if hasattr(self.root, "update_idletasks"):
            self.root.update_idletasks()

    def _finish_progress_ui(self, *, success: bool, message: str) -> None:
        self._cancel_auto_progress()
        self._set_progress_mode("determinate")
        self._progress_mode = "determinate"
        if success:
            self._set_progress_value(100.0)
        else:
            self.progress_value_var.set(0.0)
            self.progress_percent_var.set("")
        self.progress_message_var.set(message)
        if success:
            self._hide_progress_overlay(delay_ms=280)
        else:
            self._hide_progress_overlay()
        if hasattr(self.root, "update_idletasks"):
            self.root.update_idletasks()

    def _set_progress_mode(self, mode: str) -> None:
        widgets = []
        if hasattr(self, "progress_bar"):
            widgets.append(self.progress_bar)
        if hasattr(self, "overlay_progress_bar"):
            widgets.append(self.overlay_progress_bar)

        for widget in widgets:
            widget.configure(mode="determinate")

    def _set_progress_value(self, value: float) -> None:
        normalized = max(0.0, min(100.0, value))
        self.progress_value_var.set(normalized)
        self.progress_percent_var.set(f"{int(round(normalized))}%")

    def _start_auto_progress(self, target_percent: float) -> None:
        self._auto_progress_target = max(0.0, min(100.0, target_percent))
        if getattr(self, "_auto_progress_job", None) is None:
            self._drive_auto_progress()

    def _drive_auto_progress(self) -> None:
        self._auto_progress_job = None
        if not self.busy or self._progress_mode != "auto":
            return

        target = float(getattr(self, "_auto_progress_target", self.progress_value_var.get()))
        current = float(self.progress_value_var.get())
        if current >= target - 0.1:
            return

        step = max(0.35, min(1.8, (target - current) * 0.12))
        self._set_progress_value(min(target, current + step))
        self._auto_progress_job = self.root.after(120, self._drive_auto_progress)

    def _cancel_auto_progress(self) -> None:
        job = getattr(self, "_auto_progress_job", None)
        if job is not None:
            self.root.after_cancel(job)
            self._auto_progress_job = None
