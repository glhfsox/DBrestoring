from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from dbrestore.utils import current_time, ensure_directory, format_timestamp, json_safe


class RunLogger:
    def __init__(self, log_file: Path, console: Callable[[str], None] | None = None) -> None:
        self.log_file = log_file
        self.console = console

    def log_event(self, event: str, payload: dict[str, Any]) -> None:
        ensure_directory(self.log_file.parent)
        record = {
            "timestamp": format_timestamp(current_time()),
            "event": event,
            "payload": json_safe(payload),
        }
        with self.log_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")

    def print(self, message: str) -> None:
        if self.console:
            self.console(message)
