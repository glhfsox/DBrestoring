from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def sqlite_config_text(tmp_path: Path) -> str:
    database_path = tmp_path / "app.sqlite3"
    return f"""
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
  compression: gzip
profiles:
  sqlite_local:
    db_type: sqlite
    database: {database_path}
""".strip()
