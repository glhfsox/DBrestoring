from __future__ import annotations

import json
import sqlite3
import urllib.error
from pathlib import Path

from pydantic import SecretStr

from dbrestore.config import ControlPlaneModel
from dbrestore.control_plane import build_payload, report_run
from dbrestore.logging import RunLogger


def _settings(**kwargs) -> ControlPlaneModel:
    base = {"url": "https://cp.example.test", "token": SecretStr("secret-token")}
    base.update(kwargs)
    return ControlPlaneModel(**base)


def _logger(tmp_path: Path) -> RunLogger:
    return RunLogger(tmp_path / "log.txt")


class _FakeResponse:
    status = 201

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, *args) -> bytes:
        return b'{"ok": true}'


def test_build_payload_maps_fields_and_defaults_server_id():
    settings = _settings(server_id="srv-1", server_name="Prod 1")
    run = {
        "run_id": "abc123",
        "profile": "prod",
        "db_type": "postgres",
        "backup_type": "full",
        "duration_ms": 4200,
        "size_bytes": 999,
        "started_at": "s",
        "finished_at": "f",
    }
    payload = build_payload(settings, run, "success")
    assert payload["server"] == {"id": "srv-1", "name": "Prod 1"}
    assert payload["run"]["id"] == "abc123"
    assert payload["run"]["status"] == "success"
    assert payload["run"]["size_bytes"] == 999
    assert payload["run"]["duration_ms"] == 4200
    assert payload["run"]["error"] is None


def test_build_payload_falls_back_to_hostname(monkeypatch):
    monkeypatch.setattr("dbrestore.control_plane.socket.gethostname", lambda: "myhost")
    payload = build_payload(
        _settings(), {"profile": "p", "db_type": "sqlite"}, "failed", error="boom"
    )
    assert payload["server"] == {"id": "myhost", "name": "myhost"}
    assert payload["run"]["status"] == "failed"
    assert payload["run"]["error"] == "boom"


def test_report_run_posts_expected_request(tmp_path, monkeypatch):
    captured: dict = {}

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        captured["auth"] = request.get_header("Authorization")
        captured["body"] = json.loads(request.data.decode("utf-8"))
        captured["method"] = request.get_method()
        return _FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    ok = report_run(
        _settings(server_id="s1"),
        {"run_id": "r1", "profile": "p", "db_type": "sqlite", "backup_type": "full"},
        "success",
        _logger(tmp_path),
    )

    assert ok is True
    assert captured["url"] == "https://cp.example.test/api/v1/runs"
    assert captured["auth"] == "Bearer secret-token"
    assert captured["method"] == "POST"
    assert captured["body"]["run"]["id"] == "r1"
    assert captured["body"]["server"]["id"] == "s1"


def test_report_run_swallows_network_errors(tmp_path, monkeypatch):
    def boom(request, timeout=None):
        raise urllib.error.URLError("down")

    monkeypatch.setattr("urllib.request.urlopen", boom)

    # Must not raise, returns False.
    assert report_run(_settings(), {"run_id": "r"}, "success", _logger(tmp_path)) is False


def _make_config(tmp_path: Path) -> Path:
    source = tmp_path / "source.sqlite3"
    with sqlite3.connect(source) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO t DEFAULT VALUES")
        conn.commit()

    config_path = tmp_path / "dbrestore.yaml"
    config_path.write_text(
        f"""
version: 1
defaults:
  output_dir: {tmp_path / "backups"}
  log_dir: {tmp_path / "logs"}
  control_plane:
    url: https://cp.example.test
    token: "secret-token"
    server_id: test-server
profiles:
  source:
    db_type: sqlite
    database: {source}
""".strip(),
        encoding="utf-8",
    )
    return config_path


def test_run_backup_reports_success(tmp_path, monkeypatch):
    from dbrestore.operations import run_backup

    calls: list[dict] = []

    def fake_report(settings, run, status, logger, *, error=None):
        calls.append({"status": status, "run": run, "error": error})
        return True

    monkeypatch.setattr("dbrestore.operations.backup_restore.report_run", fake_report)

    config_path = _make_config(tmp_path)
    run_backup(profile_name="source", config_path=config_path)

    assert len(calls) == 1
    assert calls[0]["status"] == "success"
    assert calls[0]["run"].get("run_id")
    assert "size_bytes" in calls[0]["run"]
