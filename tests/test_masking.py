from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from dbrestore.errors import ConfigError, DBRestoreError
from dbrestore.masking import (
    MYSQL,
    POSTGRES,
    MaskRule,
    apply_masking,
    apply_masking_sqlite,
    derive_key,
    mask_value,
    validate_identifier,
)
from dbrestore.operations import run_sanitize

KEY = derive_key(b"test-salt")


class TestStrategies:
    def test_null_values_pass_through(self):
        for strategy in ("redact", "email", "name", "phone", "hash", "null", "constant"):
            assert mask_value(strategy, None, KEY) is None

    def test_deterministic_same_input_same_output(self):
        a = mask_value("email", "alice@corp.com", KEY)
        b = mask_value("email", "alice@corp.com", KEY)
        assert a == b
        assert a != "alice@corp.com"
        assert "@example.com" in str(a)

    def test_different_inputs_differ(self):
        assert mask_value("hash", "a", KEY) != mask_value("hash", "b", KEY)

    def test_key_changes_output(self):
        other = derive_key(b"other-salt")
        assert mask_value("email", "x@y.com", KEY) != mask_value("email", "x@y.com", other)

    def test_redact_and_constant(self):
        assert mask_value("redact", "secret", KEY) == "***"
        assert mask_value("redact", "secret", KEY, constant="X") == "X"
        assert mask_value("constant", "secret", KEY, constant="anon") == "anon"
        assert mask_value("null", "secret", KEY) is None

    def test_name_and_phone_shape(self):
        name = str(mask_value("name", "Bob Jones", KEY))
        assert " " in name
        phone = str(mask_value("phone", "555-1234", KEY))
        assert phone.startswith("+1") and len(phone) == 12


class TestIdentifierValidation:
    def test_rejects_injection(self):
        with pytest.raises(ConfigError):
            validate_identifier("users; DROP TABLE users")

    def test_accepts_plain(self):
        assert validate_identifier("user_emails") == "user_emails"


def _seed(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT, age INT)")
        conn.executemany(
            "INSERT INTO users (email, name, age) VALUES (?, ?, ?)",
            [("alice@corp.com", "Alice", 30), ("bob@corp.com", "Bob", 41), (None, None, 22)],
        )
        conn.commit()


def test_apply_masking_sqlite(tmp_path):
    db = tmp_path / "src.sqlite"
    _seed(db)
    rules = [
        MaskRule("users", "email", "email"),
        MaskRule("users", "name", "redact"),
    ]
    with sqlite3.connect(db) as conn:
        counts = apply_masking_sqlite(conn, rules, KEY)
        rows = conn.execute("SELECT email, name, age FROM users ORDER BY id").fetchall()

    # 2 non-null emails masked, NULL left alone.
    assert counts["users.email"] == 2
    assert counts["users.name"] == 2
    assert all(r[0] is None or "@example.com" in r[0] for r in rows)
    assert rows[0][1] == "***"
    assert rows[2][0] is None and rows[2][1] is None  # NULLs untouched
    assert [r[2] for r in rows] == [30, 41, 22]  # untouched column intact


def _config(tmp_path: Path, source: Path) -> Path:
    cfg = tmp_path / "dbrestore.yaml"
    cfg.write_text(
        f"""
version: 1
defaults:
  output_dir: {tmp_path / "backups"}
  log_dir: {tmp_path / "logs"}
profiles:
  prod:
    db_type: sqlite
    database: {source}
    masking:
      salt: "fixed-salt"
      rules:
        - table: users
          column: email
          strategy: email
        - table: users
          column: name
          strategy: redact
  staging:
    db_type: sqlite
    database: {tmp_path / "staging.sqlite"}
""".strip(),
        encoding="utf-8",
    )
    return cfg


def test_run_sanitize_end_to_end(tmp_path):
    source = tmp_path / "prod.sqlite"
    _seed(source)
    cfg = _config(tmp_path, source)
    out = tmp_path / "sanitized.sqlite"

    result = run_sanitize(profile_name="prod", output_path=out, config_path=cfg)

    assert result["total_masked"] == 4
    with sqlite3.connect(out) as conn:
        rows = conn.execute("SELECT email, name FROM users ORDER BY id").fetchall()
    assert all(r[0] is None or "@example.com" in r[0] for r in rows)
    assert rows[0][1] == "***"

    # Source DB must be untouched (we mask the snapshot copy, not the source).
    with sqlite3.connect(source) as conn:
        assert conn.execute("SELECT email FROM users WHERE id=1").fetchone()[0] == "alice@corp.com"


def test_run_sanitize_restores_into_target(tmp_path):
    source = tmp_path / "prod.sqlite"
    _seed(source)
    cfg = _config(tmp_path, source)
    out = tmp_path / "sanitized.sqlite"

    run_sanitize(profile_name="prod", output_path=out, config_path=cfg, target_profile="staging")

    with sqlite3.connect(tmp_path / "staging.sqlite") as conn:
        email = conn.execute("SELECT email FROM users WHERE id=1").fetchone()[0]
    assert "@example.com" in email


def test_run_sanitize_requires_rules(tmp_path):
    source = tmp_path / "prod.sqlite"
    _seed(source)
    cfg = tmp_path / "dbrestore.yaml"
    cfg.write_text(
        f"""
version: 1
defaults:
  output_dir: {tmp_path / "backups"}
  log_dir: {tmp_path / "logs"}
profiles:
  prod:
    db_type: sqlite
    database: {source}
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(DBRestoreError, match="No masking rules"):
        run_sanitize(profile_name="prod", output_path=tmp_path / "o.sqlite", config_path=cfg)


class _FakeCursor:
    def __init__(self, distinct):
        self.distinct = distinct
        self.selects: list[str] = []
        self.updates: list[tuple[str, list]] = []

    def execute(self, sql, params=None):
        self.selects.append(sql)

    def fetchall(self):
        return [(v,) for v in self.distinct]

    def executemany(self, sql, seq):
        self.updates.append((sql, list(seq)))

    def close(self):
        pass


class _FakeConn:
    def __init__(self, distinct):
        self._cur = _FakeCursor(distinct)
        self.committed = False

    def cursor(self):
        return self._cur

    def commit(self):
        self.committed = True


def test_apply_masking_postgres_dialect():
    conn = _FakeConn(["a@corp.com", "b@corp.com"])
    counts = apply_masking(conn, [MaskRule("users", "email", "email")], KEY, POSTGRES)
    cur = conn._cur
    assert cur.selects == ['SELECT DISTINCT "email" FROM "users" WHERE "email" IS NOT NULL']
    sql, seq = cur.updates[0]
    assert sql == 'UPDATE "users" SET "email" = %s WHERE "email" = %s'
    assert len(seq) == 2
    assert counts["users.email"] == 2
    assert conn.committed is True


def test_apply_masking_mysql_dialect_uses_backticks():
    conn = _FakeConn(["x"])
    apply_masking(conn, [MaskRule("t", "c", "hash")], KEY, MYSQL)
    cur = conn._cur
    assert cur.selects[0] == "SELECT DISTINCT `c` FROM `t` WHERE `c` IS NOT NULL"
    assert cur.updates[0][0] == "UPDATE `t` SET `c` = %s WHERE `c` = %s"


def _engine_config(tmp_path: Path, db_type: str) -> Path:
    cfg = tmp_path / "dbrestore.yaml"
    cfg.write_text(
        f"""
version: 1
defaults:
  output_dir: {tmp_path / "backups"}
  log_dir: {tmp_path / "logs"}
profiles:
  src:
    db_type: {db_type}
    host: localhost
    port: 5432
    username: app
    database: appdb
    masking:
      rules:
        - {{ table: users, column: email, strategy: email }}
""".strip(),
        encoding="utf-8",
    )
    return cfg


def test_run_sanitize_postgres_requires_target(tmp_path):
    cfg = _engine_config(tmp_path, "postgres")
    with pytest.raises(DBRestoreError, match="requires --target-profile"):
        run_sanitize(profile_name="src", config_path=cfg)


def test_run_sanitize_mongo_not_supported(tmp_path):
    cfg = _engine_config(tmp_path, "mongo")
    with pytest.raises(DBRestoreError, match="not supported"):
        run_sanitize(profile_name="src", config_path=cfg)
