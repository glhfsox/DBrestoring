"""PII masking / anonymization transforms for cached database snapshots.

Strategies are deterministic given a key: the same input value always maps to the
same masked value, which preserves referential integrity (a masked email joins
the same way across tables) while being irreversible (HMAC-SHA256, no plaintext
stored). NULLs are left untouched.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import sqlite3
from dataclasses import dataclass

from dbrestore.errors import ConfigError

STRATEGIES = ("redact", "null", "constant", "email", "name", "phone", "hash")

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_FIRST_NAMES = (
    "Alex",
    "Sam",
    "Jordan",
    "Taylor",
    "Morgan",
    "Casey",
    "Riley",
    "Jamie",
    "Avery",
    "Quinn",
    "Robin",
    "Drew",
    "Skyler",
    "Reese",
    "Cameron",
    "Devon",
)
_LAST_NAMES = (
    "Carter",
    "Reed",
    "Shaw",
    "Lane",
    "Hayes",
    "Blair",
    "Frost",
    "Wells",
    "Nolan",
    "Pierce",
    "Vance",
    "Cross",
    "Hale",
    "Knox",
    "Page",
    "Sloan",
)


@dataclass(frozen=True)
class MaskRule:
    table: str
    column: str
    strategy: str = "redact"
    value: str | None = None


def derive_key(salt: bytes) -> bytes:
    return hashlib.sha256(salt).digest()


def validate_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ConfigError(
            f"Invalid SQL identifier in masking rule: {name!r} "
            "(only letters, digits, and underscores are supported)."
        )
    return name


def _digest(key: bytes, value: str) -> str:
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()


def mask_value(strategy: str, value: object, key: bytes, *, constant: str | None = None) -> object:
    if value is None:
        return None
    if strategy == "null":
        return None
    if strategy == "redact":
        return constant if constant is not None else "***"
    if strategy == "constant":
        return constant

    text = str(value)
    h = _digest(key, text)
    if strategy == "hash":
        return h[:32]
    if strategy == "email":
        return f"user{int(h[:8], 16)}@example.com"
    if strategy == "name":
        first = _FIRST_NAMES[int(h[0:4], 16) % len(_FIRST_NAMES)]
        last = _LAST_NAMES[int(h[4:8], 16) % len(_LAST_NAMES)]
        return f"{first} {last}"
    if strategy == "phone":
        digits = str(int(h[:12], 16)).rjust(10, "0")[:10]
        return f"+1{digits}"
    raise ConfigError(f"Unknown masking strategy: {strategy!r}. Expected one of {STRATEGIES}.")


@dataclass(frozen=True)
class Dialect:
    quote_open: str
    quote_close: str
    placeholder: str


SQLITE = Dialect('"', '"', "?")
POSTGRES = Dialect('"', '"', "%s")
MYSQL = Dialect("`", "`", "%s")


def _quote(dialect: Dialect, identifier: str) -> str:
    return f"{dialect.quote_open}{validate_identifier(identifier)}{dialect.quote_close}"


def apply_masking(conn, rules: list[MaskRule], key: bytes, dialect: Dialect) -> dict[str, int]:
    """Apply masking rules in place over a DB-API 2.0 connection.

    Uses value-based UPDATEs (no rowid/primary key required), so the same logic
    works for SQLite, PostgreSQL, and MySQL/MariaDB — only quoting and the
    parameter placeholder differ per dialect. Masking is deterministic, so every
    row sharing a value gets the same masked value. Returns distinct values
    changed per rule.
    """
    counts: dict[str, int] = {}
    ph = dialect.placeholder
    cursor = conn.cursor()
    try:
        for rule in rules:
            if rule.strategy not in STRATEGIES:
                raise ConfigError(
                    f"Unknown masking strategy: {rule.strategy!r}. Expected one of {STRATEGIES}."
                )
            table = _quote(dialect, rule.table)
            column = _quote(dialect, rule.column)

            cursor.execute(f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL")
            mapping: list[tuple[object, object]] = []
            for (value,) in cursor.fetchall():
                masked = mask_value(rule.strategy, value, key, constant=rule.value)
                if masked != value:
                    mapping.append((masked, value))

            if mapping:
                cursor.executemany(
                    f"UPDATE {table} SET {column} = {ph} WHERE {column} = {ph}", mapping
                )
            counts[f"{rule.table}.{rule.column}"] = len(mapping)
    finally:
        cursor.close()
    conn.commit()
    return counts


def apply_masking_sqlite(
    conn: sqlite3.Connection, rules: list[MaskRule], key: bytes
) -> dict[str, int]:
    return apply_masking(conn, rules, key, SQLITE)
