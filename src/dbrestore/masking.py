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


def apply_masking_sqlite(
    conn: sqlite3.Connection, rules: list[MaskRule], key: bytes
) -> dict[str, int]:
    """Apply masking rules in place to a SQLite connection. Returns rows changed per rule."""
    counts: dict[str, int] = {}
    for rule in rules:
        if rule.strategy not in STRATEGIES:
            raise ConfigError(
                f"Unknown masking strategy: {rule.strategy!r}. Expected one of {STRATEGIES}."
            )
        table = validate_identifier(rule.table)
        column = validate_identifier(rule.column)

        rows = conn.execute(f'SELECT rowid, "{column}" FROM "{table}"').fetchall()
        updates: list[tuple[object, int]] = []
        for rowid, value in rows:
            masked = mask_value(rule.strategy, value, key, constant=rule.value)
            if masked != value:
                updates.append((masked, rowid))

        if updates:
            conn.executemany(f'UPDATE "{table}" SET "{column}" = ? WHERE rowid = ?', updates)
        counts[f"{table}.{column}"] = len(updates)

    conn.commit()
    return counts
