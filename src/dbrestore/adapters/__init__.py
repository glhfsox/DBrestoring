from __future__ import annotations

from dbrestore.adapters.base import DatabaseAdapter
from dbrestore.adapters.mongo import MongoAdapter
from dbrestore.adapters.mysql import MySQLAdapter
from dbrestore.adapters.postgres import PostgresAdapter
from dbrestore.adapters.sqlite import SQLiteAdapter

ADAPTERS: dict[str, DatabaseAdapter] = {
    "postgres": PostgresAdapter(),
    "mysql": MySQLAdapter("mysql"),
    "mariadb": MySQLAdapter("mariadb"),
    "mongo": MongoAdapter(),
    "sqlite": SQLiteAdapter(),
}


def get_adapter(db_type: str) -> DatabaseAdapter:
    return ADAPTERS[db_type]
