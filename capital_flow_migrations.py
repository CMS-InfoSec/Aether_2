"""Schema migration helpers for the capital flow service."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any, Iterable

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    select,
    text,
)
try:  # SQLAlchemy types are optional in test environments
    from sqlalchemy.engine import Connection, Engine
except ImportError:  # pragma: no cover - depends on SQLAlchemy availability
    Connection = Any  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]

try:  # SQLAlchemy is optional in some environments
    from sqlalchemy import inspect
except ImportError:  # pragma: no cover - exercised when SQLAlchemy isn't installed
    inspect = None  # type: ignore[assignment]


_PRECISION = 38
_SCALE = 18
_QUANT = Decimal("1").scaleb(-_SCALE)
_SQLITE_STORAGE = String(_PRECISION + _SCALE + 2)


def upgrade(engine: Engine) -> None:
    """Upgrade legacy float columns to decimals while preserving historical data."""

    dialect = engine.dialect.name

    if dialect == "sqlite":
        _upgrade_sqlite(engine)
    elif dialect == "postgresql":
        _upgrade_postgres(engine)
    else:
        _rewrite_tables(engine)


def _upgrade_postgres(engine: Engine) -> None:
    ddl_type = f"NUMERIC({_PRECISION}, {_SCALE})"
    with engine.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE capital_flows "
                f"ALTER COLUMN amount TYPE {ddl_type} USING amount::{ddl_type}"
            )
        )
        connection.execute(
            text(
                "ALTER TABLE nav_baselines "
                f"ALTER COLUMN baseline TYPE {ddl_type} USING baseline::{ddl_type}"
            )
        )
        connection.execute(text("ALTER TABLE nav_baselines ALTER COLUMN baseline SET DEFAULT 0"))


def _upgrade_sqlite(engine: Engine) -> None:
    with engine.begin() as connection:
        if not _table_exists(connection, "capital_flows"):
            return

        column_types = _column_type_map(connection, "capital_flows")
        if column_types.get("amount", "").upper().startswith("NUMERIC"):
            return

        connection.execute(text("ALTER TABLE capital_flows RENAME TO capital_flows_old"))
        nav_exists = _table_exists(connection, "nav_baselines")
        if nav_exists:
            connection.execute(text("ALTER TABLE nav_baselines RENAME TO nav_baselines_old"))

        metadata = MetaData()

        capital_flows = Table(
            "capital_flows",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("account_id", String, nullable=False),
            Column("type", String, nullable=False),
            Column("amount", _SQLITE_STORAGE, nullable=False),
            Column("currency", String, nullable=False),
            Column("ts", DateTime(timezone=True), nullable=False),
        )

        nav_baselines = Table(
            "nav_baselines",
            metadata,
            Column("account_id", String, primary_key=True),
            Column("currency", String, nullable=False),
            Column(
                "baseline",
                _SQLITE_STORAGE,
                nullable=False,
                server_default=text("0"),
            ),
            Column("updated_at", DateTime(timezone=True), nullable=False),
        )

        metadata.create_all(connection)

        _copy_rows(
            connection,
            source="capital_flows_old",
            target=capital_flows,
            numeric_columns={"amount"},
        )
        if nav_exists:
            _copy_rows(
                connection,
                source="nav_baselines_old",
                target=nav_baselines,
                numeric_columns={"baseline"},
            )
        connection.execute(text("DROP TABLE capital_flows_old"))
        if nav_exists:
            connection.execute(text("DROP TABLE nav_baselines_old"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_capital_flows_account_id ON capital_flows (account_id)"))


def _rewrite_tables(engine: Engine) -> None:
    with engine.begin() as connection:
        if not _table_exists(connection, "capital_flows"):
            return

        nav_exists = _table_exists(connection, "nav_baselines")

        metadata = MetaData()

        capital_flows = Table(
            "capital_flows_tmp",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("account_id", String, nullable=False),
            Column("type", String, nullable=False),
            Column("amount", _SQLITE_STORAGE, nullable=False),
            Column("currency", String, nullable=False),
            Column("ts", DateTime(timezone=True), nullable=False),
        )

        nav_baselines = Table(
            "nav_baselines_tmp",
            metadata,
            Column("account_id", String, primary_key=True),
            Column("currency", String, nullable=False),
            Column(
                "baseline",
                _SQLITE_STORAGE,
                nullable=False,
                server_default=text("0"),
            ),
            Column("updated_at", DateTime(timezone=True), nullable=False),
        )

        metadata.create_all(connection)

        _copy_rows(
            connection,
            source="capital_flows",
            target=capital_flows,
            numeric_columns={"amount"},
        )
        if nav_exists:
            _copy_rows(
                connection,
                source="nav_baselines",
                target=nav_baselines,
                numeric_columns={"baseline"},
            )

        connection.execute(text("DROP TABLE capital_flows"))
        if nav_exists:
            connection.execute(text("DROP TABLE nav_baselines"))

        connection.execute(text("ALTER TABLE capital_flows_tmp RENAME TO capital_flows"))
        connection.execute(text("ALTER TABLE nav_baselines_tmp RENAME TO nav_baselines"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_capital_flows_account_id ON capital_flows (account_id)"))


def _copy_rows(
    connection: Connection,
    *,
    source: str,
    target: Table,
    numeric_columns: Iterable[str],
) -> None:
    source_table = Table(source, MetaData(), autoload_with=connection)
    rows = list(connection.execute(select(source_table)).mappings())
    if not rows:
        return

    inserts = []
    numeric_columns = {column.lower(): column for column in numeric_columns}
    for row in rows:
        mapping = dict(row)
        for lower_name, original_name in numeric_columns.items():
            key = next((k for k in mapping.keys() if k.lower() == lower_name), None)
            if key is None or mapping.get(key) is None:
                continue
            decimal_value = Decimal(str(mapping[key])).quantize(_QUANT, rounding=ROUND_HALF_EVEN)
            target_column = target.c.get(key)
            if isinstance(target_column.type, String):
                mapping[key] = format(decimal_value, f".{_SCALE}f")
            else:
                mapping[key] = decimal_value
        inserts.append(mapping)

    connection.execute(target.insert(), inserts)


def _column_type_map(connection: Connection, table: str) -> dict[str, str]:
    results = connection.execute(text(f"PRAGMA table_info({table})")).mappings()
    return {row["name"].lower(): row["type"] for row in results}


def _table_exists(connection: Connection, table: str) -> bool:
    if inspect is not None:
        return table in inspect(connection).get_table_names()

    dialect_name = getattr(getattr(connection, "dialect", None), "name", "")
    if dialect_name == "sqlite":
        result = connection.execute(
            text(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name = :table_name"
            ),
            {"table_name": table},
        ).first()
        if result is not None:
            return True

        temp_result = connection.execute(
            text(
                "SELECT name FROM sqlite_temp_master "
                "WHERE type='table' AND name = :table_name"
            ),
            {"table_name": table},
        ).first()
        return temp_result is not None

    try:
        connection.execute(text(f"SELECT 1 FROM {table} LIMIT 1"))
    except Exception:  # pragma: no cover - depends on backend availability
        return False
    return True
