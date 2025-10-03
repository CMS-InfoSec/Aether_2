"""Backfill config_versions into the shared Timescale cluster."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

# revision identifiers, used by Alembic.
revision = "0004_config_service_postgres"
down_revision = "0003_update_risk_numeric"
branch_labels = None
depends_on = None


def _table_exists(conn: Connection, table_name: str) -> bool:
    inspector = sa.inspect(conn)
    return table_name in inspector.get_table_names()


def _create_config_versions(conn: Connection) -> None:
    if _table_exists(conn, "config_versions"):
        return

    op.create_table(
        "config_versions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.String(), nullable=False, server_default="global"),
        sa.Column("key", sa.String(), nullable=False),
        sa.Column("value_json", sa.JSON(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("approvers", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW() AT TIME ZONE 'UTC'"),
        ),
        sa.UniqueConstraint("account_id", "key", "version", name="uq_config_version"),
    )


def _coerce_json(value: object) -> object:
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return value
    try:
        return json.loads(value)  # type: ignore[arg-type]
    except (TypeError, json.JSONDecodeError):
        return value


def _coerce_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iter_legacy_rows() -> Iterable[Mapping[str, object]]:
    configured_url = os.getenv("LEGACY_CONFIG_DATABASE_URL")
    sqlite_path = Path(os.getenv("LEGACY_CONFIG_SQLITE_PATH", "/tmp/config.db"))

    if configured_url:
        source_url = configured_url
    elif sqlite_path.exists():
        source_url = f"sqlite+pysqlite:///{sqlite_path}"
    else:
        return []

    engine = sa.create_engine(source_url, future=True)
    try:
        with engine.connect() as legacy_conn:
            if not legacy_conn.dialect.has_table(legacy_conn, "config_versions"):
                return []
            try:
                result = legacy_conn.execute(
                    text(
                        """
                        SELECT id, account_id, key, value_json, version, approvers, ts
                        FROM config_versions
                        ORDER BY id
                        """
                    )
                )
            except SQLAlchemyError:
                return []
            return [row._mapping for row in result]
    finally:
        engine.dispose()


def _import_legacy_rows(conn: Connection) -> None:
    try:
        existing_count = conn.execute(text("SELECT COUNT(1) FROM config_versions")).scalar()
    except SQLAlchemyError:
        existing_count = None
    if existing_count and existing_count > 0:
        return

    rows = list(_iter_legacy_rows())
    if not rows:
        return

    config_versions = sa.table(
        "config_versions",
        sa.column("id", sa.Integer()),
        sa.column("account_id", sa.String()),
        sa.column("key", sa.String()),
        sa.column("value_json", sa.JSON()),
        sa.column("version", sa.Integer()),
        sa.column("approvers", sa.JSON()),
        sa.column("ts", sa.DateTime(timezone=True)),
    )

    payload = []
    for row in rows:
        payload.append(
            {
                "id": row.get("id"),
                "account_id": row.get("account_id") or "global",
                "key": row.get("key"),
                "value_json": _coerce_json(row.get("value_json")),
                "version": row.get("version") or 1,
                "approvers": _coerce_json(row.get("approvers")) or [],
                "ts": _coerce_timestamp(row.get("ts")) or datetime.now(timezone.utc),
            }
        )

    op.bulk_insert(config_versions, payload)

    if conn.dialect.name == "postgresql":
        conn.execute(
            text(
                "SELECT setval(pg_get_serial_sequence('config_versions', 'id'), "
                "COALESCE((SELECT MAX(id) FROM config_versions), 0))"
            )
        )


def upgrade() -> None:
    conn = op.get_bind()
    _create_config_versions(conn)
    _import_legacy_rows(conn)


def downgrade() -> None:
    if _table_exists(op.get_bind(), "config_versions"):
        op.drop_table("config_versions")
