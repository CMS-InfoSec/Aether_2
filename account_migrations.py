"""Database migrations for the account management service."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    JSON,
    MetaData,
    String,
    Table,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.engine import Engine


metadata = MetaData()


accounts = Table(
    "accounts",
    metadata,
    Column("account_id", PGUUID(as_uuid=True), primary_key=True, default=uuid4),
    Column("name", String, nullable=False),
    Column("owner_user_id", PGUUID(as_uuid=True), nullable=False),
    Column("base_currency", String, nullable=False, server_default="USD"),
    Column("sim_mode", Boolean, nullable=False, server_default="false"),
    Column("hedge_auto", Boolean, nullable=False, server_default="true"),
    Column("active", Boolean, nullable=False, server_default="true"),
    Column("created_at", DateTime(timezone=True), nullable=False, default=datetime.utcnow),
    Column("updated_at", DateTime(timezone=True), nullable=False, default=datetime.utcnow),
)


kraken_keys = Table(
    "kraken_keys",
    metadata,
    Column("id", PGUUID(as_uuid=True), primary_key=True, default=uuid4),
    Column(
        "account_id",
        PGUUID(as_uuid=True),
        ForeignKey("accounts.account_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("encrypted_api_key", Text, nullable=False),
    Column("encrypted_api_secret", Text, nullable=False),
    Column("last_rotated_at", DateTime(timezone=True), nullable=False, default=datetime.utcnow),
)


account_configs = Table(
    "account_configs",
    metadata,
    Column("id", PGUUID(as_uuid=True), primary_key=True, default=uuid4),
    Column(
        "account_id",
        PGUUID(as_uuid=True),
        ForeignKey("accounts.account_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("config_type", String, nullable=False),
    Column("payload", JSON, nullable=False, default=dict),
    Column("created_at", DateTime(timezone=True), nullable=False, default=datetime.utcnow),
    Column("updated_at", DateTime(timezone=True), nullable=False, default=datetime.utcnow),
)


audit_logs = Table(
    "audit_logs",
    metadata,
    Column("id", PGUUID(as_uuid=True), primary_key=True, default=uuid4),
    Column("account_id", PGUUID(as_uuid=True), nullable=False),
    Column("user_id", PGUUID(as_uuid=True), nullable=False),
    Column("action", String, nullable=False),
    Column("details", JSON, nullable=False, default=dict),
    Column("timestamp", DateTime(timezone=True), nullable=False, default=datetime.utcnow),
)


def upgrade(engine: Engine) -> None:
    """Create the account management tables if they do not exist."""

    existing = set(engine.table_names())
    tables = [accounts, kraken_keys, account_configs, audit_logs]
    create: list[Table] = [table for table in tables if table.name not in existing]
    if not create:
        return
    metadata.create_all(engine, tables=create)


__all__ = ["upgrade", "metadata"]

