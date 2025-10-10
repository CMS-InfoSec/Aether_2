"""Utilities for enforcing account-scoped database models.

This module centralises the logic for declaring `account_id` columns that
reference the canonical `accounts.account_id` primary key.  The helpers expose a
SQLAlchemy ``TypeDecorator`` that stores UUID values in PostgreSQL/Timescale but
falls back to plain text when tests run against SQLite or the lightweight ORM
shim.  Calling :func:`account_id_column` ensures that every persisted model
declares a foreign-key relationship back to the accounts table instead of using
free-form ``String`` columns.

In environments where SQLAlchemy is unavailable we still expose a placeholder
type so modules can import without crashing; the actual declarative models are
only constructed when the real dependency stack is present.
"""

from __future__ import annotations

from uuid import UUID

SQLALCHEMY_AVAILABLE = True

try:  # pragma: no cover - exercised when SQLAlchemy is installed
    from sqlalchemy import Column, ForeignKey
    from sqlalchemy.dialects.postgresql import UUID as PGUUID
    from sqlalchemy.types import CHAR, TypeDecorator
except Exception:  # pragma: no cover - executed in dependency-light environments
    SQLALCHEMY_AVAILABLE = False
    Column = ForeignKey = PGUUID = CHAR = None  # type: ignore[assignment]
    TypeDecorator = object  # type: ignore[assignment]


if SQLALCHEMY_AVAILABLE:

    class AccountId(TypeDecorator):
        """Store UUID account identifiers across PostgreSQL and SQLite backends."""

        impl = PGUUID(as_uuid=True)
        cache_ok = True

        def load_dialect_impl(self, dialect):  # pragma: no cover - SQLAlchemy hook
            if dialect.name == "sqlite":
                return dialect.type_descriptor(CHAR(36))
            return dialect.type_descriptor(PGUUID(as_uuid=True))

        def process_bind_param(self, value, dialect):  # pragma: no cover - SQLAlchemy hook
            if value is None:
                return None
            uuid_value = value if isinstance(value, UUID) else UUID(str(value))
            if dialect.name == "sqlite":
                return str(uuid_value)
            return uuid_value

        def process_result_value(self, value, dialect):  # pragma: no cover - SQLAlchemy hook
            if value is None:
                return None
            if isinstance(value, UUID):
                return value
            return UUID(str(value))


    def account_id_column(
        *,
        primary_key: bool = False,
        index: bool = False,
        nullable: bool = False,
        unique: bool = False,
        ondelete: str = "CASCADE",
    ):
        """Return a ``Column`` configured to reference ``accounts.account_id``."""

        fk = ForeignKey("accounts.account_id", ondelete=ondelete)
        return Column(
            AccountId(),
            fk,
            primary_key=primary_key,
            index=index,
            nullable=nullable,
            unique=unique,
        )

else:  # pragma: no cover - exercised in dependency-light environments

    class AccountId:  # type: ignore[override]
        """Placeholder type when SQLAlchemy is not installed."""

        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs


    def account_id_column(**_: object):
        raise RuntimeError("SQLAlchemy is required to declare account_id columns")


__all__ = ["AccountId", "SQLALCHEMY_AVAILABLE", "account_id_column"]

