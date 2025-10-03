"""Command-line utility for migrating configuration versions.

This module provides an ``apply`` command for loading configuration
payloads from a YAML file and persisting them into the ``config_versions``
store, and a ``rollback`` command for restoring a previous configuration
version.  All migrations are recorded in ``config_migrations`` so that
changes can be audited later.
"""

from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Tuple

import yaml
from sqlalchemy import JSON, Column, DateTime, Integer, String, UniqueConstraint, create_engine, select
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool


LOGGER = logging.getLogger("config_migrate")
_SQLITE_FALLBACK_FLAG = "CONFIG_ALLOW_SQLITE_FOR_TESTS"


def _require_database_url() -> str:
    url = os.getenv("CONFIG_DATABASE_URL")
    if not url:
        raise RuntimeError(
            "CONFIG_DATABASE_URL must be defined when running config migrations."
        )

    normalized = url.lower()
    allowed_prefixes = ("postgresql://", "postgresql+psycopg://", "postgresql+psycopg2://")
    if normalized.startswith("postgres://"):
        url = "postgresql://" + url.split("://", 1)[1]
        normalized = url.lower()

    if normalized.startswith(allowed_prefixes):
        return url

    if os.getenv(_SQLITE_FALLBACK_FLAG) == "1":
        LOGGER.warning(
            "Allowing non-Postgres CONFIG_DATABASE_URL '%s' because %s=1.",
            url,
            _SQLITE_FALLBACK_FLAG,
        )
        return url

    raise RuntimeError(
        "CONFIG_DATABASE_URL must point to a PostgreSQL/TimescaleDB instance; "
        f"received '{url}'."
    )


def _create_engine(database_url: str):
    """Create a SQLAlchemy engine with driver-appropriate safeguards."""

    connect_args: Dict[str, Any] = {}
    engine_kwargs: Dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
    }

    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        engine_kwargs["connect_args"] = connect_args
        if ":memory:" in database_url:
            engine_kwargs["poolclass"] = StaticPool
    else:
        connect_args["sslmode"] = os.getenv("CONFIG_DB_SSLMODE", "require")
        engine_kwargs["connect_args"] = connect_args
        engine_kwargs.update(
            pool_size=int(os.getenv("CONFIG_DB_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("CONFIG_DB_MAX_OVERFLOW", "5")),
            pool_timeout=int(os.getenv("CONFIG_DB_POOL_TIMEOUT", "30")),
            pool_recycle=int(os.getenv("CONFIG_DB_POOL_RECYCLE", "1800")),
        )

    return create_engine(database_url, **engine_kwargs)


DATABASE_URL = _require_database_url()
engine = _create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)

Base = declarative_base()


class ConfigVersion(Base):
    __tablename__ = "config_versions"

    id = Column(Integer, primary_key=True)
    account_id = Column(String, nullable=False, default="global")
    key = Column(String, nullable=False)
    value_json = Column(JSON, nullable=False)
    version = Column(Integer, nullable=False)
    approvers = Column(JSON, nullable=False, default=list)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint("account_id", "key", "version", name="uq_config_version"),
    )


class ConfigMigration(Base):
    __tablename__ = "config_migrations"

    id = Column(Integer, primary_key=True)
    actor = Column(String, nullable=False)
    version = Column(Integer, nullable=False)
    action = Column(String, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


Base.metadata.create_all(bind=engine)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class LoadedConfig:
    account_id: str
    actor: str
    entries: Mapping[str, Any]


_META_KEYS = {"account_id", "actor", "entries"}


def load_yaml_config(path: Path, *, default_actor: str) -> LoadedConfig:
    """Load configuration payload and associated metadata from *path*."""

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if not isinstance(data, MutableMapping):
        raise ValueError(f"YAML root must be a mapping, got {type(data)!r}")

    account_id = str(data.get("account_id", "global"))
    actor = str(data.get("actor", default_actor))

    if "entries" in data:
        entries = data["entries"]
        if not isinstance(entries, Mapping):
            raise ValueError("'entries' must be a mapping of config keys to values")
    else:
        entries = {k: v for k, v in data.items() if k not in _META_KEYS}

    if not entries:
        raise ValueError("No configuration entries found in YAML file")

    return LoadedConfig(account_id=account_id, actor=actor, entries=entries)


def _latest_version(session: Session, *, account_id: str, key: str) -> Optional[ConfigVersion]:
    stmt = (
        select(ConfigVersion)
        .where(ConfigVersion.account_id == account_id, ConfigVersion.key == key)
        .order_by(ConfigVersion.version.desc())
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def _previous_version(session: Session, *, account_id: str, key: str, before_version: int) -> Optional[ConfigVersion]:
    stmt = (
        select(ConfigVersion)
        .where(
            ConfigVersion.account_id == account_id,
            ConfigVersion.key == key,
            ConfigVersion.version < before_version,
        )
        .order_by(ConfigVersion.version.desc())
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def _serialize(value: Any) -> Any:
    """Ensure *value* can be stored as JSON by round-tripping through ``json``."""

    return json.loads(json.dumps(value))


def apply_config(session: Session, *, config: LoadedConfig) -> Tuple[int, Dict[str, Tuple[Any, Any]]]:
    """Apply *config* entries, returning the number of versions written."""

    changed: Dict[str, Tuple[Any, Any]] = {}
    applied_versions = 0

    for key, payload in config.entries.items():
        normalized_payload = _serialize(payload)
        latest = _latest_version(session, account_id=config.account_id, key=key)
        if latest and latest.value_json == normalized_payload:
            continue

        next_version = (latest.version + 1) if latest else 1
        record = ConfigVersion(
            account_id=config.account_id,
            key=key,
            value_json=normalized_payload,
            version=next_version,
            approvers=[],
            ts=_utcnow(),
        )
        session.add(record)
        session.flush()  # ensure ``record.id`` is populated for logging
        session.add(
            ConfigMigration(actor=config.actor, version=record.id, action="apply", ts=_utcnow())
        )
        changed[key] = (latest.value_json if latest else None, normalized_payload)
        applied_versions += 1

    if applied_versions:
        session.commit()
    else:
        session.rollback()

    return applied_versions, changed


def rollback_version(session: Session, *, version_id: int, actor: str) -> ConfigVersion:
    """Rollback the configuration to the version preceding *version_id*."""

    record = session.get(ConfigVersion, version_id)
    if record is None:
        raise ValueError(f"No config version found with id={version_id}")

    previous = _previous_version(
        session, account_id=record.account_id, key=record.key, before_version=record.version
    )
    if previous is None:
        raise ValueError(
            "Cannot rollback because there is no prior version for "
            f"account '{record.account_id}' key '{record.key}'."
        )

    latest = _latest_version(session, account_id=record.account_id, key=record.key)
    next_version = (latest.version + 1) if latest else 1

    restored = ConfigVersion(
        account_id=record.account_id,
        key=record.key,
        value_json=_serialize(previous.value_json),
        version=next_version,
        approvers=[],
        ts=_utcnow(),
    )
    session.add(restored)
    session.flush()
    session.add(ConfigMigration(actor=actor, version=restored.id, action="rollback", ts=_utcnow()))
    session.commit()
    return restored


def describe_changes(changes: Mapping[str, Tuple[Any, Any]]) -> str:
    lines = []
    for key, (before, after) in sorted(changes.items()):
        lines.append(f"- {key}")
        lines.append(f"    before: {json.dumps(before, indent=2, sort_keys=True)}")
        lines.append(f"    after:  {json.dumps(after, indent=2, sort_keys=True)}")
    return "\n".join(lines)


def _default_actor(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    return os.getenv("MIGRATION_ACTOR") or getpass.getuser()


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Configuration migration utility")
    subparsers = parser.add_subparsers(dest="command", required=True)

    apply_parser = subparsers.add_parser("apply", help="Apply configuration from a YAML file")
    apply_parser.add_argument("config_path", type=Path, help="Path to YAML configuration file")
    apply_parser.add_argument("--actor", dest="actor", help="Override actor recorded in migrations")

    rollback_parser = subparsers.add_parser("rollback", help="Rollback to the previous version for a key")
    rollback_parser.add_argument("version_id", type=int, help="Identifier of the version to rollback")
    rollback_parser.add_argument("--actor", dest="actor", help="Override actor recorded in migrations")

    args = parser.parse_args(list(argv) if argv is not None else None)

    actor = _default_actor(args.actor)

    if args.command == "apply":
        config_path: Path = args.config_path
        if not config_path.exists():
            parser.error(f"Configuration file '{config_path}' does not exist")

        loaded = load_yaml_config(config_path, default_actor=actor)
        with SessionLocal() as session:
            applied, changes = apply_config(session, config=loaded)
        if not applied:
            print("No changes detected; configuration already up to date.")
            return 0
        print(f"Applied {applied} configuration version(s) for account '{loaded.account_id}'.")
        print(describe_changes(changes))
        return 0

    if args.command == "rollback":
        with SessionLocal() as session:
            restored = rollback_version(session, version_id=args.version_id, actor=actor)
        print(
            "Rolled back key '{key}' for account '{account}' to version {version}.".format(
                key=restored.key, account=restored.account_id, version=restored.version
            )
        )
        return 0

    parser.error("Unknown command")


if __name__ == "__main__":
    raise SystemExit(main())
