"""Configuration helpers for dependency wiring across services."""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

from shared.postgres import normalize_postgres_dsn, normalize_postgres_schema


@dataclass(frozen=True)
class RedisClient:
    dsn: str


@dataclass(frozen=True)
class FeastClient:
    project: str
    account_namespace: str


@dataclass(frozen=True)
class KafkaProducer:
    bootstrap_servers: str
    topic_prefix: str


@dataclass(frozen=True)
class NATSProducer:
    servers: str
    subject_prefix: str


@dataclass(frozen=True)
class TimescaleSession:
    dsn: str
    account_schema: str


@dataclass(frozen=True)
class KrakenCredentials:
    key: str
    secret: str


def _env(account_id: str, suffix: str, default: str) -> str:
    env_key = f"AETHER_{account_id.upper()}_{suffix}"
    return os.getenv(env_key, default)


def _require_account_env(account_id: str, suffix: str, *, label: str) -> str:
    """Return a required account-scoped environment variable."""

    env_key = f"AETHER_{account_id.upper()}_{suffix}"
    raw_value = os.getenv(env_key)
    if raw_value is None:
        raise RuntimeError(
            f"{label} is not configured. Set {env_key} to a redis:// or memory:// DSN."
        )

    value = raw_value.strip()
    if not value:
        raise RuntimeError(
            f"{env_key} is set but empty; configure {label} with a redis:// or memory:// DSN."
        )
    return value


@lru_cache(maxsize=None)
def get_redis_client(account_id: str) -> RedisClient:
    """Return the configured Redis client settings for an account."""

    dsn = _require_account_env(account_id, "REDIS_DSN", label="Redis DSN")
    return RedisClient(dsn=dsn)


@lru_cache(maxsize=None)
def get_feast_client(account_id: str) -> FeastClient:
    project = _env(account_id, "FEAST_PROJECT", "default")
    account_namespace = _env(account_id, "FEATURE_NAMESPACE", account_id)
    return FeastClient(project=project, account_namespace=account_namespace)


@lru_cache(maxsize=None)
def get_kafka_producer(account_id: str) -> KafkaProducer:
    bootstrap = _env(account_id, "KAFKA_BOOTSTRAP", "kafka:9092")
    prefix = _env(account_id, "KAFKA_TOPIC_PREFIX", account_id)
    return KafkaProducer(bootstrap_servers=bootstrap, topic_prefix=prefix)


@lru_cache(maxsize=None)
def get_nats_producer(account_id: str) -> NATSProducer:
    servers = _env(account_id, "NATS_SERVERS", "nats://localhost:4222")
    subject_prefix = _env(account_id, "NATS_SUBJECT_PREFIX", account_id)
    return NATSProducer(servers=servers, subject_prefix=subject_prefix)


_SCHEMA_INVALID_CHARS = re.compile(r"[^a-z0-9_]")


def _sanitize_schema_name(raw: str, *, default: bool) -> str:
    """Return a Postgres-safe schema identifier.

    Identifiers may only contain ``[a-z0-9_]`` once normalised and must not
    begin with a digit.  For defaults we prefix ``acct_`` when not already
    present so each account receives an isolated namespace.
    """

    candidate = raw.strip().lower().replace("-", "_")
    candidate = _SCHEMA_INVALID_CHARS.sub("", candidate)
    candidate = re.sub(r"_+", "_", candidate).strip("_")

    if not candidate:
        raise RuntimeError("Timescale schema cannot be empty once configured")

    if candidate[0].isdigit():
        if default:
            candidate = f"acct_{candidate}"
        else:
            raise RuntimeError(
                "Timescale schema must not start with a digit; adjust the configured value"
            )

    if default and not candidate.startswith("acct_"):
        candidate = f"acct_{candidate}"

    if len(candidate) > 63:
        raise RuntimeError(
            "Timescale schema must be 63 characters or fewer once normalised"
        )

    return candidate


def _resolve_timescale_dsn(account_id: str) -> str:
    """Return a configured Timescale/PostgreSQL DSN for the given account."""

    env_keys = [f"AETHER_{account_id.upper()}_TIMESCALE_DSN", "TIMESCALE_DSN"]
    for key in env_keys:
        raw = os.getenv(key)
        if raw is None:
            continue
        stripped = raw.strip()
        if not stripped:
            raise RuntimeError(
                f"{key} is set but empty; configure a valid Timescale/PostgreSQL DSN."
            )
        return normalize_postgres_dsn(stripped, label="Timescale DSN")

    raise RuntimeError(
        "Timescale DSN is not configured. Set TIMESCALE_DSN or "
        "AETHER_<ACCOUNT>_TIMESCALE_DSN for each trading account."
    )


@lru_cache(maxsize=None)
def get_timescale_session(account_id: str) -> TimescaleSession:
    dsn = _resolve_timescale_dsn(account_id)
    override = os.getenv(f"AETHER_{account_id.upper()}_TIMESCALE_SCHEMA")
    if override is not None:
        if not override.strip():
            raise RuntimeError(
                f"AETHER_{account_id.upper()}_TIMESCALE_SCHEMA is set but empty; "
                "configure a valid schema identifier"
            )
        schema = normalize_postgres_schema(
            override,
            label="Timescale schema",
            prefix_if_missing=None,
        )
    else:
        schema = normalize_postgres_schema(
            account_id,
            label="Timescale schema",
            prefix_if_missing="acct_",
            allow_leading_digit_prefix=True,
        )
    return TimescaleSession(dsn=dsn, account_schema=schema)


def get_kraken_credentials(account_id: str) -> KrakenCredentials:
    secret_path = Path(_env(account_id, "KRAKEN_SECRET_PATH", "/var/run/secrets/kraken.json"))
    if not secret_path.exists():
        raise FileNotFoundError(f"Kraken credentials not found at {secret_path}")

    data: Dict[str, Any] = json.loads(secret_path.read_text())
    key = data.get("key")
    secret = data.get("secret")
    if not key or not secret:
        raise ValueError("Kraken credential file is missing 'key' or 'secret'")
    return KrakenCredentials(key=key, secret=secret)


__all__ = [
    "RedisClient",
    "FeastClient",
    "KafkaProducer",
    "NATSProducer",
    "TimescaleSession",
    "KrakenCredentials",
    "get_redis_client",
    "get_feast_client",
    "get_kafka_producer",
    "get_nats_producer",
    "get_timescale_session",
    "get_kraken_credentials",
]
