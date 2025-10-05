"""Configuration helpers for dependency wiring across services."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict


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


@lru_cache(maxsize=None)
def get_redis_client(account_id: str) -> RedisClient:
    return RedisClient(dsn=_env(account_id, "REDIS_DSN", "redis://localhost:6379/0"))


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



_SUPPORTED_POSTGRES_SCHEMES = {
    "postgres",
    "postgresql",
    "timescale",
    "postgresql+psycopg",
    "postgresql+psycopg2",
}

_SUPPORTED_SQLITE_SCHEMES = {
    "sqlite",
    "sqlite+pysqlite",
}


def _normalize_timescale_dsn(raw_dsn: str) -> str:
    """Coerce supported PostgreSQL-compatible schemes to the psycopg default."""

    stripped = raw_dsn.strip()
    if not stripped:
        raise RuntimeError("Timescale DSN cannot be empty once configured.")

    scheme, separator, remainder = stripped.partition("://")
    if not separator:
        raise RuntimeError(
            "Timescale DSN must include a URI scheme such as postgresql:// or sqlite://."
        )

    normalized_scheme = scheme.lower()

    if normalized_scheme in _SUPPORTED_POSTGRES_SCHEMES:
        return f"postgresql://{remainder}"

    if normalized_scheme in _SUPPORTED_SQLITE_SCHEMES:
        return f"{normalized_scheme}://{remainder}"

    raise RuntimeError(
        "Timescale DSN must use a PostgreSQL/Timescale compatible scheme or sqlite:// for testing."
    )



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
        return _normalize_timescale_dsn(stripped)

    raise RuntimeError(
        "Timescale DSN is not configured. Set TIMESCALE_DSN or "
        "AETHER_<ACCOUNT>_TIMESCALE_DSN for each trading account."
    )


@lru_cache(maxsize=None)
def get_timescale_session(account_id: str) -> TimescaleSession:
    dsn = _resolve_timescale_dsn(account_id)
    schema = _env(account_id, "TIMESCALE_SCHEMA", f"acct_{account_id}")
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
