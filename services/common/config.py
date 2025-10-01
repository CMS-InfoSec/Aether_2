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


@lru_cache(maxsize=None)
def get_timescale_session(account_id: str) -> TimescaleSession:
    dsn = _env(account_id, "TIMESCALE_DSN", "postgresql://timescale:password@localhost:5432/telemetry")
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
