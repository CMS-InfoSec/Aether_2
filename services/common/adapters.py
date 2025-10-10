from __future__ import annotations


import asyncio
import json
import logging
import os
import threading
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    ContextManager,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    cast,
)
from urllib.parse import urlparse
from weakref import WeakSet

import httpx


from common.utils.tracing import attach_correlation, current_correlation_id
from shared.k8s import ANNOTATION_ROTATED_AT as K8S_ROTATED_AT, KrakenSecretStore
from shared.spot import filter_spot_symbols, is_spot_symbol, normalize_spot_symbol
from services.secrets.secure_secrets import (
    EncryptedSecretEnvelope,
    EnvelopeEncryptor,
)

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

from services.common.config import (
    FeastClient,
    KafkaProducer,
    NATSProducer,
    RedisClient,
    TimescaleSession,
    _allow_test_fallbacks,
    get_feast_client,
    get_kafka_producer,
    get_nats_producer,
    get_redis_client,
    get_timescale_session,
)
from services.universe.repository import UniverseRepository

try:  # pragma: no cover - psycopg may be unavailable in minimal environments
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - allow unit tests without psycopg dependency
    psycopg = None
    dict_row = None

if psycopg is not None:  # pragma: no cover - executed when psycopg available
    PsycopgError = psycopg.Error
else:  # pragma: no cover - fallback for type checkers when psycopg missing
    class _PsycopgErrorFallback(Exception):
        """Fallback error used when psycopg is unavailable."""

    PsycopgError = _PsycopgErrorFallback

FeatureStore: Any
try:
    from feast import FeatureStore as _RuntimeFeatureStore
except Exception:  # pragma: no cover - Feast is optional during testing
    _RuntimeFeatureStore = None

FeatureStore = _RuntimeFeatureStore


class _LocalFeastStore:
    """Minimal Feast-compatible store for insecure-default environments."""

    def __init__(self, account_id: str, feast_client: FeastClient) -> None:
        self._account_id = account_id
        self._feast_client = feast_client
        self._lock = threading.RLock()
        root = Path(os.getenv("AETHER_STATE_DIR", ".aether_state"))
        self._state_path = root / "redis_feast" / account_id / "state.json"
        self._state: Dict[str, Any] | None = None

    def get_historical_features(
        self,
        *,
        entity_rows: Iterable[Mapping[str, Any]],
        feature_refs: Iterable[str],
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict[str, Any]]:
        del entity_rows  # unused in local fallback
        state = self._load_state()
        suffix = self._extract_suffix(feature_refs)
        records = list(state.get(suffix, []))
        if not records:
            return []
        filtered: List[Dict[str, Any]] = []
        for record in records:
            timestamp = record.get("event_timestamp")
            if isinstance(timestamp, str):
                try:
                    ts = datetime.fromisoformat(timestamp)
                except ValueError:
                    ts = datetime.fromtimestamp(0, tz=timezone.utc)
            elif isinstance(timestamp, datetime):
                ts = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
            else:
                ts = datetime.fromtimestamp(0, tz=timezone.utc)
            if ts < start_date or ts > end_date:
                continue
            filtered.append(dict(record))
        if filtered:
            return filtered
        return [dict(record) for record in records]

    def get_online_features(
        self,
        *,
        entity_rows: Iterable[Mapping[str, Any]],
        features: Iterable[str],
    ) -> Dict[str, Any]:
        state = self._load_state()
        instrument = None
        for row in entity_rows:
            candidate = row.get("instrument")
            if isinstance(candidate, str) and candidate:
                instrument = normalize_spot_symbol(candidate)
                break
        instrument = instrument or "btc-usd"
        online = state.setdefault("online", {})
        payload = online.get(instrument)
        if payload is None:
            payload = self._default_online_payload(instrument)
            online[instrument] = payload
            self._persist(state)
        response: Dict[str, Any] = {}
        view = self._view_name("instrument_features")
        for ref in features:
            field = ref.split(":", 1)[-1]
            response[f"{view}:{field}"] = payload.get(field)
        return response

    def _load_state(self) -> Dict[str, Any]:
        with self._lock:
            if self._state is not None:
                return self._state
            path = self._state_path
            try:
                if path.exists():
                    raw = path.read_text(encoding="utf-8")
                    data = _json_loads(raw)
                else:
                    data = {}
            except Exception:
                data = {}
            if not data:
                data = self._default_state()
                self._persist(data)
            self._state = data
            return data

    def _persist(self, state: Mapping[str, Any]) -> None:
        with self._lock:
            path = self._state_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(_json_dumps(state), encoding="utf-8")
            self._state = dict(state)

    def _default_state(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        iso = now.isoformat()
        return {
            "approved_instruments": [
                {"instrument": "BTC-USD", "approved": True, "event_timestamp": iso},
                {"instrument": "ETH-USD", "approved": True, "event_timestamp": iso},
            ],
            "fee_overrides": [
                {
                    "instrument": "BTC-USD",
                    "maker_bps": 6.0,
                    "taker_bps": 8.0,
                    "currency": "USD",
                    "event_timestamp": iso,
                }
            ],
            "fee_tiers": [
                {
                    "pair": "BTC-USD",
                    "tier": 1,
                    "maker_bps": 6.0,
                    "taker_bps": 8.0,
                    "notional_threshold": 0.0,
                    "event_timestamp": iso,
                },
                {
                    "pair": "DEFAULT",
                    "tier": 1,
                    "maker_bps": 10.0,
                    "taker_bps": 12.0,
                    "notional_threshold": 0.0,
                    "event_timestamp": iso,
                },
            ],
            "online": {
                "btc-usd": self._default_online_payload("btc-usd"),
            },
        }

    def _default_online_payload(self, instrument: str) -> Dict[str, Any]:
        seed = sum(ord(char) for char in instrument)
        base_price = 30_000 + (seed % 500) * 10
        base_spread = round(2.5 + (seed % 5) * 0.25, 4)
        feature_vector = [round(((seed + index) % 10) / 10.0, 4) for index in range(3)]
        confidence = {
            "model_confidence": 0.55,
            "state_confidence": 0.5,
            "execution_confidence": 0.52,
        }
        confidence["overall_confidence"] = sum(confidence.values()) / len(confidence)
        return {
            "features": feature_vector,
            "book_snapshot": {
                "mid_price": float(base_price),
                "spread_bps": base_spread,
                "imbalance": 0.1,
            },
            "state": {
                "regime": "neutral",
                "volatility": 0.32,
                "liquidity_score": 0.6,
                "conviction": 0.55,
            },
            "expected_edge_bps": 12.0,
            "take_profit_bps": 22.0,
            "stop_loss_bps": 8.0,
            "confidence": confidence,
            "drift_score": 0.0,
        }

    def _extract_suffix(self, feature_refs: Iterable[str]) -> str:
        for ref in feature_refs:
            view = ref.split(":", 1)[0]
            if "__" in view:
                return view.split("__", 1)[1]
            return view
        return ""

    def _view_name(self, suffix: str) -> str:
        return f"{self._feast_client.account_namespace}__{suffix}"

def _normalize_account_id(account_id: str) -> str:
    """Convert human readable admin labels into canonical keys."""

    return account_id.strip().lower().replace(" ", "-")


logger = logging.getLogger(__name__)


def _mask_secret(value: str) -> str:
    if not value:
        return "***"
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:2]}{'*' * (len(value) - 4)}{value[-2:]}"


def _sanitize_identifier(raw: str) -> str:
    value = raw.strip().lower().replace("-", "_")
    sanitized = "".join(ch for ch in value if ch.isalnum() or ch == "_")
    if not sanitized:
        sanitized = "acct"
    if sanitized[0].isdigit():
        sanitized = f"acct_{sanitized}"
    return sanitized
def _isoformat(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()


def _deserialize_timestamp(raw: str) -> datetime:
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=timezone.utc)
        return raw.astimezone(timezone.utc)
    try:
        return datetime.fromisoformat(raw)
    except ValueError:  # pragma: no cover - defensive guard for malformed data
        return datetime.fromtimestamp(0, tz=timezone.utc)


def _json_dumps(payload: Mapping[str, Any]) -> str:
    def _default(value: Any) -> Any:
        if isinstance(value, datetime):
            return _isoformat(value)
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, set):
            return sorted(value)
        return value

    return json.dumps(payload, default=_default)


def _json_loads(raw: str) -> Dict[str, Any]:
    if isinstance(raw, Mapping):
        return dict(raw)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return data
    return {"value": data}


def _require_psycopg() -> None:
    if psycopg is None:  # pragma: no cover - executed only when psycopg is absent
        raise RuntimeError(
            "psycopg is required for Timescale persistence but is not installed in this environment."
        )


@dataclass
class _TimescaleConnectionState:
    connection: "psycopg.Connection[Any]"
    lock: threading.RLock
    tables: Dict[str, str]
    dsn: str
    schema: str


class PublishError(RuntimeError):
    """Raised when a publish attempt fails for every configured transport."""


def _first_endpoint(value: str) -> str:
    return value.split(",")[0].strip()


def _as_base_url(endpoint: str) -> str:
    parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid endpoint '{endpoint}'")
    base = f"{parsed.scheme}://{parsed.netloc}"
    if parsed.path and parsed.path != "/":
        base = f"{base}{parsed.path.rstrip('/')}"
    return base


class _TimescaleStore:
    """Lightweight persistence layer backed by TimescaleDB/PostgreSQL."""

    _lock: ClassVar[threading.Lock] = threading.Lock()
    _connections: ClassVar[Dict[str, _TimescaleConnectionState]] = {}
    _TABLE_DEFINITIONS: ClassVar[Dict[str, str]] = {
    "acks": """
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            recorded_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL
        )
    """.strip(),
    "fills": """
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            recorded_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL
        )
    """.strip(),
    "shadow_fills": """
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            recorded_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL
        )
    """.strip(),
    "events": """
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            payload JSONB NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL
        )
    """.strip(),
    "telemetry": """
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            order_id TEXT,
            payload JSONB NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL
        )
    """.strip(),
    "audit_logs": """
        CREATE TABLE IF NOT EXISTS {table} (
            id TEXT PRIMARY KEY,
            payload JSONB NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL
        )
    """.strip(),
    "credential_events": """
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            event TEXT NOT NULL,
            event_type TEXT NOT NULL,
            secret_name TEXT,
            metadata JSONB NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL
        )
    """.strip(),
    "credential_rotations": """
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            secret_name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            rotated_at TIMESTAMPTZ NOT NULL,
            kms_key_id TEXT
        )
    """.strip(),
        "risk_configs": """
        CREATE TABLE IF NOT EXISTS {table} (
            account_id TEXT PRIMARY KEY,
            config JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )
    """.strip(),
    }
    _HYPER_TABLES: ClassVar[Tuple[str, ...]] = (
        "acks",
        "fills",
        "shadow_fills",
        "events",
        "telemetry",
    )

    def __init__(
        self,
        account_id: str,
        *,
        session_factory: Callable[[str], "TimescaleSession"],
        max_retries: int,
        backoff_seconds: float,
    ) -> None:
        self.account_id = account_id
        self._session_factory = session_factory
        self._max_retries = max(1, int(max_retries))
        self._backoff_seconds = float(backoff_seconds)
        self._state = self._ensure_connection()

    @property
    def state(self) -> _TimescaleConnectionState:
        return self._state

    def _ensure_connection(self) -> _TimescaleConnectionState:
        with self._lock:
            state = self._connections.get(self.account_id)
            if state is not None:
                return state
            session = self._session_factory(self.account_id)
            state = self._create_connection_state(session)
            self._connections[self.account_id] = state
            return state

    def _create_connection_state(self, session: "TimescaleSession") -> _TimescaleConnectionState:
        _require_psycopg()
        schema = session.account_schema or f"acct_{self.account_id}"
        normalized_schema = _sanitize_identifier(schema)
        connection = self._open_connection(session.dsn, normalized_schema)
        tables = {
            name: f'"{normalized_schema}"."{name}"' for name in self._TABLE_DEFINITIONS
        }
        state = _TimescaleConnectionState(
            connection=connection,
            lock=threading.RLock(),
            tables=tables,
            dsn=session.dsn,
            schema=normalized_schema,
        )
        self._initialize_schema(state)
        return state

    @staticmethod
    def _open_connection(
        dsn: str, schema: str
    ) -> "psycopg.Connection[Any]":  # pragma: no cover - requires psycopg
        connection = psycopg.connect(dsn)
        connection.autocommit = True
        if dict_row is not None:
            connection.row_factory = dict_row
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute(f'SET search_path TO "{schema}", public')
        return connection

    def _initialize_schema(self, state: _TimescaleConnectionState) -> None:
        with state.lock:
            conn = state.connection
            with conn.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{state.schema}"')
                try:
                    cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
                except Exception:  # pragma: no cover - extension may require elevated privileges
                    logger.debug("Unable to ensure timescaledb extension is installed", exc_info=True)
                for name, ddl in self._TABLE_DEFINITIONS.items():
                    table = state.tables[name]
                    cursor.execute(ddl.format(table=table))
                for name in self._HYPER_TABLES:
                    qualified = f"{state.schema}.{name}"
                    try:
                        cursor.execute(
                            "SELECT create_hypertable(%s, 'recorded_at', if_not_exists => TRUE, migrate_data => TRUE)",
                            (qualified,),
                        )
                    except Exception:  # pragma: no cover - tolerate missing extension
                        logger.debug("create_hypertable call failed", exc_info=True)

    def _with_retry(
        self,
        operation: Callable[["psycopg.Connection[Any]"], Any],
        *,
        state: _TimescaleConnectionState | None = None,
    ) -> Any:
        active_state = state or self._state
        delay = self._backoff_seconds
        for attempt in range(1, self._max_retries + 1):
            try:
                with active_state.lock:
                    connection = active_state.connection
                    if getattr(connection, "closed", False):
                        connection = self._open_connection(active_state.dsn, active_state.schema)
                        active_state.connection = connection
                    return operation(connection)
            except (PsycopgError, AttributeError) as exc:
                if attempt == self._max_retries:
                    raise RuntimeError("Timescale storage operation failed") from exc
                time.sleep(delay)
                delay *= 2

    def _execute(
        self,
        connection: "psycopg.Connection[Any]",
        sql: str,
        params: Tuple[Any, ...] | None = None,
        *,
        fetch: str | None = None,
    ) -> Any:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            if fetch == "all":
                return cursor.fetchall()
            if fetch == "one":
                return cursor.fetchone()
            return None

    # ------------------------------------------------------------------
    # Insert helpers
    # ------------------------------------------------------------------
    def record_ack(self, payload: Mapping[str, Any]) -> None:
        recorded_at = datetime.now(timezone.utc)
        table = self._state.tables["acks"]
        payload_json = _json_dumps(dict(payload))
        sql = f"INSERT INTO {table} (recorded_at, payload) VALUES (%s, %s::jsonb)"
        self._with_retry(lambda conn: self._execute(conn, sql, (recorded_at, payload_json)))

    def record_fill(self, payload: Mapping[str, Any], *, shadow: bool = False) -> None:
        recorded_at = datetime.now(timezone.utc)
        table_key = "shadow_fills" if shadow else "fills"
        table = self._state.tables[table_key]
        payload_json = _json_dumps(dict(payload))
        sql = f"INSERT INTO {table} (recorded_at, payload) VALUES (%s, %s::jsonb)"
        self._with_retry(lambda conn: self._execute(conn, sql, (recorded_at, payload_json)))

    def record_event(self, event_type: str, payload: Mapping[str, Any]) -> None:
        timestamp = datetime.now(timezone.utc)
        table = self._state.tables["events"]
        payload_json = _json_dumps(dict(payload))
        sql = (
            f"INSERT INTO {table} (event_type, payload, recorded_at) VALUES (%s, %s::jsonb, %s)"
        )
        self._with_retry(
            lambda conn: self._execute(conn, sql, (event_type, payload_json, timestamp))
        )

    def record_audit_log(self, record: Mapping[str, Any]) -> None:
        stored = dict(record)
        entry_id = stored.setdefault("id", str(uuid.uuid4()))
        created_at = stored.setdefault("created_at", datetime.now(timezone.utc))
        table = self._state.tables["audit_logs"]
        payload_json = _json_dumps(stored)
        sql = (
            f"INSERT INTO {table} (id, payload, recorded_at) VALUES (%s, %s::jsonb, %s)"
            " ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload,"
            " recorded_at = EXCLUDED.recorded_at"
        )
        self._with_retry(
            lambda conn: self._execute(conn, sql, (str(entry_id), payload_json, created_at))
        )

    def record_decision(self, order_id: str, payload: Mapping[str, Any]) -> None:
        table = self._state.tables["telemetry"]
        recorded_at = datetime.now(timezone.utc)
        payload_json = _json_dumps(dict(payload))
        sql = f"INSERT INTO {table} (order_id, payload, recorded_at) VALUES (%s, %s::jsonb, %s)"
        self._with_retry(
            lambda conn: self._execute(conn, sql, (order_id, payload_json, recorded_at))
        )

    def record_credential_event(
        self,
        *,
        event: str,
        event_type: str,
        secret_name: str | None,
        metadata: Mapping[str, Any],
        recorded_at: datetime,
    ) -> None:
        table = self._state.tables["credential_events"]
        metadata_json = _json_dumps(dict(metadata))
        sql = (
            f"INSERT INTO {table} (event, event_type, secret_name, metadata, recorded_at)"
            " VALUES (%s, %s, %s, %s::jsonb, %s)"
        )
        self._with_retry(
            lambda conn: self._execute(
                conn,
                sql,
                (event, event_type, secret_name, metadata_json, recorded_at),
            )
        )

    def upsert_credential_rotation(
        self,
        *,
        secret_name: str,
        created_at: datetime,
        rotated_at: datetime,
        kms_key_id: str | None,
    ) -> Dict[str, Any]:
        metadata = {
            "secret_name": secret_name,
            "created_at": created_at,
            "rotated_at": rotated_at,
        }
        if kms_key_id is not None:
            metadata["kms_key_id"] = kms_key_id
        table = self._state.tables["credential_rotations"]
        sql = (
            f"INSERT INTO {table} (secret_name, created_at, rotated_at, kms_key_id)"
            " VALUES (%s, %s, %s, %s)"
        )
        self._with_retry(
            lambda conn: self._execute(conn, sql, (secret_name, created_at, rotated_at, kms_key_id))
        )
        return metadata

    def upsert_risk_config(self, config: Mapping[str, Any]) -> None:
        table = self._state.tables["risk_configs"]
        payload = _json_dumps(dict(config))
        updated_at = datetime.now(timezone.utc)
        sql = (
            f"INSERT INTO {table} (account_id, config, updated_at) VALUES (%s, %s::jsonb, %s)"
            " ON CONFLICT(account_id) DO UPDATE SET config=EXCLUDED.config,"
            " updated_at=EXCLUDED.updated_at"
        )
        self._with_retry(
            lambda conn: self._execute(
                conn,
                sql,
                (self.account_id, payload, updated_at),
            )
        )

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def fetch_events(self) -> Dict[str, List[Dict[str, Any]]]:
        state = self._state

        def _rows(table_key: str, columns: str) -> List[Mapping[str, Any]]:
            table = state.tables[table_key]
            sql = f"SELECT {columns} FROM {table} ORDER BY id ASC"
            rows = self._with_retry(
                lambda conn: self._execute(conn, sql, fetch="all"), state=state
            )
            return list(rows or [])

        acks = [
            {
                **_json_loads(row["payload"]),
                "recorded_at": _deserialize_timestamp(row["recorded_at"]),
            }
            for row in _rows("acks", "payload, recorded_at")
        ]
        fills = [
            {
                **_json_loads(row["payload"]),
                "recorded_at": _deserialize_timestamp(row["recorded_at"]),
            }
            for row in _rows("fills", "payload, recorded_at")
        ]
        shadow_fills = [
            {
                **_json_loads(row["payload"]),
                "recorded_at": _deserialize_timestamp(row["recorded_at"]),
            }
            for row in _rows("shadow_fills", "payload, recorded_at")
        ]
        generic_events = [
            {
                "type": row["event_type"],
                "event_type": row["event_type"],
                "payload": _json_loads(row["payload"]),
                "timestamp": _deserialize_timestamp(row["recorded_at"]),
            }
            for row in _rows("events", "event_type, payload, recorded_at")
        ]
        rotation_events: List[Dict[str, Any]] = []
        for row in self._fetch_credential_events(event="rotation"):
            metadata = _json_loads(row["metadata"])
            created_at = metadata.get("created_at")
            rotated_at = metadata.get("rotated_at")
            if created_at is not None:
                metadata["created_at"] = _deserialize_timestamp(created_at)
            if rotated_at is not None:
                metadata["rotated_at"] = _deserialize_timestamp(rotated_at)
            metadata["timestamp"] = _deserialize_timestamp(row["recorded_at"])
            rotation_events.append(metadata)

        return {
            "acks": acks,
            "fills": fills,
            "shadow_fills": shadow_fills,
            "events": generic_events,
            "credential_rotations": rotation_events,
        }

    def fetch_telemetry(self) -> List[Dict[str, Any]]:
        table = self._state.tables["telemetry"]
        sql = f"SELECT order_id, payload, recorded_at FROM {table} ORDER BY id DESC LIMIT 250"
        rows = self._with_retry(lambda conn: self._execute(conn, sql, fetch="all"))
        return [
            {
                "order_id": row["order_id"],
                "payload": _json_loads(row["payload"]),
                "timestamp": _deserialize_timestamp(row["recorded_at"]),
            }
            for row in rows
        ]

    def fetch_audit_logs(self) -> List[Dict[str, Any]]:
        table = self._state.tables["audit_logs"]
        sql = f"SELECT id, payload, recorded_at FROM {table} ORDER BY recorded_at ASC"
        rows = self._with_retry(lambda conn: self._execute(conn, sql, fetch="all"))
        results: List[Dict[str, Any]] = []
        for row in rows:
            payload = _json_loads(row["payload"])
            payload.setdefault("id", row["id"])
            payload.setdefault("created_at", _deserialize_timestamp(row["recorded_at"]))
            results.append(payload)
        return results

    def _fetch_credential_events(self, event: str | None = None) -> List[Mapping[str, Any]]:
        table = self._state.tables["credential_events"]
        if event is None:
            sql = f"SELECT event, event_type, secret_name, metadata, recorded_at FROM {table} ORDER BY id ASC"
            rows = self._with_retry(lambda conn: self._execute(conn, sql, fetch="all"))
            return list(rows or [])
        sql = (
            f"SELECT event, event_type, secret_name, metadata, recorded_at FROM {table}"
            " WHERE event = %s ORDER BY id ASC"
        )
        rows = self._with_retry(
            lambda conn: self._execute(conn, sql, (event,), fetch="all")
        )
        return list(rows or [])

    def fetch_credential_events(self) -> List[Dict[str, Any]]:
        rows = self._fetch_credential_events(None)
        results: List[Dict[str, Any]] = []
        for row in rows:
            metadata = _json_loads(row["metadata"])
            created_at = metadata.get("created_at")
            rotated_at = metadata.get("rotated_at")
            if created_at is not None:
                metadata["created_at"] = _deserialize_timestamp(created_at)
            if rotated_at is not None:
                metadata["rotated_at"] = _deserialize_timestamp(rotated_at)
            results.append(
                {
                    "event": row["event"],
                    "event_type": row["event_type"],
                    "secret_name": row["secret_name"],
                    "metadata": metadata,
                    "timestamp": _deserialize_timestamp(row["recorded_at"]),
                }
            )
        return results

    def fetch_rotation_status(self) -> Dict[str, Any] | None:
        table = self._state.tables["credential_rotations"]
        sql = (
            f"SELECT secret_name, created_at, rotated_at, kms_key_id"
            f" FROM {table} ORDER BY rotated_at DESC LIMIT 1"
        )
        row = self._with_retry(lambda conn: self._execute(conn, sql, fetch="one"))
        if row is None:
            return None
        payload: Dict[str, Any] = {
            "secret_name": row["secret_name"],
            "created_at": _deserialize_timestamp(row["created_at"]),
            "rotated_at": _deserialize_timestamp(row["rotated_at"]),
        }
        if row["kms_key_id"] is not None:
            payload["kms_key_id"] = row["kms_key_id"]
        return payload

    def fetch_risk_config(self, default: Mapping[str, Any]) -> Dict[str, Any]:
        table = self._state.tables["risk_configs"]
        sql = f"SELECT config FROM {table} WHERE account_id = %s"
        row = self._with_retry(
            lambda conn: self._execute(conn, sql, (self.account_id,), fetch="one")
        )
        if row is None:
            self.upsert_risk_config(default)
            return dict(default)
        stored = _json_loads(row["config"])
        merged = dict(default)
        merged.update(stored)
        return merged

    # ------------------------------------------------------------------
    # Maintenance helpers
    # ------------------------------------------------------------------
    def clear_rotation_state(self) -> None:
        rotation_table = self._state.tables["credential_rotations"]
        events_table = self._state.tables["credential_events"]
        self._with_retry(lambda conn: self._execute(conn, f"DELETE FROM {rotation_table}"))
        self._with_retry(
            lambda conn: self._execute(
                conn,
                f"DELETE FROM {events_table} WHERE event = %s",
                ("rotation",),
            )
        )

    @classmethod
    async def flush_all(cls) -> Dict[str, Dict[str, int]]:
        await asyncio.sleep(0)
        return {}

    @classmethod
    def clear_all_rotation_state(cls, account_id: str | None = None) -> None:
        if account_id is not None:
            with cls._lock:
                state = cls._connections.get(account_id)
            if state is None:
                return
            with state.lock:
                rotation_table = state.tables["credential_rotations"]
                events_table = state.tables["credential_events"]
                cls._execute_static(state, f"DELETE FROM {rotation_table}")
                cls._execute_static(
                    state,
                    f"DELETE FROM {events_table} WHERE event = %s",
                    ("rotation",),
                )
            return

        with cls._lock:
            states = list(cls._connections.items())
        for _, state in states:
            with state.lock:
                rotation_table = state.tables["credential_rotations"]
                events_table = state.tables["credential_events"]
                cls._execute_static(state, f"DELETE FROM {rotation_table}")
                cls._execute_static(
                    state,
                    f"DELETE FROM {events_table} WHERE event = %s",
                    ("rotation",),
                )

    @classmethod
    def reset_account(cls, account_id: str) -> None:
        with cls._lock:
            state = cls._connections.pop(account_id, None)
        if state is None:
            return
        with state.lock:
            for table in state.tables.values():
                cls._execute_static(state, f"DELETE FROM {table}")
        try:
            state.connection.close()
        except Exception:  # pragma: no cover - ensure cleanup continues
            logger.debug("Error while closing Timescale connection", exc_info=True)

    @classmethod
    def reset_all(cls) -> None:
        with cls._lock:
            states = list(cls._connections.items())
            cls._connections.clear()
        for account_id, state in states:
            with state.lock:
                for table in state.tables.values():
                    cls._execute_static(state, f"DELETE FROM {table}")
            try:
                state.connection.close()
            except Exception:  # pragma: no cover - connection might already be closed
                logger.debug("Error while closing Timescale connection", exc_info=True)


    @classmethod
    def _execute_static(
        cls, state: _TimescaleConnectionState, sql: str, params: Tuple[Any, ...] | None = None
    ) -> None:
        connection = state.connection
        if getattr(connection, "closed", False):
            connection = cls._open_connection(state.dsn, state.schema)
            state.connection = connection
        with connection.cursor() as cursor:
            cursor.execute(sql, params)


class _InMemoryTimescaleStore:
    """Fallback Timescale store used when psycopg is unavailable."""

    _lock: ClassVar[threading.RLock] = threading.RLock()
    _connections: ClassVar[Dict[str, Dict[str, Any]]] = {}

    def __init__(
        self,
        account_id: str,
        *,
        session_factory: Callable[[str], "TimescaleSession"],
        max_retries: int,
        backoff_seconds: float,
    ) -> None:
        self.account_id = account_id
        with self._lock:
            state = self._connections.get(account_id)
            if state is None:
                state = {
                    "acks": [],
                    "fills": [],
                    "shadow_fills": [],
                    "events": [],
                    "telemetry": [],
                    "audit_logs": [],
                    "credential_events": [],
                    "credential_rotations": [],
                    "risk_config": None,
                }
                self._connections[account_id] = state
            self._state = state

    # ------------------------------------------------------------------
    # Insert helpers
    # ------------------------------------------------------------------
    def record_ack(self, payload: Mapping[str, Any]) -> None:
        entry = {**dict(payload), "recorded_at": datetime.now(timezone.utc)}
        with self._lock:
            self._state["acks"].append(entry)

    def record_fill(self, payload: Mapping[str, Any], *, shadow: bool = False) -> None:
        entry = {**dict(payload), "recorded_at": datetime.now(timezone.utc)}
        bucket = "shadow_fills" if shadow else "fills"
        with self._lock:
            self._state[bucket].append(entry)

    def record_event(self, event_type: str, payload: Mapping[str, Any]) -> None:
        entry = {
            "type": event_type,
            "event_type": event_type,
            "payload": dict(payload),
            "timestamp": datetime.now(timezone.utc),
        }
        with self._lock:
            self._state["events"].append(entry)

    def record_audit_log(self, record: Mapping[str, Any]) -> None:
        payload = dict(record)
        entry_id = payload.setdefault("id", str(uuid.uuid4()))
        payload.setdefault("created_at", datetime.now(timezone.utc))
        with self._lock:
            logs: List[Dict[str, Any]] = self._state["audit_logs"]
            for index, existing in enumerate(logs):
                if existing.get("id") == entry_id:
                    logs[index] = deepcopy(payload)
                    break
            else:
                logs.append(deepcopy(payload))

    def record_decision(self, order_id: str, payload: Mapping[str, Any]) -> None:
        entry = {
            "order_id": order_id,
            "payload": dict(payload),
            "timestamp": datetime.now(timezone.utc),
        }
        with self._lock:
            self._state["telemetry"].append(entry)
            if len(self._state["telemetry"]) > 500:
                self._state["telemetry"] = self._state["telemetry"][-500:]

    def record_credential_event(
        self,
        *,
        event: str,
        event_type: str,
        secret_name: str | None,
        metadata: Mapping[str, Any],
        recorded_at: datetime,
    ) -> None:
        entry = {
            "event": event,
            "event_type": event_type,
            "secret_name": secret_name,
            "metadata": dict(metadata),
            "recorded_at": recorded_at,
        }
        with self._lock:
            self._state["credential_events"].append(entry)

    def upsert_credential_rotation(
        self,
        *,
        secret_name: str,
        created_at: datetime,
        rotated_at: datetime,
        kms_key_id: str | None,
    ) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {
            "secret_name": secret_name,
            "created_at": created_at,
            "rotated_at": rotated_at,
        }
        if kms_key_id is not None:
            metadata["kms_key_id"] = kms_key_id
        with self._lock:
            self._state["credential_rotations"].append(deepcopy(metadata))
        return deepcopy(metadata)

    def upsert_risk_config(self, config: Mapping[str, Any]) -> None:
        with self._lock:
            stored = self._state.get("risk_config")
            if stored is None:
                self._state["risk_config"] = dict(config)
            else:
                stored.update(dict(config))

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def fetch_events(self) -> Dict[str, List[Dict[str, Any]]]:
        with self._lock:
            acks = [deepcopy(entry) for entry in self._state["acks"]]
            fills = [deepcopy(entry) for entry in self._state["fills"]]
            shadow_fills = [deepcopy(entry) for entry in self._state["shadow_fills"]]
            events = [deepcopy(entry) for entry in self._state["events"]]
            rotation_events: List[Dict[str, Any]] = []
            for entry in self._state["credential_events"]:
                if entry.get("event") != "rotation":
                    continue
                metadata = deepcopy(entry.get("metadata", {}))
                metadata["timestamp"] = entry.get("recorded_at")
                rotation_events.append(metadata)
        return {
            "acks": acks,
            "fills": fills,
            "shadow_fills": shadow_fills,
            "events": events,
            "credential_rotations": rotation_events,
        }

    def fetch_telemetry(self) -> List[Dict[str, Any]]:
        with self._lock:
            telemetry = sorted(
                (deepcopy(entry) for entry in self._state["telemetry"]),
                key=lambda item: item["timestamp"],
                reverse=True,
            )
        return telemetry[:250]

    def fetch_audit_logs(self) -> List[Dict[str, Any]]:
        with self._lock:
            logs = [deepcopy(entry) for entry in self._state["audit_logs"]]
        logs.sort(key=lambda item: item.get("created_at", datetime.now(timezone.utc)))
        return logs

    def fetch_credential_events(self) -> List[Dict[str, Any]]:
        with self._lock:
            events = [deepcopy(entry) for entry in self._state["credential_events"]]
        results: List[Dict[str, Any]] = []
        for entry in events:
            results.append(
                {
                    "event": entry.get("event"),
                    "event_type": entry.get("event_type"),
                    "secret_name": entry.get("secret_name"),
                    "metadata": entry.get("metadata"),
                    "timestamp": entry.get("recorded_at"),
                }
            )
        return results

    def fetch_rotation_status(self) -> Dict[str, Any] | None:
        with self._lock:
            rotations = [deepcopy(entry) for entry in self._state["credential_rotations"]]
        if not rotations:
            return None
        rotations.sort(key=lambda item: item.get("rotated_at", datetime.min.replace(tzinfo=timezone.utc)))
        return rotations[-1]

    def fetch_risk_config(self, default: Mapping[str, Any]) -> Dict[str, Any]:
        with self._lock:
            stored = self._state.get("risk_config")
            if stored is None:
                snapshot = dict(default)
                self._state["risk_config"] = dict(default)
            else:
                snapshot = dict(default | stored)
        return snapshot

    # ------------------------------------------------------------------
    # Maintenance helpers
    # ------------------------------------------------------------------
    def clear_rotation_state(self) -> None:
        with self._lock:
            self._state["credential_rotations"].clear()
            self._state["credential_events"] = [
                entry
                for entry in self._state["credential_events"]
                if entry.get("event") != "rotation"
            ]

    @classmethod
    async def flush_all(cls) -> Dict[str, Dict[str, int]]:
        await asyncio.sleep(0)
        return {}

    @classmethod
    def clear_all_rotation_state(cls, account_id: str | None = None) -> None:
        with cls._lock:
            if account_id is not None:
                state = cls._connections.get(account_id)
                if state is None:
                    return
                state["credential_rotations"].clear()
                state["credential_events"] = [
                    entry for entry in state["credential_events"] if entry.get("event") != "rotation"
                ]
                return
            for state in cls._connections.values():
                state["credential_rotations"].clear()
                state["credential_events"] = [
                    entry for entry in state["credential_events"] if entry.get("event") != "rotation"
                ]

    @classmethod
    def reset_account(cls, account_id: str) -> None:
        with cls._lock:
            cls._connections.pop(account_id, None)

    @classmethod
    def reset_all(cls) -> None:
        with cls._lock:
            cls._connections.clear()


_ORIGINAL_TIMESCALE_STORE = _TimescaleStore
_STORE_SWITCH_LOCK = threading.Lock()

@dataclass(eq=False)
class KafkaNATSAdapter:
    account_id: str
    kafka_config_factory: Callable[[str], KafkaProducer] = field(
        default=get_kafka_producer, repr=False
    )
    nats_config_factory: Callable[[str], NATSProducer] = field(
        default=get_nats_producer, repr=False
    )
    max_retries: int = 3
    backoff_seconds: float = 0.25
    request_timeout: float = 5.0

    _fallback_buffer: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _published_events: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _instances: ClassVar["WeakSet[KafkaNATSAdapter]"] = WeakSet()

    def __post_init__(self) -> None:
        normalized = _normalize_account_id(self.account_id)
        object.__setattr__(self, "account_id", normalized)

        self._fallback_buffer.setdefault(self.account_id, [])
        self._published_events.setdefault(self.account_id, [])
        self._instances.add(self)

        self._lock = threading.Lock()
        self._kafka_client: httpx.AsyncClient | None = None
        self._nats_client: httpx.AsyncClient | None = None
        self._kafka_client_task: asyncio.Task[httpx.AsyncClient | None] | None = None
        self._nats_client_task: asyncio.Task[httpx.AsyncClient | None] | None = None
        self._kafka_prefix = ""
        self._nats_prefix = ""

        self._bootstrap_clients()

    async def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        enriched = attach_correlation(payload)
        record = {
            "topic": topic,
            "payload": enriched,
            "timestamp": datetime.now(timezone.utc),
            "correlation_id": enriched.get("correlation_id") or current_correlation_id(),
            "delivered": False,
            "partial_delivery": False,
        }

        try:
            status = await self._attempt_publish(record)
        except PublishError:
            self._buffer_event(record)
            self._record_event(record)
            raise

        if status == "all":
            record["delivered"] = True
            self._record_event(record)
        elif status == "partial":
            record["partial_delivery"] = True
            self._record_event(record)
        else:
            # Broker unavailable, retain the record for a later flush. This path
            # should be unreachable because `_attempt_publish` raises when no
            # transports are available, but we keep it to remain compatible with
            # subclasses overriding the publish behaviour.
            self._buffer_event(record)
            self._record_event(record)

    def history(self, correlation_id: str | None = None) -> List[Dict[str, Any]]:
        records = list(self._published_events.get(self.account_id, []))
        if correlation_id:
            return [record for record in records if record.get("correlation_id") == correlation_id]
        return records

    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        cache_clear = getattr(get_kafka_producer, "cache_clear", None)
        if callable(cache_clear):  # pragma: no cover - depends on lru_cache availability
            cache_clear()
        cache_clear = getattr(get_nats_producer, "cache_clear", None)
        if callable(cache_clear):  # pragma: no cover - depends on lru_cache availability
            cache_clear()

        if account_id is None:
            cls.shutdown()
            cls._fallback_buffer.clear()
            cls._published_events.clear()
            return

        normalized = _normalize_account_id(account_id)
        cls._fallback_buffer.pop(normalized, None)
        cls._published_events.pop(normalized, None)
        for instance in list(cls._instances):
            if instance.account_id == normalized:
                instance._shutdown()

    @classmethod
    async def flush_events(cls) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for instance in list(cls._instances):
            drained = await instance._drain_buffer()
            if drained:
                counts[instance.account_id] = drained
        return counts

    @classmethod
    def shutdown(cls) -> None:
        for instance in list(cls._instances):
            instance._shutdown()

    def _bootstrap_clients(self) -> None:
        try:
            kafka_config = self.kafka_config_factory(self.account_id)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Unable to load Kafka configuration", exc_info=exc)
            kafka_config = None

        if kafka_config:
            self._kafka_prefix = kafka_config.topic_prefix.strip()
            endpoint = _first_endpoint(kafka_config.bootstrap_servers)
            kafka_client = self._build_client(endpoint)
            if isinstance(kafka_client, asyncio.Task):
                self._kafka_client_task = kafka_client
            else:
                self._kafka_client = kafka_client

        try:
            nats_config = self.nats_config_factory(self.account_id)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Unable to load NATS configuration", exc_info=exc)
            nats_config = None

        if nats_config and nats_config.servers:
            self._nats_prefix = nats_config.subject_prefix.strip()
            endpoint = _first_endpoint(nats_config.servers)
            nats_client = self._build_client(endpoint)
            if isinstance(nats_client, asyncio.Task):
                self._nats_client_task = nats_client
            else:
                self._nats_client = nats_client

    def _build_client(
        self, endpoint: str
    ) -> httpx.AsyncClient | asyncio.Task[httpx.AsyncClient | None] | None:
        async def _initialise() -> httpx.AsyncClient | None:
            try:
                base_url = _as_base_url(endpoint)
                client = httpx.AsyncClient(base_url=base_url, timeout=self.request_timeout)
            except Exception:
                logger.exception(
                    "Failed to initialise broker client", extra={"endpoint": endpoint}
                )
                return None

            attempt = 0
            while True:
                attempt += 1
                try:
                    response = await client.get("/health")
                    response.raise_for_status()
                    if response.status_code >= 500:
                        raise PublishError(
                            f"Broker health check failed ({response.status_code})"
                        )
                    return client
                except httpx.HTTPStatusError as exc:
                    await client.aclose()
                    raise PublishError("Broker health check failed") from exc
                except httpx.RequestError:
                    if attempt >= self.max_retries:
                        await client.aclose()
                        return None
                    await asyncio.sleep(self.backoff_seconds * (2 ** (attempt - 1)))

        async def _wrapper() -> httpx.AsyncClient | None:
            try:
                return await _initialise()
            except PublishError:
                raise
            except Exception:
                logger.exception(
                    "Failed to initialise broker client", extra={"endpoint": endpoint}
                )
                return None

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            return loop.create_task(_wrapper())

        try:
            return asyncio.run(_wrapper())
        except PublishError:
            return None

    async def _ensure_client(
        self,
        client_attr: str,
        task_attr: str,
        transport: str,
    ) -> httpx.AsyncClient | None:
        client = getattr(self, client_attr)
        if client is not None:
            return client

        task: asyncio.Task[httpx.AsyncClient | None] | None = getattr(self, task_attr)
        if task is None:
            return None

        try:
            client = await task
        except PublishError:
            client = None
        except Exception:
            logger.exception(
                "Broker bootstrap task failed", extra={"transport": transport}
            )
            client = None

        setattr(self, task_attr, None)
        setattr(self, client_attr, client)
        return client

    async def _resolve_transports(self, topic: str) -> List[tuple[httpx.AsyncClient, str]]:
        transports: List[tuple[httpx.AsyncClient, str]] = []

        kafka_client = await self._ensure_client("_kafka_client", "_kafka_client_task", "kafka")
        if kafka_client is not None:
            transports.append((kafka_client, self._topic_path(topic)))

        nats_client = await self._ensure_client("_nats_client", "_nats_client_task", "nats")
        if nats_client is not None:
            transports.append((nats_client, self._subject_path(topic)))

        return transports

    async def _attempt_publish(self, record: Dict[str, Any]) -> str:
        transports = await self._resolve_transports(record["topic"])

        if not transports:
            # Attempt a fresh bootstrap in case configuration or broker readiness
            # changed since this adapter instance was constructed.
            self._bootstrap_clients()
            transports = await self._resolve_transports(record["topic"])

        if not transports:
            raise PublishError("No broker transports available for publish")

        errors: List[BaseException] = []
        successes = 0
        for client, path in transports:
            try:
                await self._send_with_retry(client, path, record["payload"])
            except BaseException as exc:
                errors.append(exc)
                logger.warning(
                    "Publish transport failed", extra={"topic": record["topic"], "error": str(exc)}
                )
            else:
                successes += 1

        if successes == 0:
            reason = errors[-1] if errors else RuntimeError("publish failed")
            raise PublishError("All publish transports failed") from reason

        if errors:
            logger.warning(
                "Partial publish delivery", extra={"topic": record["topic"], "failed": len(errors)}
            )

        if successes == len(transports):
            return "all"
        return "partial"

    async def _send_with_retry(
        self, client: httpx.AsyncClient, path: str, payload: Dict[str, Any]
    ) -> None:
        attempt = 0
        while True:
            attempt += 1
            try:
                response = await client.post(path, json=payload)
                response.raise_for_status()
                return
            except httpx.HTTPStatusError as exc:
                if attempt >= self.max_retries:
                    raise PublishError(f"Broker responded with status {exc.response.status_code}") from exc
            except httpx.RequestError as exc:
                if attempt >= self.max_retries:
                    raise PublishError("Transport error during publish") from exc
            await asyncio.sleep(self.backoff_seconds * (2 ** (attempt - 1)))

    def _topic_path(self, topic: str) -> str:
        full = topic
        if self._kafka_prefix and not topic.startswith(self._kafka_prefix):
            full = f"{self._kafka_prefix}.{topic}" if topic else self._kafka_prefix
        encoded = full.replace("/", ".")
        return f"/topics/{encoded}"

    def _subject_path(self, topic: str) -> str:
        subject = topic
        if self._nats_prefix and not topic.startswith(self._nats_prefix):
            subject = f"{self._nats_prefix}.{topic}" if topic else self._nats_prefix
        encoded = subject.replace("/", ".")
        return f"/subjects/{encoded}"

    def _buffer_event(self, record: Dict[str, Any]) -> None:
        buffered = dict(record)
        buffered["delivered"] = False
        self._fallback_buffer[self.account_id].append(buffered)

    def _record_event(self, record: Dict[str, Any]) -> None:
        events = self._published_events.setdefault(self.account_id, [])
        correlation_id = record.get("correlation_id")
        for index in range(len(events) - 1, -1, -1):
            existing = events[index]
            if (
                existing.get("topic") == record.get("topic")
                and existing.get("correlation_id") == correlation_id
            ):
                events[index] = dict(record)
                return
        events.append(dict(record))

    async def _drain_buffer(self) -> int:
        drained = 0
        buffer = self._fallback_buffer.get(self.account_id, [])
        while buffer:
            record = buffer[0]
            try:
                status = await self._attempt_publish(record)
            except PublishError:
                break
            if status == "all":
                record["delivered"] = True
                record["partial_delivery"] = False
                self._record_event(record)
                buffer.pop(0)
                drained += 1
            elif status == "partial":
                record["delivered"] = False
                record["partial_delivery"] = True
                self._record_event(record)
                buffer.pop(0)
                drained += 1
            else:
                break
        return drained

    def _shutdown(self) -> None:
        clients: List[httpx.AsyncClient] = []
        with self._lock:
            if self._kafka_client_task is not None:
                self._kafka_client_task.cancel()
                self._kafka_client_task = None
            if self._nats_client_task is not None:
                self._nats_client_task.cancel()
                self._nats_client_task = None

            if self._kafka_client is not None:
                clients.append(self._kafka_client)
                self._kafka_client = None
            if self._nats_client is not None:
                clients.append(self._nats_client)
                self._nats_client = None

        if not clients:
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        for client in clients:
            if loop and loop.is_running():
                loop.create_task(client.aclose())
            else:
                try:
                    asyncio.run(client.aclose())
                except RuntimeError:
                    logger.exception("Failed to close broker client", exc_info=True)


class TimescaleAdapter:
    _metrics: ClassVar[Dict[str, Dict[str, float]]] = {}
    _kill_events: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _daily_usage: ClassVar[Dict[str, Dict[str, Dict[str, float]]]] = {}
    _instrument_exposures: ClassVar[Dict[str, Dict[str, float]]] = {}
    _rolling_volume: ClassVar[Dict[str, Dict[str, Dict[str, Any]]]] = {}
    _cvar_results: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _nav_forecasts: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}

    _default_risk_config: ClassVar[Dict[str, Any]] = {
        "kill_switch": False,
        "safe_mode": False,
        "loss_cap": 50_000.0,
        "fee_cap": 5_000.0,
        "nav": 1_000_000.0,
        "max_nav_percent": 0.20,
        "var_limit": 250_000.0,
        "spread_limit_bps": 25.0,
        "latency_limit_ms": 250.0,
        "diversification_rules": {"max_single_instrument_percent": 0.35},
        "volatility_overrides": {},
        "correlation_matrix": {},
        "circuit_breakers": {},
        "position_sizer": {
            "max_trade_risk_pct_nav": 0.1,
            "max_trade_risk_pct_cash": 0.5,
            "volatility_floor": 0.05,
            "slippage_bps": 2.0,
            "safety_margin_bps": 5.0,
            "min_trade_notional": 10.0,
        },
    }

    def __init__(
        self,
        account_id: str,
        *,
        session_factory: Callable[[str], "TimescaleSession"] = get_timescale_session,
        max_retries: int = 5,
        backoff_seconds: float = 0.25,
    ) -> None:
        normalized = _normalize_account_id(account_id)
        self.account_id = normalized
        store_cls = _TimescaleStore
        try:
            self._store = store_cls(
                normalized,
                session_factory=session_factory,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
        except (RuntimeError, PsycopgError, OSError) as exc:
            if store_cls is _InMemoryTimescaleStore:
                raise
            logger.warning(
                "Falling back to in-memory Timescale store for %s: %s",
                normalized,
                exc,
            )
            with _STORE_SWITCH_LOCK:
                current_cls = _TimescaleStore
                if (
                    current_cls is store_cls
                    and store_cls is _ORIGINAL_TIMESCALE_STORE
                ):
                    globals()["_TimescaleStore"] = _InMemoryTimescaleStore
                store_cls = _TimescaleStore
            self._store = store_cls(
                normalized,
                session_factory=session_factory,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )

        self._metrics.setdefault(self.account_id, {"limit": 1_000_000.0, "usage": 0.0})
        self._daily_usage.setdefault(self.account_id, {})
        self._instrument_exposures.setdefault(self.account_id, {})
        self._kill_events.setdefault(self.account_id, [])
        self._rolling_volume.setdefault(self.account_id, {})
        self._cvar_results.setdefault(self.account_id, [])
        self._nav_forecasts.setdefault(self.account_id, [])

    # ------------------------------------------------------------------
    # OMS-inspired metrics
    # ------------------------------------------------------------------
    def record_usage(self, notional: float) -> None:
        self._metrics[self.account_id]["usage"] += float(notional)

    def check_limits(self, notional: float) -> bool:
        projected = self._metrics[self.account_id]["usage"] + float(notional)
        return projected <= self._metrics[self.account_id]["limit"]

    def record_ack(self, payload: Dict[str, Any]) -> None:
        self._store.record_ack(payload)

    def record_fill(self, payload: Dict[str, Any]) -> None:
        self._store.record_fill(payload)

    def record_shadow_fill(self, payload: Dict[str, Any]) -> None:
        self._store.record_fill(payload, shadow=True)

    def record_audit_log(self, record: Mapping[str, Any]) -> None:
        self._store.record_audit_log(record)

    def audit_logs(self) -> List[Dict[str, Any]]:
        return [deepcopy(entry) for entry in self._store.fetch_audit_logs()]

    def events(self) -> Dict[str, List[Dict[str, Any]]]:
        fetched = self._store.fetch_events()
        return {
            bucket: [deepcopy(entry) for entry in entries]
            for bucket, entries in fetched.items()
        }

    # ------------------------------------------------------------------
    # Event persistence helpers
    # ------------------------------------------------------------------
    def record_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        self._store.record_event(event_type, payload)

    @classmethod
    async def flush_event_buffers(cls) -> Dict[str, Dict[str, int]]:
        """Flush pending Timescale writes for every account.

        This ensures any buffered inserts are durably committed before
        subsequent assertions or shutdown operations.
        """
        summary = await _TimescaleStore.flush_all()
        if summary:
            for account, buckets in summary.items():
                logger.info(
                    "Flushing Timescale adapter buffers",
                    extra={"account_id": account, "buckets": buckets},
                )
        return summary

    # ------------------------------------------------------------------
    # Risk configuration helpers
    # ------------------------------------------------------------------
    def load_risk_config(self) -> Dict[str, Any]:
        config = self._store.fetch_risk_config(self._default_risk_config)
        return deepcopy(config)

    def _persist_risk_config(self, config: Mapping[str, Any]) -> None:
        self._store.upsert_risk_config(config)

    def save_risk_config(self, config: Mapping[str, Any]) -> None:
        self._persist_risk_config(config)

    def set_kill_switch(
        self,
        *,
        engaged: bool,
        reason: str | None = None,
        actor: str | None = None,
    ) -> None:
        config = self._store.fetch_risk_config(self._default_risk_config)
        config["kill_switch"] = bool(engaged)
        self._persist_risk_config(config)

        event_payload: Dict[str, Any] = {"state": "engaged" if engaged else "released"}
        if reason:
            event_payload["reason"] = reason
        if actor:
            event_payload["actor"] = actor

        event_type = "kill_switch_engaged" if engaged else "kill_switch_released"
        self.record_event(event_type, event_payload)

    def set_safe_mode(
        self,
        *,
        engaged: bool,
        reason: str | None = None,
        actor: str | None = None,
    ) -> None:
        config = self._store.fetch_risk_config(self._default_risk_config)
        config["safe_mode"] = bool(engaged)
        self._persist_risk_config(config)

        event_payload: Dict[str, Any] = {"state": "engaged" if engaged else "released"}
        if reason:
            event_payload["reason"] = reason
        if actor:
            event_payload["actor"] = actor

        event_type = "safe_mode_engaged" if engaged else "safe_mode_released"
        self.record_event(event_type, event_payload)

    # ------------------------------------------------------------------
    # Usage tracking helpers
    # ------------------------------------------------------------------
    def get_daily_usage(self) -> Dict[str, float]:
        date_key = datetime.now(timezone.utc).date().isoformat()
        usage = self._daily_usage.setdefault(self.account_id, {})
        day_usage = usage.setdefault(date_key, {"loss": 0.0, "fee": 0.0})
        return deepcopy(day_usage)

    def record_daily_usage(self, loss: float, fee: float) -> None:
        date_key = datetime.now(timezone.utc).date().isoformat()
        usage = self._daily_usage.setdefault(self.account_id, {})
        day_usage = usage.setdefault(date_key, {"loss": 0.0, "fee": 0.0})
        day_usage["loss"] += float(loss)
        day_usage["fee"] += float(fee)

    def record_instrument_exposure(self, instrument: str, notional: float) -> None:
        exposures = self._instrument_exposures.setdefault(self.account_id, {})
        exposures[instrument] = exposures.get(instrument, 0.0) + float(notional)

    def instrument_exposure(self, instrument: str) -> float:
        exposures = self._instrument_exposures.get(self.account_id, {})
        return float(exposures.get(instrument, 0.0))

    def open_positions(self) -> Dict[str, float]:
        exposures = self._instrument_exposures.get(self.account_id, {})
        return {symbol: float(notional) for symbol, notional in exposures.items()}

    # ------------------------------------------------------------------
    # Telemetry helpers
    # ------------------------------------------------------------------
    def record_decision(self, order_id: str, payload: Dict[str, Any]) -> None:
        self._store.record_decision(order_id, payload)

    def telemetry(self) -> List[Dict[str, Any]]:
        return [deepcopy(entry) for entry in self._store.fetch_telemetry()]

    # ------------------------------------------------------------------
    # Credential lifecycle helpers
    # ------------------------------------------------------------------
    def record_credential_rotation(
        self,
        *,
        secret_name: str,
        rotated_at: datetime,
        kms_key_id: str | None = None,
    ) -> Dict[str, Any]:
        status = self.credential_rotation_status()
        created_at_value = status.get("created_at") if status else None
        if isinstance(created_at_value, datetime):
            created_at = created_at_value
        elif isinstance(created_at_value, str):
            created_at = _deserialize_timestamp(created_at_value)
        else:
            created_at = rotated_at
        metadata = self._store.upsert_credential_rotation(
            secret_name=secret_name,
            created_at=created_at,
            rotated_at=rotated_at,
            kms_key_id=kms_key_id,
        )
        self._store.record_credential_event(
            event="rotation",
            event_type="kraken.credentials.rotation",
            secret_name=secret_name,
            metadata=metadata,
            recorded_at=rotated_at,
        )
        return deepcopy(metadata)

    def credential_rotation_status(self) -> Optional[Dict[str, Any]]:
        status = self._store.fetch_rotation_status()
        if status is None:
            return None
        return deepcopy(status)

    def record_credential_access(self, *, secret_name: str, metadata: Dict[str, Any]) -> None:
        sanitized = deepcopy(metadata)
        for key in ("api_key", "api_secret"):
            if key in sanitized and sanitized[key]:
                sanitized[key] = "***"
        if "material_present" not in sanitized:
            sanitized["material_present"] = bool(
                sanitized.get("api_key") and sanitized.get("api_secret")
            )
        recorded_at = datetime.now(timezone.utc)
        self._store.record_credential_event(
            event="access",
            event_type="kraken.credentials.access",
            secret_name=secret_name,
            metadata=sanitized,
            recorded_at=recorded_at,
        )

    def credential_events(self) -> List[Dict[str, Any]]:
        return [deepcopy(event) for event in self._store.fetch_credential_events()]

    # ------------------------------------------------------------------
    # Kill event helpers
    # ------------------------------------------------------------------
    def record_kill_event(
        self,
        *,
        reason_code: str,
        triggered_at: datetime,
        channels_sent: Iterable[str],
    ) -> Dict[str, Any]:
        payload = {
            "account_id": self.account_id,
            "reason": reason_code,
            "ts": triggered_at,
            "channels_sent": list(channels_sent),
        }
        events = self._kill_events.setdefault(self.account_id, [])
        events.append(deepcopy(payload))
        return deepcopy(payload)

    def kill_events(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        entries = list(self._kill_events.get(self.account_id, []))
        entries.sort(key=lambda entry: entry["ts"], reverse=True)
        if limit is not None:
            entries = entries[: int(limit)]
        return [deepcopy(entry) for entry in entries]

    @classmethod
    def all_kill_events(
        cls, *, account_id: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        if account_id is not None:
            entries = list(cls._kill_events.get(account_id, []))
        else:
            entries = [
                entry for events in cls._kill_events.values() for entry in events
            ]
        entries.sort(key=lambda entry: entry["ts"], reverse=True)
        if limit is not None:
            entries = entries[: int(limit)]
        return [deepcopy(entry) for entry in entries]

    # ------------------------------------------------------------------
    # CVaR/Nav helpers (in-memory)
    # ------------------------------------------------------------------
    def record_cvar_result(
        self,
        *,
        horizon: str,
        var95: float,
        cvar95: float,
        prob_cap_hit: float,
        timestamp: datetime | None = None,
    ) -> Dict[str, Any]:
        ts = timestamp or datetime.now(timezone.utc)
        record = {
            "account_id": self.account_id,
            "horizon": horizon,
            "var95": float(var95),
            "cvar95": float(cvar95),
            "prob_cap_hit": float(prob_cap_hit),
            "ts": ts,
        }
        history = self._cvar_results.setdefault(self.account_id, [])
        history.append(record)
        return dict(record)

    def cvar_results(self) -> List[Dict[str, Any]]:
        records = self._cvar_results.get(self.account_id, [])
        return [dict(entry) for entry in records]

    def record_nav_forecast(
        self,
        *,
        horizon: str,
        metrics: Mapping[str, float],
        timestamp: datetime | None = None,
    ) -> Dict[str, Any]:
        ts = timestamp or datetime.now(timezone.utc)
        normalized_metrics = {key: float(value) for key, value in metrics.items()}
        record = {
            "account_id": self.account_id,
            "horizon": horizon,
            "metrics_json": normalized_metrics,
            "ts": ts,
        }
        history = self._nav_forecasts.setdefault(self.account_id, [])
        history.append(record)
        return dict(record)

    def nav_forecasts(self) -> List[Dict[str, Any]]:
        records = self._nav_forecasts.get(self.account_id, [])
        return [dict(entry) for entry in records]

    # ------------------------------------------------------------------
    # Rolling volume helpers
    # ------------------------------------------------------------------
    def rolling_volume(self, pair: str) -> Dict[str, Any]:
        account_payload = self._rolling_volume.setdefault(self.account_id, {})
        volume_entry = account_payload.get(pair)
        if volume_entry is None:
            volume_entry = {
                "notional": 0.0,
                "basis_ts": datetime.now(timezone.utc),
            }
            account_payload[pair] = deepcopy(volume_entry)
        return {
            "notional": float(volume_entry.get("notional", 0.0)),
            "basis_ts": volume_entry.get("basis_ts", datetime.now(timezone.utc)),
        }

    @classmethod
    def seed_rolling_volume(
        cls, payload: Mapping[str, Mapping[str, Dict[str, Any]]]
    ) -> None:
        cls._rolling_volume = {
            account: {
                pair: {
                    "notional": float(entry.get("notional", 0.0)),
                    "basis_ts": entry.get("basis_ts", datetime.now(timezone.utc)),
                }
                for pair, entry in pairs.items()
            }
            for account, pairs in payload.items()
        }

    # ------------------------------------------------------------------
    # Test helpers
    # ------------------------------------------------------------------
    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        caches = (
            cls._metrics,
            cls._daily_usage,
            cls._instrument_exposures,
            cls._kill_events,
            cls._rolling_volume,
            cls._cvar_results,
            cls._nav_forecasts,
        )
        if account_id is None:
            for cache in caches:
                cache.clear()
            _TimescaleStore.reset_all()
            return

        normalized = _normalize_account_id(account_id)
        for cache in caches:
            cache.pop(normalized, None)
        store_account_id = cls._resolve_store_account_id(account_id)
        _TimescaleStore.reset_account(store_account_id)

    @classmethod
    def reset_rotation_state(cls, account_id: str | None = None) -> None:
        if account_id is None:
            _TimescaleStore.clear_all_rotation_state()
            return

        store_account_id = cls._resolve_store_account_id(account_id)
        _TimescaleStore.clear_all_rotation_state(store_account_id)

    @staticmethod
    def _resolve_store_account_id(account_id: str) -> str:
        trimmed = account_id.strip()
        connections: Mapping[str, Any] = getattr(_TimescaleStore, "_connections", {})
        lock = getattr(_TimescaleStore, "_lock", None)
        normalized = _normalize_account_id(account_id)

        def _find_match() -> str | None:
            if trimmed and trimmed in connections:
                return trimmed
            for key in connections.keys():
                if _normalize_account_id(key) == normalized:
                    return key
            return None

        if lock is not None and hasattr(lock, "__enter__") and hasattr(lock, "__exit__"):
            with cast(ContextManager[Any], lock):
                match = _find_match()
        else:
            match = _find_match()

        if match is not None:
            return match
        if trimmed:
            return trimmed
        return normalized


@dataclass
class RedisFeastAdapter:
    account_id: str
    repository_factory: Callable[[str], UniverseRepository] = field(
        default=UniverseRepository, repr=False
    )
    repository: UniverseRepository | None = field(default=None, repr=False)
    feast_client_factory: Callable[[str], FeastClient] = field(
        default=get_feast_client, repr=False
    )
    redis_client_factory: Callable[[str], RedisClient] = field(
        default=get_redis_client, repr=False
    )
    feature_store_factory: Callable[[FeastClient, RedisClient], Any] | None = field(
        default=None, repr=False
    )
    cache_ttl: int = 60

    _features: ClassVar[Dict[str, Dict[str, Any]]] = {}
    _fee_tiers: ClassVar[Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}
    _online_feature_store: ClassVar[Dict[str, Dict[str, Dict[str, Any]]]] = {}
    _feature_expirations: ClassVar[Dict[str, datetime]] = {}
    _fee_tier_expirations: ClassVar[Dict[str, datetime]] = {}
    _online_feature_expirations: ClassVar[Dict[str, Dict[str, datetime]]] = {}

    _APPROVED_FIELDS: ClassVar[Tuple[str, ...]] = ("instrument", "approved")
    _FEE_OVERRIDE_FIELDS: ClassVar[Tuple[str, ...]] = (
        "instrument",
        "maker_bps",
        "taker_bps",
        "currency",
    )
    _FEE_TIER_FIELDS: ClassVar[Tuple[str, ...]] = (
        "pair",
        "tier",
        "maker_bps",
        "taker_bps",
        "notional_threshold",
    )
    _ONLINE_FIELDS: ClassVar[Tuple[str, ...]] = (
        "features",
        "book_snapshot",
        "state",
        "expected_edge_bps",
        "take_profit_bps",
        "stop_loss_bps",
        "confidence",
    )

    def __post_init__(self) -> None:
        repository = self.repository
        if repository is not None:
            self._repository = repository
        else:
            self._repository = self.repository_factory(self.account_id)

        feast_client = self.feast_client_factory(self.account_id)
        redis_client = self.redis_client_factory(self.account_id)
        factory = self.feature_store_factory or self._default_feature_store_factory
        self._feast_client = feast_client
        self._redis_client = redis_client
        self._store = factory(feast_client, redis_client)

    def approved_instruments(self) -> List[str]:
        cached = self._features.get(self.account_id)
        expires = self._feature_expirations.get(self.account_id)
        if cached and cached.get("approved") and (
            expires is None or not self._cache_expired(expires)
        ):
            return [str(symbol) for symbol in cached["approved"]]

        records = self._load_historical_records(
            view_suffix="approved_instruments",
            fields=self._APPROVED_FIELDS,
            window_minutes=60,
        )

        approved_candidates = [
            record.get("instrument")
            for record in records
            if record.get("approved")
        ]
        instruments = sorted(
            filter_spot_symbols(approved_candidates, logger=logger)
        )
        if not instruments:
            raise RuntimeError(
                "Feast returned no approved spot instruments for account '%s'"
                % self.account_id
            )
        payload = self._features.setdefault(self.account_id, {})
        payload["approved"] = instruments
        self._feature_expirations[self.account_id] = datetime.now(timezone.utc).replace(
            microsecond=0
        ) + timedelta(seconds=self.cache_ttl)
        return instruments

    def fee_override(self, instrument: str) -> Dict[str, Any] | None:
        normalized_instrument = normalize_spot_symbol(instrument)
        if not normalized_instrument or not is_spot_symbol(normalized_instrument):
            raise ValueError(
                f"Fee overrides are only available for spot instruments: {instrument}"
            )

        cache_key = normalized_instrument
        cached_account = self._features.setdefault(self.account_id, {})
        cached_fees = cached_account.setdefault("fees", {})
        expires = self._feature_expirations.get(self.account_id)
        if cache_key in cached_fees and (
            expires is None or not self._cache_expired(expires)
        ):
            return dict(cached_fees[cache_key])

        records = self._load_historical_records(
            view_suffix="fee_overrides",
            fields=self._FEE_OVERRIDE_FIELDS,
            window_minutes=240,
        )

        matched = None
        for record in sorted(
            records,
            key=lambda item: item.get("event_timestamp", datetime.min),
            reverse=True,
        ):
            record_symbol = normalize_spot_symbol(record.get("instrument", ""))
            if record_symbol == cache_key:
                matched = record
                break

        if matched is None:
            return None

        override = {
            "currency": matched.get("currency", "USD"),
            "maker": float(matched.get("maker_bps", matched.get("maker", 0.0))),
            "taker": float(matched.get("taker_bps", matched.get("taker", 0.0))),
        }
        cached_fees[cache_key] = dict(override)
        self._feature_expirations[self.account_id] = datetime.now(timezone.utc).replace(
            microsecond=0
        ) + timedelta(seconds=self.cache_ttl)
        return override

    def fee_tiers(self, pair: str) -> List[Dict[str, Any]]:
        account_cache = self._fee_tiers.setdefault(self.account_id, {})
        normalized_pair = normalize_spot_symbol(pair)
        if not normalized_pair or not is_spot_symbol(normalized_pair):
            raise ValueError(
                f"Fee tiers are only available for spot pairs: {pair}"
            )

        expires = self._fee_tier_expirations.get(self.account_id)
        if normalized_pair in account_cache and (
            expires is None or not self._cache_expired(expires)
        ):
            return [dict(entry) for entry in account_cache[normalized_pair]]

        records = self._load_historical_records(
            view_suffix="fee_tiers",
            fields=self._FEE_TIER_FIELDS,
            window_minutes=240,
        )

        tiers: List[Dict[str, Any]] = []
        for record in records:
            record_pair = normalize_spot_symbol(record.get("pair", ""))
            if record_pair not in {normalized_pair, "DEFAULT"}:
                continue
            tiers.append(
                {
                    "tier": record.get("tier"),
                    "maker": float(record.get("maker_bps", 0.0)),
                    "taker": float(record.get("taker_bps", 0.0)),
                    "notional_threshold": float(record.get("notional_threshold", 0.0)),
                }
            )

        if not tiers:
            raise RuntimeError(f"Feast returned no fee tiers for {pair}")

        tiers.sort(key=lambda item: item.get("notional_threshold", 0.0))
        account_cache[normalized_pair] = [dict(entry) for entry in tiers]
        self._fee_tier_expirations[self.account_id] = datetime.now(timezone.utc).replace(
            microsecond=0
        ) + timedelta(seconds=self.cache_ttl)
        return tiers

    @classmethod
    def seed_fee_tiers(
        cls, tiers: Mapping[str, Mapping[str, Iterable[Mapping[str, Any]]]]
    ) -> None:
        normalized: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for account, account_tiers in tiers.items():
            account_bucket: Dict[str, List[Dict[str, Any]]] = {}
            for pair, entries in account_tiers.items():
                normalized_pair = normalize_spot_symbol(pair)
                if normalized_pair == "DEFAULT":
                    account_bucket[normalized_pair] = [dict(entry) for entry in entries]
                    continue
                if not normalized_pair or not is_spot_symbol(normalized_pair):
                    continue
                account_bucket[normalized_pair] = [dict(entry) for entry in entries]
            if account_bucket:
                normalized[account] = account_bucket
        cls._fee_tiers = normalized
        cls._fee_tier_expirations.clear()

    def fetch_online_features(self, instrument: str) -> Dict[str, Any]:
        account_cache = self._online_feature_store.setdefault(self.account_id, {})
        normalized_instrument = normalize_spot_symbol(instrument)
        if not normalized_instrument or not is_spot_symbol(normalized_instrument):
            raise ValueError(
                f"Online features are only available for spot instruments: {instrument}"
            )

        instrument_key = normalized_instrument
        expires = self._online_feature_expirations.setdefault(self.account_id, {})
        cached = account_cache.get(instrument_key)
        if cached:
            cached_expiry = expires.get(instrument_key)
            if cached_expiry is None or not self._cache_expired(cached_expiry):
                return dict(cached)

        features = self._online_feature_refs()
        entity_rows = [
            {
                "account_id": self._feast_client.account_namespace,
                "instrument": instrument_key,
            }
        ]
        try:
            response = self._store.get_online_features(
                features=features,
                entity_rows=entity_rows,
            )
        except Exception as exc:  # pragma: no cover - safety net
            raise RuntimeError("Failed to fetch online features from Feast") from exc

        payload = self._coerce_online_payload(response)
        if not payload:
            raise RuntimeError(
                f"Feast returned empty online feature payload for {instrument}"
            )

        account_cache[instrument_key] = dict(payload)
        expires[instrument_key] = datetime.now(timezone.utc).replace(
            microsecond=0
        ) + timedelta(seconds=self.cache_ttl)
        return dict(payload)

    def _default_feature_store_factory(
        self, feast_client: FeastClient, redis_client: RedisClient
    ) -> Any:
        if FeatureStore is None:
            if _allow_test_fallbacks():
                return _LocalFeastStore(self.account_id, feast_client)
            raise RuntimeError(
                "feast.FeatureStore is unavailable; install Feast to use RedisFeastAdapter"
            )
        repo_path = Path(os.getenv("FEAST_REPO_PATH", "data/feast"))
        if repo_path.exists():
            return FeatureStore(repo_path=str(repo_path))
        try:
            from feast.infra.online_stores.redis import RedisOnlineStoreConfig
            from feast.repo_config import RepoConfig
        except Exception as exc:  # pragma: no cover - depends on Feast install
            if _allow_test_fallbacks():
                return _LocalFeastStore(self.account_id, feast_client)
            raise RuntimeError("Unable to construct Feast configuration") from exc

        config = RepoConfig(
            project=feast_client.project,
            provider="local",
            registry=str(repo_path / "registry.db"),
            online_store=RedisOnlineStoreConfig(
                connection_string=redis_client.dsn,
            ),
            entity_key_serialization_version=2,
        )
        return FeatureStore(config=config)

    def _load_historical_records(
        self,
        *,
        view_suffix: str,
        fields: Tuple[str, ...],
        window_minutes: int,
    ) -> List[Dict[str, Any]]:
        feature_refs = [f"{self._view_name(view_suffix)}:{field}" for field in fields]
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=window_minutes)
        entity_rows = [
            {
                "account_id": self._feast_client.account_namespace,
            }
        ]
        try:
            job = self._store.get_historical_features(
                entity_rows=entity_rows,
                feature_refs=feature_refs,
                start_date=start,
                end_date=end,
            )
        except Exception as exc:
            raise RuntimeError("Failed to query Feast historical store") from exc

        records = self._coerce_records(job)
        if not records:
            raise RuntimeError(
                f"Feast returned no data for feature view '{self._view_name(view_suffix)}'"
            )
        return records

    def _view_name(self, suffix: str) -> str:
        return f"{self._feast_client.account_namespace}__{suffix}"

    def _coerce_records(self, job: Any) -> List[Dict[str, Any]]:
        if hasattr(job, "to_dicts"):
            return [dict(record) for record in job.to_dicts()]
        if hasattr(job, "to_df"):
            frame = job.to_df()
            to_dict = getattr(frame, "to_dict", None)
            if callable(to_dict):
                records = to_dict(orient="records")
                if isinstance(records, list):
                    return [dict(record) for record in records]
        if isinstance(job, Iterable):
            return [dict(record) for record in job]
        return []

    def _coerce_online_payload(self, payload: Any) -> Dict[str, Any]:
        data: Mapping[str, Any]
        if hasattr(payload, "to_dict"):
            data = payload.to_dict()
        elif isinstance(payload, Mapping):
            data = payload
        else:
            try:
                data = dict(payload)
            except Exception:  # pragma: no cover - fallback
                return {}

        normalized: Dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(key, str) and ":" in key:
                normalized_key = key.split(":", 1)[-1]
            else:
                normalized_key = str(key)
            normalized[normalized_key] = value
        return normalized

    def _online_feature_refs(self) -> List[str]:
        view = self._view_name("instrument_features")
        return [f"{view}:{field}" for field in self._ONLINE_FIELDS]

    def _cache_expired(self, expires: Optional[datetime]) -> bool:
        if expires is None:
            return True
        return datetime.now(timezone.utc) >= expires


def _extract_secret_rotated_at(metadata: Mapping[str, Any]) -> Optional[str]:
    rotated = metadata.get("rotated_at") or metadata.get("last_rotated_at")
    if rotated:
        return str(rotated)
    annotations = metadata.get("annotations")
    if isinstance(annotations, Mapping):
        value = annotations.get(K8S_ROTATED_AT)
        if value:
            return str(value)
    return None


@dataclass
class KrakenSecretManager:
    account_id: str
    namespace: str = "aether-secrets"
    secret_store: Optional[KrakenSecretStore] = None
    timescale: Optional[TimescaleAdapter] = None
    encryptor: Optional[EnvelopeEncryptor] = None

    def __post_init__(self) -> None:
        if self.secret_store is None:
            self.secret_store = KrakenSecretStore(namespace=self.namespace)
        if self.timescale is None:
            self.timescale = TimescaleAdapter(account_id=self.account_id)
        if self.encryptor is None:
            self.encryptor = EnvelopeEncryptor()

    @property
    def secret_name(self) -> str:
        secret_store = self.secret_store
        if secret_store is None:
            raise RuntimeError("Secret store has not been initialised")
        name = secret_store.secret_name(self.account_id)
        if not isinstance(name, str):  # pragma: no cover - defensive programming
            raise TypeError("Secret store returned a non-string secret name")
        return name


    def rotate_credentials(self, *, api_key: str, api_secret: str) -> Dict[str, Any]:
        assert self.secret_store is not None  # for type checkers
        assert self.timescale is not None

        before_status = self.timescale.credential_rotation_status()
        rotated_at = datetime.now(timezone.utc)
        assert self.encryptor is not None
        envelope = self.encryptor.encrypt_credentials(
            self.account_id,
            api_key=api_key,
            api_secret=api_secret,
        )
        assert self.secret_store is not None
        if hasattr(self.secret_store, "write_encrypted_secret"):
            self.secret_store.write_encrypted_secret(
                self.account_id,
                envelope=envelope,
            )
        else:  # pragma: no cover - compatibility guard
            self.secret_store.write_credentials(
                self.account_id,
                api_key=api_key,
                api_secret=api_secret,
            )
        metadata = self.timescale.record_credential_rotation(
            secret_name=self.secret_name,
            rotated_at=rotated_at,
            kms_key_id=envelope.kms_key_id,
        )
        metadata.setdefault("secret_name", self.secret_name)
        metadata.setdefault("created_at", rotated_at)
        metadata.setdefault("rotated_at", rotated_at)

        return {
            "metadata": metadata,
            "before": before_status,
        }

    def status(self) -> Optional[Dict[str, Any]]:
        assert self.timescale is not None
        return self.timescale.credential_rotation_status()

    def get_credentials(self) -> Dict[str, Any]:
        """Load Kraken API credentials for the account from the secret store."""

        assert self.secret_store is not None
        assert self.timescale is not None

        secret_payload = self.secret_store.get_secret(self.secret_name)
        if not secret_payload:
            raise RuntimeError(
                f"Kraken credentials secret '{self.secret_name}' not found in namespace "
                f"'{self.namespace}'."
            )

        data: Dict[str, Any] = secret_payload.get("data") or {}
        api_key: Optional[str]
        api_secret: Optional[str]
        if {
            "encrypted_api_key",
            "encrypted_api_key_nonce",
            "encrypted_api_secret",
            "encrypted_api_secret_nonce",
            "encrypted_data_key",
        }.issubset(data):
            envelope = EncryptedSecretEnvelope.from_secret_data(data)
            assert self.encryptor is not None
            decrypted = self.encryptor.decrypt_credentials(
                self.account_id,
                envelope,
            )
            api_key = decrypted.api_key
            api_secret = decrypted.api_secret
        else:
            api_key = data.get("api_key")
            api_secret = data.get("api_secret")
        if not api_key or not api_secret:
            raise RuntimeError(
                "Kraken credentials are missing required fields 'api_key' or 'api_secret'."
            )

        metadata = deepcopy(secret_payload.get("metadata") or {})
        metadata.setdefault("secret_name", self.secret_name)
        metadata.setdefault("namespace", self.namespace)
        metadata["material_present"] = True

        rotated_at = _extract_secret_rotated_at(metadata)
        if rotated_at is None:
            raise RuntimeError(
                "Kraken credentials metadata missing rotation timestamp; cannot determine freshness."
            )

        sanitized_metadata = deepcopy(metadata)
        for sensitive_field in (
            "api_key",
            "api_secret",
            "encrypted_api_key",
            "encrypted_api_secret",
        ):
            if sensitive_field in sanitized_metadata and sanitized_metadata[sensitive_field]:
                sanitized_metadata[sensitive_field] = "***"
        sanitized_metadata.setdefault("api_key", "***")
        sanitized_metadata.setdefault("api_secret", "***")
        annotations = sanitized_metadata.get("annotations")
        if isinstance(annotations, Mapping):
            annotations = dict(annotations)
        else:
            annotations = {}
        annotations.setdefault(K8S_ROTATED_AT, rotated_at)
        sanitized_metadata["annotations"] = annotations
        sanitized_metadata["rotated_at"] = rotated_at

        self.timescale.record_credential_access(
            secret_name=self.secret_name,
            metadata=metadata,
        )

        self.timescale.record_event(
            "kraken.credentials.access",
            {
                "secret_name": self.secret_name,
                "metadata": sanitized_metadata,
            },
        )

        return {
            "api_key": api_key,
            "api_secret": api_secret,
            "metadata": sanitized_metadata,
        }


def default_fee(currency: str = "USD") -> Dict[str, float | str]:
    return {"currency": currency, "maker": 0.1, "taker": 0.2}

