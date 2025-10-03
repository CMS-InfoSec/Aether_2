from __future__ import annotations


import asyncio
import base64
import hashlib
import json
import logging

import os

import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
import threading


from datetime import datetime, timedelta, timezone
from typing import Any, Callable, ClassVar, Dict, Iterable, List, Mapping, Optional, Tuple
from weakref import WeakSet


from common.utils.tracing import attach_correlation, current_correlation_id
from shared.k8s import ANNOTATION_ROTATED_AT as K8S_ROTATED_AT, KrakenSecretStore
from services.secrets.secure_secrets import (
    EncryptedSecretEnvelope,
    EnvelopeEncryptor,
)

from services.common.config import (
    get_feast_client,
    get_redis_client,
    get_kafka_producer,
    get_nats_producer,
    get_timescale_session,
)
import tempfile
from services.universe.repository import UniverseRepository

try:
    from feast import FeatureStore
except Exception:  # pragma: no cover - Feast is optional during testing
    FeatureStore = None  # type: ignore[assignment]

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


_DB_ROOT = Path(tempfile.gettempdir()) / "aether_timescale"


def _ensure_db_root() -> None:
    try:
        _DB_ROOT.mkdir(parents=True, exist_ok=True)
    except Exception:  # pragma: no cover - directory creation failures should not crash tests
        logging.getLogger(__name__).exception("Failed to ensure Timescale adapter DB directory")


def _sanitize_identifier(raw: str) -> str:
    value = raw.strip().lower().replace("-", "_")
    sanitized = "".join(ch for ch in value if ch.isalnum() or ch == "_")
    if not sanitized:
        sanitized = "acct"
    if sanitized[0].isdigit():
        sanitized = f"acct_{sanitized}"
    return sanitized


def _database_path(*, dsn: str, schema: str) -> Path:
    _ensure_db_root()
    key = f"{dsn}|{schema}"
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return _DB_ROOT / f"{digest}.db"


def _isoformat(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()


def _deserialize_timestamp(raw: str) -> datetime:
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
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return data
    return {"value": data}


@dataclass
class _TimescaleConnectionState:
    connection: sqlite3.Connection
    lock: threading.RLock
    tables: Dict[str, str]
    pending_counts: Dict[str, int] = field(default_factory=dict)
    dirty: bool = False
    dsn: str = ""
    schema: str = ""
    path: Path | None = None


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
    """Lightweight persistence layer backed by sqlite for Timescale-like data."""

    _lock: ClassVar[threading.Lock] = threading.Lock()
    _connections: ClassVar[Dict[str, _TimescaleConnectionState]] = {}
    _TABLE_DEFINITIONS: ClassVar[Dict[str, str]] = {
        "acks": """
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
        """.strip(),
        "fills": """
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
        """.strip(),
        "shadow_fills": """
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
        """.strip(),
        "events": """
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            )
        """.strip(),
        "telemetry": """
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT,
                payload TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            )
        """.strip(),
        "audit_logs": """
            CREATE TABLE IF NOT EXISTS {table} (
                id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            )
        """.strip(),
        "credential_events": """
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event TEXT NOT NULL,
                event_type TEXT NOT NULL,
                secret_name TEXT,
                metadata TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            )
        """.strip(),
        "credential_rotations": """
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                secret_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                rotated_at TEXT NOT NULL,
                kms_key_id TEXT
            )
        """.strip(),
        "risk_configs": """
            CREATE TABLE IF NOT EXISTS {table} (
                account_id TEXT PRIMARY KEY,
                config TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """.strip(),
    }

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
        path = _database_path(dsn=session.dsn, schema=session.account_schema)
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            path,
            detect_types=sqlite3.PARSE_DECLTYPES,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            logger.debug("journal_mode WAL unsupported on this sqlite build")
        prefix = _sanitize_identifier(session.account_schema or self.account_id)
        tables = {name: f'"{prefix}_{name}"' for name in self._TABLE_DEFINITIONS}
        state = _TimescaleConnectionState(
            connection=connection,
            lock=threading.RLock(),
            tables=tables,
            dsn=session.dsn,
            schema=session.account_schema,
            path=path,
        )
        self._initialize_schema(state)
        return state

    def _initialize_schema(self, state: _TimescaleConnectionState) -> None:
        with state.lock:
            for name, ddl in self._TABLE_DEFINITIONS.items():
                table = state.tables[name]
                state.connection.execute(ddl.format(table=table))
            state.connection.commit()
            state.pending_counts.clear()
            state.dirty = False

    def _with_retry(
        self,
        operation: Callable[[sqlite3.Connection], Any],
        *,
        state: _TimescaleConnectionState | None = None,
    ) -> Any:
        active_state = state or self._state
        delay = self._backoff_seconds
        for attempt in range(1, self._max_retries + 1):
            try:
                with active_state.lock:
                    return operation(active_state.connection)
            except sqlite3.OperationalError as exc:
                if attempt == self._max_retries:
                    raise RuntimeError("Timescale storage operation failed") from exc
                time.sleep(delay)
                delay *= 2

    def _mark_pending(
        self,
        bucket: str,
        *,
        count: int = 1,
        state: _TimescaleConnectionState | None = None,
    ) -> None:
        active_state = state or self._state
        current = active_state.pending_counts.get(bucket, 0)
        active_state.pending_counts[bucket] = current + count
        active_state.dirty = True

    # ------------------------------------------------------------------
    # Insert helpers
    # ------------------------------------------------------------------
    def record_ack(self, payload: Mapping[str, Any]) -> None:
        recorded_at = _isoformat(datetime.now(timezone.utc))
        table = self._state.tables["acks"]
        self._with_retry(
            lambda conn: conn.execute(
                f"INSERT INTO {table} (recorded_at, payload) VALUES (?, ?)",
                (recorded_at, _json_dumps(dict(payload))),
            )
        )
        self._mark_pending("acks")

    def record_fill(self, payload: Mapping[str, Any], *, shadow: bool = False) -> None:
        recorded_at = _isoformat(datetime.now(timezone.utc))
        table_key = "shadow_fills" if shadow else "fills"
        table = self._state.tables[table_key]
        self._with_retry(
            lambda conn: conn.execute(
                f"INSERT INTO {table} (recorded_at, payload) VALUES (?, ?)",
                (recorded_at, _json_dumps(dict(payload))),
            )
        )
        self._mark_pending(table_key)

    def record_event(self, event_type: str, payload: Mapping[str, Any]) -> None:
        timestamp = _isoformat(datetime.now(timezone.utc))
        table = self._state.tables["events"]
        self._with_retry(
            lambda conn: conn.execute(
                f"INSERT INTO {table} (event_type, payload, recorded_at) VALUES (?, ?, ?)",
                (event_type, _json_dumps(dict(payload)), timestamp),
            )
        )
        self._mark_pending("events")

    def record_audit_log(self, record: Mapping[str, Any]) -> None:
        stored = dict(record)
        entry_id = stored.setdefault("id", str(uuid.uuid4()))
        created_at = _isoformat(
            stored.setdefault("created_at", datetime.now(timezone.utc))
        )
        table = self._state.tables["audit_logs"]
        self._with_retry(
            lambda conn: conn.execute(
                f"INSERT OR REPLACE INTO {table} (id, payload, recorded_at) VALUES (?, ?, ?)",
                (str(entry_id), _json_dumps(stored), created_at),
            )
        )
        self._mark_pending("audit_logs")

    def record_decision(self, order_id: str, payload: Mapping[str, Any]) -> None:
        table = self._state.tables["telemetry"]
        recorded_at = _isoformat(datetime.now(timezone.utc))
        self._with_retry(
            lambda conn: conn.execute(
                f"INSERT INTO {table} (order_id, payload, recorded_at) VALUES (?, ?, ?)",
                (order_id, _json_dumps(dict(payload)), recorded_at),
            )
        )
        self._mark_pending("telemetry")

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
        self._with_retry(
            lambda conn: conn.execute(
                f"INSERT INTO {table} (event, event_type, secret_name, metadata, recorded_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (
                    event,
                    event_type,
                    secret_name,
                    _json_dumps(dict(metadata)),
                    _isoformat(recorded_at),
                ),
            )
        )
        self._mark_pending("credential_events")

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
        self._with_retry(
            lambda conn: conn.execute(
                f"INSERT INTO {table} (secret_name, created_at, rotated_at, kms_key_id)"
                " VALUES (?, ?, ?, ?)",
                (
                    secret_name,
                    _isoformat(created_at),
                    _isoformat(rotated_at),
                    kms_key_id,
                ),
            )
        )
        self._mark_pending("credential_rotations")
        return metadata

    def upsert_risk_config(self, config: Mapping[str, Any]) -> None:
        table = self._state.tables["risk_configs"]
        payload = _json_dumps(dict(config))
        updated_at = _isoformat(datetime.now(timezone.utc))
        self._with_retry(
            lambda conn: conn.execute(
                f"INSERT INTO {table} (account_id, config, updated_at) VALUES (?, ?, ?)"
                " ON CONFLICT(account_id) DO UPDATE SET config=excluded.config,"
                " updated_at=excluded.updated_at",
                (self.account_id, payload, updated_at),
            )
        )
        self._mark_pending("risk_configs")

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def fetch_events(self) -> Dict[str, List[Dict[str, Any]]]:
        state = self._state

        def _rows(table_key: str, columns: str) -> List[sqlite3.Row]:
            table = state.tables[table_key]
            sql = f"SELECT {columns} FROM {table} ORDER BY id ASC"
            return self._with_retry(lambda conn: conn.execute(sql).fetchall(), state=state)

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
                metadata["created_at"] = _deserialize_timestamp(str(created_at))
            if rotated_at is not None:
                metadata["rotated_at"] = _deserialize_timestamp(str(rotated_at))
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
        rows = self._with_retry(
            lambda conn: conn.execute(
                f"SELECT order_id, payload, recorded_at FROM {table} ORDER BY id ASC"
            ).fetchall()
        )
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
        rows = self._with_retry(
            lambda conn: conn.execute(
                f"SELECT id, payload, recorded_at FROM {table} ORDER BY recorded_at ASC"
            ).fetchall()
        )
        results: List[Dict[str, Any]] = []
        for row in rows:
            payload = _json_loads(row["payload"])
            payload.setdefault("id", row["id"])
            payload.setdefault("created_at", _deserialize_timestamp(row["recorded_at"]))
            results.append(payload)
        return results

    def _fetch_credential_events(self, event: str | None = None) -> List[sqlite3.Row]:
        table = self._state.tables["credential_events"]
        if event is None:
            sql = f"SELECT event, event_type, secret_name, metadata, recorded_at FROM {table} ORDER BY id ASC"
            return self._with_retry(lambda conn: conn.execute(sql).fetchall())
        sql = (
            f"SELECT event, event_type, secret_name, metadata, recorded_at FROM {table}"
            " WHERE event = ? ORDER BY id ASC"
        )
        return self._with_retry(
            lambda conn: conn.execute(sql, (event,)).fetchall()
        )

    def fetch_credential_events(self) -> List[Dict[str, Any]]:
        rows = self._fetch_credential_events(None)
        results: List[Dict[str, Any]] = []
        for row in rows:
            metadata = _json_loads(row["metadata"])
            created_at = metadata.get("created_at")
            rotated_at = metadata.get("rotated_at")
            if created_at is not None:
                metadata["created_at"] = _deserialize_timestamp(str(created_at))
            if rotated_at is not None:
                metadata["rotated_at"] = _deserialize_timestamp(str(rotated_at))
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
        row = self._with_retry(
            lambda conn: conn.execute(
                f"SELECT secret_name, created_at, rotated_at, kms_key_id"
                f" FROM {table} ORDER BY rotated_at DESC LIMIT 1"
            ).fetchone()
        )
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
        row = self._with_retry(
            lambda conn: conn.execute(
                f"SELECT config FROM {table} WHERE account_id = ?",
                (self.account_id,),
            ).fetchone()
        )
        if row is None:
            self.upsert_risk_config(default)
            return dict(default)
        stored = _json_loads(row["config"])
        return dict(default | stored)

    # ------------------------------------------------------------------
    # Maintenance helpers
    # ------------------------------------------------------------------
    def clear_rotation_state(self) -> None:
        rotation_table = self._state.tables["credential_rotations"]
        events_table = self._state.tables["credential_events"]
        self._with_retry(lambda conn: conn.execute(f"DELETE FROM {rotation_table}"))
        self._with_retry(
            lambda conn: conn.execute(
                f"DELETE FROM {events_table} WHERE event = ?",
                ("rotation",),
            )
        )
        self._state.pending_counts.pop("credential_rotations", None)
        self._state.pending_counts.pop("credential_events", None)
        self._state.dirty = True

    @classmethod
    async def flush_all(cls) -> Dict[str, Dict[str, int]]:
        await asyncio.sleep(0)
        summary: Dict[str, Dict[str, int]] = {}
        with cls._lock:
            items = list(cls._connections.items())
        for account_id, state in items:
            with state.lock:
                if not state.dirty:
                    continue
                state.connection.commit()
                if state.pending_counts:
                    summary[account_id] = dict(state.pending_counts)
                state.pending_counts.clear()
                state.dirty = False
        return summary

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
                state.connection.execute(f"DELETE FROM {rotation_table}")
                state.connection.execute(
                    f"DELETE FROM {events_table} WHERE event = ?",
                    ("rotation",),
                )
                state.connection.commit()
                state.pending_counts.pop("credential_rotations", None)
                state.pending_counts.pop("credential_events", None)
                state.dirty = False
            return

        with cls._lock:
            states = list(cls._connections.items())
        for _, state in states:
            with state.lock:
                rotation_table = state.tables["credential_rotations"]
                events_table = state.tables["credential_events"]
                state.connection.execute(f"DELETE FROM {rotation_table}")
                state.connection.execute(
                    f"DELETE FROM {events_table} WHERE event = ?",
                    ("rotation",),
                )
                state.connection.commit()
                state.pending_counts.pop("credential_rotations", None)
                state.pending_counts.pop("credential_events", None)
                state.dirty = False

    @classmethod
    def reset_account(cls, account_id: str) -> None:
        with cls._lock:
            state = cls._connections.pop(account_id, None)
        if state is None:
            return
        with state.lock:
            for table in state.tables.values():
                state.connection.execute(f"DELETE FROM {table}")
            state.connection.commit()
            state.pending_counts.clear()
            state.dirty = False
        try:
            state.connection.close()
        finally:
            if state.path and state.path.exists():
                try:
                    state.path.unlink()
                except FileNotFoundError:
                    pass

    @classmethod
    def reset_all(cls) -> None:
        with cls._lock:
            states = list(cls._connections.items())
            cls._connections.clear()
        for account_id, state in states:
            with state.lock:
                for table in state.tables.values():
                    state.connection.execute(f"DELETE FROM {table}")
                state.connection.commit()
                state.pending_counts.clear()
                state.dirty = False
            try:
                state.connection.close()
            finally:
                if state.path and state.path.exists():
                    try:
                        state.path.unlink()
                    except FileNotFoundError:
                        pass

@dataclass(eq=False)
class KafkaNATSAdapter:
    account_id: str
    kafka_config_factory: Callable[[str], "KafkaProducer"] = field(
        default=get_kafka_producer, repr=False
    )
    nats_config_factory: Callable[[str], "NATSProducer"] = field(
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
        self._kafka_client: httpx.Client | None = None
        self._nats_client: httpx.Client | None = None
        self._kafka_prefix = ""
        self._nats_prefix = ""

        self._bootstrap_clients()

    def publish(self, topic: str, payload: Dict[str, Any]) -> None:
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
            status = self._attempt_publish(record)
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
            # Broker unavailable, retain the record for a later flush.
            self._buffer_event(record)
            self._record_event(record)

    def history(self, correlation_id: str | None = None) -> List[Dict[str, Any]]:
        records = list(self._published_events.get(self.account_id, []))
        if correlation_id:
            return [record for record in records if record.get("correlation_id") == correlation_id]
        return records

    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        try:
            get_kafka_producer.cache_clear()  # type: ignore[attr-defined]
        except AttributeError:  # pragma: no cover - defensive
            pass
        try:
            get_nats_producer.cache_clear()  # type: ignore[attr-defined]
        except AttributeError:  # pragma: no cover - defensive
            pass

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
    def flush_events(cls) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for instance in list(cls._instances):
            drained = instance._drain_buffer()
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
            self._kafka_client = self._build_client(endpoint)

        try:
            nats_config = self.nats_config_factory(self.account_id)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Unable to load NATS configuration", exc_info=exc)
            nats_config = None

        if nats_config and nats_config.servers:
            self._nats_prefix = nats_config.subject_prefix.strip()
            endpoint = _first_endpoint(nats_config.servers)
            self._nats_client = self._build_client(endpoint)

    def _build_client(self, endpoint: str) -> httpx.Client | None:
        try:
            base_url = _as_base_url(endpoint)
            client = httpx.Client(base_url=base_url, timeout=self.request_timeout)
            attempt = 0
            while True:
                attempt += 1
                try:
                    response = client.get("/health")
                    if response.status_code >= 500:
                        raise PublishError(
                            f"Broker health check failed ({response.status_code})"
                        )
                    break
                except httpx.HTTPStatusError as exc:
                    client.close()
                    raise PublishError("Broker health check failed") from exc
                except httpx.RequestError:
                    if attempt >= self.max_retries:
                        client.close()
                        return None
                    time.sleep(self.backoff_seconds * (2 ** (attempt - 1)))
            return client
        except Exception:
            logger.exception("Failed to initialise broker client", extra={"endpoint": endpoint})
            return None

    def _attempt_publish(self, record: Dict[str, Any]) -> str:
        transports = []
        if self._kafka_client is not None:
            transports.append((self._kafka_client, self._topic_path(record["topic"])))
        if self._nats_client is not None:
            transports.append((self._nats_client, self._subject_path(record["topic"])) )

        if not transports:
            return "offline"

        errors: List[BaseException] = []
        successes = 0
        for client, path in transports:
            try:
                self._send_with_retry(client, path, record["payload"])
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

    def _send_with_retry(self, client: httpx.Client, path: str, payload: Dict[str, Any]) -> None:
        attempt = 0
        while True:
            attempt += 1
            try:
                response = client.post(path, json=payload)
                response.raise_for_status()
                return
            except httpx.HTTPStatusError as exc:
                if attempt >= self.max_retries:
                    raise PublishError(f"Broker responded with status {exc.response.status_code}") from exc
            except httpx.RequestError as exc:
                if attempt >= self.max_retries:
                    raise PublishError("Transport error during publish") from exc
            time.sleep(self.backoff_seconds * (2 ** (attempt - 1)))

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

    def _drain_buffer(self) -> int:
        drained = 0
        buffer = self._fallback_buffer.get(self.account_id, [])
        while buffer:
            record = buffer[0]
            try:
                status = self._attempt_publish(record)
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
        with self._lock:
            if self._kafka_client is not None:
                self._kafka_client.close()
                self._kafka_client = None
            if self._nats_client is not None:
                self._nats_client.close()
                self._nats_client = None


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
        self._store = _TimescaleStore(
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
        created_at = status.get("created_at") if status else rotated_at
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
        _TimescaleStore.reset_account(normalized)

    @classmethod
    def reset_rotation_state(cls, account_id: str | None = None) -> None:
        if account_id is None:
            _TimescaleStore.clear_all_rotation_state()
            return


        for cache in rotation_caches:
            cache.pop(account_id, None)

        account_events = cls._events.get(account_id)
        if account_events is not None and "credential_rotations" in account_events:
            rotations = account_events["credential_rotations"]
            if isinstance(rotations, list):
                rotations.clear()
            else:
                account_events["credential_rotations"] = []




    def record_credential_access(self, *, secret_name: str, metadata: Dict[str, Any]) -> None:
        sanitized = deepcopy(metadata)
        for key in ("api_key", "api_secret"):
            if key in sanitized and sanitized[key]:
                sanitized[key] = "***"
        if "material_present" not in sanitized:
            sanitized["material_present"] = bool(
                sanitized.get("api_key") and sanitized.get("api_secret")
            )
        payload = {
            "event": "access",
            "event_type": "kraken.credentials.access",
            "secret_name": secret_name,
            "metadata": sanitized,
            "timestamp": datetime.now(timezone.utc),
        }
        events = self._credential_events.setdefault(self.account_id, [])
        events.append(deepcopy(payload))


    def credential_events(self) -> List[Dict[str, Any]]:
        return [deepcopy(event) for event in self._credential_events.get(self.account_id, [])]

@dataclass
class RedisFeastAdapter:
    account_id: str
    repository_factory: Callable[[str], UniverseRepository] = field(
        default=UniverseRepository, repr=False
    )
    repository: UniverseRepository | None = field(default=None, repr=False)
    feast_client_factory: Callable[[str], "FeastClient"] = field(
        default=get_feast_client, repr=False
    )
    redis_client_factory: Callable[[str], "RedisClient"] = field(
        default=get_redis_client, repr=False
    )
    feature_store_factory: Callable[["FeastClient", "RedisClient"], Any] | None = field(
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
            self._repository = self.repository_factory(account_id=self.account_id)

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

        approved = {
            str(record["instrument"]): record
            for record in records
            if record.get("approved")
        }
        if not approved:
            raise RuntimeError(
                "Feast returned no approved instruments for account '%s'" % self.account_id
            )

        instruments = sorted(approved.keys())
        payload = self._features.setdefault(self.account_id, {})
        payload["approved"] = instruments
        self._feature_expirations[self.account_id] = datetime.now(timezone.utc).replace(
            microsecond=0
        ) + timedelta(seconds=self.cache_ttl)
        return instruments

    def fee_override(self, instrument: str) -> Dict[str, Any] | None:
        cache_key = instrument.upper()
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
            if str(record.get("instrument", "")).upper() == cache_key:
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
        expires = self._fee_tier_expirations.get(self.account_id)
        if pair in account_cache and (
            expires is None or not self._cache_expired(expires)
        ):
            return [dict(entry) for entry in account_cache[pair]]

        records = self._load_historical_records(
            view_suffix="fee_tiers",
            fields=self._FEE_TIER_FIELDS,
            window_minutes=240,
        )

        tiers: List[Dict[str, Any]] = []
        for record in records:
            record_pair = str(record.get("pair", "")).upper()
            if record_pair not in {pair.upper(), "DEFAULT"}:
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
        account_cache[pair] = [dict(entry) for entry in tiers]
        self._fee_tier_expirations[self.account_id] = datetime.now(timezone.utc).replace(
            microsecond=0
        ) + timedelta(seconds=self.cache_ttl)
        return tiers

    @classmethod
    def seed_fee_tiers(
        cls, tiers: Mapping[str, Mapping[str, Iterable[Mapping[str, Any]]]]
    ) -> None:
        cls._fee_tiers = {
            account: {
                pair: [dict(entry) for entry in entries]
                for pair, entries in account_tiers.items()
            }
            for account, account_tiers in tiers.items()
        }
        cls._fee_tier_expirations.clear()

    def fetch_online_features(self, instrument: str) -> Dict[str, Any]:
        account_cache = self._online_feature_store.setdefault(self.account_id, {})
        instrument_key = instrument.upper()
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
                "instrument": instrument,
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
        self, feast_client: "FeastClient", redis_client: "RedisClient"
    ) -> Any:
        if FeatureStore is None:
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
            if hasattr(frame, "to_dict"):
                records = frame.to_dict(orient="records")  # type: ignore[arg-type]
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
        assert self.secret_store is not None
        return self.secret_store.secret_name(self.account_id)


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

