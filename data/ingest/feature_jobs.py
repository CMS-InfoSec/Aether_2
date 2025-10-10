"""Streaming jobs that populate Feast feature views from market data."""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import importlib
import json
import logging
import os
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple


class MissingDependencyError(RuntimeError):
    """Raised when an optional dependency is required for feature ingestion."""


aiokafka_spec = importlib.util.find_spec("aiokafka")
if aiokafka_spec is not None:  # pragma: no cover - optional dependency
    from aiokafka import AIOKafkaConsumer
else:  # pragma: no cover - optional dependency
    AIOKafkaConsumer = None  # type: ignore

pandas_spec = importlib.util.find_spec("pandas")
if pandas_spec is not None:  # pragma: no cover - optional dependency
    import pandas as pd  # type: ignore
else:  # pragma: no cover - optional dependency
    pd = None  # type: ignore

feast_spec = importlib.util.find_spec("feast")
if feast_spec is not None:  # pragma: no cover - optional dependency
    from feast import FeatureStore
else:  # pragma: no cover - optional dependency
    FeatureStore = None  # type: ignore

_SQLALCHEMY_AVAILABLE = True
_SQLALCHEMY_IMPORT_ERROR: Exception | None = None

try:  # pragma: no cover - optional dependency
    from sqlalchemy import Column, DateTime, Float, MetaData, String, Table, Text, create_engine
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.engine import Engine
except Exception as exc:  # pragma: no cover - executed when SQLAlchemy absent
    _SQLALCHEMY_AVAILABLE = False
    _SQLALCHEMY_IMPORT_ERROR = exc
else:
    if not hasattr(Table, "c"):
        _SQLALCHEMY_AVAILABLE = False
        _SQLALCHEMY_IMPORT_ERROR = RuntimeError("SQLAlchemy table metadata is unavailable")

if not _SQLALCHEMY_AVAILABLE:
    Column = DateTime = Float = String = Text = None  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]

    def create_engine(*_: object, **__: object) -> "_InMemoryEngine":  # type: ignore[override]
        return _InMemoryEngine(url="memory://feature-jobs")

    def pg_insert(*_: object, **__: object) -> None:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for feature job persistence")


from shared.postgres import normalize_sqlalchemy_dsn

from services.ingest import EventOrderingBuffer, OrderedEvent

LOGGER = logging.getLogger(__name__)

_SQLITE_FALLBACK_FLAG = "FEATURE_JOBS_ALLOW_SQLITE_FOR_TESTS"
_INSECURE_DEFAULTS_FLAG = "FEATURE_JOBS_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "AETHER_STATE_DIR"


def _state_root() -> Path:
    root = Path(os.getenv(_STATE_DIR_ENV, ".aether_state")) / "feature_jobs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _insecure_defaults_enabled() -> bool:
    return os.getenv(_INSECURE_DEFAULTS_FLAG) == "1" or "pytest" in sys.modules


def _resolve_database_url() -> str:
    """Return the managed Timescale/PostgreSQL DSN required for ingestion."""

    raw_url = os.getenv("DATABASE_URL", "")
    if not raw_url.strip():
        if _insecure_defaults_enabled():
            state_path = _state_root() / "feature_jobs.sqlite"
            LOGGER.warning(
                "DATABASE_URL missing; using insecure local sqlite fallback at %s", state_path
            )
            return f"sqlite+pysqlite:///{state_path}"
        raise RuntimeError(
            "Feature jobs ingestion requires DATABASE_URL to be set to a PostgreSQL/Timescale DSN."
        )

    allow_sqlite = "pytest" in sys.modules or os.getenv(_SQLITE_FALLBACK_FLAG) == "1"
    database_url = normalize_sqlalchemy_dsn(
        raw_url.strip(),
        allow_sqlite=allow_sqlite,
        label="Feature jobs database URL",
    )

    if database_url.startswith("sqlite"):
        LOGGER.warning(
            "Using SQLite database '%s' for feature jobs ingestion; allowed only for tests.",
            database_url,
        )

    return database_url


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_CONSUMER_GROUP = os.getenv("FEATURE_JOB_CONSUMER_GROUP", "aether-feature-jobs")
FEATURE_REPO_PATH = os.getenv("FEATURE_REPO_PATH", os.path.join("data", "feast"))
DATABASE_URL = _resolve_database_url()
FEATURE_VIEW_NAME = os.getenv(
    "FEATURE_VIEW_NAME", "market_microstructure_features"
)
ROLLING_WINDOW_SECONDS = int(os.getenv("ROLLING_WINDOW_SECONDS", "300"))
BOOK_LEVELS = int(os.getenv("BOOK_LEVELS", "5"))
MAX_EVENT_LATENESS_MS = int(os.getenv("MAX_EVENT_LATENESS_MS", "5000"))


@dataclass
class TradeEvent:
    """Represents an individual trade coming from md.trades."""

    symbol: str
    price: float
    quantity: float
    event_ts: dt.datetime


@dataclass
class BookLevel:
    price: float
    quantity: float


@dataclass
class BookEvent:
    """Represents a top of book snapshot from md.book."""

    symbol: str
    bids: List[BookLevel]
    asks: List[BookLevel]
    event_ts: dt.datetime


@dataclass
class FeatureState:
    """Maintains rolling state necessary to compute engineered features."""

    window: dt.timedelta
    depth: int
    trades: Deque[TradeEvent] = field(default_factory=deque)
    notional: float = 0.0
    volume: float = 0.0
    bids: List[BookLevel] = field(default_factory=list)
    asks: List[BookLevel] = field(default_factory=list)
    last_timestamp: Optional[dt.datetime] = None

    def update_trade(self, trade: TradeEvent) -> None:
        self.trades.append(trade)
        self.notional += trade.price * trade.quantity
        self.volume += trade.quantity
        self._expire_old_trades(reference_ts=trade.event_ts)
        self.last_timestamp = trade.event_ts

    def update_book(self, book: BookEvent) -> None:
        self.bids = book.bids[: self.depth]
        self.asks = book.asks[: self.depth]
        self.last_timestamp = book.event_ts

    def _expire_old_trades(self, reference_ts: dt.datetime) -> None:
        cutoff = reference_ts - self.window
        while self.trades and self.trades[0].event_ts < cutoff:
            expired = self.trades.popleft()
            self.notional -= expired.price * expired.quantity
            self.volume -= expired.quantity
        if self.volume < 1e-9:
            self.volume = 0.0
            self.notional = 0.0

    def compute_features(self) -> Mapping[str, float]:
        if self.volume > 0:
            rolling_vwap = self.notional / self.volume
        else:
            rolling_vwap = 0.0

        best_bid = self.bids[0].price if self.bids else 0.0
        best_ask = self.asks[0].price if self.asks else 0.0
        spread = best_ask - best_bid if best_ask and best_bid else 0.0

        bid_volume = sum(level.quantity for level in self.bids[: self.depth])
        ask_volume = sum(level.quantity for level in self.asks[: self.depth])
        total_volume = bid_volume + ask_volume
        if total_volume > 0:
            imbalance = (bid_volume - ask_volume) / total_volume
        else:
            imbalance = 0.0

        return {
            "rolling_vwap": float(rolling_vwap),
            "spread": float(spread),
            "order_book_imbalance": float(imbalance),
        }


if _SQLALCHEMY_AVAILABLE:
    metadata = MetaData()

    microstructure_table = Table(
        "market_microstructure_features",
        metadata,
        Column("symbol", String, primary_key=True),
        Column("event_timestamp", DateTime(timezone=True), primary_key=True),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("rolling_vwap", Float),
        Column("spread", Float),
        Column("order_book_imbalance", Float),
    )

    late_events_table = Table(
        "market_data_late_events",
        metadata,
        Column("stream", String, primary_key=True),
        Column("symbol", String, primary_key=True),
        Column("event_timestamp", DateTime(timezone=True), primary_key=True),
        Column("arrival_timestamp", DateTime(timezone=True), primary_key=True),
        Column("payload", Text, nullable=False),
    )
else:
    class _FallbackMetadata:
        def create_all(self, *_: object, **__: object) -> None:
            return None

    metadata = _FallbackMetadata()
    microstructure_table = None
    late_events_table = None


@dataclass
class _InMemoryFeatureStorage:
    offline: Dict[Tuple[str, dt.datetime], Mapping[str, Any]] = field(default_factory=dict)
    created_at: Dict[Tuple[str, dt.datetime], dt.datetime] = field(default_factory=dict)
    late_events: Dict[Tuple[str, str, dt.datetime, dt.datetime], Dict[str, Any]] = field(
        default_factory=dict
    )

    def upsert_feature(
        self,
        *,
        symbol: str,
        event_ts: dt.datetime,
        created_at: dt.datetime,
        features: Mapping[str, float],
    ) -> None:
        key = (symbol, event_ts)
        self.offline[key] = {
            "symbol": symbol,
            "event_timestamp": event_ts,
            "created_at": created_at,
            **features,
        }
        self.created_at[key] = created_at

    def list_features(self) -> List[Mapping[str, Any]]:
        return list(self.offline.values())

    def record_late_event(
        self,
        *,
        stream: str,
        symbol: str,
        event_ts: dt.datetime,
        arrival_ts: dt.datetime,
        payload: str,
    ) -> None:
        key = (stream, symbol, event_ts, arrival_ts)
        self.late_events[key] = {
            "stream": stream,
            "symbol": symbol,
            "event_timestamp": event_ts,
            "arrival_timestamp": arrival_ts,
            "payload": payload,
        }

    def list_late_events(self) -> List[Mapping[str, Any]]:
        return list(self.late_events.values())


@dataclass
class _InMemoryEngine:
    url: str
    storage: _InMemoryFeatureStorage = field(default_factory=_InMemoryFeatureStorage)

    def begin(self) -> "_InMemoryConnection":
        return _InMemoryConnection(self.storage)


class _InMemoryConnection:
    def __init__(self, storage: _InMemoryFeatureStorage) -> None:
        self._storage = storage

    def __enter__(self) -> "_InMemoryConnection":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        return False

    def execute(self, *_: object, **__: object) -> None:
        raise MissingDependencyError("SQLAlchemy is required for feature job persistence")


def _json_default(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


class FeatureWriter:
    """Persists engineered features to TimescaleDB and Redis via Feast."""

    def __init__(
        self,
        *,
        engine: Engine,
        store: FeatureStore | None,
        feature_view: str,
        state_dir: Optional[Path] = None,
    ) -> None:
        self.engine = engine
        self.store = store
        self.feature_view = feature_view
        self._state_dir = state_dir
        self._memory_storage: _InMemoryFeatureStorage | None = None
        self._offline_supported = bool(
            _SQLALCHEMY_AVAILABLE
            and microstructure_table is not None
            and late_events_table is not None
            and hasattr(engine, "begin")
        )
        if self._offline_supported:
            metadata.create_all(
                engine, tables=[microstructure_table, late_events_table]
            )
        else:
            if isinstance(engine, _InMemoryEngine):
                self._memory_storage = engine.storage
            else:
                self._memory_storage = _InMemoryFeatureStorage()
                # ensure a consistent engine attribute for callers expecting one.
                self.engine = _InMemoryEngine(url=getattr(engine, "url", "memory://feature-jobs"))
                self.engine.storage = self._memory_storage

    def persist(
        self, *, symbol: str, event_ts: dt.datetime, feature_payload: Mapping[str, float]
    ) -> None:
        created_at = dt.datetime.now(tz=dt.timezone.utc)
        LOGGER.debug(
            "Persisting features",
            extra={
                "symbol": symbol,
                "event_ts": event_ts.isoformat(),
                "features": feature_payload,
            },
        )
        self._write_offline(symbol, event_ts, created_at, feature_payload)
        self._write_online(symbol, event_ts, created_at, feature_payload)

    def _write_offline(
        self,
        symbol: str,
        event_ts: dt.datetime,
        created_at: dt.datetime,
        features: Mapping[str, float],
    ) -> None:
        if not self._offline_supported or microstructure_table is None:
            if self._memory_storage is None:
                self._memory_storage = _InMemoryFeatureStorage()
            self._memory_storage.upsert_feature(
                symbol=symbol,
                event_ts=event_ts,
                created_at=created_at,
                features=features,
            )
            self._persist_memory_snapshot()
            return
        stmt = pg_insert(microstructure_table).values(
            symbol=symbol,
            event_timestamp=event_ts,
            created_at=created_at,
            **features,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                microstructure_table.c.symbol,
                microstructure_table.c.event_timestamp,
            ],
            set_={
                "created_at": stmt.excluded.created_at,
                "rolling_vwap": stmt.excluded.rolling_vwap,
                "spread": stmt.excluded.spread,
                "order_book_imbalance": stmt.excluded.order_book_imbalance,
            },
        )
        with self.engine.begin() as connection:
            connection.execute(stmt)

    def _write_online(
        self,
        symbol: str,
        event_ts: dt.datetime,
        created_at: dt.datetime,
        features: Mapping[str, float],
    ) -> None:
        if self.store is None:
            LOGGER.debug("Feast not available; skipping online write")
            return
        if pd is None:
            LOGGER.warning("pandas is required for writing to the online store")
            return
        payload = {
            "symbol": symbol,
            "event_timestamp": event_ts,
            "created_ts": created_at,
            **features,
        }
        dataframe = pd.DataFrame([payload])
        self.store.write_to_online_store(self.feature_view, dataframe)

    def record_late_event(self, stream: str, event: OrderedEvent) -> None:
        symbol = str(event.payload.get("symbol", "UNKNOWN")).upper()
        payload_copy = dict(event.payload)
        payload_copy["late"] = True
        payload_copy["lateness_ms"] = event.lateness_ms
        payload_copy["stream"] = stream
        serialised = json.dumps(payload_copy, default=_json_default)
        if not self._offline_supported or late_events_table is None:
            if self._memory_storage is None:
                self._memory_storage = _InMemoryFeatureStorage()
            self._memory_storage.record_late_event(
                stream=stream,
                symbol=symbol,
                event_ts=event.event_ts,
                arrival_ts=event.arrival_ts,
                payload=serialised,
            )
            self._persist_late_events_snapshot()
            return
        stmt = pg_insert(late_events_table).values(
            stream=stream,
            symbol=symbol,
            event_timestamp=event.event_ts,
            arrival_timestamp=event.arrival_ts,
            payload=serialised,
        )
        stmt = stmt.on_conflict_do_nothing()
        with self.engine.begin() as connection:
            connection.execute(stmt)

    @property
    def memory_storage(self) -> _InMemoryFeatureStorage | None:
        return self._memory_storage

    def _persist_memory_snapshot(self) -> None:
        if not (_insecure_defaults_enabled() and self._state_dir and self._memory_storage):
            return
        snapshot = {
            "features": self._memory_storage.list_features(),
        }
        path = self._state_dir / "offline_features.json"
        path.write_text(json.dumps(snapshot, default=_json_default, indent=2), encoding="utf-8")

    def _persist_late_events_snapshot(self) -> None:
        if not (_insecure_defaults_enabled() and self._state_dir and self._memory_storage):
            return
        payload = {
            "late_events": self._memory_storage.list_late_events(),
        }
        path = self._state_dir / "late_events.json"
        path.write_text(json.dumps(payload, default=_json_default, indent=2), encoding="utf-8")


class MarketFeatureJob:
    """Consumes Kafka topics and emits engineered features."""

    def __init__(
        self,
        *,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        group_id: str = KAFKA_CONSUMER_GROUP,
        repo_path: str = FEATURE_REPO_PATH,
        feature_view: str = FEATURE_VIEW_NAME,
        window_seconds: int = ROLLING_WINDOW_SECONDS,
        book_levels: int = BOOK_LEVELS,
        engine: Engine | None = None,
        max_lateness_ms: int = MAX_EVENT_LATENESS_MS,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.repo_path = repo_path
        self.feature_view = feature_view
        self.window = dt.timedelta(seconds=window_seconds)
        self.book_levels = book_levels
        self.state: MutableMapping[str, FeatureState] = {}
        self._state_dir = _state_root()
        self._events_path = self._state_dir / "events.jsonl"
        self.engine = engine or create_engine(DATABASE_URL)
        self.max_lateness_ms = max_lateness_ms
        self.store = (
            FeatureStore(repo_path=repo_path)
            if FeatureStore is not None
            else None
        )
        self.writer = FeatureWriter(
            engine=self.engine,
            store=self.store,
            feature_view=self.feature_view,
            state_dir=self._state_dir,
        )
        # Normalise the engine reference in case the writer swapped to an in-memory
        # backend because SQLAlchemy was unavailable.
        self.engine = self.writer.engine
        self.service_name = "feature_jobs"
        self.trade_ordering = EventOrderingBuffer(
            stream_name="md.trades",
            max_lateness_ms=self.max_lateness_ms,
            timestamp_getter=self._timestamp_from_payload,
            key_getter=lambda payload: str(payload.get("symbol", "")).upper(),
            service_name=self.service_name,
        )
        self.book_ordering = EventOrderingBuffer(
            stream_name="md.book",
            max_lateness_ms=self.max_lateness_ms,
            timestamp_getter=self._timestamp_from_payload,
            key_getter=lambda payload: str(payload.get("symbol", "")).upper(),
            service_name=self.service_name,
        )
        self._late_events: Deque[OrderedEvent] = deque()

    def _state_for(self, symbol: str) -> FeatureState:
        state = self.state.get(symbol)
        if state is None:
            state = FeatureState(window=self.window, depth=self.book_levels)
            self.state[symbol] = state
        return state

    def _timestamp_from_payload(self, payload: Mapping[str, Any]) -> dt.datetime:
        return self._parse_timestamp(payload.get("event_ts") or payload.get("timestamp"))

    def _process_local_events(self) -> None:
        events = self._load_local_events()
        for payload in events:
            stream = str(payload.get("stream") or payload.get("type") or "md.trades")
            event_ts = self._timestamp_from_payload(payload)
            ordered = OrderedEvent(
                stream=stream,
                payload=payload,
                event_ts=event_ts,
                arrival_ts=event_ts,
                key=str(payload.get("symbol", "")).upper() or None,
                lateness_ms=0,
                is_late=False,
            )
            if stream == "md.book" or payload.get("type") == "book":
                self.process_book(ordered)
            else:
                self.process_trade(ordered)
        self._flush_ordering_buffers()

    def _load_local_events(self) -> List[Mapping[str, Any]]:
        events: List[Mapping[str, Any]] = []
        if self._events_path.exists():
            for line in self._events_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    LOGGER.warning("Skipping malformed local event line")
        if events:
            return events
        events = self._generate_synthetic_events()
        self._persist_local_events(events)
        return events

    def _persist_local_events(self, events: Iterable[Mapping[str, Any]]) -> None:
        lines = [json.dumps(event, default=_json_default) for event in events]
        self._events_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    def _generate_synthetic_events(self) -> List[Mapping[str, Any]]:
        now = dt.datetime.now(tz=dt.timezone.utc)
        events: List[Mapping[str, Any]] = []
        price = 20_000.0
        for offset in range(10):
            ts = now - dt.timedelta(minutes=10 - offset)
            price += 25.0
            trade = {
                "stream": "md.trades",
                "type": "trade",
                "symbol": "BTC/USD",
                "price": price,
                "quantity": 0.1 + offset * 0.01,
                "event_ts": ts.isoformat(),
            }
            book = {
                "stream": "md.book",
                "type": "book",
                "symbol": "BTC/USD",
                "bids": [[price - 30.0, 1.0 + offset * 0.05]],
                "asks": [[price + 30.0, 0.9 + offset * 0.05]],
                "event_ts": ts.isoformat(),
            }
            events.extend([trade, book])
        return events

    def process_trade(self, event: OrderedEvent) -> None:
        payload = event.payload
        symbol = str(payload["symbol"]).upper()
        price = float(payload["price"])
        quantity = float(payload.get("quantity", payload.get("size", 0.0)))
        timestamp = event.event_ts
        trade = TradeEvent(symbol=symbol, price=price, quantity=quantity, event_ts=timestamp)
        state = self._state_for(symbol)
        state.update_trade(trade)
        features = state.compute_features()
        self.writer.persist(symbol=symbol, event_ts=timestamp, feature_payload=features)

    def process_book(self, event: OrderedEvent) -> None:
        payload = event.payload
        symbol = str(payload["symbol"]).upper()
        timestamp = event.event_ts
        bids = self._parse_levels(payload.get("bids", []))
        asks = self._parse_levels(payload.get("asks", []))
        book = BookEvent(symbol=symbol, bids=bids, asks=asks, event_ts=timestamp)
        state = self._state_for(symbol)
        state.update_book(book)
        features = state.compute_features()
        self.writer.persist(symbol=symbol, event_ts=timestamp, feature_payload=features)

    def _handle_late_trade(self, event: OrderedEvent) -> None:
        self._record_late_event(event)

    def _handle_late_book(self, event: OrderedEvent) -> None:
        self._record_late_event(event)

    def _record_late_event(self, event: OrderedEvent) -> None:
        payload_symbol = str(event.payload.get("symbol", "UNKNOWN")).upper()
        LOGGER.warning(
            "Late event routed to compensating path",
            extra={
                "stream": event.stream,
                "symbol": payload_symbol,
                "event_ts": event.event_ts.isoformat(),
                "lateness_ms": event.lateness_ms,
            },
        )
        self.writer.record_late_event(event.stream, event)
        self._late_events.append(event)

    def _flush_ordering_buffers(self) -> None:
        for event in self.trade_ordering.drain(force=True):
            self.process_trade(event)
        for event in self.book_ordering.drain(force=True):
            self.process_book(event)

    async def run(self) -> None:
        if AIOKafkaConsumer is None:
            if not _insecure_defaults_enabled():
                raise RuntimeError("aiokafka is required to run the streaming feature job")
            LOGGER.warning(
                "aiokafka missing; replaying local feature events from %s",
                self._events_path,
            )
            self._process_local_events()
            return
        consumer = AIOKafkaConsumer(
            "md.trades",
            "md.book",
            bootstrap_servers=self.bootstrap_servers,
            enable_auto_commit=False,
            group_id=self.group_id,
            value_deserializer=lambda data: json.loads(data.decode("utf-8")),
        )
        await consumer.start()
        LOGGER.info(
            "Started market feature job",
            extra={"bootstrap": self.bootstrap_servers, "group_id": self.group_id},
        )
        try:
            async for message in consumer:
                try:
                    if message.topic == "md.trades":
                        ready, late = self.trade_ordering.add(message.value)
                        for event in ready:
                            self.process_trade(event)
                        for event in late:
                            self._handle_late_trade(event)
                    elif message.topic == "md.book":
                        ready, late = self.book_ordering.add(message.value)
                        for event in ready:
                            self.process_book(event)
                        for event in late:
                            self._handle_late_book(event)
                    await consumer.commit()
                except Exception as exc:  # pragma: no cover - defensive logging
                    LOGGER.exception("Failed to process message", exc_info=exc)
        finally:
            self._flush_ordering_buffers()
            await consumer.stop()
            LOGGER.info("Market feature job stopped")

    @staticmethod
    def _parse_levels(levels: Iterable[Iterable[Any]]) -> List[BookLevel]:
        parsed: List[BookLevel] = []
        for level in levels:
            price, quantity = level
            parsed.append(BookLevel(price=float(price), quantity=float(quantity)))
        return parsed

    @staticmethod
    def _parse_timestamp(value: Any) -> dt.datetime:
        if isinstance(value, dt.datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=dt.timezone.utc)
            return value.astimezone(dt.timezone.utc)
        if isinstance(value, (int, float)):
            return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
        if isinstance(value, str):
            try:
                parsed = dt.datetime.fromisoformat(value)
            except ValueError:
                parsed = dt.datetime.utcfromtimestamp(float(value))
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            else:
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=dt.timezone.utc)
                else:
                    parsed = parsed.astimezone(dt.timezone.utc)
            return parsed
        return dt.datetime.now(tz=dt.timezone.utc)


def register_feature_definitions(repo_path: str | None = None) -> None:
    """Registers market microstructure feature views with Feast."""

    if FeatureStore is None:
        raise RuntimeError("Feast is required to register feature definitions")

    from data.feast.microstructure import (  # pylint: disable=import-outside-toplevel
        market_microstructure_feature_service,
        market_microstructure_feature_view,
        market_symbol,
    )

    store = FeatureStore(repo_path=repo_path or FEATURE_REPO_PATH)
    store.apply([
        market_symbol,
        market_microstructure_feature_view,
        market_microstructure_feature_service,
    ])


def materialize_features(repo_path: str | None = None) -> None:
    if FeatureStore is None:
        raise RuntimeError("Feast is required to materialize features")
    store = FeatureStore(repo_path=repo_path or FEATURE_REPO_PATH)
    end_date = dt.datetime.now(tz=dt.timezone.utc)
    store.materialize_incremental(end_date=end_date)


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market feature streaming jobs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the streaming job")
    run_parser.add_argument("--repo", default=FEATURE_REPO_PATH, help="Feast repo path")
    run_parser.add_argument(
        "--bootstrap", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers"
    )
    run_parser.add_argument(
        "--group", default=KAFKA_CONSUMER_GROUP, help="Kafka consumer group"
    )
    run_parser.add_argument(
        "--max-lateness-ms",
        type=int,
        default=MAX_EVENT_LATENESS_MS,
        help="Maximum tolerated event lateness before routing to compensation",
    )

    register_parser = subparsers.add_parser(
        "register", help="Register feature definitions with Feast"
    )
    register_parser.add_argument(
        "--repo", default=FEATURE_REPO_PATH, help="Feast repo path"
    )

    materialize_parser = subparsers.add_parser(
        "materialize", help="Materialize features from offline to online"
    )
    materialize_parser.add_argument(
        "--repo", default=FEATURE_REPO_PATH, help="Feast repo path"
    )

    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)
    if args.command == "run":
        job = MarketFeatureJob(
            bootstrap_servers=args.bootstrap,
            group_id=args.group,
            repo_path=args.repo,
            max_lateness_ms=args.max_lateness_ms,
        )
        asyncio.run(job.run())
    elif args.command == "register":
        register_feature_definitions(repo_path=args.repo)
    elif args.command == "materialize":
        materialize_features(repo_path=args.repo)


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()

