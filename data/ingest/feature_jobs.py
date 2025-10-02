"""Streaming jobs that populate Feast feature views from market data."""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import importlib
import json
import logging
import os
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Iterable, List, Mapping, MutableMapping, Optional

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

from sqlalchemy import Column, DateTime, Float, MetaData, String, Table, Text, create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from services.ingest import EventOrderingBuffer, OrderedEvent

LOGGER = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_CONSUMER_GROUP = os.getenv("FEATURE_JOB_CONSUMER_GROUP", "aether-feature-jobs")
FEATURE_REPO_PATH = os.getenv("FEATURE_REPO_PATH", os.path.join("data", "feast"))
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+psycopg2://aether:aether@localhost:5432/aether"
)
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
    ) -> None:
        self.engine = engine
        self.store = store
        self.feature_view = feature_view
        metadata.create_all(
            engine, tables=[microstructure_table, late_events_table]
        )

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
        )
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
            raise RuntimeError("aiokafka is required to run the streaming feature job")
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

