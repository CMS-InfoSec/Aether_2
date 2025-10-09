
"""Async consumers for Kraken WebSocket market data."""
from __future__ import annotations

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging

import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence


class MissingDependencyError(RuntimeError):
    """Raised when optional ingest dependencies are unavailable."""


try:  # pragma: no cover - optional dependency
    import aiohttp
except Exception as exc:  # pragma: no cover - executed when aiohttp missing
    aiohttp = None  # type: ignore[assignment]
    _AIOHTTP_IMPORT_ERROR = exc
else:
    _AIOHTTP_IMPORT_ERROR = None

_SQLALCHEMY_AVAILABLE = True
_SQLALCHEMY_IMPORT_ERROR: Exception | None = None

try:  # pragma: no cover - optional dependency
    from sqlalchemy import BigInteger, Column, DateTime, JSON, MetaData, Numeric, String, Table
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.engine import Engine, create_engine
except Exception as exc:  # pragma: no cover - executed when SQLAlchemy absent
    _SQLALCHEMY_AVAILABLE = False
    _SQLALCHEMY_IMPORT_ERROR = exc
else:
    if not hasattr(Table, "c"):
        _SQLALCHEMY_AVAILABLE = False
        _SQLALCHEMY_IMPORT_ERROR = RuntimeError("SQLAlchemy table metadata is unavailable")

if not _SQLALCHEMY_AVAILABLE:
    BigInteger = Column = DateTime = JSON = Numeric = String = Table = None  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]

    def create_engine(*_: object, **__: object) -> None:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for Kraken ingest") from _SQLALCHEMY_IMPORT_ERROR

    def pg_insert(*_: object, **__: object) -> None:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for Kraken ingest") from _SQLALCHEMY_IMPORT_ERROR

    metadata = None
    orderbook_events_table = None
else:
    metadata = MetaData()
    orderbook_events_table = Table(
        "orderbook_events",
        metadata,
        Column("symbol", String(32), primary_key=True),
        Column("ts", DateTime(timezone=True), primary_key=True),
        Column("side", String(4), primary_key=True),
        Column("price", Numeric(28, 10), primary_key=True),
        Column("size", Numeric(28, 10), nullable=False),
        Column("action", String(16), nullable=False),
        Column("sequence", BigInteger, nullable=True),
        Column("meta", JSON, nullable=True),
    )

from shared.postgres import normalize_sqlalchemy_dsn

try:
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover - optional dependency
    Producer = None  # type: ignore

try:
    from nats.aio.client import Client as NATS
except ImportError:  # pragma: no cover - optional dependency
    NATS = None  # type: ignore

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _require_aiohttp() -> None:
    if aiohttp is None:  # pragma: no cover - executed when aiohttp missing
        raise MissingDependencyError("aiohttp is required for Kraken ingest") from _AIOHTTP_IMPORT_ERROR


def _require_sqlalchemy() -> None:
    if not _SQLALCHEMY_AVAILABLE or metadata is None or orderbook_events_table is None:
        raise MissingDependencyError("SQLAlchemy is required for Kraken ingest") from _SQLALCHEMY_IMPORT_ERROR


KRAKEN_WS_URL = os.getenv("KRAKEN_WS_URL", "wss://ws.kraken.com")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "kraken.marketdata")
_SQLITE_FALLBACK_FLAG = "KRAKEN_WS_ALLOW_SQLITE_FOR_TESTS"


def _resolve_database_url() -> str:
    """Return the configured Timescale/PostgreSQL DSN used for persistence."""

    raw_url = os.getenv("DATABASE_URL", "")
    if not raw_url.strip():
        raise RuntimeError(
            "Kraken ingest requires DATABASE_URL to be set to a PostgreSQL/Timescale DSN."
        )

    allow_sqlite = "pytest" in sys.modules or os.getenv(_SQLITE_FALLBACK_FLAG) == "1"
    database_url = normalize_sqlalchemy_dsn(
        raw_url.strip(),
        allow_sqlite=allow_sqlite,
        label="Kraken ingest database URL",
    )

    if database_url.startswith("sqlite"):
        LOGGER.warning(
            "Using SQLite database '%s' for Kraken ingest; allowed only for tests.",
            database_url,
        )

    return database_url


DATABASE_URL = _resolve_database_url()
NATS_SERVERS = os.getenv("NATS_SERVERS", "nats://localhost:4222").split(",")
NATS_SUBJECT = os.getenv("NATS_SUBJECT", "marketdata.kraken.orderbook")


@dataclass
class OrderBookEvent:
    symbol: str
    ts: dt.datetime
    side: str
    price: float
    size: float
    action: str
    sequence: int | None
    meta: Dict[str, Any]


def kafka_producer() -> Producer | None:
    if Producer is None:
        LOGGER.warning("Kafka producer not available; messages will not be published")
        return None
    return Producer({"bootstrap.servers": KAFKA_BROKERS})


async def subscribe(session: aiohttp.ClientSession, pairs: Sequence[str]) -> aiohttp.ClientWebSocketResponse:
    _require_aiohttp()
    LOGGER.info("Connecting to Kraken WebSocket", extra={"url": KRAKEN_WS_URL, "pairs": pairs})
    ws = await session.ws_connect(KRAKEN_WS_URL)
    subscribe_message = {
        "event": "subscribe",
        "pair": list(pairs),
        "subscription": {"name": "book", "depth": 25},
    }
    await ws.send_str(json.dumps(subscribe_message))
    return ws


def flatten_updates(symbol: str, payload: Dict[str, Any]) -> List[OrderBookEvent]:
    """Normalise Kraken order book payloads into orderbook_events rows."""
    bids = payload.get("b") or payload.get("bs") or []
    asks = payload.get("a") or payload.get("as") or []
    timestamp = payload.get('timestamp')
    if not timestamp and bids:
        timestamp = bids[0][2] if len(bids[0]) > 2 else None
    if not timestamp and asks:
        timestamp = asks[0][2] if len(asks[0]) > 2 else None
    if timestamp is None:
        return []
    event_time = datetime_from_kraken(timestamp)
    sequence_value = payload.get("sequence", payload.get("checksum"))
    sequence = int(sequence_value) if sequence_value not in (None, "") else None
    action = "snapshot" if any(key in payload for key in ("as", "bs")) else "update"
    updates: List[OrderBookEvent] = []
    for side, levels in (("bid", bids), ("ask", asks)):
        for level in levels:
            price = float(level[0])
            size = float(level[1]) if len(level) > 1 else 0.0
            updates.append(
                OrderBookEvent(
                    symbol=symbol,
                    ts=event_time,
                    side=side,
                    price=price,
                    size=size,
                    action=action,
                    sequence=sequence,
                    meta={"payload": payload, "side": side, "price": price, "size": size},
                )
            )
    return updates


def datetime_from_kraken(timestamp: Any) -> dt.datetime:
    from datetime import datetime, timezone

    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
    if isinstance(timestamp, str):
        try:
            return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
        except ValueError:
            dt_obj = datetime.fromisoformat(timestamp)
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            return dt_obj
    dt_obj = datetime.fromisoformat(str(timestamp))
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return dt_obj


def persist_updates(engine: Engine, updates: Sequence[OrderBookEvent]) -> None:
    _require_sqlalchemy()
    if not updates:
        return
    with engine.begin() as connection:
        for update in updates:
            record = {
                "symbol": update.symbol,
                "ts": update.ts,
                "side": update.side,
                "price": update.price,
                "size": update.size,
                "action": update.action,
                "sequence": update.sequence,
                "meta": update.meta,
            }
            stmt = pg_insert(orderbook_events_table).values(**record)
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    orderbook_events_table.c.symbol,
                    orderbook_events_table.c.ts,
                    orderbook_events_table.c.side,
                    orderbook_events_table.c.price,
                ],
                set_={
                    "size": stmt.excluded.size,
                    "action": stmt.excluded.action,
                    "sequence": stmt.excluded.sequence,
                    "meta": stmt.excluded.meta,
                },
            )
            connection.execute(stmt)


def publish_updates(producer: Producer | None, updates: Sequence[OrderBookEvent]) -> None:
    if producer is None:
        return
    for update in updates:
        payload = json.dumps(
            {
                "symbol": update.symbol,
                "ts": update.ts.isoformat(),
                "side": update.side,
                "price": update.price,
                "size": update.size,
                "action": update.action,
                "sequence": update.sequence,
                "meta": update.meta,
            }
        ).encode("utf-8")
        producer.produce(KAFKA_TOPIC, payload)
    producer.flush()


async def publish_to_nats(updates: Sequence[OrderBookEvent]) -> None:
    if NATS is None or not updates:
        return
    client = NATS()
    await client.connect(servers=NATS_SERVERS)
    try:
        for update in updates:
            payload = json.dumps(
                {
                    "symbol": update.symbol,
                    "ts": update.ts.isoformat(),
                    "side": update.side,
                    "price": update.price,
                    "size": update.size,
                    "action": update.action,
                    "sequence": update.sequence,
                    "meta": update.meta,
                }
            ).encode("utf-8")
            await client.publish(NATS_SUBJECT, payload)
    finally:
        await client.drain()


async def consume(pairs: Sequence[str]) -> None:
    _require_sqlalchemy()
    _require_aiohttp()
    engine = create_engine(DATABASE_URL, future=True)
    producer = kafka_producer()
    async with aiohttp.ClientSession() as session:
        ws = await subscribe(session, pairs)
        async for message in ws:
            if message.type != aiohttp.WSMsgType.TEXT:
                continue
            data = json.loads(message.data)
            if isinstance(data, dict) and data.get("event") == "subscriptionStatus":
                LOGGER.info("Subscription update", extra=data)
                continue
            if not isinstance(data, list) or len(data) < 4:
                continue
            channel_data = data[1]
            if not isinstance(channel_data, dict):
                continue
            market = data[3]
            updates = flatten_updates(market, channel_data)
            if not updates:
                continue
            persist_updates(engine, updates)
            publish_updates(producer, updates)
            await publish_to_nats(updates)


def main() -> None:
    pairs = os.getenv("KRAKEN_PAIRS", "BTC/USD,ETH/USD").split(",")
    asyncio.run(consume(pairs))


if __name__ == "__main__":
    main()

