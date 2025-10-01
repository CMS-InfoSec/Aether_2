
"""Async consumers for Kraken WebSocket market data."""
from __future__ import annotations


import asyncio
import datetime as dt
import json
import logging

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence

import aiohttp
from sqlalchemy import BigInteger, Column, DateTime, JSON, MetaData, Numeric, String, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine, create_engine

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

KRAKEN_WS_URL = os.getenv("KRAKEN_WS_URL", "wss://ws.kraken.com")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "kraken.marketdata")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://localhost:5432/aether")
NATS_SERVERS = os.getenv("NATS_SERVERS", "nats://localhost:4222").split(",")
NATS_SUBJECT = os.getenv("NATS_SUBJECT", "marketdata.kraken.orderbook")

metadata = MetaData()
order_book_table = Table(
    "order_book_events",
    metadata,
    Column("market", String, primary_key=True),
    Column("event_time", DateTime(timezone=True), primary_key=True),
    Column("side", String, nullable=False),
    Column("price", Numeric, nullable=False),
    Column("size", Numeric, nullable=False),
    Column("event_type", String, nullable=False),
    Column("sequence", BigInteger, nullable=False),
    Column("raw", JSON, nullable=False),
)


@dataclass
class OrderBookUpdate:
    market: str
    event_time: dt.datetime
    side: str
    price: float
    size: float
    sequence: int
    raw: Dict[str, Any]


def kafka_producer() -> Producer | None:
    if Producer is None:
        LOGGER.warning("Kafka producer not available; messages will not be published")
        return None
    return Producer({"bootstrap.servers": KAFKA_BROKERS})


async def subscribe(session: aiohttp.ClientSession, pairs: Sequence[str]) -> aiohttp.ClientWebSocketResponse:
    LOGGER.info("Connecting to Kraken WebSocket", extra={"url": KRAKEN_WS_URL, "pairs": pairs})
    ws = await session.ws_connect(KRAKEN_WS_URL)
    subscribe_message = {
        "event": "subscribe",
        "pair": list(pairs),
        "subscription": {"name": "book", "depth": 25},
    }
    await ws.send_str(json.dumps(subscribe_message))
    return ws


def flatten_updates(market: str, payload: Dict[str, Any]) -> List[OrderBookUpdate]:
    # Kraken payloads contain snapshots ('as'/'bs') and updates ('a'/'b').
    bids = payload.get('b') or payload.get('bs') or []
    asks = payload.get('a') or payload.get('as') or []
    timestamp = payload.get('timestamp')
    if not timestamp and bids:
        timestamp = bids[0][2] if len(bids[0]) > 2 else None
    if not timestamp and asks:
        timestamp = asks[0][2] if len(asks[0]) > 2 else None
    if timestamp is None:
        return []
    event_time = datetime_from_kraken(timestamp)
    sequence = int(payload.get('sequence', payload.get('checksum', 0)) or 0)
    updates: List[OrderBookUpdate] = []
    for side, levels in (('buy', bids), ('sell', asks)):
        for level in levels:
            price = float(level[0])
            size = float(level[1]) if len(level) > 1 else 0.0
            updates.append(
                OrderBookUpdate(
                    market=market,
                    event_time=event_time,
                    side=side,
                    price=price,
                    size=size,
                    sequence=sequence,
                    raw={'payload': payload, 'side': side, 'price': price, 'size': size, 'sequence': sequence},
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


def persist_updates(engine: Engine, updates: Sequence[OrderBookUpdate]) -> None:
    if not updates:
        return
    with engine.begin() as connection:
        for update in updates:
            record = {
                "market": update.market,
                "event_time": update.event_time,
                "side": update.side,
                "price": update.price,
                "size": update.size,
                "event_type": "book",
                "sequence": update.sequence,
                "raw": update.raw,
            }
            stmt = pg_insert(order_book_table).values(**record)
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    order_book_table.c.market,
                    order_book_table.c.event_time,
                    order_book_table.c.sequence,
                ],
                set_={"raw": stmt.excluded.raw, "size": stmt.excluded.size, "price": stmt.excluded.price},
            )
            connection.execute(stmt)
    LOGGER.info("Persisted Kraken updates", extra={"count": len(updates)})


def publish_updates(producer: Producer | None, updates: Sequence[OrderBookUpdate]) -> None:
    if producer is None:
        return
    for update in updates:
        payload = json.dumps(
            {
                "market": update.market,
                "event_time": update.event_time.isoformat(),
                "raw": update.raw,
            }
        ).encode("utf-8")
        producer.produce(KAFKA_TOPIC, payload)
    producer.flush()


async def publish_to_nats(updates: Sequence[OrderBookUpdate]) -> None:
    if NATS is None or not updates:
        return
    client = NATS()
    await client.connect(servers=NATS_SERVERS)
    try:
        for update in updates:
            payload = json.dumps(
                {
                    "market": update.market,
                    "event_time": update.event_time.isoformat(),
                    "raw": update.raw,
                }
            ).encode("utf-8")
            await client.publish(NATS_SUBJECT, payload)
    finally:
        await client.drain()


async def consume(pairs: Sequence[str]) -> None:
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

