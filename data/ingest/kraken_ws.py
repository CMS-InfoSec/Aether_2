"""Streaming ingestion from Kraken's public WebSocket API.

The consumer multiplexes trades and level-2 order-book updates into Kafka or
NATS topics and optionally archives periodic snapshots into TimescaleDB.

The implementation embraces asyncio to handle high-throughput real-time data
while remaining dependency-light. Downstream systems can choose either Kafka or
NATS by providing the relevant connection URLs.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

import websockets

logger = logging.getLogger(__name__)

KRAKEN_WS_URL = "wss://ws.kraken.com"
TRADES_TOPIC = "md.trades"
BOOK_TOPIC = "md.book"
SNAPSHOT_DEPTH = 10


@dataclass
class PublisherConfig:
    kafka_bootstrap: Optional[str] = None
    nats_url: Optional[str] = None

    def validate(self) -> None:
        if not self.kafka_bootstrap and not self.nats_url:
            raise ValueError("At least one of kafka_bootstrap or nats_url must be provided")


class BasePublisher:
    async def start(self) -> None:  # pragma: no cover - interface
        return None

    async def stop(self) -> None:  # pragma: no cover - interface
        return None

    async def publish(self, topic: str, payload: Dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class KafkaPublisher(BasePublisher):
    def __init__(self, bootstrap_servers: str) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        await self._producer.send_and_wait(topic, json.dumps(payload).encode("utf-8"))


class NatsPublisher(BasePublisher):
    def __init__(self, url: str) -> None:
        from nats.aio.client import Client as NATS

        self._client = NATS()
        self._url = url

    async def start(self) -> None:
        await self._client.connect(servers=[self._url])

    async def stop(self) -> None:
        await self._client.close()

    async def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        await self._client.publish(topic, json.dumps(payload).encode("utf-8"))


class TimescaleArchiver:
    def __init__(self, dsn: str) -> None:
        import asyncpg

        self._dsn = dsn
        self._pool: Optional[asyncpg.pool.Pool] = None

    async def start(self) -> None:
        import asyncpg

        self._pool = await asyncpg.create_pool(self._dsn)

    async def stop(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def archive_book(self, symbol: str, snapshot: Dict[str, Any]) -> None:
        if not self._pool:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO orderbook_snapshots(symbol, depth, as_of, bids, asks)
                VALUES($1, $2, $3, $4, $5)
                ON CONFLICT (symbol, depth, as_of) DO NOTHING
                """,
                symbol,
                SNAPSHOT_DEPTH,
                snapshot["as_of"],
                json.dumps(snapshot["bids"]),
                json.dumps(snapshot["asks"]),
            )


class KrakenIngestor:
    def __init__(
        self,
        symbols: Sequence[str],
        publisher: BasePublisher,
        archiver: Optional[TimescaleArchiver] = None,
        snapshot_interval: int = 60,
    ) -> None:
        self.symbols = symbols
        self.publisher = publisher
        self.archiver = archiver
        self.snapshot_interval = snapshot_interval
        self._last_snapshot: Dict[str, dt.datetime] = {}

    async def run(self) -> None:
        subscribe_msg = {
            "event": "subscribe",
            "pair": list(self.symbols),
            "subscription": {"name": "book", "depth": SNAPSHOT_DEPTH},
        }
        subscribe_trades = {
            "event": "subscribe",
            "pair": list(self.symbols),
            "subscription": {"name": "trade"},
        }
        async with websockets.connect(KRAKEN_WS_URL, ping_interval=20) as ws:
            await ws.send(json.dumps(subscribe_msg))
            await ws.send(json.dumps(subscribe_trades))
            logger.info("Subscribed to Kraken order book and trade streams: %s", ", ".join(self.symbols))
            async for raw in ws:
                await self._handle_message(raw)

    async def _handle_message(self, raw: str) -> None:
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("Ignoring non-JSON message: %s", raw)
            return

        if isinstance(message, dict):
            if message.get("event") == "heartbeat":
                return
            if message.get("event") == "systemStatus":
                logger.info("Kraken WS status: %s", message)
            elif message.get("event") == "subscriptionStatus":
                logger.info("Subscription update: %s", message)
            else:
                logger.debug("Unhandled control message: %s", message)
            return

        if not isinstance(message, list) or len(message) < 4:
            logger.debug("Unexpected payload: %s", message)
            return

        _, payload, channel_name, pair = message[0], message[1], message[2], message[3]
        symbol = pair.replace("/", "")
        if channel_name.startswith("trade"):
            await self._handle_trades(symbol, payload)
        elif channel_name.startswith("book"):
            await self._handle_book(symbol, payload)
        else:
            logger.debug("Unknown channel %s", channel_name)

    async def _handle_trades(self, symbol: str, trades: List[List[str]]) -> None:
        for trade in trades:
            price, volume, time, side, order_type, misc = trade
            event = {
                "symbol": symbol,
                "price": float(price),
                "volume": float(volume),
                "ts": dt.datetime.fromtimestamp(float(time), tz=dt.timezone.utc).isoformat(),
                "side": side,
                "type": order_type,
                "misc": misc,
            }
            await self.publisher.publish(TRADES_TOPIC, event)

    async def _handle_book(self, symbol: str, data: Dict[str, Any]) -> None:
        book_event = {
            "symbol": symbol,
            "ts": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "payload": data,
        }
        await self.publisher.publish(BOOK_TOPIC, book_event)

        now = dt.datetime.now(tz=dt.timezone.utc)
        last_snapshot = self._last_snapshot.get(symbol)
        if self.archiver and (last_snapshot is None or (now - last_snapshot).total_seconds() >= self.snapshot_interval):
            bids = data.get("b", data.get("bs", []))
            asks = data.get("a", data.get("as", []))
            snapshot = {
                "symbol": symbol,
                "as_of": now,
                "bids": bids,
                "asks": asks,
            }
            await self.archiver.archive_book(symbol, snapshot)
            self._last_snapshot[symbol] = now


async def build_publisher(config: PublisherConfig) -> BasePublisher:
    publishers: List[BasePublisher] = []
    if config.kafka_bootstrap:
        publishers.append(KafkaPublisher(config.kafka_bootstrap))
    if config.nats_url:
        publishers.append(NatsPublisher(config.nats_url))

    if len(publishers) == 1:
        publisher = publishers[0]
    else:
        publisher = MultiPublisher(publishers)
    await publisher.start()
    return publisher


class MultiPublisher(BasePublisher):
    def __init__(self, publishers: Iterable[BasePublisher]) -> None:
        self.publishers = list(publishers)

    async def start(self) -> None:
        for publisher in self.publishers:
            await publisher.start()

    async def stop(self) -> None:
        for publisher in self.publishers:
            await publisher.stop()

    async def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        await asyncio.gather(*(publisher.publish(topic, payload) for publisher in self.publishers))


async def run_ingestor(args: argparse.Namespace) -> None:
    publisher_config = PublisherConfig(args.kafka_bootstrap, args.nats_url)
    publisher_config.validate()

    publisher = await build_publisher(publisher_config)
    archiver = TimescaleArchiver(args.database_url) if args.database_url else None
    if archiver:
        await archiver.start()

    ingestor = KrakenIngestor(args.symbols, publisher, archiver, args.snapshot_interval)

    try:
        await ingestor.run()
    finally:
        await publisher.stop()
        if archiver:
            await archiver.stop()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Kraken market data into Kafka/NATS and TimescaleDB")
    parser.add_argument("symbols", nargs="+", help="Trading pairs in Kraken format (e.g. BTC/USD)")
    parser.add_argument("--kafka-bootstrap", dest="kafka_bootstrap", help="Kafka bootstrap servers, comma separated")
    parser.add_argument("--nats-url", dest="nats_url", help="NATS connection URL")
    parser.add_argument("--database-url", help="TimescaleDB DSN for snapshot archiving")
    parser.add_argument("--snapshot-interval", type=int, default=60, help="Seconds between archived snapshots per symbol")
    parser.add_argument("--log-level", default="INFO", help="Python logging level")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    asyncio.run(run_ingestor(args))


if __name__ == "__main__":
    main()
