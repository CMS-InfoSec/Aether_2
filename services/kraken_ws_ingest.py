"""Kraken WebSocket market data ingestor.

This module connects to the public Kraken WebSocket API, normalizes trade and
order book events, and publishes them to Kafka topics using aiokafka.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import websockets
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError


KRAKEN_WS_URL = "wss://ws.kraken.com"
DEFAULT_TRADE_TOPIC = "md.trades"
DEFAULT_BOOK_TOPIC = "md.book"
HEARTBEAT_TIMEOUT_SECONDS = 30.0
HEARTBEAT_CHECK_INTERVAL_SECONDS = 5.0
RECONNECT_DELAY_SECONDS = 5.0


class HeartbeatTimeout(RuntimeError):
    """Raised when Kraken heartbeats stop flowing."""


@dataclass
class KrakenConfig:
    pairs: List[str]
    kafka_bootstrap_servers: str
    trade_topic: str = DEFAULT_TRADE_TOPIC
    book_topic: str = DEFAULT_BOOK_TOPIC
    book_depth: Optional[int] = None
    reconnect_delay: float = RECONNECT_DELAY_SECONDS
    heartbeat_timeout: float = HEARTBEAT_TIMEOUT_SECONDS
    heartbeat_check_interval: float = HEARTBEAT_CHECK_INTERVAL_SECONDS


class KrakenIngestor:
    """Ingests Kraken market data via WebSocket and publishes to Kafka."""

    def __init__(self, config: KrakenConfig) -> None:
        self._config = config
        self._producer: Optional[AIOKafkaProducer] = None
        self._running = False
        self._last_heartbeat_ts = time.monotonic()
        self._active_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._active_tasks: set[asyncio.Task[Any]] = set()

    async def run(self) -> None:
        """Entrypoint for the ingestion loop with reconnection handling."""
        if not self._config.pairs:
            raise ValueError("At least one trading pair must be provided.")

        self._running = True
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._config.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        await self._producer.start()
        logging.info("Kafka producer started")

        try:
            while self._running:
                try:
                    await self._connect_and_consume()
                except asyncio.CancelledError:
                    raise
                except HeartbeatTimeout as exc:
                    logging.warning("Heartbeat timeout detected: %s", exc)
                except websockets.WebSocketException as exc:
                    logging.error("WebSocket error: %s", exc)
                except KafkaError as exc:
                    logging.error("Kafka error: %s", exc)
                except Exception:
                    logging.exception("Unexpected error in ingestion loop")

                if self._running:
                    logging.info(
                        "Reconnecting to Kraken in %.1f seconds",
                        self._config.reconnect_delay,
                    )
                    await asyncio.sleep(self._config.reconnect_delay)
        finally:
            await self._producer.stop()
            logging.info("Kafka producer stopped")

    async def stop(self) -> None:
        """Request graceful shutdown."""
        self._running = False

        if self._active_ws is not None:
            try:
                await self._active_ws.close()
            finally:
                self._active_ws = None

        if self._active_tasks:
            for task in list(self._active_tasks):
                task.cancel()
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
            self._active_tasks.clear()

    async def _connect_and_consume(self) -> None:
        """Connect to Kraken and stream messages until disconnect."""
        logging.info("Connecting to Kraken WebSocket at %s", KRAKEN_WS_URL)

        async with websockets.connect(KRAKEN_WS_URL, ping_interval=None) as ws:
            self._active_ws = ws
            await self._subscribe(ws)
            self._last_heartbeat_ts = time.monotonic()

            consumer_task = asyncio.create_task(self._consume(ws))
            heartbeat_task = asyncio.create_task(self._heartbeat_guard(ws))

            self._active_tasks.update({consumer_task, heartbeat_task})

            done, pending = await asyncio.wait(
                {consumer_task, heartbeat_task},
                return_when=asyncio.FIRST_EXCEPTION,
            )

            for task in pending:
                task.cancel()

            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            for task in done:
                try:
                    await task
                except asyncio.CancelledError:
                    continue
                finally:
                    self._active_tasks.discard(task)

            for task in pending:
                self._active_tasks.discard(task)

        self._active_ws = None

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Send subscription requests for trades and order books."""
        pairs = self._config.pairs
        logging.info("Subscribing to pairs: %s", ", ".join(pairs))

        trade_payload = {
            "event": "subscribe",
            "pair": pairs,
            "subscription": {"name": "trade"},
        }
        await ws.send(json.dumps(trade_payload))

        book_subscription: Dict[str, Any] = {"name": "book"}
        if self._config.book_depth:
            book_subscription["depth"] = self._config.book_depth

        book_payload = {
            "event": "subscribe",
            "pair": pairs,
            "subscription": book_subscription,
        }
        await ws.send(json.dumps(book_payload))

    async def _consume(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Consume Kraken messages and publish to Kafka."""
        assert self._producer is not None

        async for raw_message in ws:
            self._last_heartbeat_ts = time.monotonic()

            try:
                message = json.loads(raw_message)
            except json.JSONDecodeError:
                logging.debug("Skipping non-JSON message: %s", raw_message)
                continue

            if isinstance(message, dict):
                await self._handle_event_message(message)
                continue

            if not isinstance(message, list) or len(message) < 2:
                logging.debug("Skipping unexpected message format: %s", message)
                continue

            channel_name, pair = self._extract_channel_and_pair(message)
            if not pair:
                logging.debug("Unable to determine pair from message: %s", message)
                continue

            if channel_name and "trade" in channel_name:
                await self._handle_trade_message(message[1], pair)
            elif channel_name and "book" in channel_name:
                await self._handle_book_message(message[1], pair)
            else:
                # Some order book messages omit the channel name suffix; infer via payload type.
                payload = message[1]
                if isinstance(payload, list):
                    await self._handle_trade_message(payload, pair)
                elif isinstance(payload, dict):
                    await self._handle_book_message(payload, pair)
                else:
                    logging.debug("Unhandled message payload: %s", message)

    async def _handle_event_message(self, message: Dict[str, Any]) -> None:
        event_type = message.get("event")
        if event_type == "heartbeat":
            logging.debug("Received heartbeat")
            return

        if event_type == "subscriptionStatus":
            status = message.get("status")
            channel = message.get("subscription", {}).get("name")
            pair = message.get("pair")
            if status == "subscribed":
                logging.info("Subscribed to %s for %s", channel, pair)
            else:
                logging.warning("Subscription update: %s", message)
            return

        if event_type == "error":
            raise RuntimeError(f"Kraken error event: {message}")

        logging.debug("Unhandled event message: %s", message)

    async def _handle_trade_message(
        self, trades: Iterable[List[Any]], pair: str
    ) -> None:
        assert self._producer is not None
        for trade in trades:
            try:
                price = float(trade[0])
                size = float(trade[1])
                timestamp = float(trade[2])
                side = "buy" if trade[3] == "b" else "sell"
            except (ValueError, TypeError, IndexError) as exc:
                logging.debug("Skipping malformed trade: %s (%s)", trade, exc)
                continue

            normalized = {
                "symbol": pair,
                "ts": timestamp,
                "side": side,
                "price": price,
                "size": size,
                "type": "trade",
            }
            await self._send_to_kafka(self._config.trade_topic, normalized)

    async def _handle_book_message(
        self, book_payload: Dict[str, Any], pair: str
    ) -> None:
        assert self._producer is not None

        if not isinstance(book_payload, dict):
            logging.debug("Unexpected book payload type: %s", book_payload)
            return

        snapshots = {"as": "ask", "bs": "bid"}
        updates = {"a": "ask", "b": "bid"}

        for key, side in snapshots.items():
            levels = book_payload.get(key)
            if levels:
                await self._publish_book_levels(levels, side, pair)

        for key, side in updates.items():
            levels = book_payload.get(key)
            if levels:
                await self._publish_book_levels(levels, side, pair)

    async def _publish_book_levels(
        self, levels: Iterable[List[Any]], side: str, pair: str
    ) -> None:
        for level in levels:
            try:
                price = float(level[0])
                size = float(level[1])
                timestamp = float(level[2]) if len(level) > 2 else time.time()
            except (ValueError, TypeError, IndexError) as exc:
                logging.debug("Skipping malformed book level: %s (%s)", level, exc)
                continue

            normalized = {
                "symbol": pair,
                "ts": timestamp,
                "side": "bid" if side == "bid" else "ask",
                "price": price,
                "size": size,
                "type": "book",
            }
            await self._send_to_kafka(self._config.book_topic, normalized)

    async def _send_to_kafka(self, topic: str, payload: Dict[str, Any]) -> None:
        assert self._producer is not None
        try:
            await self._producer.send_and_wait(topic, payload)
        except KafkaError as exc:
            logging.error("Kafka send failed for topic %s: %s", topic, exc)
            raise

    async def _heartbeat_guard(self, ws: websockets.WebSocketClientProtocol) -> None:
        while True:
            await asyncio.sleep(self._config.heartbeat_check_interval)
            elapsed = time.monotonic() - self._last_heartbeat_ts
            if elapsed > self._config.heartbeat_timeout:
                logging.warning(
                    "No heartbeat received for %.1f seconds (timeout %.1f)",
                    elapsed,
                    self._config.heartbeat_timeout,
                )
                await ws.close(code=4000, reason="Heartbeat timeout")
                raise HeartbeatTimeout("Heartbeat timeout reached")

    @staticmethod
    def _extract_channel_and_pair(message: List[Any]) -> tuple[Optional[str], Optional[str]]:
        channel_name: Optional[str] = None
        pair: Optional[str] = None
        for item in reversed(message):
            if isinstance(item, str):
                if "/" in item and pair is None:
                    pair = item
                elif channel_name is None:
                    channel_name = item
        return channel_name, pair


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kraken WebSocket ingestor")
    parser.add_argument(
        "--pairs",
        type=lambda v: [item.strip() for item in v.split(",") if item.strip()],
        required=True,
        help="Comma separated list of trading pairs (e.g. XBT/USD,ETH/USD)",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        required=True,
        help="Kafka bootstrap servers (e.g. localhost:9092)",
    )
    parser.add_argument(
        "--trade-topic",
        default=DEFAULT_TRADE_TOPIC,
        help="Kafka topic for trade events",
    )
    parser.add_argument(
        "--book-topic",
        default=DEFAULT_BOOK_TOPIC,
        help="Kafka topic for book events",
    )
    parser.add_argument(
        "--book-depth",
        type=int,
        default=None,
        help="Optional Kraken order book depth subscription parameter",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (default: INFO or LOG_LEVEL env value)",
    )
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def install_signal_handlers(ingestor: KrakenIngestor) -> None:
    loop = asyncio.get_running_loop()

    def _signal_handler() -> None:
        logging.info("Shutdown signal received")
        asyncio.create_task(ingestor.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Signals may not be available on some platforms (e.g. Windows).
            signal.signal(sig, lambda *_: asyncio.create_task(ingestor.stop()))


async def async_main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    configure_logging(args.log_level)

    config = KrakenConfig(
        pairs=args.pairs,
        kafka_bootstrap_servers=args.kafka_bootstrap,
        trade_topic=args.trade_topic,
        book_topic=args.book_topic,
        book_depth=args.book_depth,
    )

    ingestor = KrakenIngestor(config)
    install_signal_handlers(ingestor)

    await ingestor.run()


def main(argv: Optional[Iterable[str]] = None) -> None:
    try:
        asyncio.run(async_main(argv))
    except KeyboardInterrupt:
        logging.info("Interrupted by user")


if __name__ == "__main__":
    main(sys.argv[1:])
