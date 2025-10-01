"""Secondary market data ingestion and reconciliation service.

This module introduces an auxiliary market data monitor that compares prices
between the primary Kraken feed and a secondary vendor (Kaiko/Amberdata).  The
secondary client is currently implemented as a lightweight stub that mimics the
behaviour of an external API.  When the price delta between the two feeds
breaches a configurable threshold the service emits an anomaly alert via Kafka
and exposes a health endpoint for observability.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from fastapi import FastAPI
from pydantic import BaseModel, Field

try:  # pragma: no cover - optional dependency during unit tests
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover - kafka producer is optional
    Producer = None  # type: ignore


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


DEFAULT_DEVIATION_BPS = float(os.getenv("ALT_DATA_DEVIATION_THRESHOLD_BPS", "50"))
DEFAULT_KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
DEFAULT_ANOMALY_TOPIC = os.getenv("ALT_DATA_ANOMALY_TOPIC", "anomalies.data")
DEFAULT_SECONDARY_SOURCE = os.getenv("ALT_DATA_SECONDARY_SOURCE", "kaiko")


@dataclass
class SecondaryPrice:
    """Container for a secondary price observation."""

    symbol: str
    price: float
    source: str
    observed_at: dt.datetime


class SecondaryMarketDataClient:
    """Stubbed client that simulates Kaiko/Amberdata price retrieval."""

    def __init__(self, source: str = DEFAULT_SECONDARY_SOURCE) -> None:
        self._source = source
        self._prices: dict[str, float] = {}
        self._lock = asyncio.Lock()

    @property
    def source(self) -> str:
        return self._source

    def set_price(self, symbol: str, price: float) -> None:
        """Inject a deterministic price for tests or offline simulations."""

        self._prices[symbol.upper()] = float(price)

    async def fetch_price(self, symbol: str) -> SecondaryPrice:
        """Return the latest stubbed price for ``symbol``.

        The stub falls back to a deterministic pseudo price based on the symbol
        to avoid surprising randomness during testing.
        """

        symbol_key = symbol.upper()
        async with self._lock:
            if symbol_key not in self._prices:
                base = abs(hash(symbol_key)) % 50_000 + 10_000
                self._prices[symbol_key] = float(base)
            price = self._prices[symbol_key]

        await asyncio.sleep(0)  # emulate asynchronous network latency
        return SecondaryPrice(
            symbol=symbol_key,
            price=float(price),
            source=self._source,
            observed_at=dt.datetime.now(tz=dt.timezone.utc),
        )


class IngestStatus(BaseModel):
    """Payload returned by the ``/ingest/status`` endpoint."""

    kraken_feed_ok: bool = Field(..., description="Whether the primary feed is healthy")
    secondary_feed_ok: bool = Field(..., description="Whether the secondary feed is healthy")
    last_diff_bps: Optional[float] = Field(
        None,
        description="Most recent Kraken vs secondary deviation in basis points",
    )


class AltDataMonitor:
    """Compares Kraken prices with a secondary source and raises anomalies."""

    def __init__(
        self,
        secondary_client: SecondaryMarketDataClient,
        *,
        kafka_producer: Producer | None = None,
        kafka_topic: str = DEFAULT_ANOMALY_TOPIC,
        deviation_threshold_bps: float = DEFAULT_DEVIATION_BPS,
    ) -> None:
        self._secondary_client = secondary_client
        self._producer = kafka_producer
        self._topic = kafka_topic
        self._threshold_bps = deviation_threshold_bps
        self._lock = asyncio.Lock()
        self._kraken_feed_ok = False
        self._secondary_feed_ok = False
        self._last_diff_bps: Optional[float] = None
        self._last_symbol: Optional[str] = None

    async def process_kraken_price(self, symbol: str, price: float) -> None:
        """Record a Kraken price update and compare it to the secondary feed."""

        symbol_key = symbol.upper()
        async with self._lock:
            self._kraken_feed_ok = True
            self._last_symbol = symbol_key

        try:
            secondary_price = await self._secondary_client.fetch_price(symbol_key)
        except Exception:  # pragma: no cover - defensive catch
            LOGGER.exception("Failed to fetch secondary price for symbol=%s", symbol_key)
            async with self._lock:
                self._secondary_feed_ok = False
            return

        if secondary_price.price <= 0:
            LOGGER.warning(
                "Ignoring comparison for symbol=%s due to non-positive secondary price", symbol_key
            )
            async with self._lock:
                self._secondary_feed_ok = False
            return

        diff_bps = self._calculate_diff_bps(price, secondary_price.price)

        async with self._lock:
            self._secondary_feed_ok = True
            self._last_diff_bps = diff_bps

        if abs(diff_bps) >= self._threshold_bps:
            self._emit_anomaly(symbol_key, price, secondary_price, diff_bps)

    async def get_status(self) -> IngestStatus:
        """Return the current ingestion status snapshot."""

        async with self._lock:
            return IngestStatus(
                kraken_feed_ok=self._kraken_feed_ok,
                secondary_feed_ok=self._secondary_feed_ok,
                last_diff_bps=self._last_diff_bps,
            )

    def _calculate_diff_bps(self, kraken_price: float, secondary_price: float) -> float:
        if secondary_price == 0:
            return float("inf") if kraken_price != 0 else 0.0
        return ((kraken_price - secondary_price) / secondary_price) * 10_000

    def _emit_anomaly(
        self,
        symbol: str,
        kraken_price: float,
        secondary_price: SecondaryPrice,
        diff_bps: float,
    ) -> None:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "kraken_price": float(kraken_price),
            "secondary_price": float(secondary_price.price),
            "secondary_source": secondary_price.source,
            "diff_bps": float(diff_bps),
            "threshold_bps": float(self._threshold_bps),
            "observed_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        }

        if self._producer is None:
            LOGGER.warning(
                "Kafka producer unavailable; anomaly payload not published", extra=payload
            )
            return

        try:
            message = json.dumps(payload).encode("utf-8")
            self._producer.produce(self._topic, message)
            self._producer.poll(0)
            LOGGER.info(
                "Emitted anomaly alert for %s (%.2f bps)", symbol, diff_bps
            )
        except Exception:  # pragma: no cover - depends on Kafka runtime
            LOGGER.exception("Failed to publish anomaly alert", extra=payload)


def _create_kafka_producer() -> Producer | None:
    if Producer is None:
        LOGGER.warning("confluent_kafka.Producer unavailable; alerts will not be emitted")
        return None
    try:
        return Producer({"bootstrap.servers": DEFAULT_KAFKA_BROKERS})
    except Exception:  # pragma: no cover - depends on environment configuration
        LOGGER.exception("Unable to instantiate Kafka producer")
        return None


_secondary_client = SecondaryMarketDataClient()
_monitor = AltDataMonitor(
    _secondary_client,
    kafka_producer=_create_kafka_producer(),
)

app = FastAPI(title="Alt Market Data Monitor", version="1.0.0")


@app.get("/ingest/status", response_model=IngestStatus)
async def ingest_status() -> IngestStatus:
    """Return the health of the Kraken and secondary data feeds."""

    return await _monitor.get_status()


async def record_kraken_price(symbol: str, price: float) -> None:
    """Public helper used by ingestion pipelines to feed Kraken prices."""

    await _monitor.process_kraken_price(symbol, price)


__all__ = [
    "AltDataMonitor",
    "SecondaryMarketDataClient",
    "IngestStatus",
    "app",
    "record_kraken_price",
]
