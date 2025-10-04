"""Secondary market data ingestion and reconciliation service.

This module introduces an auxiliary market data monitor that compares prices
between the primary Kraken feed and a secondary vendor (Kaiko/Amberdata).
The secondary client authenticates against the vendor API, enforces timeouts
and retries, and exposes structured errors that allow the monitor to trigger
alerts and safe-mode fallbacks when the feed is unavailable or stale.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple
from urllib.parse import parse_qsl

import httpx
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
DEFAULT_SECONDARY_API_URL = os.getenv(
    "ALT_DATA_SECONDARY_API_URL", "https://gateway.kaiko.com"
)
DEFAULT_SECONDARY_ENDPOINT = os.getenv(
    "ALT_DATA_SECONDARY_ENDPOINT",
    "/v2/data/trades.v1/spot_direct_exchange_rate?base_asset={base}&quote_asset={quote}",
)
DEFAULT_SECONDARY_API_KEY = os.getenv("ALT_DATA_SECONDARY_API_KEY", "")
DEFAULT_SECONDARY_TIMEOUT = float(os.getenv("ALT_DATA_SECONDARY_TIMEOUT", "3.0"))
DEFAULT_SECONDARY_RETRIES = int(os.getenv("ALT_DATA_SECONDARY_RETRIES", "2"))
DEFAULT_SECONDARY_RETRY_BACKOFF = float(
    os.getenv("ALT_DATA_SECONDARY_RETRY_BACKOFF", "0.5")
)
DEFAULT_SECONDARY_QUOTE = os.getenv("ALT_DATA_SECONDARY_QUOTE", "USD")
DEFAULT_SECONDARY_VERIFY_SSL = os.getenv(
    "ALT_DATA_SECONDARY_VERIFY_SSL", "true"
).lower() not in {"0", "false", "no"}
DEFAULT_SECONDARY_MAX_AGE_SECONDS = float(
    os.getenv("ALT_DATA_SECONDARY_MAX_AGE_SECONDS", "45")
)

DEFAULT_SAFE_MODE_ENABLED = os.getenv("ALT_DATA_SAFE_MODE_ENABLED", "false").lower() in {
    "1",
    "true",
    "yes",
}
DEFAULT_SAFE_MODE_URL = os.getenv("ALT_DATA_SAFE_MODE_URL", "http://localhost:8000")
DEFAULT_SAFE_MODE_ENDPOINT = os.getenv("ALT_DATA_SAFE_MODE_ENDPOINT", "/safe_mode/enter")
DEFAULT_SAFE_MODE_TIMEOUT = float(os.getenv("ALT_DATA_SAFE_MODE_TIMEOUT", "2.0"))
DEFAULT_SAFE_MODE_REASON = os.getenv(
    "ALT_DATA_SAFE_MODE_REASON", "secondary_market_data_failure"
)


@dataclass
class SecondaryPrice:
    """Container for a secondary price observation."""

    symbol: str
    price: float
    source: str
    observed_at: dt.datetime


class SecondaryDataError(RuntimeError):
    """Raised when the secondary market data feed cannot return a quote."""


@dataclass
class SecondaryClientConfig:
    """Configuration payload for the Kaiko/Amberdata client."""

    base_url: str = DEFAULT_SECONDARY_API_URL
    endpoint: str = DEFAULT_SECONDARY_ENDPOINT
    api_key: str = DEFAULT_SECONDARY_API_KEY
    source: str = DEFAULT_SECONDARY_SOURCE
    timeout: float = DEFAULT_SECONDARY_TIMEOUT
    max_retries: int = DEFAULT_SECONDARY_RETRIES
    backoff_seconds: float = DEFAULT_SECONDARY_RETRY_BACKOFF
    quote: str = DEFAULT_SECONDARY_QUOTE
    verify_ssl: bool = DEFAULT_SECONDARY_VERIFY_SSL
    auth_header: Optional[str] = None
    params: Dict[str, str] = field(default_factory=dict)

    def header_name(self) -> str:
        if self.auth_header:
            return self.auth_header
        if self.source.lower() == "amberdata":
            return "x-api-key"
        return "X-Api-Key"


class SecondaryMarketDataClient:
    """Client responsible for retrieving prices from Kaiko or Amberdata."""

    def __init__(
        self,
        config: Optional[SecondaryClientConfig] = None,
        *,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        self._config = config or SecondaryClientConfig()
        self._transport = transport

    @property
    def source(self) -> str:
        return self._config.source

    async def fetch_price(self, symbol: str) -> SecondaryPrice:
        """Fetch the latest quote for ``symbol`` with retries and timeouts."""

        normalized = symbol.upper()
        path, params = self._build_request(normalized)
        headers = self._build_headers()
        timeout = httpx.Timeout(self._config.timeout)
        attempt = 0
        last_exc: Optional[Exception] = None

        while attempt <= self._config.max_retries:
            try:
                async with httpx.AsyncClient(
                    base_url=self._config.base_url,
                    timeout=timeout,
                    transport=self._transport,
                    verify=self._config.verify_ssl,
                ) as client:
                    response = await client.get(path, params=params, headers=headers)
                response.raise_for_status()
                payload = response.json()
                return self._parse_payload(normalized, payload)
            except (httpx.TimeoutException, httpx.HTTPError, ValueError) as exc:
                last_exc = exc
                LOGGER.warning(
                    "Secondary feed request failed (attempt %s/%s): %s",
                    attempt + 1,
                    self._config.max_retries + 1,
                    exc,
                )
            except SecondaryDataError:
                raise

            attempt += 1
            if attempt > self._config.max_retries:
                break
            await asyncio.sleep(self._config.backoff_seconds * (2 ** (attempt - 1)))

        raise SecondaryDataError(
            f"Unable to retrieve secondary quote for {normalized}: {last_exc}"
        )

    def _build_headers(self) -> Dict[str, str]:
        header_name = self._config.header_name()
        headers = {"Accept": "application/json"}
        if self._config.api_key:
            headers[header_name] = self._config.api_key
        return headers

    def _build_request(self, symbol: str) -> Tuple[str, Dict[str, str]]:
        base, quote = self._split_symbol(symbol)
        mapping = {
            "base": base.lower(),
            "BASE": base,
            "quote": quote.lower(),
            "QUOTE": quote,
            "symbol": symbol,
            "SYMBOL": symbol,
        }
        try:
            endpoint = self._config.endpoint.format(**mapping)
        except KeyError as exc:  # pragma: no cover - defensive guard
            raise SecondaryDataError(f"Endpoint template missing key: {exc}") from exc

        path, params = self._split_query(endpoint)
        if self._config.params:
            merged = dict(self._config.params)
            merged.update(params)
            params = merged
        return path, params

    def _split_symbol(self, symbol: str) -> Tuple[str, str]:
        if "/" in symbol:
            base, quote = symbol.split("/", 1)
        elif "-" in symbol:
            base, quote = symbol.split("-", 1)
        else:
            base, quote = symbol, self._config.quote
        return base.strip().upper(), quote.strip().upper()

    @staticmethod
    def _split_query(endpoint: str) -> Tuple[str, Dict[str, str]]:
        if "?" not in endpoint:
            return endpoint, {}
        path, _, query = endpoint.partition("?")
        params = dict(parse_qsl(query, keep_blank_values=False))
        return path, params

    def _parse_payload(self, symbol: str, payload: Dict[str, Any]) -> SecondaryPrice:
        entry = self._select_entry(payload)
        if entry is None:
            raise SecondaryDataError("Secondary feed response missing price data")

        price_raw = self._extract_price(entry)
        if price_raw is None:
            raise SecondaryDataError("Secondary feed response did not include a price")

        try:
            price = float(price_raw)
        except (TypeError, ValueError) as exc:
            raise SecondaryDataError("Secondary feed returned a non-numeric price") from exc

        observed_at = self._extract_timestamp(entry, payload)
        if observed_at is None:
            observed_at = dt.datetime.now(tz=dt.timezone.utc)

        return SecondaryPrice(
            symbol=symbol,
            price=price,
            source=self._config.source,
            observed_at=observed_at,
        )

    @staticmethod
    def _select_entry(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data:
            for candidate in data:
                if isinstance(candidate, dict):
                    return candidate
        result = payload.get("result")
        if isinstance(result, dict):
            return result
        if isinstance(result, list) and result:
            for candidate in result:
                if isinstance(candidate, dict):
                    return candidate
        return None

    @staticmethod
    def _extract_price(entry: Dict[str, Any]) -> Optional[float]:
        for key in ("price", "mid", "close", "value", "rate", "last", "marketPrice"):
            if key in entry:
                return entry[key]
        return None

    @staticmethod
    def _extract_timestamp(
        entry: Dict[str, Any], payload: Dict[str, Any]
    ) -> Optional[dt.datetime]:
        timestamp = (
            entry.get("timestamp")
            or entry.get("time")
            or entry.get("ts")
            or payload.get("timestamp")
        )
        if timestamp is None:
            return None
        if isinstance(timestamp, (int, float)):
            return dt.datetime.fromtimestamp(float(timestamp), tz=dt.timezone.utc)
        if isinstance(timestamp, str):
            normalized = timestamp.replace("Z", "+00:00")
            try:
                value = dt.datetime.fromisoformat(normalized)
            except ValueError:
                return None
            if value.tzinfo is None:
                value = value.replace(tzinfo=dt.timezone.utc)
            return value.astimezone(dt.timezone.utc)
        return None


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
        max_secondary_age_seconds: float = DEFAULT_SECONDARY_MAX_AGE_SECONDS,
        safe_mode_enabled: bool = DEFAULT_SAFE_MODE_ENABLED,
        safe_mode_url: str = DEFAULT_SAFE_MODE_URL,
        safe_mode_endpoint: str = DEFAULT_SAFE_MODE_ENDPOINT,
        safe_mode_timeout: float = DEFAULT_SAFE_MODE_TIMEOUT,
        safe_mode_reason: str = DEFAULT_SAFE_MODE_REASON,
        safe_mode_hook: Optional[Callable[[str], Awaitable[None]]] = None,
    ) -> None:
        self._secondary_client = secondary_client
        self._producer = kafka_producer
        self._topic = kafka_topic
        self._threshold_bps = deviation_threshold_bps
        self._max_secondary_age = dt.timedelta(seconds=max_secondary_age_seconds)
        self._safe_mode_enabled = safe_mode_enabled
        self._safe_mode_url = safe_mode_url
        self._safe_mode_endpoint = safe_mode_endpoint
        self._safe_mode_timeout = safe_mode_timeout
        self._safe_mode_reason = safe_mode_reason
        self._safe_mode_hook = safe_mode_hook
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
        except SecondaryDataError as exc:
            LOGGER.error(
                "Secondary price retrieval failed for %s: %s", symbol_key, exc
            )
            await self._handle_secondary_failure(symbol_key, "secondary_fetch_failed", str(exc))
            return
        except Exception:  # pragma: no cover - defensive catch
            LOGGER.exception("Unexpected error fetching secondary price for symbol=%s", symbol_key)
            await self._handle_secondary_failure(symbol_key, "secondary_fetch_exception")
            return

        now = dt.datetime.now(tz=dt.timezone.utc)
        if secondary_price.observed_at < now - self._max_secondary_age:
            LOGGER.warning(
                "Secondary price for %s is stale (age=%ss)",
                symbol_key,
                (now - secondary_price.observed_at).total_seconds(),
            )
            await self._handle_secondary_failure(symbol_key, "secondary_feed_stale")
            return

        if secondary_price.price <= 0:
            LOGGER.warning(
                "Ignoring comparison for symbol=%s due to non-positive secondary price", symbol_key
            )
            await self._handle_secondary_failure(symbol_key, "secondary_price_invalid")
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

    async def _handle_secondary_failure(
        self,
        symbol: str,
        reason: str,
        detail: Optional[str] = None,
    ) -> None:
        async with self._lock:
            self._secondary_feed_ok = False
            self._last_diff_bps = None
        self._publish_secondary_failure(symbol, reason, detail)
        await self._trigger_safe_mode(reason, symbol)

    def _publish_secondary_failure(
        self, symbol: str, reason: str, detail: Optional[str]
    ) -> None:
        if self._producer is None:
            LOGGER.warning(
                "Kafka producer unavailable; secondary feed failure not published",
                extra={"symbol": symbol, "reason": reason, "detail": detail},
            )
            return

        payload: Dict[str, Any] = {
            "event": "secondary_feed_failure",
            "symbol": symbol,
            "reason": reason,
            "detail": detail,
            "observed_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        }
        try:
            message = json.dumps(payload).encode("utf-8")
            self._producer.produce(self._topic, message)
            self._producer.poll(0)
            LOGGER.error(
                "Recorded secondary feed failure for %s (%s)", symbol, reason
            )
        except Exception:  # pragma: no cover - depends on Kafka runtime
            LOGGER.exception("Failed to publish secondary feed failure", extra=payload)

    async def _trigger_safe_mode(self, failure_reason: str, symbol: Optional[str]) -> None:
        if self._safe_mode_hook is not None:
            await self._safe_mode_hook(failure_reason)
            return

        if not self._safe_mode_enabled:
            return

        url = f"{self._safe_mode_url.rstrip('/')}{self._safe_mode_endpoint}"
        payload = {"reason": self._safe_mode_reason, "source": failure_reason}
        if symbol:
            payload["symbol"] = symbol
        try:
            async with httpx.AsyncClient(timeout=self._safe_mode_timeout) as client:
                response = await client.post(url, json=payload)
            response.raise_for_status()
            LOGGER.warning("Triggered safe mode due to secondary feed issue: %s", failure_reason)
        except httpx.HTTPError as exc:  # pragma: no cover - network guard
            LOGGER.error("Failed to trigger safe mode: %s", exc)


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
    "SecondaryClientConfig",
    "SecondaryDataError",
    "SecondaryMarketDataClient",
    "IngestStatus",
    "app",
    "record_kraken_price",
]
