"""Adapters for accessing authoritative market data feeds for analytics services.

This module provides a synchronous :class:`TimescaleMarketDataAdapter` that pulls
recent trades, order book snapshots, and price history directly from the
TimescaleDB tables maintained by the ingestion pipeline.  The adapter is
intentionally lightweight so it can be reused by FastAPI request handlers without
requiring a dedicated event loop or background consumer.  Tests can provide their
own adapters that satisfy the :class:`MarketDataAdapter` protocol when
deterministic fixtures are required.
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Mapping, MutableMapping, Protocol, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import StaticPool

from shared.postgres import normalize_sqlalchemy_dsn
from shared.spot import require_spot_symbol


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class Trade:
    """Canonical representation of an executed trade used by analytics."""

    side: str
    volume: float
    price: float
    ts: datetime


class MarketDataUnavailable(RuntimeError):
    """Raised when market data cannot be retrieved from the underlying store."""


class MarketDataAdapter(Protocol):
    """Protocol describing the required market data access methods."""

    def recent_trades(self, symbol: str, window: int) -> Sequence[Trade]:
        """Return executed trades for ``symbol`` within the last ``window`` seconds."""

    def order_book_snapshot(self, symbol: str, depth: int = 10) -> Mapping[str, Sequence[Sequence[float]]]:
        """Return the most recent order book snapshot for ``symbol``."""

    def price_history(self, symbol: str, length: int) -> Sequence[float]:
        """Return the most recent ``length`` closing prices for ``symbol``."""

    def latest_price_timestamp(self, symbol: str) -> datetime | None:
        """Return the timestamp of the most recent price observation for ``symbol``."""


class TimescaleMarketDataAdapter:
    """Timescale-backed implementation of :class:`MarketDataAdapter`."""

    def __init__(
        self,
        *,
        database_url: str | None = None,
        engine: Engine | None = None,
        schema: str | None = None,
        trades_table: str = "fills",
        orders_table: str = "orderbook_snapshots",
        prices_table: str = "ohlcv_bars",
        orders_side_table: str = "orders",
    ) -> None:
        if engine is not None:
            self._engine = engine
        else:
            allow_sqlite = "pytest" in sys.modules
            resolved_url: str
            if database_url is None:
                if allow_sqlite:
                    LOGGER.warning(
                        "TimescaleMarketDataAdapter instantiated without database_url or engine; "
                        "defaulting to in-memory sqlite fallback for tests."
                    )
                    resolved_url = "sqlite+pysqlite:///:memory:"
                else:
                    raise RuntimeError(
                        "Timescale market data DSN must be configured with a PostgreSQL/Timescale URI."
                    )
            else:
                candidate = database_url.strip()
                if not candidate:
                    raise RuntimeError(
                        "Timescale market data DSN cannot be empty once configured."
                    )
                resolved_url = normalize_sqlalchemy_dsn(
                    candidate,
                    allow_sqlite=allow_sqlite,
                    label="Timescale market data DSN",
                )

            engine_options: dict[str, object] = {"future": True}
            if resolved_url.startswith(("sqlite://", "sqlite+pysqlite://")):
                engine_options.setdefault("connect_args", {"check_same_thread": False})
                if resolved_url.endswith(":memory:"):
                    engine_options["poolclass"] = StaticPool
            self._engine = create_engine(resolved_url, **engine_options)
        self._schema = schema
        self._trades_table = trades_table
        self._orderbook_table = orders_table
        self._prices_table = prices_table
        self._orders_side_table = orders_side_table
        self._price_timestamps: MutableMapping[str, datetime] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def recent_trades(self, symbol: str, window: int) -> Sequence[Trade]:
        normalized = require_spot_symbol(symbol)
        start = datetime.now(timezone.utc) - timedelta(seconds=max(1, window))
        limit = max(50, min(1000, window // 2))

        stmt = text(
            f"""
            SELECT o.side AS side,
                   f.size AS volume,
                   f.price AS price,
                   f.fill_time AS ts
            FROM {self._qualified(self._trades_table)} AS f
            JOIN {self._qualified(self._orders_side_table)} AS o
              ON o.order_id = f.order_id
            WHERE upper(o.market) = :symbol
              AND f.fill_time >= :start
            ORDER BY f.fill_time DESC
            LIMIT :limit
            """
        )

        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    stmt,
                    {"symbol": normalized, "start": start, "limit": limit},
                ).all()
        except SQLAlchemyError as exc:
            LOGGER.exception("Failed to load trades for %s", normalized)
            raise MarketDataUnavailable(f"Unable to load trades for {normalized}") from exc

        trades: list[Trade] = []
        for side, volume, price, ts in rows:
            if ts is None:
                continue
            ts_dt = ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts))
            if ts_dt.tzinfo is None:
                ts_dt = ts_dt.replace(tzinfo=timezone.utc)
            trades.append(
                Trade(
                    side=str(side or "").lower() or "buy",
                    volume=float(volume or 0.0),
                    price=float(price or 0.0),
                    ts=ts_dt.astimezone(timezone.utc),
                )
            )
        return trades

    def order_book_snapshot(self, symbol: str, depth: int = 10) -> Mapping[str, Sequence[Sequence[float]]]:
        normalized = require_spot_symbol(symbol)
        stmt = text(
            f"""
            SELECT bids, asks, as_of
            FROM {self._qualified(self._orderbook_table)}
            WHERE upper(symbol) = :symbol
              AND depth = :depth
            ORDER BY as_of DESC
            LIMIT 1
            """
        )

        try:
            with self._engine.connect() as conn:
                row = conn.execute(stmt, {"symbol": normalized, "depth": depth}).first()
        except SQLAlchemyError as exc:
            LOGGER.exception("Failed to load order book for %s", normalized)
            raise MarketDataUnavailable(f"Unable to load order book for {normalized}") from exc

        if row is None:
            raise MarketDataUnavailable(f"No order book snapshot available for {normalized}")

        bids_raw, asks_raw, as_of = row
        bids = self._parse_levels(bids_raw)
        asks = self._parse_levels(asks_raw)
        if not bids or not asks:
            raise MarketDataUnavailable(f"Order book for {normalized} is empty")

        return {"bids": bids, "asks": asks, "as_of": as_of}

    def price_history(self, symbol: str, length: int) -> Sequence[float]:
        normalized = require_spot_symbol(symbol)
        stmt = text(
            f"""
            SELECT close, bucket_start
            FROM {self._qualified(self._prices_table)}
            WHERE upper(market) = :symbol
            ORDER BY bucket_start DESC
            LIMIT :limit
            """
        )

        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt, {"symbol": normalized, "limit": max(1, length)}).all()
        except SQLAlchemyError as exc:
            LOGGER.exception("Failed to load price history for %s", normalized)
            raise MarketDataUnavailable(f"Unable to load price history for {normalized}") from exc

        prices: List[float] = []
        latest_ts: datetime | None = None
        for price, bucket_start in rows:
            if price is None:
                continue
            prices.append(float(price))
            if latest_ts is None:
                ts_candidate = self._coerce_datetime(bucket_start)
                if ts_candidate is not None:
                    latest_ts = ts_candidate
        prices.reverse()
        if not prices:
            raise MarketDataUnavailable(f"No price history available for {normalized}")
        if latest_ts is not None:
            self._price_timestamps[normalized] = latest_ts.astimezone(timezone.utc)
        return prices

    def latest_price_timestamp(self, symbol: str) -> datetime | None:
        """Return the timestamp of the last price observation for ``symbol``."""

        normalized = require_spot_symbol(symbol)
        return self._price_timestamps.get(normalized)

    @staticmethod
    def _coerce_datetime(value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value)
            except ValueError:
                return None
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _qualified(self, table: str) -> str:
        if self._schema:
            return f"{self._schema}.{table}"
        return table

    @staticmethod
    def _parse_levels(raw: object) -> List[List[float]]:
        if isinstance(raw, str):
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = []
        elif isinstance(raw, (bytes, bytearray)):
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:  # pragma: no cover - defensive
                payload = []
        elif isinstance(raw, Iterable):
            payload = list(raw)
        else:
            payload = []

        levels: List[List[float]] = []
        for entry in payload:
            if isinstance(entry, Mapping):
                price = float(entry.get("price", 0.0))
                size = float(entry.get("size", entry.get("volume", 0.0)))
                levels.append([price, size])
            elif isinstance(entry, Sequence) and len(entry) >= 2:
                price = float(entry[0])
                size = float(entry[1])
                levels.append([price, size])
        return levels


__all__ = [
    "MarketDataAdapter",
    "MarketDataUnavailable",
    "TimescaleMarketDataAdapter",
    "Trade",
]
