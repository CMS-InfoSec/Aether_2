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
import os
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Mapping, MutableMapping, Protocol, Sequence

try:  # pragma: no cover - optional dependency guard
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.pool import StaticPool

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - executed in dependency-light environments
    SQLALCHEMY_AVAILABLE = False

    Engine = object  # type: ignore[assignment]

    class SQLAlchemyError(Exception):
        """Fallback error used when SQLAlchemy is unavailable."""

    def text(query: str) -> str:  # type: ignore[override]
        return query

    class StaticPool:  # pragma: no cover - marker placeholder
        pass

    def create_engine(*_: object, **__: object):  # type: ignore[override]
        raise RuntimeError(
            "SQLAlchemy is required for Timescale market data access; enable insecure defaults "
            "to use the local fallback store instead."
        )

from shared.postgres import normalize_sqlalchemy_dsn
from shared.spot import require_spot_symbol


LOGGER = logging.getLogger(__name__)

_USE_LOCAL_STORE_FLAG = "MARKET_DATA_USE_LOCAL_STORE"


def _use_local_store() -> bool:
    flag = os.getenv(_USE_LOCAL_STORE_FLAG)
    if flag == "1":
        return True
    if flag == "0":
        return False
    return not SQLALCHEMY_AVAILABLE


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


class LocalMarketDataStore:
    """In-memory fallback store mirroring the Timescale access patterns."""

    def __init__(self) -> None:
        self._orders: dict[str, dict[str, Any]] = {}
        self._fills: list[dict[str, Any]] = []
        self._orderbooks: dict[tuple[str, int], list[dict[str, Any]]] = {}
        self._bars: dict[str, list[dict[str, Any]]] = {}
        self._price_timestamps: dict[str, datetime] = {}
        self._lock = threading.Lock()

    def seed(self, payload: Mapping[str, Any]) -> None:
        with self._lock:
            orders: dict[str, dict[str, Any]] = {}
            for row in payload.get("orders", []):
                if not isinstance(row, Mapping):
                    continue
                order_id = str(row.get("order_id"))
                market = str(row.get("market", "")).upper()
                submitted = _coerce_datetime(row.get("submitted_at"))
                orders[order_id] = {
                    "order_id": order_id,
                    "market": market,
                    "side": str(row.get("side", "buy")).lower() or "buy",
                    "submitted_at": submitted,
                }

            fills: list[dict[str, Any]] = []
            for row in payload.get("fills", []):
                if not isinstance(row, Mapping):
                    continue
                fill_time = _coerce_datetime(row.get("fill_time"))
                fills.append(
                    {
                        "order_id": str(row.get("order_id")),
                        "fill_time": fill_time,
                        "price": float(row.get("price", 0.0)),
                        "size": float(row.get("size", 0.0)),
                    }
                )

            orderbooks: dict[tuple[str, int], list[dict[str, Any]]] = {}
            for row in payload.get("orderbook_snapshots", []):
                if not isinstance(row, Mapping):
                    continue
                symbol = str(row.get("symbol", "")).upper()
                depth = int(row.get("depth", 0))
                as_of = _coerce_datetime(row.get("as_of"))
                record = {
                    "symbol": symbol,
                    "depth": depth,
                    "as_of": as_of,
                    "bids": row.get("bids", []),
                    "asks": row.get("asks", []),
                }
                orderbooks.setdefault((symbol, depth), []).append(record)

            bars: dict[str, list[dict[str, Any]]] = {}
            price_timestamps: dict[str, datetime] = {}
            for symbol, rows in payload.get("bars", {}).items():
                if not isinstance(rows, Iterable):
                    continue
                normalised_symbol = str(symbol).upper()
                series: list[dict[str, Any]] = []
                for row in rows:
                    if not isinstance(row, Mapping):
                        continue
                    bucket = _coerce_datetime(row.get("bucket_start"))
                    close = row.get("close")
                    if bucket is None or close is None:
                        continue
                    series.append({
                        "bucket_start": bucket,
                        "close": float(close),
                        "open": float(row.get("open", close)),
                        "high": float(row.get("high", close)),
                        "low": float(row.get("low", close)),
                        "volume": float(row.get("volume", 0.0)),
                    })
                series.sort(key=lambda entry: entry["bucket_start"])
                if series:
                    price_timestamps[normalised_symbol] = series[-1]["bucket_start"]
                    bars[normalised_symbol] = series

            self._orders = orders
            self._fills = fills
            self._orderbooks = orderbooks
            self._bars = bars
            self._price_timestamps = price_timestamps

    def recent_trades(self, symbol: str, window: int) -> Sequence[Trade]:
        normalized = require_spot_symbol(symbol)
        start = datetime.now(timezone.utc) - timedelta(seconds=max(1, window))
        limit = max(50, min(1000, window // 2))

        with self._lock:
            fills = list(self._fills)
            orders = dict(self._orders)

        trades: list[Trade] = []
        for row in fills:
            fill_time = row.get("fill_time")
            if fill_time is None or fill_time < start:
                continue
            order_id = row.get("order_id")
            order = orders.get(str(order_id))
            if order is None or order.get("market") != normalized:
                continue
            trades.append(
                Trade(
                    side=str(order.get("side", "buy")),
                    volume=float(row.get("size", 0.0)),
                    price=float(row.get("price", 0.0)),
                    ts=fill_time,
                )
            )

        trades.sort(key=lambda trade: trade.ts, reverse=True)
        return trades[:limit]

    def order_book_snapshot(self, symbol: str, depth: int) -> Mapping[str, Sequence[Sequence[float]]]:
        normalized = require_spot_symbol(symbol)
        with self._lock:
            candidates = list(self._orderbooks.get((normalized, depth), []))

        if not candidates:
            raise MarketDataUnavailable(f"No order book snapshot available for {normalized}")

        latest = max(
            candidates,
            key=lambda entry: entry.get("as_of") or datetime.fromtimestamp(0, tz=timezone.utc),
        )
        bids = TimescaleMarketDataAdapter._parse_levels(latest.get("bids"))
        asks = TimescaleMarketDataAdapter._parse_levels(latest.get("asks"))
        if not bids or not asks:
            raise MarketDataUnavailable(f"Order book for {normalized} is empty")

        return {"bids": bids, "asks": asks, "as_of": latest.get("as_of")}

    def price_history(self, symbol: str, length: int) -> Sequence[float]:
        normalized = require_spot_symbol(symbol)
        with self._lock:
            series = list(self._bars.get(normalized, ()))
        if not series:
            raise MarketDataUnavailable(f"No price history available for {normalized}")
        closes = [float(row.get("close", 0.0)) for row in series[-length:]]
        return closes

    def latest_price_timestamp(self, symbol: str) -> datetime | None:
        normalized = require_spot_symbol(symbol)
        with self._lock:
            return self._price_timestamps.get(normalized)


_LOCAL_STORE: LocalMarketDataStore | None = None


def _ensure_local_store() -> LocalMarketDataStore:
    global _LOCAL_STORE
    if _LOCAL_STORE is None:
        _LOCAL_STORE = LocalMarketDataStore()
    return _LOCAL_STORE


def seed_local_market_data(payload: Mapping[str, Any]) -> None:
    """Expose a helper for tests to provide deterministic market data."""

    store = _ensure_local_store()
    store.seed(payload)


def using_local_market_data() -> bool:
    """Return ``True`` when the adapter should defer to the fallback store."""

    return _use_local_store()


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
        self._local_store: LocalMarketDataStore | None = None
        if engine is not None:
            self._engine = engine
        else:
            allow_sqlite = "pytest" in sys.modules
            if _use_local_store():
                self._engine = None
                self._local_store = _ensure_local_store()
            else:
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

                if not SQLALCHEMY_AVAILABLE:
                    raise RuntimeError(
                        "SQLAlchemy is required for the Timescale market data adapter; set "
                        "MARKET_DATA_USE_LOCAL_STORE=1 to activate the fallback store."
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

        if self._local_store is not None:
            trades = list(self._local_store.recent_trades(normalized, window))
            return trades[:limit]

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
            with self._engine.connect() as conn:  # type: ignore[union-attr]
                result = conn.execute(
                    stmt,
                    {"symbol": normalized, "start": start, "limit": limit},
                )
                rows = self._all_rows(result)
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

        if self._local_store is not None:
            return self._local_store.order_book_snapshot(normalized, depth)

        try:
            with self._engine.connect() as conn:  # type: ignore[union-attr]
                result = conn.execute(stmt, {"symbol": normalized, "depth": depth})
                row = self._first_row(result)
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
        if self._local_store is not None:
            return self._local_store.price_history(normalized, length)
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
            with self._engine.connect() as conn:  # type: ignore[union-attr]
                result = conn.execute(
                    stmt, {"symbol": normalized, "limit": max(1, length)}
                )
                rows = self._all_rows(result)
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
        if self._local_store is not None:
            return self._local_store.latest_price_timestamp(normalized)
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

    @staticmethod
    def _all_rows(result: object) -> List[Sequence[object]]:
        """Normalise SQLAlchemy result objects into row sequences."""

        if result is None:
            return []

        for accessor in ("all", "fetchall"):
            method = getattr(result, accessor, None)
            if callable(method):
                rows = method()
                if rows is None:
                    return []
                return list(rows)

        rows_attr = getattr(result, "rows", None)
        if callable(rows_attr):
            rows_candidate = rows_attr()
            if rows_candidate is not None:
                return list(rows_candidate)
        elif rows_attr is not None:
            return list(rows_attr)

        data_attr = getattr(result, "data", None)
        if callable(data_attr):
            data_candidate = data_attr()
            if data_candidate is not None:
                return list(data_candidate)
        elif data_attr is not None:
            return list(data_attr)

        if isinstance(result, Iterable) and not isinstance(result, (str, bytes, bytearray)):
            return list(result)

        return []

    @classmethod
    def _first_row(cls, result: object) -> Sequence[object] | None:
        """Return the first row from a SQLAlchemy result or fallback shim."""

        if result is None:
            return None

        first = getattr(result, "first", None)
        if callable(first):
            return first()

        fetchone = getattr(result, "fetchone", None)
        if callable(fetchone):
            return fetchone()

        rows = cls._all_rows(result)
        if rows:
            return rows[0]
        return None


__all__ = [
    "MarketDataAdapter",
    "MarketDataUnavailable",
    "TimescaleMarketDataAdapter",
    "Trade",
    "seed_local_market_data",
    "using_local_market_data",
]
