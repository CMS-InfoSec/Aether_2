"""Order flow analytics API surface.

This module provides lightweight analytical endpoints derived from recent
trading activity and order book depth sourced from the production market data
pipeline.  Metrics are computed using trades and book snapshots stored in
TimescaleDB (populated by the ingestion service) and are persisted for
historical analysis when the optional psycopg dependency is available.  If
psycopg is unavailable, the service falls back to an in-memory store which
keeps the API functional for unit tests and local development sessions.
"""

from __future__ import annotations

import json
import logging
import os

import statistics
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query

from auth.service import (
    InMemorySessionStore,
    SessionStoreProtocol,
    build_session_store_from_url,
)

from services.analytics.market_data_store import (
    MarketDataAdapter,
    MarketDataUnavailable,
    TimescaleMarketDataAdapter,
)
from services.common.config import get_timescale_session
from services.common import security
from services.common.security import require_admin_account
from shared.session_config import load_session_ttl_minutes

try:  # pragma: no cover - optional dependency during CI
    import psycopg
    from psycopg import sql
except Exception:  # pragma: no cover - gracefully degrade without psycopg
    psycopg = None  # type: ignore
    sql = None  # type: ignore


LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Market data provider
# ---------------------------------------------------------------------------


class MarketDataProvider:
    """Client that reads trades and order books from the market data pipeline."""

    def __init__(
        self,
        *,
        adapter: MarketDataAdapter | None = None,
        order_book_depth: int = 10,
    ) -> None:
        self._adapter = adapter or TimescaleMarketDataAdapter()
        self._depth = max(1, order_book_depth)

    def get_recent_trades(self, symbol: str, window: int) -> List[Dict[str, float]]:
        trades = self._adapter.recent_trades(symbol, window)
        normalized: List[Dict[str, float]] = []
        for trade in trades:
            side = getattr(trade, "side", "")
            volume = getattr(trade, "volume", 0.0)
            price = getattr(trade, "price", 0.0)
            if not side:
                continue
            normalized.append(
                {
                    "side": str(side).lower(),
                    "volume": float(volume),
                    "price": float(price),
                }
            )
        return normalized

    def get_order_book(self, symbol: str) -> Dict[str, List[List[float]]]:
        snapshot = self._adapter.order_book_snapshot(symbol, depth=self._depth)
        bids = self._normalize_levels(snapshot.get("bids", []))
        asks = self._normalize_levels(snapshot.get("asks", []))
        if not bids or not asks:
            raise MarketDataUnavailable(f"Order book for {symbol} is empty")
        return {"bids": bids, "asks": asks}

    @staticmethod
    def _normalize_levels(levels: Sequence[Sequence[float]] | Iterable[Mapping[str, float]]) -> List[List[float]]:
        normalized: List[List[float]] = []
        for entry in levels:
            price: float
            size: float
            if isinstance(entry, Mapping):
                price = float(entry.get("price", 0.0))
                size = float(entry.get("size", entry.get("volume", 0.0)))
            elif isinstance(entry, Sequence) and len(entry) >= 2:
                price = float(entry[0])
                size = float(entry[1])
            else:
                continue
            if price <= 0 or size <= 0:
                continue
            normalized.append([price, size])
        return normalized


# ---------------------------------------------------------------------------
# Metrics representation and persistence
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class OrderflowSnapshot:
    """Structured representation of the computed order flow metrics."""

    symbol: str
    window: int
    buy_sell_imbalance: float
    depth_imbalance: float
    bid_depth: float
    ask_depth: float
    liquidity_holes: List[Dict[str, float]]
    impact_estimates: Dict[str, Mapping[str, Optional[float]]]
    ts: datetime

    def to_record(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["ts"] = self.ts
        return payload


class OrderflowMetricsStore:
    """Persist order flow snapshots to Timescale or fall back to memory."""

    def __init__(self, *, account_id: str = "analytics") -> None:
        self._account_id = account_id
        self._lock = Lock()
        self._initialized: set[str] = set()
        self._memory: Dict[str, List[Dict[str, Any]]] = {}

    async def persist(self, snapshot: OrderflowSnapshot) -> None:
        record = {
            "symbol": snapshot.symbol,
            "buy_sell_imbalance": snapshot.buy_sell_imbalance,
            "depth_imbalance": snapshot.depth_imbalance,
            "liquidity_holes": snapshot.liquidity_holes,
            "impact_estimates": snapshot.impact_estimates,
            "bid_depth": snapshot.bid_depth,
            "ask_depth": snapshot.ask_depth,
            "ts": snapshot.ts,
        }

        if psycopg is None:
            self._append_memory(record)
            return

        from asyncio import to_thread

        await to_thread(self._persist_sync, record)

    # ------------------------------------------------------------------
    # Synchronous helpers guarded by async wrapper
    # ------------------------------------------------------------------
    def _persist_sync(self, record: Mapping[str, Any]) -> None:
        assert psycopg is not None and sql is not None  # nosec - guarded by caller

        try:
            session = get_timescale_session(self._account_id)
        except Exception as exc:  # pragma: no cover - environment misconfiguration
            LOGGER.warning("Unable to load Timescale session for %s: %s", self._account_id, exc)
            self._append_memory(record)
            return

        try:
            with psycopg.connect(session.dsn, autocommit=True) as conn:
                self._ensure_schema(conn, session.account_schema)
                insert_sql = sql.SQL(
                    """
                    INSERT INTO {}.{} (
                        ts,
                        symbol,
                        buy_sell_imbalance,
                        depth_imbalance,
                        liquidity_holes,
                        impact_estimates
                    )
                    VALUES (
                        %(ts)s,
                        %(symbol)s,
                        %(buy_sell_imbalance)s,
                        %(depth_imbalance)s,
                        %(liquidity_holes)s::jsonb,
                        %(impact_estimates)s::jsonb
                    )
                """
                ).format(
                    sql.Identifier(session.account_schema),
                    sql.Identifier("orderflow_metrics"),
                )
                params = {
                    "ts": record["ts"],
                    "symbol": record["symbol"],
                    "buy_sell_imbalance": record["buy_sell_imbalance"],
                    "depth_imbalance": record["depth_imbalance"],
                    "liquidity_holes": json.dumps(record["liquidity_holes"]),
                    "impact_estimates": json.dumps(record["impact_estimates"]),
                }
                with conn.cursor() as cursor:
                    cursor.execute(insert_sql, params)
        except Exception as exc:  # pragma: no cover - fallback for DB errors
            LOGGER.warning(
                "Failed to persist order flow metrics for %s: %s. Falling back to in-memory store.",
                record["symbol"],
                exc,
            )
            self._append_memory(record)

    def _ensure_schema(self, conn: "psycopg.Connection[Any]", schema: str) -> None:
        assert sql is not None  # nosec - guarded by caller
        with self._lock:
            if schema in self._initialized:
                return
            with conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}" ).format(sql.Identifier(schema))
                )
                cursor.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            ts TIMESTAMPTZ NOT NULL,
                            symbol TEXT NOT NULL,
                            buy_sell_imbalance DOUBLE PRECISION NOT NULL,
                            depth_imbalance DOUBLE PRECISION NOT NULL,
                            liquidity_holes JSONB NOT NULL,
                            impact_estimates JSONB NOT NULL
                        )
                        """
                    ).format(
                        sql.Identifier(schema),
                        sql.Identifier("orderflow_metrics"),
                    )
                )
            self._initialized.add(schema)

    def _append_memory(self, record: Mapping[str, Any]) -> None:
        with self._lock:
            history = self._memory.setdefault(record["symbol"], [])
            history.append(dict(record))

    # ------------------------------------------------------------------
    # Introspection helpers for tests/debugging
    # ------------------------------------------------------------------
    def history(self, symbol: str) -> List[Mapping[str, Any]]:
        return list(self._memory.get(symbol, []))


# ---------------------------------------------------------------------------
# Order flow analytics computation
# ---------------------------------------------------------------------------


def _compute_buy_sell_imbalance(trades: Iterable[Mapping[str, float]]) -> float:
    buy_volume = sum(trade["volume"] for trade in trades if trade.get("side") == "buy")
    sell_volume = sum(trade["volume"] for trade in trades if trade.get("side") == "sell")
    total = buy_volume + sell_volume
    if total <= 0:
        return 0.0
    imbalance = (buy_volume - sell_volume) / total
    return float(max(-1.0, min(1.0, imbalance)))


def _compute_depth_imbalance(order_book: Mapping[str, Iterable[Iterable[float]]]) -> tuple[float, float, float]:
    bids = order_book.get("bids", [])
    asks = order_book.get("asks", [])
    bid_depth = float(sum(level[1] for level in bids))
    ask_depth = float(sum(level[1] for level in asks))
    total = bid_depth + ask_depth
    if total <= 0:
        return 0.0, bid_depth, ask_depth
    imbalance = (bid_depth - ask_depth) / total
    return float(max(-1.0, min(1.0, imbalance))), bid_depth, ask_depth


def _detect_liquidity_holes(order_book: Mapping[str, Iterable[Iterable[float]]]) -> List[Dict[str, float]]:
    holes: List[Dict[str, float]] = []
    for side_name in ("bids", "asks"):
        levels = list(order_book.get(side_name, []))
        if len(levels) < 2:
            continue
        sizes = [level[1] for level in levels]
        median_depth = statistics.median(sizes)
        prev_size = sizes[0]
        for idx, size in enumerate(sizes[1:], start=2):
            if prev_size <= 0:
                prev_size = size
                continue
            drop_ratio = 1.0 - (size / prev_size)
            if drop_ratio > 0.45 and size < median_depth * 0.6:
                holes.append(
                    {
                        "side": side_name[:-1],
                        "level": float(idx),
                        "drop_ratio": round(drop_ratio, 4),
                        "depth": round(size, 6),
                    }
                )
            prev_size = size
    return holes


def _calc_mid(order_book: Mapping[str, Iterable[Iterable[float]]]) -> Optional[float]:
    bids = list(order_book.get("bids", []))
    asks = list(order_book.get("asks", []))
    if not bids or not asks:
        return None
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    return (best_bid + best_ask) / 2.0


def _estimate_slippage(
    levels: Iterable[Iterable[float]],
    target_qty: float,
    *,
    mid: float,
    side: str,
) -> Optional[float]:
    if target_qty <= 0 or mid <= 0:
        return 0.0

    remaining = float(target_qty)
    executed = 0.0
    cost = 0.0

    for price, size in levels:
        if remaining <= 0:
            break
        take = min(float(size), remaining)
        if take <= 0:
            continue
        cost += take * float(price)
        executed += take
        remaining -= take

    if executed <= 0:
        return None

    avg_price = cost / executed
    if side == "buy":
        slippage = (avg_price - mid) / mid * 10_000
    else:
        slippage = (mid - avg_price) / mid * 10_000

    if remaining > 0 and executed > 0:
        coverage = executed / max(target_qty, 1e-9)
        slippage += (1 - min(coverage, 1.0)) * 10_000

    return round(slippage, 4)


def _compute_market_impact(
    trades: Iterable[Mapping[str, float]],
    order_book: Mapping[str, Iterable[Iterable[float]]],
) -> Dict[str, Mapping[str, Optional[float]]]:
    volume = sum(trade["volume"] for trade in trades)
    if volume <= 0:
        depth_volume = sum(level[1] for level in order_book.get("asks", []))
        volume = max(depth_volume, 1.0)

    mid = _calc_mid(order_book)
    if mid is None or mid <= 0:
        return {"buy": {}, "sell": {}}

    percentages = [0.01, 0.05, 0.10]
    asks = list(order_book.get("asks", []))
    bids = list(order_book.get("bids", []))

    buy_impact: Dict[str, Optional[float]] = {}
    sell_impact: Dict[str, Optional[float]] = {}

    for pct in percentages:
        qty = volume * pct
        buy_impact[f"{int(pct * 100)}%"] = _estimate_slippage(asks, qty, mid=mid, side="buy")
        sell_impact[f"{int(pct * 100)}%"] = _estimate_slippage(bids, qty, mid=mid, side="sell")

    return {"buy": buy_impact, "sell": sell_impact}


# ---------------------------------------------------------------------------
# Service façade
# ---------------------------------------------------------------------------


class OrderflowService:
    """High level façade that orchestrates metric computation and storage."""

    def __init__(
        self,
        *,
        data_provider: Optional[MarketDataProvider] = None,
        store: Optional[OrderflowMetricsStore] = None,
    ) -> None:
        self._provider = data_provider or MarketDataProvider()
        self._store = store or OrderflowMetricsStore()

    async def snapshot(self, symbol: str, window: int) -> OrderflowSnapshot:
        if not symbol:
            raise HTTPException(status_code=400, detail="symbol must be provided")
        if window <= 0:
            raise HTTPException(status_code=400, detail="window must be positive")

        try:
            trades = self._provider.get_recent_trades(symbol, window)
            order_book = self._provider.get_order_book(symbol)
        except MarketDataUnavailable as exc:
            LOGGER.warning("Market data unavailable for %s: %s", symbol, exc)
            raise HTTPException(status_code=503, detail=f"market data unavailable for {symbol}") from exc

        buy_sell_imbalance = _compute_buy_sell_imbalance(trades)
        depth_imbalance, bid_depth, ask_depth = _compute_depth_imbalance(order_book)
        liquidity_holes = _detect_liquidity_holes(order_book)
        impact_estimates = _compute_market_impact(trades, order_book)
        ts = datetime.now(timezone.utc)

        snapshot = OrderflowSnapshot(
            symbol=symbol,
            window=window,
            buy_sell_imbalance=buy_sell_imbalance,
            depth_imbalance=depth_imbalance,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            liquidity_holes=liquidity_holes,
            impact_estimates=impact_estimates,
            ts=ts,
        )

        await self._store.persist(snapshot)
        return snapshot


# ---------------------------------------------------------------------------
# FastAPI router definitions
# ---------------------------------------------------------------------------


router = APIRouter(prefix="/orderflow", tags=["orderflow"])
_service = OrderflowService()


@router.get("/imbalance")
async def buy_sell_imbalance(
    *,
    symbol: str = Query(..., description="Market symbol to analyse"),
    window: int = Query(300, ge=1, le=3_600, description="Lookback window in seconds"),
    _: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    snapshot = await _service.snapshot(symbol, window)
    return {
        "symbol": snapshot.symbol,
        "window": snapshot.window,
        "buy_sell_imbalance": snapshot.buy_sell_imbalance,
        "ts": snapshot.ts.isoformat(),
    }


@router.get("/queue")
async def queue_depth(
    *,
    symbol: str = Query(..., description="Market symbol to analyse"),
    window: int = Query(300, ge=1, le=3_600, description="Lookback window in seconds"),
    _: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    snapshot = await _service.snapshot(symbol, window)
    return {
        "symbol": snapshot.symbol,
        "depth_imbalance": snapshot.depth_imbalance,
        "bid_depth": snapshot.bid_depth,
        "ask_depth": snapshot.ask_depth,
        "ts": snapshot.ts.isoformat(),
    }


@router.get("/liquidity_holes")
async def liquidity_holes(
    *,
    symbol: str = Query(..., description="Market symbol to analyse"),
    window: int = Query(300, ge=1, le=3_600, description="Lookback window in seconds"),
    _: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    snapshot = await _service.snapshot(symbol, window)
    return {
        "symbol": snapshot.symbol,
        "liquidity_holes": snapshot.liquidity_holes,
        "impact_estimates": snapshot.impact_estimates,
        "ts": snapshot.ts.isoformat(),
    }


app = FastAPI(title="Orderflow Analytics Service", version="1.0.0")


def _configure_session_store(application: FastAPI) -> SessionStoreProtocol:
    existing = getattr(application.state, "session_store", None)
    if isinstance(existing, SessionStoreProtocol):
        return existing

    redis_url = (os.getenv("SESSION_REDIS_URL") or "").strip()
    ttl_minutes = load_session_ttl_minutes()

    if not redis_url:
        LOGGER.info("SESSION_REDIS_URL not configured. Using in-memory session store.")
        store: SessionStoreProtocol = InMemorySessionStore(ttl_minutes=ttl_minutes)
    elif redis_url.lower().startswith("memory://"):
        store = InMemorySessionStore(ttl_minutes=ttl_minutes)
    else:
        store = build_session_store_from_url(redis_url, ttl_minutes=ttl_minutes)

    return store


@app.on_event("startup")
async def _initialize_session_store() -> None:
    store = _configure_session_store(app)
    app.state.session_store = store
    security.set_default_session_store(store)

app.include_router(router)


__all__ = [
    "app",
    "router",
    "OrderflowService",
    "OrderflowMetricsStore",
    "MarketDataProvider",
]
