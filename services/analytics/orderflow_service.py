"""Order flow analytics API surface.

This module provides lightweight analytical endpoints derived from recent
trading activity and order book depth.  The implementation is intentionally
self contained so it can operate in environments without direct market data
feeds or a live TimescaleDB instance.  When the optional psycopg dependency is
available the computed metrics are persisted into an ``orderflow_metrics``
hypertable for historical analysis.  If psycopg is unavailable, the service
falls back to an in-memory store which keeps the API functional for unit tests
and local development sessions.
"""

from __future__ import annotations

import json
import logging
import math
import statistics
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

from fastapi import APIRouter, HTTPException, Query

from services.common.config import get_timescale_session

try:  # pragma: no cover - optional dependency during CI
    import psycopg
    from psycopg import sql
except Exception:  # pragma: no cover - gracefully degrade without psycopg
    psycopg = None  # type: ignore
    sql = None  # type: ignore


LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Synthetic market data provider
# ---------------------------------------------------------------------------


class MarketDataProvider:
    """Deterministic synthetic market data used when live feeds are absent."""

    def __init__(self) -> None:
        self._cache: MutableMapping[str, Dict[str, Any]] = {}

    @staticmethod
    def _base_price(symbol: str) -> float:
        seed = sum(ord(char) for char in symbol.upper())
        return max(25.0, 100.0 + (seed % 5_000))

    @staticmethod
    def _rng(symbol: str, window: int) -> "random.Random":
        import random

        today_bucket = datetime.now(timezone.utc).strftime("%Y%m%d%H")
        return random.Random(f"{symbol}:{window}:{today_bucket}")

    def get_recent_trades(self, symbol: str, window: int) -> List[Dict[str, float]]:
        rng = self._rng(symbol, window)
        base_price = self._base_price(symbol)
        bias = math.sin(len(symbol)) * 0.05
        trades: List[Dict[str, float]] = []
        for _ in range(max(10, min(120, window // 5))):
            side = "buy" if rng.random() < 0.5 + bias else "sell"
            volume = round(rng.uniform(0.05, 5.0), 6)
            price = round(base_price * (1 + rng.uniform(-0.0015, 0.0015)), 2)
            trades.append({"side": side, "volume": volume, "price": price})
        return trades

    def get_order_book(self, symbol: str) -> Dict[str, List[List[float]]]:
        window = 300
        rng = self._rng(symbol, window)
        base_price = self._base_price(symbol)
        spread = max(0.5, base_price * 0.0008)
        levels = 10

        bids: List[List[float]] = []
        asks: List[List[float]] = []
        depth_scale = 1.0 + len(symbol) / 10.0

        for level in range(levels):
            decay = 0.88 ** level
            bid_price = round(base_price - spread * (level + 1), 2)
            ask_price = round(base_price + spread * (level + 1), 2)
            bid_size = round(max(0.05, rng.uniform(0.5, 8.0) * depth_scale * decay), 6)
            ask_size = round(max(0.05, rng.uniform(0.5, 8.0) * depth_scale * decay), 6)
            bids.append([bid_price, bid_size])
            asks.append([ask_price, ask_size])

        return {"bids": bids, "asks": asks}


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

        trades = self._provider.get_recent_trades(symbol, window)
        order_book = self._provider.get_order_book(symbol)

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
) -> Dict[str, Any]:
    snapshot = await _service.snapshot(symbol, window)
    return {
        "symbol": snapshot.symbol,
        "liquidity_holes": snapshot.liquidity_holes,
        "impact_estimates": snapshot.impact_estimates,
        "ts": snapshot.ts.isoformat(),
    }


__all__ = [
    "router",
    "OrderflowService",
    "OrderflowMetricsStore",
    "MarketDataProvider",
]
