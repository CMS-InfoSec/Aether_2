"""Shadow OMS utilities for simulating fills alongside live execution.

This module keeps the core OMS implementation lean while providing a
best-effort shadow execution path that mirrors every live order.  A lightweight
policy is fed into :mod:`backtest_engine` so that the simulator produces the
expected fills, fees and slippage attribution.  Results are persisted via the
``TimescaleAdapter`` and an in-memory tracker that powers the
``/oms/shadow_pnl`` diagnostic endpoint.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from backtest_engine import Backtester, FeeSchedule, OrderIntent, Policy


class _SingleOrderPolicy(Policy):
    """Policy wrapper that emits a single :class:`OrderIntent`."""

    def __init__(self, intent: OrderIntent) -> None:
        self._intent = intent
        self._emitted = False

    def generate(
        self, timestamp: pd.Timestamp, market_state: Dict[str, Any]
    ) -> Iterable[OrderIntent]:  # pragma: no cover - exercised via Backtester
        if not self._emitted:
            self._emitted = True
            yield self._intent
        return []

    def reset(self) -> None:  # pragma: no cover - Backtester will call
        self._emitted = False


@dataclass
class _FillSnapshot:
    symbol: str
    side: str
    quantity: float
    price: float
    timestamp: datetime
    fee: float = 0.0
    slippage_bps: float = 0.0


class _ShadowPnLTracker:
    """Track realized PnL, fees and open positions for real vs shadow fills."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._buckets: Dict[bool, Dict[str, Any]] = {
            False: {
                "fills": [],
                "positions": {},
                "realized_pnl": 0.0,
                "fees": 0.0,
                "slippage": 0.0,
            },
            True: {
                "fills": [],
                "positions": {},
                "realized_pnl": 0.0,
                "fees": 0.0,
                "slippage": 0.0,
            },
        }

    def record_fill(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Decimal | float,
        price: Decimal | float,
        shadow: bool,
        timestamp: Optional[datetime] = None,
        fee: float = 0.0,
        slippage_bps: float = 0.0,
    ) -> None:
        ts = timestamp or datetime.now(timezone.utc)
        qty = float(quantity)
        px = float(price)
        snapshot = _FillSnapshot(
            symbol=symbol,
            side=side.lower(),
            quantity=qty,
            price=px,
            timestamp=ts,
            fee=float(fee or 0.0),
            slippage_bps=float(slippage_bps or 0.0),
        )

        with self._lock:
            bucket = self._buckets[shadow]
            bucket["fills"].append(snapshot)
            realized = self._apply_to_positions(bucket["positions"], snapshot)
            bucket["realized_pnl"] += realized
            bucket["fees"] += snapshot.fee
            bucket["slippage"] += snapshot.slippage_bps

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            real = self._format_bucket(False)
            shadow = self._format_bucket(True)
        delta = {
            "realized_pnl": shadow["realized_pnl"] - real["realized_pnl"],
            "fees": shadow["fees"] - real["fees"],
            "slippage": shadow["slippage"] - real["slippage"],
        }
        return {"real": real, "shadow": shadow, "delta": delta}

    def reset(self) -> None:
        with self._lock:
            for bucket in self._buckets.values():
                bucket["fills"].clear()
                bucket["positions"].clear()
                bucket["realized_pnl"] = 0.0
                bucket["fees"] = 0.0
                bucket["slippage"] = 0.0

    def _format_bucket(self, shadow: bool) -> Dict[str, Any]:
        bucket = self._buckets[shadow]
        positions = {
            symbol: {
                "quantity": position["quantity"],
                "avg_price": position["avg_price"],
            }
            for symbol, position in bucket["positions"].items()
        }
        fills = [
            {
                "symbol": fill.symbol,
                "side": fill.side,
                "quantity": fill.quantity,
                "price": fill.price,
                "timestamp": fill.timestamp.isoformat(),
                "fee": fill.fee,
                "slippage_bps": fill.slippage_bps,
            }
            for fill in bucket["fills"]
        ]
        return {
            "realized_pnl": bucket["realized_pnl"],
            "fees": bucket["fees"],
            "slippage": bucket["slippage"],
            "positions": positions,
            "fills": fills,
        }

    def _apply_to_positions(self, positions: Dict[str, Dict[str, float]], fill: _FillSnapshot) -> float:
        book = positions.setdefault(fill.symbol, {"quantity": 0.0, "avg_price": 0.0})
        qty = fill.quantity
        price = fill.price
        realized = 0.0

        if fill.side == "buy":
            if book["quantity"] < 0:
                close_qty = min(qty, -book["quantity"])
                realized += (book["avg_price"] - price) * close_qty
                book["quantity"] += close_qty
                qty -= close_qty
                if book["quantity"] == 0:
                    book["avg_price"] = 0.0
            if qty > 0:
                total_cost = max(book["quantity"], 0.0) * book["avg_price"] + price * qty
                book["quantity"] += qty
                if book["quantity"] > 0:
                    book["avg_price"] = total_cost / book["quantity"]
        else:
            if book["quantity"] > 0:
                close_qty = min(qty, book["quantity"])
                realized += (price - book["avg_price"]) * close_qty
                book["quantity"] -= close_qty
                qty -= close_qty
                if book["quantity"] <= 0:
                    book["avg_price"] = 0.0
            if qty > 0:
                total_proceeds = abs(min(book["quantity"], 0.0)) * book["avg_price"] + price * qty
                book["quantity"] -= qty
                if book["quantity"] < 0:
                    book["avg_price"] = total_proceeds / abs(book["quantity"])

        return realized


class ShadowOMS:
    """Coordinator responsible for generating and tracking shadow orders."""

    def __init__(self) -> None:
        self._trackers: Dict[str, _ShadowPnLTracker] = {}
        self._shadow_fills: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = Lock()

    def reset(self, account_id: Optional[str] = None) -> None:
        with self._lock:
            if account_id is None:
                self._trackers.clear()
                self._shadow_fills.clear()
                return
            self._trackers.pop(account_id, None)
            self._shadow_fills.pop(account_id, None)

    def record_real_fill(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str,
        quantity: Decimal | float,
        price: Decimal | float,
        timestamp: Optional[datetime] = None,
        fee: float = 0.0,
        slippage_bps: float = 0.0,
    ) -> None:
        tracker = self._get_tracker(account_id)
        tracker.record_fill(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            shadow=False,
            timestamp=timestamp,
            fee=fee,
            slippage_bps=slippage_bps,
        )

    def generate_shadow_fills(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str,
        quantity: Decimal | float,
        price: Decimal | float,
        timestamp: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        tracker = self._get_tracker(account_id)
        fills = self._simulate(symbol=symbol, side=side, quantity=quantity, price=price, timestamp=timestamp)
        records: List[Dict[str, Any]] = []
        with self._lock:
            store = self._shadow_fills.setdefault(account_id, [])
            for fill in fills:
                tracker.record_fill(
                    symbol=symbol,
                    side=fill.side,
                    quantity=fill.quantity,
                    price=fill.price,
                    shadow=True,
                    timestamp=fill.timestamp,
                    fee=fill.fee,
                    slippage_bps=fill.slippage_bps,
                )
                record = {
                    "account_id": account_id,
                    "symbol": symbol,
                    "qty": fill.quantity,
                    "price": fill.price,
                    "ts": fill.timestamp.isoformat(),
                }
                store.append(record)
                records.append(record)
        return records

    def snapshot(self, account_id: str) -> Dict[str, Any]:
        tracker = self._trackers.get(account_id)
        if tracker is None:
            tracker = self._get_tracker(account_id)
        return tracker.snapshot()

    def shadow_fills(self, account_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._shadow_fills.get(account_id, []))

    def _get_tracker(self, account_id: str) -> _ShadowPnLTracker:
        with self._lock:
            tracker = self._trackers.get(account_id)
            if tracker is None:
                tracker = _ShadowPnLTracker()
                self._trackers[account_id] = tracker
            return tracker

    def _simulate(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Decimal | float,
        price: Decimal | float,
        timestamp: Optional[datetime],
    ) -> List[_FillSnapshot]:
        qty = float(quantity)
        if qty <= 0:
            return []
        px = float(price)
        ts = timestamp or datetime.now(timezone.utc)
        order_intent = OrderIntent(
            side=side.lower(),
            quantity=qty,
            price=px,
            order_type="limit",
            time_in_force="GTC",
        )
        policy = _SingleOrderPolicy(order_intent)
        bar_event = {
            "timestamp": pd.Timestamp(ts),
            "type": "bar",
            "open": px,
            "high": px,
            "low": px,
            "close": px,
            "volume": qty,
        }
        book_event = {
            "timestamp": pd.Timestamp(ts) + pd.Timedelta(milliseconds=1),
            "type": "book",
            "bid": px if side.lower() == "sell" else px * 0.999,
            "ask": px if side.lower() == "buy" else px * 1.001,
            "bid_size": qty * 10.0,
            "ask_size": qty * 10.0,
        }
        backtester = Backtester(
            bar_events=[bar_event],
            book_events=[book_event],
            policy=policy,
            fee_schedule=FeeSchedule(maker=0.0, taker=0.0),
            slippage_bps=0.0,
            initial_cash=0.0,
        )
        backtester.run()
        state = backtester._last_state  # pragma: no cover - documented attribute
        fills: List[_FillSnapshot] = []
        if state is None:
            return fills
        for fill in state.fills:
            timestamp_value = (
                fill.timestamp.to_pydatetime().replace(tzinfo=timezone.utc)
                if fill.timestamp.tzinfo is None
                else fill.timestamp.to_pydatetime()
            )
            fills.append(
                _FillSnapshot(
                    symbol=symbol,
                    side=fill.side,
                    quantity=float(fill.quantity),
                    price=float(fill.price),
                    timestamp=timestamp_value,
                    fee=float(fill.fee),
                    slippage_bps=float(fill.slippage_bps),
                )
            )
        return fills


shadow_oms = ShadowOMS()


__all__ = ["ShadowOMS", "shadow_oms", "_ShadowPnLTracker"]

