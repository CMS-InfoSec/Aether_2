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
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING, cast

if TYPE_CHECKING:  # pragma: no cover - imported for static analysis only
    class _PolicyBase:  # pylint: disable=too-few-public-methods
        def generate(self, timestamp: datetime, market_state: Dict[str, Any]) -> Iterable[Any]:
            ...

        def reset(self) -> None:
            ...

    import pandas as pd
else:  # pragma: no cover - lightweight fallbacks for runtime when deps missing

    class _PolicyBase:  # pylint: disable=too-few-public-methods
        """Minimal policy stub so optional imports remain type-safe."""

        def generate(self, timestamp: datetime, market_state: Dict[str, Any]) -> Iterable[Any]:
            return []

        def reset(self) -> None:
            return None

    try:
        import pandas as pd
    except ImportError:  # pragma: no cover - pandas is optional for tests
        pd = None


def _load_policy_base() -> "type[_PolicyBase]":
    """Return the backtest policy base class if the dependency is available."""

    try:
        from backtest_engine import Policy as policy_cls
    except ImportError:
        return _PolicyBase
    return cast("type[_PolicyBase]", policy_cls)


_ACTUAL_POLICY = _load_policy_base()


def _load_backtest_dependencies() -> tuple[Any, Any, Any]:
    """Resolve optional backtest engine classes when available."""

    try:
        from backtest_engine import Backtester as backtester_cls
        from backtest_engine import FeeSchedule as fee_schedule_cls
        from backtest_engine import OrderIntent as order_intent_cls
    except ImportError as exc:  # pragma: no cover - exercised in integration tests
        raise RuntimeError("Backtest engine dependencies are unavailable") from exc
    return backtester_cls, fee_schedule_cls, order_intent_cls


def _to_backtest_timestamp(value: datetime) -> Any:
    """Return a pandas timestamp when the dependency is available."""

    pandas_module = globals().get("pd")
    if pandas_module is not None:
        return pandas_module.Timestamp(value)
    return value


def _offset_backtest_timestamp(value: datetime, *, milliseconds: int) -> Any:
    """Offset a timestamp for backtest events without importing pandas eagerly."""

    pandas_module = globals().get("pd")
    if pandas_module is not None:
        return pandas_module.Timestamp(value) + pandas_module.Timedelta(
            milliseconds=milliseconds
        )
    return value + timedelta(milliseconds=milliseconds)


class _SingleOrderPolicy(_PolicyBase):
    """Policy wrapper that emits a single :class:`OrderIntent`."""

    def __init__(self, intent: object) -> None:
        self._intent = intent
        self._emitted = False

    def generate(
        self, timestamp: datetime, market_state: Dict[str, Any]
    ) -> Iterable[object]:  # pragma: no cover - exercised via Backtester
        if not self._emitted:
            self._emitted = True
            yield self._intent

    def reset(self) -> None:  # pragma: no cover - Backtester will call
        self._emitted = False


if not TYPE_CHECKING and _ACTUAL_POLICY is not _PolicyBase:
    _SingleOrderPolicy.__bases__ = (_ACTUAL_POLICY,)


@dataclass
class _FillSnapshot:
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    timestamp: datetime
    fee: Decimal = Decimal("0")
    slippage_bps: Decimal = Decimal("0")


def _to_decimal(value: Decimal | float | int | str | None) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal("0")
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    return Decimal(value)


def _decimal_to_str(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


class _ShadowPnLTracker:
    """Track realized PnL, fees and open positions for real vs shadow fills."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._buckets: Dict[bool, Dict[str, Any]] = {
            False: {
                "fills": [],
                "positions": {},
                "realized_pnl": Decimal("0"),
                "fees": Decimal("0"),
                "slippage": Decimal("0"),
            },
            True: {
                "fills": [],
                "positions": {},
                "realized_pnl": Decimal("0"),
                "fees": Decimal("0"),
                "slippage": Decimal("0"),
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
        fee: Decimal | float = Decimal("0"),
        slippage_bps: Decimal | float = Decimal("0"),
    ) -> None:
        ts = timestamp or datetime.now(timezone.utc)
        qty = _to_decimal(quantity)
        px = _to_decimal(price)
        snapshot = _FillSnapshot(
            symbol=symbol,
            side=side.lower(),
            quantity=qty,
            price=px,
            timestamp=ts,
            fee=_to_decimal(fee or Decimal("0")),
            slippage_bps=_to_decimal(slippage_bps or Decimal("0")),
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
            real_bucket = self._format_bucket(False)
            shadow_bucket = self._format_bucket(True)
            real_values = self._buckets[False]
            shadow_values = self._buckets[True]
            delta = {
                "realized_pnl": _decimal_to_str(
                    shadow_values["realized_pnl"] - real_values["realized_pnl"]
                ),
                "fees": _decimal_to_str(shadow_values["fees"] - real_values["fees"]),
                "slippage": _decimal_to_str(
                    shadow_values["slippage"] - real_values["slippage"]
                ),
            }
        return {"real": real_bucket, "shadow": shadow_bucket, "delta": delta}

    def reset(self) -> None:
        with self._lock:
            for bucket in self._buckets.values():
                bucket["fills"].clear()
                bucket["positions"].clear()
                bucket["realized_pnl"] = Decimal("0")
                bucket["fees"] = Decimal("0")
                bucket["slippage"] = Decimal("0")

    def _format_bucket(self, shadow: bool) -> Dict[str, Any]:
        bucket = self._buckets[shadow]
        positions = {
            symbol: {
                "quantity": _decimal_to_str(position["quantity"]),
                "avg_price": _decimal_to_str(position["avg_price"]),
            }
            for symbol, position in bucket["positions"].items()
        }
        fills = [
            {
                "symbol": fill.symbol,
                "side": fill.side,
                "quantity": _decimal_to_str(fill.quantity),
                "price": _decimal_to_str(fill.price),
                "timestamp": fill.timestamp.isoformat(),
                "fee": _decimal_to_str(fill.fee),
                "slippage_bps": _decimal_to_str(fill.slippage_bps),
            }
            for fill in bucket["fills"]
        ]
        return {
            "realized_pnl": _decimal_to_str(bucket["realized_pnl"]),
            "fees": _decimal_to_str(bucket["fees"]),
            "slippage": _decimal_to_str(bucket["slippage"]),
            "positions": positions,
            "fills": fills,
        }

    def _apply_to_positions(
        self, positions: Dict[str, Dict[str, Decimal]], fill: _FillSnapshot
    ) -> Decimal:
        zero = Decimal("0")
        book = positions.setdefault(
            fill.symbol, {"quantity": zero, "avg_price": zero}
        )
        qty = fill.quantity
        price = fill.price
        realized = Decimal("0")

        if fill.side == "buy":
            if book["quantity"] < zero:
                close_qty = min(qty, -book["quantity"])
                if close_qty > zero:
                    realized += (book["avg_price"] - price) * close_qty
                    book["quantity"] += close_qty
                    qty -= close_qty
                    if book["quantity"] == zero:
                        book["avg_price"] = zero
            if qty > zero:
                existing_qty = book["quantity"] if book["quantity"] > zero else zero
                total_cost = existing_qty * book["avg_price"] + price * qty
                book["quantity"] += qty
                if book["quantity"] > zero:
                    book["avg_price"] = total_cost / book["quantity"]
        else:
            if book["quantity"] > zero:
                close_qty = min(qty, book["quantity"])
                if close_qty > zero:
                    realized += (price - book["avg_price"]) * close_qty
                    book["quantity"] -= close_qty
                    qty -= close_qty
                    if book["quantity"] <= zero:
                        book["avg_price"] = zero
            if qty > zero:
                existing_qty = (-book["quantity"] if book["quantity"] < zero else zero)
                total_proceeds = existing_qty * book["avg_price"] + price * qty
                book["quantity"] -= qty
                if book["quantity"] < zero:
                    book["avg_price"] = total_proceeds / (-book["quantity"])

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
        fee: Decimal | float = Decimal("0"),
        slippage_bps: Decimal | float = Decimal("0"),
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
                    "qty": _decimal_to_str(fill.quantity),
                    "price": _decimal_to_str(fill.price),
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
        qty_decimal = _to_decimal(quantity)
        if qty_decimal <= 0:
            return []
        px_decimal = _to_decimal(price)
        backtester_cls, fee_schedule_cls, order_intent_cls = _load_backtest_dependencies()
        qty = float(qty_decimal)
        px = float(px_decimal)
        ts = timestamp or datetime.now(timezone.utc)

        order_intent = order_intent_cls(
            side=side.lower(),
            quantity=qty,
            price=px,
            order_type="limit",
            time_in_force="GTC",
        )
        policy = _SingleOrderPolicy(order_intent)
        bar_event = {
            "timestamp": _to_backtest_timestamp(ts),
            "type": "bar",
            "open": px,
            "high": px,
            "low": px,
            "close": px,
            "volume": qty,
        }
        book_event = {
            "timestamp": _offset_backtest_timestamp(ts, milliseconds=1),
            "type": "book",
            "bid": px if side.lower() == "sell" else px * 0.999,
            "ask": px if side.lower() == "buy" else px * 1.001,
            "bid_size": qty * 10.0,
            "ask_size": qty * 10.0,
        }
        backtester = backtester_cls(
            bar_events=[bar_event],
            book_events=[book_event],
            policy=policy,
            fee_schedule=fee_schedule_cls(maker=0.0, taker=0.0),
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
                    quantity=_to_decimal(fill.quantity),
                    price=_to_decimal(fill.price),
                    timestamp=timestamp_value,
                    fee=_to_decimal(fill.fee),
                    slippage_bps=_to_decimal(fill.slippage_bps),
                )
            )
        return fills


shadow_oms = ShadowOMS()


__all__ = ["ShadowOMS", "shadow_oms", "_ShadowPnLTracker"]

