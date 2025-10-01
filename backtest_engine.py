"""Simplified backtest engine with order-book execution logic.

This module provides a :class:`Backtester` that consumes historical bar and
order-book event streams together with an intent generator (``Policy``).  The
implementation focuses on the bookkeeping necessary for analysing execution
quality – queue position, maker/taker routing, fee attribution and PnL curves –
while leaving concrete strategy or data integrations to be implemented by the
caller.

The goal of this file is to offer a well documented, NumPy/pandas powered
backtesting harness that can later be extended with production ready data
adapters.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Protocol, Tuple

import numpy as np
import pandas as pd


class Policy(Protocol):
    """Minimal protocol expected from a strategy intent generator.

    Implementations should inspect the current market state and return an
    iterable of :class:`OrderIntent` objects describing the desired actions.
    """

    def generate(self, timestamp: pd.Timestamp, market_state: Dict[str, Any]) -> Iterable["OrderIntent"]:
        ...


@dataclass
class FeeSchedule:
    """Simple maker/taker fee representation (expressed in decimals)."""

    maker: float = 0.0
    taker: float = 0.0


@dataclass
class OrderIntent:
    """Instruction emitted by a :class:`Policy` instance."""

    side: str  # "buy" or "sell"
    quantity: float
    price: Optional[float] = None
    order_type: str = "limit"  # "limit" or "market"
    allow_partial: bool = True
    queue_position: float = 0.0  # 0 == front of queue, 1 == back of queue

    def __post_init__(self) -> None:
        self.side = self.side.lower()
        if self.order_type not in {"limit", "market"}:
            raise ValueError(f"Unsupported order_type: {self.order_type}")


@dataclass
class Order:
    """Internal order representation used while simulating execution."""

    order_id: int
    intent: OrderIntent
    timestamp: pd.Timestamp
    remaining: float
    filled: float = 0.0
    status: str = "open"  # "open", "filled", "cancelled"
    is_taker: bool = False
    fee_paid: float = 0.0

    def record_fill(self, quantity: float) -> None:
        self.remaining = max(self.remaining - quantity, 0.0)
        self.filled += quantity
        if self.remaining <= 0:
            self.status = "filled"


@dataclass
class Fill:
    order_id: int
    timestamp: pd.Timestamp
    side: str
    price: float
    quantity: float
    fee: float
    liquidity_flag: str  # "maker" or "taker"


class Backtester:
    """Order-book based execution simulator producing portfolio statistics."""

    def __init__(
        self,
        bar_events: Iterable[Dict[str, Any]],
        book_events: Iterable[Dict[str, Any]],
        policy: Policy,
        fee_schedule: Optional[FeeSchedule] = None,
        slippage_bps: float = 0.0,
        initial_cash: float = 0.0,
    ) -> None:
        self.policy = policy
        self.fee_schedule = fee_schedule or FeeSchedule()
        self.slippage_bps = slippage_bps
        self.initial_cash = initial_cash

        self._orders: Dict[int, Order] = {}
        self._fills: List[Fill] = []
        self._equity_curve: List[Tuple[pd.Timestamp, float]] = []
        self._order_id_counter: int = 0

        self._bar_events = list(self._normalise_stream(bar_events, "bar"))
        self._book_events = list(self._normalise_stream(book_events, "book"))
        self._events = sorted(self._bar_events + self._book_events, key=lambda item: item["timestamp"])

        self._position: float = 0.0
        self._cash: float = float(initial_cash)

        self._best_bid: Optional[float] = None
        self._best_ask: Optional[float] = None
        self._bid_size: float = 0.0
        self._ask_size: float = 0.0

    @staticmethod
    def _normalise_stream(events: Iterable[Dict[str, Any]], event_type: str) -> Iterator[Dict[str, Any]]:
        """Ensure each event has a ``timestamp`` and ``type`` key."""

        for payload in events:
            record = dict(payload)
            if "timestamp" not in record:
                raise KeyError(f"{event_type} event missing 'timestamp': {payload}")
            if not isinstance(record["timestamp"], pd.Timestamp):
                record["timestamp"] = pd.Timestamp(record["timestamp"])
            record.setdefault("type", event_type)
            yield record

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self) -> Dict[str, float]:
        """Execute the simulation and return aggregated performance metrics."""

        for event in self._events:
            timestamp: pd.Timestamp = event["timestamp"]
            if event["type"] == "book":
                self._handle_book_event(event)
            elif event["type"] == "bar":
                self._handle_bar_event(event)

            self._mark_to_market(timestamp)

        metrics = self._compute_performance()
        return metrics

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------
    def _handle_book_event(self, event: Dict[str, Any]) -> None:
        self._best_bid = event.get("bid")
        self._best_ask = event.get("ask")
        self._bid_size = float(event.get("bid_size", np.inf))
        self._ask_size = float(event.get("ask_size", np.inf))

        if self._best_bid is None or self._best_ask is None:
            return

        self._match_orders(event["timestamp"])

    def _handle_bar_event(self, event: Dict[str, Any]) -> None:
        timestamp = event["timestamp"]
        market_state = {
            "bar": event,
            "best_bid": self._best_bid,
            "best_ask": self._best_ask,
            "position": self._position,
            "cash": self._cash,
        }
        intents = list(self.policy.generate(timestamp, market_state))
        for intent in intents:
            self._submit_order(intent, timestamp)

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------
    def _submit_order(self, intent: OrderIntent, timestamp: pd.Timestamp) -> None:
        self._order_id_counter += 1
        order = Order(
            order_id=self._order_id_counter,
            intent=intent,
            timestamp=timestamp,
            remaining=float(intent.quantity),
        )
        # Market orders execute immediately as taker
        if intent.order_type == "market":
            order.is_taker = True
            self._orders[order.order_id] = order
            self._execute_market_order(order, timestamp)
        else:
            self._orders[order.order_id] = order
            self._attempt_limit_cross(order, timestamp)

    def _execute_market_order(self, order: Order, timestamp: pd.Timestamp) -> None:
        if self._best_bid is None or self._best_ask is None:
            # No liquidity snapshot – treat as no fill but keep order open for future book events
            return

        target_price = self._best_ask if order.intent.side == "buy" else self._best_bid
        slipped_price = self._apply_slippage(target_price, order.intent.side)
        liquidity_flag = "taker"
        available = self._ask_size if order.intent.side == "buy" else self._bid_size
        fill_qty = min(order.remaining, available)
        if fill_qty <= 0:
            return

        self._register_fill(order, timestamp, slipped_price, fill_qty, liquidity_flag)

    def _attempt_limit_cross(self, order: Order, timestamp: pd.Timestamp) -> None:
        if self._best_bid is None or self._best_ask is None:
            return

        crossed = False
        target_price: Optional[float] = None
        liquidity_flag = "maker"

        if order.intent.side == "buy":
            if order.intent.price is not None and order.intent.price >= self._best_ask:
                crossed = True
                target_price = self._best_ask
        else:  # sell
            if order.intent.price is not None and order.intent.price <= self._best_bid:
                crossed = True
                target_price = self._best_bid

        if not crossed:
            return

        # If the order crosses we treat it as taking liquidity unless queue position indicates maker participation.
        maker_fraction = max(0.0, min(1.0, 1.0 - order.intent.queue_position))
        taker_qty = order.remaining * (1.0 - maker_fraction)
        maker_qty = order.remaining - taker_qty

        fills: List[Tuple[float, str]] = []
        if taker_qty > 0:
            liquidity_flag = "taker"
            fills.append((taker_qty, liquidity_flag))
        if maker_qty > 0:
            liquidity_flag = "maker"
            fills.append((maker_qty, liquidity_flag))

        for qty, flag in fills:
            if qty <= 0:
                continue
            available = self._ask_size if order.intent.side == "buy" else self._bid_size
            available *= max(0.0, 1.0 - order.intent.queue_position)
            fill_qty = min(qty, available)
            if fill_qty <= 0:
                continue
            if fill_qty < qty and not order.intent.allow_partial:
                continue
            exec_price = target_price if target_price is not None else self._best_ask
            if flag == "taker":
                exec_price = self._apply_slippage(exec_price, order.intent.side)
            self._register_fill(order, timestamp, exec_price, fill_qty, flag)

    def _match_orders(self, timestamp: pd.Timestamp) -> None:
        for order in list(self._orders.values()):
            if order.status != "open":
                continue
            if order.intent.order_type == "market":
                self._execute_market_order(order, timestamp)
            else:
                self._attempt_limit_cross(order, timestamp)

    def _register_fill(
        self,
        order: Order,
        timestamp: pd.Timestamp,
        price: float,
        quantity: float,
        liquidity_flag: str,
    ) -> None:
        if quantity <= 0:
            return

        fee_rate = self.fee_schedule.taker if liquidity_flag == "taker" else self.fee_schedule.maker
        fee = abs(price * quantity) * fee_rate
        self._fills.append(
            Fill(
                order_id=order.order_id,
                timestamp=timestamp,
                side=order.intent.side,
                price=price,
                quantity=quantity,
                fee=fee,
                liquidity_flag=liquidity_flag,
            )
        )

        order.record_fill(quantity)
        order.fee_paid += fee
        if order.status == "filled":
            self._orders.pop(order.order_id, None)

        if order.intent.side == "buy":
            self._position += quantity
            self._cash -= price * quantity + fee
        else:
            self._position -= quantity
            self._cash += price * quantity - fee

    # ------------------------------------------------------------------
    # Portfolio analytics
    # ------------------------------------------------------------------
    def _apply_slippage(self, price: float, side: str) -> float:
        adjustment = price * (self.slippage_bps / 10_000.0)
        return price + adjustment if side == "buy" else price - adjustment

    def _mark_to_market(self, timestamp: pd.Timestamp) -> None:
        if self._best_bid is None or self._best_ask is None:
            equity = self._cash
        else:
            mid_price = (self._best_bid + self._best_ask) / 2.0
            equity = self._cash + self._position * mid_price
        self._equity_curve.append((timestamp, equity))

    def _compute_performance(self) -> Dict[str, float]:
        if not self._equity_curve:
            return {
                "sharpe": 0.0,
                "max_drawdown": 0.0,
                "fee_spend": 0.0,
                "turnover": 0.0,
            }

        curve = pd.DataFrame(self._equity_curve, columns=["timestamp", "equity"]).set_index("timestamp").sort_index()
        returns = curve["equity"].pct_change().fillna(0.0)
        sharpe = self._sharpe_ratio(returns)
        max_dd = self._max_drawdown(curve["equity"])
        fee_spend = float(sum(fill.fee for fill in self._fills))
        turnover = float(sum(abs(fill.quantity) for fill in self._fills))
        return {
            "sharpe": sharpe,
            "max_drawdown": max_dd,
            "fee_spend": fee_spend,
            "turnover": turnover,
        }

    @staticmethod
    def _sharpe_ratio(returns: pd.Series, periods: int = 252) -> float:
        if returns.std(ddof=0) == 0:
            return 0.0
        return np.sqrt(periods) * returns.mean() / returns.std(ddof=0)

    @staticmethod
    def _max_drawdown(equity: pd.Series) -> float:
        running_max = equity.cummax()
        drawdown = equity / running_max - 1.0
        return float(drawdown.min())

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------
    @property
    def fills(self) -> pd.DataFrame:
        return pd.DataFrame([fill.__dict__ for fill in self._fills])

    @property
    def equity_curve(self) -> pd.DataFrame:
        return pd.DataFrame(self._equity_curve, columns=["timestamp", "equity"]).set_index("timestamp")

    @property
    def orders(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                "order_id": order.order_id,
                "side": order.intent.side,
                "quantity": order.intent.quantity,
                "filled": order.filled,
                "remaining": order.remaining,
                "status": order.status,
                "fee_paid": order.fee_paid,
                "timestamp": order.timestamp,
            }
            for order in self._orders.values()
        ])
