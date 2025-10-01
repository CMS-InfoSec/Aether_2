"""Advanced backtest engine with order-book execution, fee tiers and stress testing.

This module exposes a :class:`Backtester` that replays bar and book events while
consulting a :class:`Policy` for trading intents.  The simulator aims to mimic
realistic exchange behaviour including queue positioning, time-in-force rules,
stop logic and halts.  It also provides stress scenario tooling and rich
performance attribution so that downstream unit tests can focus on individual
components.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterable, Iterator, List, Optional, Protocol, Tuple

import numpy as np
import pandas as pd


class Policy(Protocol):
    """Minimal protocol expected from a strategy intent generator."""

    def generate(self, timestamp: pd.Timestamp, market_state: Dict[str, Any]) -> Iterable["OrderIntent"]:
        ...

    def reset(self) -> None:  # pragma: no cover - optional protocol
        """Optional hook allowing stateful policies to reset between runs."""
        raise NotImplementedError


@dataclass
class FeeSchedule:
    """Maker/taker fee schedule expressed in decimals."""

    maker: float = 0.0
    taker: float = 0.0


@dataclass
class FeeTier:
    """Rolling notional fee tier configuration."""

    threshold: float
    schedule: FeeSchedule


@dataclass
class OrderIntent:
    """Instruction emitted by a :class:`Policy` instance."""

    side: str  # "buy" or "sell"
    quantity: float
    price: Optional[float] = None
    order_type: str = "limit"  # "limit" or "market"
    time_in_force: str = "GTC"  # "GTC", "IOC", "FOK"
    allow_partial: bool = True
    queue_position: float = 0.0  # 0 == front of queue, 1 == back of queue
    post_only: bool = False
    stop_price: Optional[float] = None
    trailing_offset: Optional[float] = None

    def __post_init__(self) -> None:
        self.side = self.side.lower()
        if self.side not in {"buy", "sell"}:
            raise ValueError(f"Unsupported side: {self.side}")
        self.order_type = self.order_type.lower()
        if self.order_type not in {"limit", "market"}:
            raise ValueError(f"Unsupported order_type: {self.order_type}")
        self.time_in_force = self.time_in_force.upper()
        if self.time_in_force not in {"GTC", "IOC", "FOK"}:
            raise ValueError(f"Unsupported time_in_force: {self.time_in_force}")
        if self.queue_position < 0.0 or self.queue_position > 1.0:
            raise ValueError("queue_position must be within [0, 1]")
        if self.trailing_offset is not None and self.trailing_offset <= 0:
            raise ValueError("trailing_offset must be positive")


@dataclass
class Order:
    """Internal order representation used during simulation."""

    order_id: int
    intent: OrderIntent
    timestamp: pd.Timestamp
    remaining: float
    filled: float = 0.0
    status: str = "open"  # "open", "filled", "cancelled"
    fee_paid: float = 0.0
    triggered: bool = False
    active: bool = True
    trigger_reference: Optional[float] = None
    maker_probability: float = 0.0

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
    slippage_bps: float = 0.0


@dataclass
class SimulationState:
    """Mutable state maintained while running a simulation."""

    cash: float
    rng: np.random.Generator
    orders: Dict[int, Order] = field(default_factory=dict)
    fills: List[Fill] = field(default_factory=list)
    equity_curve: List[Tuple[pd.Timestamp, float]] = field(default_factory=list)
    position: float = 0.0
    maker_target_qty: float = 0.0
    maker_fill_qty: float = 0.0
    total_notional: float = 0.0
    total_fee: float = 0.0
    slippage_sum_bps: float = 0.0
    slippage_samples: int = 0
    slippage_cost_total: float = 0.0
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    bid_size: float = 0.0
    ask_size: float = 0.0
    mid_price: Optional[float] = None
    halted: bool = False


class RollingFeeModel:
    """Maintains a rolling 30-day notional to determine applicable fee tiers."""

    def __init__(self, base_schedule: FeeSchedule, tiers: Optional[List[FeeTier]] = None) -> None:
        self.base_schedule = base_schedule
        self.tiers = sorted(tiers or [], key=lambda tier: tier.threshold)
        self.window = pd.Timedelta(days=30)
        self._notional_history: Deque[Tuple[pd.Timestamp, float]] = deque()
        self._rolling_notional: float = 0.0

    def _purge(self, now: pd.Timestamp) -> None:
        cutoff = now - self.window
        while self._notional_history and self._notional_history[0][0] < cutoff:
            _, notional = self._notional_history.popleft()
            self._rolling_notional -= notional

    def current_schedule(self, now: pd.Timestamp) -> FeeSchedule:
        self._purge(now)
        schedule = self.base_schedule
        for tier in self.tiers:
            if self._rolling_notional >= tier.threshold:
                schedule = tier.schedule
            else:
                break
        return schedule

    def calculate_fee(self, timestamp: pd.Timestamp, price: float, quantity: float, liquidity_flag: str) -> Tuple[float, float]:
        schedule = self.current_schedule(timestamp)
        rate = schedule.taker if liquidity_flag == "taker" else schedule.maker
        notional = abs(price * quantity)
        fee = notional * rate
        self._register_notional(timestamp, notional)
        return fee, rate

    def _register_notional(self, timestamp: pd.Timestamp, notional: float) -> None:
        self._purge(timestamp)
        self._notional_history.append((timestamp, notional))
        self._rolling_notional += notional


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
        fee_tiers: Optional[List[FeeTier]] = None,
        seed: Optional[int] = None,
    ) -> None:
        self.policy = policy
        self.base_fee_schedule = fee_schedule or FeeSchedule()
        self.fee_tiers = fee_tiers or []
        self.slippage_bps = slippage_bps
        self.initial_cash = float(initial_cash)
        self.seed = seed

        bar_stream = list(self._normalise_stream(bar_events, "bar"))
        book_stream = list(self._normalise_stream(book_events, "book"))
        self._base_events = sorted(bar_stream + book_stream, key=lambda item: item["timestamp"])
        self._last_state: Optional[SimulationState] = None

    @staticmethod
    def _normalise_stream(events: Iterable[Dict[str, Any]], event_type: str) -> Iterator[Dict[str, Any]]:
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
    def run(self) -> Dict[str, Any]:
        """Execute the simulation returning performance metrics and stress runs."""

        base_metrics = self._run_once(self._base_events)
        stress_metrics = {
            name: self._run_once(self._apply_stress(name)) for name in [
                "flash_crash",
                "spread_widen",
                "liquidity_halving",
                "feed_outage",
            ]
        }
        result: Dict[str, Any] = dict(base_metrics)
        result["stress"] = stress_metrics
        return result

    def run_with_events(self, events: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute the simulation against a custom event stream.

        Parameters
        ----------
        events:
            Iterable of events adhering to the same schema as the constructor
            inputs. Each event must define a ``timestamp`` key and a ``type``
            key ("bar" or "book").

        Returns
        -------
        Dict[str, Any]
            Performance metrics as returned by :meth:`run`.
        """

        normalised_events = [dict(event) for event in events]
        for event in normalised_events:
            if "timestamp" not in event:
                raise KeyError("event missing 'timestamp'")
            if not isinstance(event["timestamp"], pd.Timestamp):
                event["timestamp"] = pd.Timestamp(event["timestamp"])
            if "type" not in event:
                raise KeyError("event missing 'type'")
        normalised_events.sort(key=lambda item: item["timestamp"])
        return self._run_once(normalised_events)

    # ------------------------------------------------------------------
    # Core simulation loop
    # ------------------------------------------------------------------
    def _run_once(self, events: List[Dict[str, Any]]) -> Dict[str, float]:
        self._reset_policy()
        rng = np.random.default_rng(self.seed)
        state = SimulationState(cash=self.initial_cash, rng=rng)
        fee_model = RollingFeeModel(self.base_fee_schedule, self.fee_tiers)

        for event in events:
            timestamp: pd.Timestamp = event["timestamp"]
            if event["type"] == "book":
                self._handle_book_event(event, state, fee_model)
            elif event["type"] == "bar":
                self._handle_bar_event(event, state, fee_model)
            self._mark_to_market(timestamp, state)

        metrics = self._compute_performance(state)
        self._last_state = state
        return metrics

    def _reset_policy(self) -> None:
        reset_fn = getattr(self.policy, "reset", None)
        if callable(reset_fn):
            reset_fn()

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------
    def _handle_bar_event(self, event: Dict[str, Any], state: SimulationState, fee_model: RollingFeeModel) -> None:
        timestamp = event["timestamp"]
        market_state = {
            "bar": event,
            "best_bid": state.best_bid,
            "best_ask": state.best_ask,
            "position": state.position,
            "cash": state.cash,
        }
        for intent in self.policy.generate(timestamp, market_state):
            self._submit_order(intent, timestamp, state, fee_model)

    def _handle_book_event(self, event: Dict[str, Any], state: SimulationState, fee_model: RollingFeeModel) -> None:
        state.best_bid = event.get("bid", state.best_bid)
        state.best_ask = event.get("ask", state.best_ask)
        state.bid_size = float(event.get("bid_size", state.bid_size or 0.0))
        state.ask_size = float(event.get("ask_size", state.ask_size or 0.0))
        state.halted = bool(event.get("halted", state.halted))

        if state.best_bid is not None and state.best_ask is not None:
            state.mid_price = (state.best_bid + state.best_ask) / 2.0

        self._update_trailing_orders(state)
        self._activate_stop_orders(state)
        if not state.halted:
            self._match_orders(state, event["timestamp"], fee_model)

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------
    def _submit_order(
        self,
        intent: OrderIntent,
        timestamp: pd.Timestamp,
        state: SimulationState,
        fee_model: RollingFeeModel,
    ) -> None:
        order_id = len(state.orders) + len(state.fills) + 1
        order = Order(
            order_id=order_id,
            intent=intent,
            timestamp=timestamp,
            remaining=float(intent.quantity),
            triggered=not (intent.stop_price or intent.trailing_offset),
            active=not (intent.stop_price or intent.trailing_offset),
        )

        crosses_book = False
        if intent.order_type == "limit" and intent.price is not None:
            if intent.side == "buy" and state.best_ask is not None and intent.price >= state.best_ask:
                crosses_book = True
            if intent.side == "sell" and state.best_bid is not None and intent.price <= state.best_bid:
                crosses_book = True

        if intent.order_type == "limit" and intent.price is not None:
            order.maker_probability = self._calculate_maker_probability(intent, state, crosses_book)
            state.maker_target_qty += intent.quantity * order.maker_probability

        if intent.trailing_offset is not None:
            order.active = False
            reference = state.best_bid if intent.side == "sell" else state.best_ask
            order.trigger_reference = reference
            order.intent.stop_price = reference - intent.trailing_offset if intent.side == "sell" else reference + intent.trailing_offset

        state.orders[order.order_id] = order

        if intent.order_type == "limit" and not intent.post_only and crosses_book:
            order.triggered = True
            order.active = True

        if intent.post_only and intent.price is not None:
            if intent.side == "buy" and state.best_ask is not None and intent.price >= state.best_ask:
                order.status = "cancelled"
                order.active = False
                return
            if intent.side == "sell" and state.best_bid is not None and intent.price <= state.best_bid:
                order.status = "cancelled"
                order.active = False
                return

        if order.triggered and order.active:
            self._execute_order(order, timestamp, state, fee_model)
            if order.status == "filled":
                state.orders.pop(order.order_id, None)
            self._handle_time_in_force(order, state)

    def _handle_time_in_force(self, order: Order, state: SimulationState) -> None:
        if order.intent.time_in_force == "GTC":
            return
        if order.status == "filled":
            return
        if order.intent.time_in_force == "FOK":
            order.status = "cancelled"
            order.remaining = 0.0
            order.active = False
            state.orders.pop(order.order_id, None)
        elif order.intent.time_in_force == "IOC":
            order.status = "cancelled"
            order.remaining = 0.0
            order.active = False
            state.orders.pop(order.order_id, None)

    def _update_trailing_orders(self, state: SimulationState) -> None:
        if state.best_bid is None or state.best_ask is None:
            return
        for order in state.orders.values():
            intent = order.intent
            if intent.trailing_offset is None or order.status != "open":
                continue
            if intent.side == "sell":
                reference = state.best_bid
                if order.trigger_reference is None or reference > order.trigger_reference:
                    order.trigger_reference = reference
                intent.stop_price = order.trigger_reference - intent.trailing_offset
            else:
                reference = state.best_ask
                if order.trigger_reference is None or reference < order.trigger_reference:
                    order.trigger_reference = reference
                intent.stop_price = order.trigger_reference + intent.trailing_offset

    def _activate_stop_orders(self, state: SimulationState) -> None:
        if state.best_bid is None or state.best_ask is None:
            return
        for order in state.orders.values():
            if order.triggered or order.status != "open":
                continue
            stop_price = order.intent.stop_price
            if stop_price is None:
                continue
            if order.intent.side == "buy" and state.best_ask <= stop_price:
                order.triggered = True
            elif order.intent.side == "sell" and state.best_bid >= stop_price:
                order.triggered = True
            if order.triggered:
                order.active = True
                if order.intent.order_type == "limit" and order.intent.price is None:
                    order.intent.price = stop_price

    def _match_orders(self, state: SimulationState, timestamp: pd.Timestamp, fee_model: RollingFeeModel) -> None:
        for order in list(state.orders.values()):
            if order.status != "open" or not order.active:
                continue
            self._execute_order(order, timestamp, state, fee_model)
            if order.status == "filled":
                state.orders.pop(order.order_id, None)
            else:
                prev_status = order.status
                self._handle_time_in_force(order, state)
                if prev_status != order.status and order.status != "open":
                    state.orders.pop(order.order_id, None)

    def _execute_order(
        self,
        order: Order,
        timestamp: pd.Timestamp,
        state: SimulationState,
        fee_model: Optional[RollingFeeModel],
    ) -> None:
        if order.intent.order_type == "market":
            self._execute_market_order(order, timestamp, state, fee_model)
        else:
            self._execute_limit_order(order, timestamp, state, fee_model)

    def _calculate_maker_probability(
        self,
        intent: OrderIntent,
        state: SimulationState,
        crosses_book: bool,
    ) -> float:
        if intent.order_type != "limit" or intent.price is None:
            return 0.0
        if not crosses_book:
            return 1.0
        queue_bias = max(0.0, min(1.0, 1.0 - intent.queue_position))
        depth = state.ask_size if intent.side == "buy" else state.bid_size
        if depth <= 0:
            return 0.0
        depth_factor = min(1.0, depth / max(intent.quantity, 1e-9))
        return max(0.0, min(1.0, queue_bias * depth_factor))

    def _sample_latency(self, state: SimulationState) -> pd.Timedelta:
        latency_ms = int(state.rng.integers(50, 301))
        return pd.Timedelta(milliseconds=latency_ms)

    def _execute_market_order(
        self,
        order: Order,
        timestamp: pd.Timestamp,
        state: SimulationState,
        fee_model: Optional[RollingFeeModel],
    ) -> None:
        if state.best_bid is None or state.best_ask is None:
            return
        available = state.ask_size if order.intent.side == "buy" else state.bid_size
        if available <= 0:
            return
        fill_qty = min(order.remaining, available)
        if fill_qty <= 0:
            return
        if order.intent.time_in_force == "FOK" and fill_qty < order.remaining:
            return
        base_price = state.best_ask if order.intent.side == "buy" else state.best_bid
        exec_price, slippage_cost, slippage_bps = self._apply_slippage(
            base_price,
            order.intent.side,
            fill_qty,
            state,
            "taker",
        )
        latency = self._sample_latency(state)
        fill_timestamp = timestamp + latency
        self._register_fill(
            order,
            fill_timestamp,
            base_price,
            exec_price,
            fill_qty,
            "taker",
            state,
            fee_model,
            slippage_cost=slippage_cost,
            slippage_bps=slippage_bps,
        )
        if order.intent.side == "buy":
            state.ask_size = max(0.0, state.ask_size - fill_qty)
        else:
            state.bid_size = max(0.0, state.bid_size - fill_qty)
        if order.intent.time_in_force in {"FOK", "IOC"} and order.status != "filled":
            self._handle_time_in_force(order, state)

    def _execute_limit_order(
        self,
        order: Order,
        timestamp: pd.Timestamp,
        state: SimulationState,
        fee_model: Optional[RollingFeeModel],
    ) -> None:
        if state.best_bid is None or state.best_ask is None:
            return
        price = order.intent.price
        if price is None:
            return

        if order.intent.side == "buy":
            crossed = price >= state.best_ask
            taker_reference_price = state.best_ask
            maker_liquidity = max(state.ask_size * (1.0 - order.intent.queue_position), 0.0)
            taker_liquidity = state.ask_size
        else:
            crossed = price <= state.best_bid
            taker_reference_price = state.best_bid
            maker_liquidity = max(state.bid_size * (1.0 - order.intent.queue_position), 0.0)
            taker_liquidity = state.bid_size

        if not crossed:
            return

        maker_probability = order.maker_probability or self._calculate_maker_probability(
            order.intent, state, crossed
        )
        maker_probability = max(0.0, min(1.0, maker_probability))
        liquidity_flag = "maker"
        if maker_probability < 1.0:
            draw = float(state.rng.random())
            if draw >= maker_probability or maker_liquidity <= 0:
                liquidity_flag = "taker"
        if liquidity_flag == "maker" and maker_liquidity <= 0:
            liquidity_flag = "taker"

        if liquidity_flag == "maker":
            available = maker_liquidity
            fill_qty = min(order.remaining, available)
            if fill_qty <= 0:
                return
            if fill_qty < order.remaining and not order.intent.allow_partial:
                return
            if order.intent.time_in_force == "FOK" and fill_qty < order.remaining:
                return
            latency = self._sample_latency(state)
            fill_timestamp = timestamp + latency
            self._register_fill(
                order,
                fill_timestamp,
                price,
                price,
                fill_qty,
                "maker",
                state,
                fee_model,
                slippage_cost=0.0,
                slippage_bps=0.0,
            )
        else:
            available = taker_liquidity
            fill_qty = min(order.remaining, available)
            if order.intent.time_in_force == "FOK" and fill_qty < order.remaining:
                return
            if fill_qty <= 0:
                return
            reference_price = taker_reference_price
            exec_price, slippage_cost, slippage_bps = self._apply_slippage(
                reference_price,
                order.intent.side,
                fill_qty,
                state,
                "taker",
            )
            latency = self._sample_latency(state)
            fill_timestamp = timestamp + latency
            self._register_fill(
                order,
                fill_timestamp,
                reference_price,
                exec_price,
                fill_qty,
                "taker",
                state,
                fee_model,
                slippage_cost=slippage_cost,
                slippage_bps=slippage_bps,
            )

    def _register_fill(
        self,
        order: Order,
        timestamp: pd.Timestamp,
        base_price: float,
        exec_price: float,
        quantity: float,
        liquidity_flag: str,
        state: SimulationState,
        fee_model: Optional[RollingFeeModel],
        *,
        slippage_cost: float = 0.0,
        slippage_bps: float = 0.0,
    ) -> None:
        if quantity <= 0:
            return

        fee = 0.0
        if fee_model is not None:
            fee, _ = fee_model.calculate_fee(timestamp, exec_price, quantity, liquidity_flag)
        state.total_fee += fee
        notional = abs(exec_price * quantity)
        state.total_notional += notional

        state.fills.append(
            Fill(
                order_id=order.order_id,
                timestamp=timestamp,
                side=order.intent.side,
                price=exec_price,
                quantity=quantity,
                fee=fee,
                liquidity_flag=liquidity_flag,
                slippage_bps=slippage_bps,
            )
        )
        order.record_fill(quantity)
        order.fee_paid += fee

        if order.intent.side == "buy":
            state.position += quantity
            state.cash -= base_price * quantity + fee + slippage_cost
        else:
            state.position -= quantity
            state.cash += base_price * quantity - fee - slippage_cost

        if liquidity_flag == "maker":
            state.maker_fill_qty += quantity

        if slippage_bps != 0.0 or slippage_cost > 0.0:
            state.slippage_sum_bps += slippage_bps
            state.slippage_samples += 1
        state.slippage_cost_total += slippage_cost

        if order.status == "filled":
            order.active = False

    # ------------------------------------------------------------------
    # Portfolio analytics
    # ------------------------------------------------------------------
    def _mark_to_market(self, timestamp: pd.Timestamp, state: SimulationState) -> None:
        if state.mid_price is None:
            equity = state.cash
        else:
            equity = state.cash + state.position * state.mid_price
        state.equity_curve.append((timestamp, equity))

    def _compute_performance(self, state: SimulationState) -> Dict[str, float]:
        if not state.equity_curve:
            return {
                "net_pnl": 0.0,
                "sharpe": 0.0,
                "sortino": 0.0,
                "max_dd": 0.0,
                "cvar_95": 0.0,
                "turnover": 0.0,
                "fee_bps": 0.0,
                "maker_hit_rate": 0.0,
                "slippage_attrib": 0.0,
            }

        curve = pd.DataFrame(state.equity_curve, columns=["timestamp", "equity"]).set_index("timestamp").sort_index()
        returns = curve["equity"].pct_change().fillna(0.0)
        sharpe = self._sharpe_ratio(returns)
        sortino = self._sortino_ratio(returns)
        max_dd = self._max_drawdown(curve["equity"])
        cvar_95 = self._cvar(returns, 0.95)
        final_equity = float(curve["equity"].iloc[-1])
        net_pnl = final_equity - self.initial_cash
        turnover = state.total_notional
        fee_bps = (state.total_fee / state.total_notional * 10_000) if state.total_notional > 0 else 0.0
        maker_hit_rate = (state.maker_fill_qty / state.maker_target_qty) if state.maker_target_qty > 0 else 0.0
        slippage_attrib = (state.slippage_sum_bps / state.slippage_samples) if state.slippage_samples > 0 else 0.0
        return {
            "net_pnl": net_pnl,
            "sharpe": sharpe,
            "sortino": sortino,
            "max_dd": max_dd,
            "cvar_95": cvar_95,
            "turnover": turnover,
            "fee_bps": fee_bps,
            "maker_hit_rate": maker_hit_rate,
            "slippage_attrib": slippage_attrib,
        }

    @staticmethod
    def _sharpe_ratio(returns: pd.Series, periods: int = 252) -> float:
        vol = returns.std(ddof=0)
        if vol == 0:
            return 0.0
        return float(np.sqrt(periods) * returns.mean() / vol)

    @staticmethod
    def _sortino_ratio(returns: pd.Series, periods: int = 252) -> float:
        downside = returns[returns < 0]
        if downside.empty:
            return 0.0
        downside_vol = downside.std(ddof=0)
        if downside_vol == 0:
            return 0.0
        return float(np.sqrt(periods) * returns.mean() / downside_vol)

    @staticmethod
    def _max_drawdown(equity: pd.Series) -> float:
        running_max = equity.cummax()
        drawdown = equity / running_max - 1.0
        return float(drawdown.min())

    @staticmethod
    def _cvar(returns: pd.Series, confidence: float) -> float:
        if returns.empty:
            return 0.0
        sorted_returns = returns.sort_values()
        cutoff_index = int(np.ceil((1 - confidence) * len(sorted_returns)))
        if cutoff_index <= 0:
            return 0.0
        tail = sorted_returns.iloc[:cutoff_index]
        return float(tail.mean())

    def _apply_slippage(
        self,
        price: float,
        side: str,
        quantity: float,
        state: SimulationState,
        liquidity_flag: str,
    ) -> Tuple[float, float, float]:
        if liquidity_flag == "maker":
            return price, 0.0, 0.0
        base_bps = self.slippage_bps if self.slippage_bps > 0 else 1.0
        depth = state.ask_size if side == "buy" else state.bid_size
        depth = max(depth, 1e-9)
        depth_ratio = min(quantity / depth, 5.0)
        dynamic_bps = base_bps * (1.0 + depth_ratio)
        noise = base_bps * 0.25 * float(state.rng.normal())
        total_bps = max(0.0, dynamic_bps + noise)
        direction = 1 if side == "buy" else -1
        price_shift = price * total_bps / 10_000.0 * direction
        exec_price = price + price_shift
        slippage_cost = abs(price_shift) * quantity
        return exec_price, slippage_cost, total_bps

    # ------------------------------------------------------------------
    # Stress scenarios
    # ------------------------------------------------------------------
    def _apply_stress(self, scenario: str) -> List[Dict[str, Any]]:
        events = [dict(event) for event in self._base_events]
        injectors = {
            "flash_crash": flash_crash,
            "spread_widen": spread_widen,
            "feed_outage": feed_outage,
        }
        if scenario in injectors:
            return injectors[scenario](events)

        book_indices = [idx for idx, event in enumerate(events) if event["type"] == "book"]
        if not book_indices:
            return events

        if scenario == "liquidity_halving":
            for idx in book_indices[::2]:
                event = events[idx]
                event["bid_size"] = float(event.get("bid_size", 1.0)) * 0.5
                event["ask_size"] = float(event.get("ask_size", 1.0)) * 0.5
        return events

    # ------------------------------------------------------------------
    # Convenience accessors (intended for unit tests)
    # ------------------------------------------------------------------
    @property
    def base_events(self) -> List[Dict[str, Any]]:
        return list(self._base_events)

    @property
    def last_state(self) -> Optional[SimulationState]:
        """Return the most recent :class:`SimulationState` after a run."""

        return self._last_state


def _book_event_indices(events: List[Dict[str, Any]]) -> List[int]:
    return [idx for idx, event in enumerate(events) if event.get("type") == "book"]


def flash_crash(events: List[Dict[str, Any]], drop: float = 0.3, depth_factor: float = 0.5) -> List[Dict[str, Any]]:
    book_indices = _book_event_indices(events)
    if not book_indices:
        return events
    start = len(book_indices) // 2
    window = book_indices[start : start + 5]
    for idx in window:
        event = events[idx]
        bid = event.get("bid")
        ask = event.get("ask")
        if bid is not None:
            event["bid"] = bid * (1.0 - drop)
        if ask is not None:
            event["ask"] = ask * (1.0 - drop * 0.9)
        event["bid_size"] = float(event.get("bid_size", 1.0)) * depth_factor
        event["ask_size"] = float(event.get("ask_size", 1.0)) * depth_factor
    return events


def spread_widen(events: List[Dict[str, Any]], widen_bps: float = 75.0) -> List[Dict[str, Any]]:
    book_indices = _book_event_indices(events)
    if not book_indices:
        return events
    width = widen_bps / 10_000.0
    for idx in book_indices[len(book_indices) // 3 : len(book_indices) // 3 + 5]:
        event = events[idx]
        bid = event.get("bid")
        ask = event.get("ask")
        if bid is None or ask is None:
            continue
        mid = (bid + ask) / 2.0
        event["bid"] = mid * (1.0 - width)
        event["ask"] = mid * (1.0 + width)
    return events


def feed_outage(events: List[Dict[str, Any]], gap_events: int = 3) -> List[Dict[str, Any]]:
    book_indices = _book_event_indices(events)
    if len(book_indices) < gap_events:
        return events
    start = max(len(book_indices) // 4, 0)
    outage = book_indices[start : start + gap_events]
    for idx in outage:
        event = events[idx]
        event["bid"] = None
        event["ask"] = None
        event["bid_size"] = 0.0
        event["ask_size"] = 0.0
        event["halted"] = True
    return events
