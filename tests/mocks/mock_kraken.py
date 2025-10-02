"""High fidelity Kraken exchange simulator for OMS integration tests.

This module exposes a small in-memory exchange that mimics the behaviour of
Kraken's private WebSocket and REST APIs.  The implementation is intentionally
stateful so that tests can validate workflow orchestration across both
transports without relying on any external services.

Features provided by the mock:

* Shared order book between the simulated WebSocket and REST transports.
* Deterministic partial fills with configurable fill schedules.
* Behaviour toggles for probabilistic partial fills, rejections and cancels.
* Latency simulation for both synchronous (WS) and asynchronous (REST) paths.
* Random latency spikes and WebSocket failure simulation.
* Runtime behaviour modes (``normal``, ``delayed``, ``failing``, ``crash``)
  that allow tests to exercise orchestration logic under adverse conditions.
* Random as well as scheduled error injection to exercise retry paths.
* Pytest fixtures that expose the mock in a convenient form for integration
  tests.

The mock focuses on the subset of the Kraken API that is used by the OMS:
``place_order``/``cancel_order`` over both transports and helper endpoints for
balances and trade history.  The return payloads follow the real API loosely –
only the fields that are consumed by tests are modelled.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterable, Iterator, List, Optional, Tuple

import pytest


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class MockKrakenError(RuntimeError):
    """Base class for mock Kraken failures."""


class MockKrakenTransportClosed(MockKrakenError):
    """Raised when a closed session is used."""


class MockKrakenRandomError(MockKrakenError):
    """Raised when a probabilistic failure is triggered."""


class MockKrakenCrashed(MockKrakenError):
    """Raised when the mock exchange is configured to crash."""


# ---------------------------------------------------------------------------
# Configuration containers
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class MockKrakenConfig:
    """Runtime configuration used to construct :class:`MockKrakenExchange`."""

    balances: Dict[str, Dict[str, float]] = field(
        default_factory=lambda: {
            "company": {"USD": 1_000_000.0, "BTC": 25.0, "ETH": 2_500.0},
            "shadow": {"USD": 50_000.0, "BTC": 1.5},
        }
    )
    base_prices: Dict[str, float] = field(
        default_factory=lambda: {
            "BTC/USD": 30_000.0,
            "ETH/USD": 2_000.0,
        }
    )
    latency: Tuple[float, float] = (0.0, 0.0)
    random_latency_chance: float = 0.0
    random_latency: Tuple[float, float] = (0.0, 0.0)
    error_rate: float = 0.0
    error_schedule: Dict[str, Iterable[Exception]] = field(default_factory=dict)
    default_fill: List[float] = field(default_factory=lambda: [1.0])
    partial_fill_probability: float = 0.0
    rejection_rate: float = 0.0
    cancel_rate: float = 0.0
    seed: Optional[int] = 11
    mode: str = "normal"


@dataclass(slots=True)
class MockOrder:
    order_id: str
    account: str
    pair: str
    side: str
    price: Optional[float]
    volume: float
    remaining: float
    status: str
    created_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "account": self.account,
            "pair": self.pair,
            "side": self.side,
            "price": self.price,
            "volume": self.volume,
            "remaining": self.remaining,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(slots=True)
class MockTrade:
    trade_id: str
    order_id: str
    account: str
    pair: str
    side: str
    price: float
    volume: float
    executed_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "order_id": self.order_id,
            "account": self.account,
            "pair": self.pair,
            "side": self.side,
            "price": self.price,
            "volume": self.volume,
            "executed_at": self.executed_at.isoformat(),
        }


# ---------------------------------------------------------------------------
# Core exchange simulator
# ---------------------------------------------------------------------------


class MockKrakenExchange:
    """Stateful simulation of the Kraken exchange used in OMS tests."""

    def __init__(self, config: Optional[MockKrakenConfig] = None) -> None:
        import random

        self.config = config or MockKrakenConfig()
        self._balances: Dict[str, Dict[str, float]] = {
            account: dict(balances) for account, balances in self.config.balances.items()
        }
        self._order_counter = 1
        self._trade_counter = 1
        self._orders: Dict[str, MockOrder] = {}
        self._trades: List[MockTrade] = []
        self._random = random.Random(self.config.seed)
        self._error_queues: Dict[str, Deque[Exception]] = {
            key: deque(errors)
            for key, errors in self.config.error_schedule.items()
        }
        self._fill_schedules: Dict[str, Deque[List[float]]] = {}
        self._rejection_reasons: Deque[str] = deque()
        self._cancel_reasons: Deque[str] = deque()
        self._default_partial_fill_probability = self.config.partial_fill_probability
        self._partial_fill_probability = self.config.partial_fill_probability
        self._default_rejection_rate = self.config.rejection_rate
        self._rejection_rate = self.config.rejection_rate
        self._default_cancel_rate = self.config.cancel_rate
        self._cancel_rate = self.config.cancel_rate
        self._default_latency_range = tuple(self.config.latency)
        self._latency_range = tuple(self.config.latency)
        self._default_random_latency_chance = self.config.random_latency_chance
        self._random_latency_chance = self.config.random_latency_chance
        self._default_random_latency = tuple(self.config.random_latency)
        self._random_latency = tuple(self.config.random_latency)
        self._default_error_rate = self.config.error_rate
        self._error_rate = self.config.error_rate
        self._mode = "normal"
        self._crashed = False
        self._ws_sessions: set["MockKrakenWSSession"] = set()
        self._apply_mode(self.config.mode)

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------
    def set_mode(self, mode: str) -> None:
        """Switch the exchange into one of the predefined behaviour modes."""

        self._apply_mode(mode)

    def schedule_fill_sequence(self, sequence: Iterable[float], *, pair: Optional[str] = None) -> None:
        """Enqueue a specific fill sequence for the next order.

        The sequence is expressed as fractional fills of the submitted quantity.
        For example ``[0.4, 0.6]`` results in two fills, the first for 40% of the
        requested size and the second for the remaining 60%.
        """

        target = pair or "*"
        queue = self._fill_schedules.setdefault(target, deque())
        queue.append(list(sequence))

    def schedule_error(self, method: str, exc: Exception) -> None:
        """Arrange for *exc* to be raised the next time ``method`` is invoked."""

        self._error_queues.setdefault(method, deque()).append(exc)

    def schedule_rejection(self, reason: str = "EOrder:Rejected") -> None:
        """Reject the next order with ``reason`` instead of accepting it."""

        self._rejection_reasons.append(reason)

    def schedule_cancel(self, reason: str = "ECancel:Cancelled") -> None:
        """Cancel the next accepted order after any fills have been applied."""

        self._cancel_reasons.append(reason)

    def configure_behaviour(
        self,
        *,
        partial_fill_probability: Optional[float] = None,
        rejection_rate: Optional[float] = None,
        cancel_rate: Optional[float] = None,
        random_latency_chance: Optional[float] = None,
        random_latency: Optional[Tuple[float, float]] = None,
        error_rate: Optional[float] = None,
    ) -> None:
        """Adjust runtime behaviour knobs without recreating the exchange."""

        if partial_fill_probability is not None:
            self._partial_fill_probability = max(0.0, min(1.0, float(partial_fill_probability)))
            self._default_partial_fill_probability = self._partial_fill_probability
        if rejection_rate is not None:
            self._rejection_rate = max(0.0, min(1.0, float(rejection_rate)))
            self._default_rejection_rate = self._rejection_rate
        if cancel_rate is not None:
            self._cancel_rate = max(0.0, min(1.0, float(cancel_rate)))
            self._default_cancel_rate = self._cancel_rate
        if random_latency_chance is not None:
            self._random_latency_chance = max(0.0, min(1.0, float(random_latency_chance)))
            self._default_random_latency_chance = self._random_latency_chance
        if random_latency is not None:
            low, high = random_latency
            if high < low:
                low, high = high, low
            self._random_latency = (float(low), float(high))
            self._default_random_latency = self._random_latency
        if error_rate is not None:
            self._error_rate = max(0.0, min(1.0, float(error_rate)))
            self._default_error_rate = self._error_rate

    def simulate_ws_failure(self) -> int:
        """Force-close every active WebSocket session and return the count."""

        closed = 0
        for session in list(self._ws_sessions):
            session._force_close()
            closed += 1
        self._ws_sessions.clear()
        return closed

    # ------------------------------------------------------------------
    # Behaviour modes
    # ------------------------------------------------------------------
    def _apply_mode(self, mode: str) -> None:
        normalised = str(mode or "normal").lower()
        base_latency = tuple(self._default_latency_range)
        base_random_latency = tuple(self._default_random_latency)
        base_random_chance = float(self._default_random_latency_chance)
        base_error_rate = float(self._default_error_rate)

        self._latency_range = base_latency
        self._random_latency = base_random_latency
        self._random_latency_chance = base_random_chance
        self._error_rate = base_error_rate
        self._partial_fill_probability = self._default_partial_fill_probability
        self._rejection_rate = self._default_rejection_rate
        self._cancel_rate = self._default_cancel_rate
        self._mode = normalised
        self._crashed = False

        if normalised == "normal":
            return
        if normalised == "delayed":
            low, high = self._latency_range
            low = max(low, 0.25)
            high = max(high, 0.75)
            if high < low:
                low, high = high, low
            self._latency_range = (low, high)
            jitter_low, jitter_high = self._random_latency
            jitter_low = max(jitter_low, 0.05)
            jitter_high = max(jitter_high, 0.25)
            if jitter_high < jitter_low:
                jitter_low, jitter_high = jitter_high, jitter_low
            self._random_latency = (jitter_low, jitter_high)
            self._random_latency_chance = max(self._random_latency_chance, 0.75)
            return
        if normalised == "failing":
            self._error_rate = max(self._error_rate, 0.75)
            self._rejection_rate = max(self._rejection_rate, 0.5)
            return
        if normalised == "crash":
            self._crashed = True
            return
        raise ValueError(f"Unknown mock Kraken mode: {mode!r}")

    def reset(self) -> None:
        """Restore balances and clear orders/trades."""

        self._balances = {
            account: dict(balances) for account, balances in self.config.balances.items()
        }
        self._orders.clear()
        self._trades.clear()
        self._order_counter = 1
        self._trade_counter = 1
        for queue in self._error_queues.values():
            queue.clear()
        self._fill_schedules.clear()
        self._rejection_reasons.clear()
        self._cancel_reasons.clear()
        self._default_latency_range = tuple(self.config.latency)
        self._default_partial_fill_probability = self.config.partial_fill_probability
        self._partial_fill_probability = self.config.partial_fill_probability
        self._default_rejection_rate = self.config.rejection_rate
        self._rejection_rate = self.config.rejection_rate
        self._default_cancel_rate = self.config.cancel_rate
        self._cancel_rate = self.config.cancel_rate
        self._default_random_latency_chance = self.config.random_latency_chance
        self._latency_range = tuple(self._default_latency_range)
        self._random_latency_chance = self._default_random_latency_chance
        self._default_random_latency = tuple(self.config.random_latency)
        self._random_latency = tuple(self._default_random_latency)
        self._default_error_rate = self.config.error_rate
        self._error_rate = self._default_error_rate
        self._mode = "normal"
        self._crashed = False
        self._apply_mode(self.config.mode)
        self.simulate_ws_failure()

    # ------------------------------------------------------------------
    # Public API used by transports
    # ------------------------------------------------------------------
    def place_order_ws(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._simulate_latency_sync()
        self._maybe_raise("place_order")
        return self._execute_order(payload, transport="websocket")

    async def place_order_rest(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        await self._simulate_latency_async()
        self._maybe_raise("place_order")
        return self._execute_order(payload, transport="rest")

    def cancel_order_ws(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._simulate_latency_sync()
        self._maybe_raise("cancel_order")
        return self._cancel_order(payload, transport="websocket")

    async def cancel_order_rest(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        await self._simulate_latency_async()
        self._maybe_raise("cancel_order")
        return self._cancel_order(payload, transport="rest")

    def open_orders(self, *, account: Optional[str] = None) -> Dict[str, Any]:
        orders = [
            order.to_dict()
            for order in self._orders.values()
            if order.remaining > 0
            and (account is None or order.account == account)
            and order.status in {"open", "partially_filled"}
        ]
        return {"open": orders, "count": len(orders)}

    async def get_balance(self, account: str = "company") -> Dict[str, float]:
        await self._simulate_latency_async()
        self._maybe_raise("get_balance")
        return dict(self._balances.setdefault(account, {}))

    async def get_trades(
        self,
        *,
        account: Optional[str] = None,
        pair: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        await self._simulate_latency_async()
        self._maybe_raise("get_trades")
        trades: Iterable[MockTrade] = self._trades
        if account is not None:
            trades = [trade for trade in trades if trade.account == account]
        if pair is not None:
            trades = [trade for trade in trades if trade.pair == pair]
        return [trade.to_dict() for trade in trades]

    # ------------------------------------------------------------------
    # Internal mechanics
    # ------------------------------------------------------------------
    def _execute_order(self, payload: Dict[str, Any], *, transport: str) -> Dict[str, Any]:
        account = str(payload.get("account") or payload.get("userref") or "company")
        pair = self._normalise_pair(str(payload.get("pair") or payload.get("instrument") or "BTC/USD"))
        side = str(payload.get("type") or payload.get("side") or "buy").lower()
        if side not in {"buy", "sell"}:
            raise ValueError(f"Unsupported order side: {side}")

        raw_volume = payload.get("volume") or payload.get("quantity") or payload.get("size")
        if raw_volume is None:
            raise ValueError("Order payload missing volume/quantity/size")
        volume = float(raw_volume)
        price_value = payload.get("price") or payload.get("limit_price")
        price = float(price_value) if price_value is not None else self.config.base_prices.get(pair, 0.0)

        rejection_reason = self._pop_rejection_reason()
        if (
            rejection_reason is not None
            or (self._rejection_rate and self._random.random() < self._rejection_rate)
        ):
            reason = rejection_reason or "EOrder:Rejected"
            return {
                "status": "rejected",
                "txid": None,
                "transport": transport,
                "error": [reason],
            }

        order_id = f"MO{self._order_counter:05d}"
        self._order_counter += 1
        order = MockOrder(
            order_id=order_id,
            account=account,
            pair=pair,
            side=side,
            price=price,
            volume=volume,
            remaining=volume,
            status="open",
            created_at=datetime.now(timezone.utc),
        )

        fills = self._generate_fills(order)
        filled_volume = sum(trade.volume for trade in fills)

        if order.remaining <= 0:
            order.status = "filled"
        elif filled_volume > 0:
            order.status = "partially_filled"
            self._orders[order.order_id] = order
        else:
            order.status = "open"
            self._orders[order.order_id] = order

        cancel_reason = self._pop_cancel_reason()
        if (
            order.status != "filled"
            and (
                cancel_reason is not None
                or (self._cancel_rate and self._random.random() < self._cancel_rate)
            )
        ):
            order.status = "cancelled"
            order.remaining = 0.0
            self._orders.pop(order.order_id, None)
            reason = cancel_reason or "ECancel:Cancelled"
        else:
            reason = None

        response = {
            "status": order.status,
            "txid": order.order_id,
            "transport": transport,
            "filled": round(filled_volume, 10),
            "remaining": round(max(order.remaining, 0.0), 10),
            "fills": [trade.to_dict() for trade in fills],
        }
        if reason:
            response["error"] = [reason]
        return response

    def _cancel_order(self, payload: Dict[str, Any], *, transport: str) -> Dict[str, Any]:
        order_id = str(payload.get("txid") or payload.get("order_id") or "")
        order = self._orders.get(order_id)
        if order is None or order.remaining <= 0:
            return {"status": "not_found", "txid": order_id or None, "transport": transport}

        order.status = "cancelled"
        order.remaining = 0.0
        self._orders.pop(order_id, None)
        return {"status": "cancelled", "txid": order_id, "transport": transport}

    def _generate_fills(self, order: MockOrder) -> List[MockTrade]:
        plan = self._next_fill_plan(order.pair)
        fills: List[MockTrade] = []
        remaining = order.volume

        for fraction in plan:
            if remaining <= 0:
                break
            fraction = max(0.0, min(1.0, float(fraction)))
            fill_volume = min(remaining, order.volume * fraction)
            if fill_volume <= 0:
                continue
            remaining -= fill_volume
            order.remaining = remaining
            trade = self._record_trade(order, fill_volume)
            fills.append(trade)

        return fills

    def _record_trade(self, order: MockOrder, volume: float) -> MockTrade:
        price = order.price or self.config.base_prices.get(order.pair, 0.0)
        timestamp = datetime.now(timezone.utc)
        trade = MockTrade(
            trade_id=f"T{self._trade_counter:05d}",
            order_id=order.order_id,
            account=order.account,
            pair=order.pair,
            side=order.side,
            price=price,
            volume=volume,
            executed_at=timestamp,
        )
        self._trade_counter += 1
        self._trades.append(trade)
        self._apply_fill(order.account, order.pair, order.side, price, volume)
        return trade

    def _apply_fill(self, account: str, pair: str, side: str, price: float, volume: float) -> None:
        base, quote = self._split_pair(pair)
        balances = self._balances.setdefault(account, {})
        balances.setdefault(base, 0.0)
        balances.setdefault(quote, 0.0)

        if side == "buy":
            balances[quote] -= price * volume
            balances[base] += volume
        else:
            balances[base] -= volume
            balances[quote] += price * volume

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    def _next_fill_plan(self, pair: str) -> List[float]:
        if pair in self._fill_schedules and self._fill_schedules[pair]:
            return list(self._fill_schedules[pair].popleft())
        if "*" in self._fill_schedules and self._fill_schedules["*"]:
            return list(self._fill_schedules["*"].popleft())

        # If no deterministic plan is configured generate a random plan when
        # partial fills are requested by the behaviour controller.
        if len(self.config.default_fill) == 1 and self.config.default_fill[0] == 1.0:
            probability = self._partial_fill_probability
            if probability and self._random.random() < probability:
                first = round(self._random.uniform(0.2, 0.7), 4)
                return [first, 1.0 - first]
        return list(self.config.default_fill)

    def _simulate_latency_sync(self) -> None:
        delay = self._latency_value()
        if delay > 0:
            time.sleep(delay)

    async def _simulate_latency_async(self) -> None:
        delay = self._latency_value()
        if delay > 0:
            await asyncio.sleep(delay)

    def _latency_value(self) -> float:
        low, high = self._latency_range
        if high < low:
            low, high = high, low
        base = 0.0
        if low != 0.0 or high != 0.0:
            base = self._random.uniform(low, high)
        if self._random_latency_chance and self._random.random() < self._random_latency_chance:
            jitter_low, jitter_high = self._random_latency
            if jitter_high < jitter_low:
                jitter_low, jitter_high = jitter_high, jitter_low
            base += self._random.uniform(jitter_low, jitter_high)
        return max(0.0, base)

    def _maybe_raise(self, method: str) -> None:
        if self._crashed:
            raise MockKrakenCrashed("Mock Kraken exchange is in crash mode")
        queue = self._error_queues.get(method)
        if queue:
            raise queue.popleft()
        if self._error_rate and self._random.random() < self._error_rate:
            raise MockKrakenRandomError(f"Random failure injected for {method}")

    def _pop_rejection_reason(self) -> Optional[str]:
        if self._rejection_reasons:
            return self._rejection_reasons.popleft()
        return None

    def _pop_cancel_reason(self) -> Optional[str]:
        if self._cancel_reasons:
            return self._cancel_reasons.popleft()
        return None

    @staticmethod
    def _normalise_pair(pair: str) -> str:
        if "-" in pair and "/" not in pair:
            base, quote = pair.split("-", 1)
            return f"{base}/{quote}"
        return pair

    @staticmethod
    def _split_pair(pair: str) -> Tuple[str, str]:
        if "/" in pair:
            base, quote = pair.split("/", 1)
            return base, quote
        if "-" in pair:
            base, quote = pair.split("-", 1)
            return base, quote
        if len(pair) >= 6:
            return pair[:-3], pair[-3:]
        raise ValueError(f"Cannot split trading pair {pair!r}")


# ---------------------------------------------------------------------------
# Transport facades used directly by tests
# ---------------------------------------------------------------------------


class MockKrakenWSSession:
    """Synchronous facade mimicking the behaviour of the Kraken WS session."""

    def __init__(self, exchange: MockKrakenExchange) -> None:
        self._exchange = exchange
        self._closed = False
        self._exchange._ws_sessions.add(self)

    def request(self, channel: str, payload: Dict[str, Any], timeout: float | None = None) -> Dict[str, Any]:
        if self._closed:
            raise MockKrakenTransportClosed("websocket session closed")
        if channel == "add_order":
            return self._exchange.place_order_ws(payload)
        if channel == "cancel_order":
            return self._exchange.cancel_order_ws(payload)
        if channel == "openOrders":
            account = payload.get("account") or payload.get("userref")
            return self._exchange.open_orders(account=account)
        if channel == "ownTrades":
            account = payload.get("account") or payload.get("userref")
            trades = [
                trade
                for trade in self._exchange._trades
                if account is None or trade.account == account
            ]
            return {"trades": [trade.to_dict() for trade in trades]}
        if channel == "getBalance":
            account = payload.get("account") or payload.get("userref") or "company"
            return {"balances": self._exchange._balances.get(account, {})}
        raise ValueError(f"Unsupported channel: {channel}")

    def close(self) -> None:
        self._closed = True
        self._exchange._ws_sessions.discard(self)

    def _force_close(self) -> None:
        self._closed = True
        self._exchange._ws_sessions.discard(self)


class MockKrakenRESTClient:
    """Async REST facade that proxies through to :class:`MockKrakenExchange`."""

    def __init__(self, exchange: MockKrakenExchange) -> None:
        self._exchange = exchange

    async def add_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._exchange.place_order_rest(payload)

    async def cancel_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._exchange.cancel_order_rest(payload)

    async def open_orders(self, *, account: Optional[str] = None) -> Dict[str, Any]:
        await self._exchange._simulate_latency_async()
        return self._exchange.open_orders(account=account)

    async def balance(self, account: str = "company") -> Dict[str, float]:
        return await self._exchange.get_balance(account)

    async def trades(
        self,
        *,
        account: Optional[str] = None,
        pair: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return await self._exchange.get_trades(account=account, pair=pair)

    async def close(self) -> None:  # pragma: no cover - symmetry with aiohttp
        return None


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


class MockKrakenControls:
    """Helper used in fixtures to adjust the mock exchange behaviour on demand."""

    def __init__(self, exchange: MockKrakenExchange) -> None:
        self._exchange = exchange

    def enable_partial_fills(self, probability: float = 1.0) -> None:
        self._exchange.configure_behaviour(partial_fill_probability=probability)

    def disable_partial_fills(self) -> None:
        self._exchange.configure_behaviour(partial_fill_probability=0.0)

    def set_rejection_rate(self, rate: float) -> None:
        self._exchange.configure_behaviour(rejection_rate=rate)

    def set_cancel_rate(self, rate: float) -> None:
        self._exchange.configure_behaviour(cancel_rate=rate)

    def set_random_latency(self, chance: float, low: float, high: float) -> None:
        self._exchange.configure_behaviour(
            random_latency_chance=chance, random_latency=(low, high)
        )

    def set_mode(self, mode: str) -> None:
        self._exchange.set_mode(mode)

    def reject_next_order(self, reason: str = "EOrder:Rejected") -> None:
        self._exchange.schedule_rejection(reason)

    def cancel_next_order(self, reason: str = "ECancel:Cancelled") -> None:
        self._exchange.schedule_cancel(reason)

    def simulate_ws_failure(self) -> int:
        return self._exchange.simulate_ws_failure()


@pytest.fixture
def mock_kraken_exchange(request: pytest.FixtureRequest) -> Iterator[MockKrakenExchange]:
    """Yield a resettable :class:`MockKrakenExchange` instance."""

    param = getattr(request, "param", None)
    if isinstance(param, MockKrakenConfig):
        config = param
    elif isinstance(param, dict):
        config = replace(MockKrakenConfig(), **param)
    else:
        config = MockKrakenConfig()

    exchange = MockKrakenExchange(config)
    try:
        yield exchange
    finally:
        exchange.reset()


@pytest.fixture
def mock_kraken_ws_session_factory(
    mock_kraken_exchange: MockKrakenExchange,
) -> Iterator[Any]:
    """Fixture returning a factory compatible with ``KrakenWSClient``."""

    def factory(_creds: Dict[str, str]) -> MockKrakenWSSession:
        return MockKrakenWSSession(mock_kraken_exchange)

    yield factory


@pytest.fixture
def mock_kraken_rest_client(mock_kraken_exchange: MockKrakenExchange) -> MockKrakenRESTClient:
    """Fixture returning the asynchronous REST facade for the mock exchange."""

    return MockKrakenRESTClient(mock_kraken_exchange)


@pytest.fixture
def mock_kraken_controls(mock_kraken_exchange: MockKrakenExchange) -> MockKrakenControls:
    """Fixture exposing helpers to adjust the exchange behaviour during tests."""

    return MockKrakenControls(mock_kraken_exchange)

