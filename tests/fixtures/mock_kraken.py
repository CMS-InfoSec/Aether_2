"""Deterministic mock of the Kraken exchange REST API for unit tests."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import count
from typing import Deque, Dict, Iterable, List, Optional

import pytest


@dataclass(slots=True)
class MockKrakenConfig:
    """Runtime configuration for :class:`MockKrakenServer`."""

    base_price: float = 30_000.0
    spread: float = 20.0
    latency: float = 0.0
    balances: Dict[str, Dict[str, float]] = field(
        default_factory=lambda: {"default": {"USD": 1_000_000.0, "BTC": 10.0}}
    )
    error_injections: Dict[str, Iterable[Exception]] = field(default_factory=dict)


@dataclass(slots=True)
class Order:
    """Internal representation of an order in the mock order book."""

    order_id: str
    account: str
    pair: str
    side: str
    ordertype: str
    price: Optional[float]
    volume: float
    remaining: float
    status: str
    created_at: datetime
    userref: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "order_id": self.order_id,
            "account": self.account,
            "pair": self.pair,
            "side": self.side,
            "ordertype": self.ordertype,
            "price": self.price,
            "volume": self.volume,
            "remaining": self.remaining,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "userref": self.userref,
        }


@dataclass(slots=True)
class Trade:
    """Execution record produced when an order is matched."""

    trade_id: str
    order_id: str
    account: str
    pair: str
    side: str
    price: float
    volume: float
    executed_at: datetime

    def to_dict(self) -> Dict[str, object]:
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


class MockKrakenServer:
    """Minimal Kraken API simulator with deterministic behaviour."""

    def __init__(self, config: Optional[MockKrakenConfig] = None) -> None:
        self.config = config or MockKrakenConfig()
        self._balances: Dict[str, Dict[str, float]] = {
            account: dict(balances)
            for account, balances in self.config.balances.items()
        }
        self._error_queues: Dict[str, Deque[Exception]] = {
            name: deque(errors)
            for name, errors in self.config.error_injections.items()
        }
        self._order_counter = count(1)
        self._trade_counter = count(1)
        self._open_orders: Dict[str, Order] = {}
        self._bids: List[Order] = []
        self._asks: List[Order] = []
        self._trades: List[Trade] = []

    # ------------------------------------------------------------------
    # Public API surface
    # ------------------------------------------------------------------
    async def add_order(
        self,
        pair: str,
        side: str,
        *,
        volume: float,
        price: Optional[float] = None,
        ordertype: str = "limit",
        account: str = "default",
        userref: Optional[str] = None,
    ) -> Dict[str, object]:
        """Submit a new order into the toy order book."""

        await self._simulate_latency()
        self._maybe_raise("add_order")

        if side not in {"buy", "sell"}:
            raise ValueError(f"Unsupported side: {side!r}")
        if ordertype not in {"limit", "market"}:
            raise ValueError(f"Unsupported order type: {ordertype!r}")
        if ordertype == "limit" and price is None:
            raise ValueError("Limit orders must provide a price")

        order_id = f"MO{next(self._order_counter)}"
        order = Order(
            order_id=order_id,
            account=account,
            pair=pair,
            side=side,
            ordertype=ordertype,
            price=price,
            volume=volume,
            remaining=volume,
            status="open",
            created_at=datetime.now(timezone.utc),
            userref=userref,
        )

        fills = self._match_order(order)

        if order.remaining > 0 and order.price is not None:
            self._add_to_book(order)
        elif order.remaining <= 0:
            order.status = "filled"
        else:
            order.status = "closed"

        return {
            "order": order.to_dict(),
            "fills": [trade.to_dict() for trade in fills],
        }

    async def cancel_order(self, order_id: str, *, account: Optional[str] = None) -> Dict[str, object]:
        """Cancel an open order if it exists."""

        await self._simulate_latency()
        self._maybe_raise("cancel_order")

        order = self._open_orders.get(order_id)
        if order is None:
            return {"order_id": order_id, "status": "not_found"}

        if account is not None and order.account != account:
            return {"order_id": order_id, "status": "forbidden"}

        self._remove_from_book(order)
        order.status = "cancelled"
        del self._open_orders[order_id]
        return {"order_id": order_id, "status": "cancelled"}

    async def get_balance(self, account: str = "default") -> Dict[str, float]:
        """Return the balances for the requested account."""

        await self._simulate_latency()
        self._maybe_raise("get_balance")
        return dict(self._balances.setdefault(account, {}))

    async def get_trades(
        self,
        *,
        account: Optional[str] = None,
        pair: Optional[str] = None,
    ) -> List[Dict[str, object]]:
        """Return trade executions filtered by account or pair."""

        await self._simulate_latency()
        self._maybe_raise("get_trades")

        trades = self._trades
        if account is not None:
            trades = [trade for trade in trades if trade.account == account]
        if pair is not None:
            trades = [trade for trade in trades if trade.pair == pair]
        return [trade.to_dict() for trade in trades]

    # ------------------------------------------------------------------
    # Test helpers
    # ------------------------------------------------------------------
    def schedule_error(self, method: str, exc: Exception) -> None:
        """Queue an exception that will be raised on the next call to *method*."""

        self._error_queues.setdefault(method, deque()).append(exc)

    def reset(self) -> None:
        """Clear all orders and trades while preserving balances."""

        self._order_counter = count(1)
        self._trade_counter = count(1)
        self._open_orders.clear()
        self._bids.clear()
        self._asks.clear()
        self._trades.clear()

    # ------------------------------------------------------------------
    # Internal mechanics
    # ------------------------------------------------------------------
    def _add_to_book(self, order: Order) -> None:
        book = self._bids if order.side == "buy" else self._asks
        index = 0
        while index < len(book):
            other = book[index]
            if order.side == "buy":
                if other.price is not None and order.price > other.price:
                    break
                if other.price == order.price and order.created_at < other.created_at:
                    break
            else:
                if other.price is not None and order.price < other.price:
                    break
                if other.price == order.price and order.created_at < other.created_at:
                    break
            index += 1
        book.insert(index, order)
        self._open_orders[order.order_id] = order

    def _remove_from_book(self, order: Order) -> None:
        book = self._bids if order.side == "buy" else self._asks
        for index, existing in enumerate(book):
            if existing.order_id == order.order_id:
                del book[index]
                break

    def _match_order(self, order: Order) -> List[Trade]:
        trades: List[Trade] = []
        book = self._asks if order.side == "buy" else self._bids

        def price_crosses(price: float) -> bool:
            if order.price is None:
                return True
            return (order.side == "buy" and order.price >= price) or (
                order.side == "sell" and order.price <= price
            )

        # Match against resting orders first.
        index = 0
        while order.remaining > 0 and index < len(book):
            resting = book[index]
            if resting.price is None or not price_crosses(resting.price):
                break

            traded_volume = min(order.remaining, resting.remaining)
            trade_price = resting.price if resting.price is not None else self._reference_price(
                resting.side
            )
            trades.extend(self._execute_trade(order, resting, trade_price, traded_volume))

            if resting.remaining <= 0:
                self._open_orders.pop(resting.order_id, None)
                del book[index]
            else:
                index += 1

        # If still remaining, match against deterministic liquidity.
        if order.remaining > 0:
            reference_price = self._reference_price(order.side)
            if price_crosses(reference_price):
                trades.extend(
                    self._execute_trade(
                        order,
                        None,
                        reference_price,
                        order.remaining,
                    )
                )

        return trades

    def _execute_trade(
        self,
        taker: Order,
        maker: Optional[Order],
        price: float,
        volume: float,
    ) -> List[Trade]:
        now = datetime.now(timezone.utc)
        taker_trades: List[Trade] = []

        taker.remaining -= volume
        taker.status = "filled" if taker.remaining <= 0 else "partially_filled"
        self._apply_fill(taker.account, taker.pair, taker.side, price, volume)

        taker_trade = Trade(
            trade_id=f"T{next(self._trade_counter)}",
            order_id=taker.order_id,
            account=taker.account,
            pair=taker.pair,
            side=taker.side,
            price=price,
            volume=volume,
            executed_at=now,
        )
        self._trades.append(taker_trade)
        taker_trades.append(taker_trade)

        if maker is not None:
            maker.remaining -= volume
            maker.status = "filled" if maker.remaining <= 0 else "partially_filled"
            maker_side = "buy" if maker.side == "buy" else "sell"
            self._apply_fill(maker.account, maker.pair, maker_side, price, volume)

            maker_trade = Trade(
                trade_id=f"T{next(self._trade_counter)}",
                order_id=maker.order_id,
                account=maker.account,
                pair=maker.pair,
                side=maker.side,
                price=price,
                volume=volume,
                executed_at=now,
            )
            self._trades.append(maker_trade)

        return taker_trades

    def _apply_fill(
        self,
        account: str,
        pair: str,
        side: str,
        price: float,
        volume: float,
    ) -> None:
        base, quote = self._split_pair(pair)
        balances = self._balances.setdefault(account, {})
        if side == "buy":
            balances[quote] = balances.get(quote, 0.0) - price * volume
            balances[base] = balances.get(base, 0.0) + volume
        else:
            balances[base] = balances.get(base, 0.0) - volume
            balances[quote] = balances.get(quote, 0.0) + price * volume

    def _reference_price(self, side: str) -> float:
        half_spread = self.config.spread / 2
        if side == "buy":
            return self.config.base_price + half_spread
        return self.config.base_price - half_spread

    @staticmethod
    def _split_pair(pair: str) -> tuple[str, str]:
        if "/" in pair:
            return tuple(pair.split("/", 1))  # type: ignore[return-value]
        if len(pair) >= 6:
            return pair[:-3], pair[-3:]
        raise ValueError(f"Unrecognised trading pair format: {pair!r}")

    async def _simulate_latency(self) -> None:
        if self.config.latency:
            await asyncio.sleep(self.config.latency)

    def _maybe_raise(self, method: str) -> None:
        queue = self._error_queues.get(method)
        if queue:
            exc = queue.popleft()
            raise exc


@pytest.fixture
def kraken_mock_server() -> MockKrakenServer:
    """Pytest fixture that exposes a configured :class:`MockKrakenServer`."""

    return MockKrakenServer()
