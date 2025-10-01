"""Event-driven backtest engine for Kraken market data."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Callable, Dict, Iterable, Iterator, List, Optional

from backtests.strategies.hooks import StrategyHooks


@dataclass
class MarketEvent:
    """Normalized market event coming from historical streams."""

    timestamp: datetime
    type: str  # "order_book" or "trade"
    data: Dict[str, float]


@dataclass
class FeeSchedule:
    maker: float
    taker: float


@dataclass
class OrderIntent:
    """Intent returned by a strategy policy."""

    side: str  # "buy" or "sell"
    quantity: float
    price: Optional[float] = None
    order_type: str = "limit"  # "limit" or "market"
    allow_partial: bool = True
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    trailing_offset: Optional[float] = None
    trailing_percentage: Optional[float] = None


@dataclass
class Order:
    """Internal representation of an order managed by the engine."""

    order_id: int
    intent: OrderIntent
    timestamp: datetime
    remaining: float
    filled: float = 0.0
    status: str = "open"
    is_taker: bool = False
    activation_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    queue_position: float = 0.0
    pending_market_fill: bool = False
    exit_reason: Optional[str] = None


@dataclass
class Fill:
    order_id: int
    timestamp: datetime
    price: float
    quantity: float
    fee: float
    liquidity_flag: str  # "maker" or "taker"


class TimescaleDataFeed:
    """Simple adapter that replays historical Timescale/Kafka/NATS events."""

    def __init__(
        self,
        order_book_fetcher: Optional[Callable[[], Iterable[Dict]]] = None,
        trade_fetcher: Optional[Callable[[], Iterable[Dict]]] = None,
        kafka_topics: Optional[Dict[str, Iterable[Dict]]] = None,
        nats_subjects: Optional[Dict[str, Iterable[Dict]]] = None,
    ) -> None:
        self.order_book_fetcher = order_book_fetcher
        self.trade_fetcher = trade_fetcher
        self.kafka_topics = kafka_topics or {}
        self.nats_subjects = nats_subjects or {}

    def __iter__(self) -> Iterator[MarketEvent]:
        events: List[MarketEvent] = []
        if self.order_book_fetcher:
            for payload in self.order_book_fetcher():
                events.append(
                    MarketEvent(
                        timestamp=payload["timestamp"],
                        type="order_book",
                        data={
                            "bid": payload.get("bid"),
                            "ask": payload.get("ask"),
                            "spread": payload.get("spread", 0.0),
                        },
                    )
                )
        if self.trade_fetcher:
            for payload in self.trade_fetcher():
                events.append(
                    MarketEvent(
                        timestamp=payload["timestamp"],
                        type="trade",
                        data={
                            "price": payload.get("price"),
                            "quantity": payload.get("quantity", 0.0),
                        },
                    )
                )
        for payload in self._iter_topics(self.kafka_topics):
            events.append(payload)
        for payload in self._iter_topics(self.nats_subjects):
            events.append(payload)
        events.sort(key=lambda e: e.timestamp)
        for event in events:
            yield event

    @staticmethod
    def _iter_topics(topics: Dict[str, Iterable[Dict]]) -> Iterator[MarketEvent]:
        for stream in topics.values():
            for payload in stream:
                yield MarketEvent(
                    timestamp=payload["timestamp"],
                    type=payload.get("type", "trade"),
                    data=payload.get("data", {}),
                )


class BacktestEngine:
    """Core event-driven simulator used for policy evaluation."""

    def __init__(
        self,
        fee_schedule: FeeSchedule,
        data_feed: Iterable[MarketEvent],
        slippage_bps: float = 0.0,
        spread_bps: float = 0.0,
        hooks: Optional[StrategyHooks] = None,
        latency_seconds: float = 0.0,
    ) -> None:
        self.fee_schedule = fee_schedule
        self.data_feed = data_feed
        self.slippage_bps = slippage_bps
        self.spread_bps = spread_bps
        self.hooks = hooks or StrategyHooks()
        self.orders: Dict[int, Order] = {}
        self.fills: List[Fill] = []
        self.position: float = 0.0
        self.cash: float = 0.0
        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None
        self.best_bid_size: float = 0.0
        self.best_ask_size: float = 0.0
        self.current_time: datetime = datetime.now(UTC)
        self.latency = timedelta(seconds=latency_seconds)

    def run(self, strategy: "Policy") -> None:
        for event in self.data_feed:
            self.current_time = event.timestamp
            if event.type == "order_book":
                self._update_book(event.data)
                self.hooks.evaluate_tp_sl(self, event.data)
            elif event.type == "trade":
                self._process_trade(event)
            decision = strategy.on_event(event, self)
            self.hooks.process_decision(decision, self.place_order)

    def place_order(
        self,
        intent: OrderIntent,
        force_taker: bool = False,
        exit_reason: Optional[str] = None,
    ) -> Order:
        order_id = self.hooks.next_order_id()
        is_taker = self._is_taker(intent, force_taker)
        activation_time = self.current_time + self.latency
        order = Order(
            order_id=order_id,
            intent=intent,
            timestamp=self.current_time,
            remaining=intent.quantity,
            is_taker=is_taker,
            activation_time=activation_time,
            queue_position=self._initial_queue_position(intent, is_taker),
            exit_reason=exit_reason,
        )
        self.orders[order_id] = order
        if is_taker and activation_time <= self.current_time:
            self._fill_order(order, self._execution_price(intent.side))
        elif is_taker:
            order.pending_market_fill = True
        return order

    def place_exit_order(self, original_order_id: int, price: float, reason: str) -> None:
        side = "sell" if self.position > 0 else "buy"
        intent = OrderIntent(side=side, quantity=abs(self.position), price=price, order_type="market")
        intent.allow_partial = False
        order = self.place_order(intent, force_taker=True, exit_reason=reason)
        order.status = reason
        self.hooks.on_exit(original_order_id)

    def _update_book(self, book: Dict[str, float]) -> None:
        spread = book.get("spread")
        self.best_bid = book.get("bid")
        self.best_ask = book.get("ask")
        self.best_bid_size = book.get("bid_size", self.best_bid_size)
        self.best_ask_size = book.get("ask_size", self.best_ask_size)
        if spread and self.best_bid and not self.best_ask:
            self.best_ask = self.best_bid + spread
        if spread and self.best_ask and not self.best_bid:
            self.best_bid = self.best_ask - spread

    def _process_trade(self, event: MarketEvent) -> None:
        price = event.data.get("price")
        quantity = event.data.get("quantity", 0.0)
        if not price or quantity <= 0:
            return
        for order in list(self.orders.values()):
            if order.status not in {"open", "partial"}:
                continue
            if order.activation_time > event.timestamp:
                continue
            if order.pending_market_fill:
                execution_price = self._execution_price(order.intent.side)
                self._fill_order(order, execution_price)
                order.pending_market_fill = False
                continue
            if self._is_fillable(order, price):
                available, consumed = self._available_quantity(order, price, quantity)
                if available <= 0:
                    quantity -= consumed
                    continue
                traded = min(order.remaining, available if order.intent.allow_partial else order.remaining)
                if traded <= 0:
                    quantity -= consumed
                    continue
                self._fill_order(order, price, traded)
                quantity -= traded + consumed
                if quantity <= 0:
                    break

    def _is_fillable(self, order: Order, trade_price: float) -> bool:
        if order.intent.order_type == "market":
            return True
        price = order.intent.price
        if price is None:
            return True
        if order.intent.side == "buy":
            return trade_price <= price
        return trade_price >= price

    def _available_quantity(self, order: Order, trade_price: float, quantity: float) -> tuple[float, float]:
        if order.is_taker:
            return quantity, 0.0
        if not self._trade_crosses_price(order, trade_price):
            return 0.0, 0.0
        if order.queue_position > 0:
            consumed = min(order.queue_position, quantity)
            order.queue_position -= consumed
            return max(0.0, quantity - consumed), consumed
        return quantity, 0.0

    def _fill_order(self, order: Order, price: float, quantity: Optional[float] = None) -> None:
        quantity = quantity or order.remaining
        if quantity <= 0:
            return
        adjusted_price = self._apply_slippage(price, order.intent.side)
        value = adjusted_price * quantity
        fee_rate = self.fee_schedule.taker if order.is_taker else self.fee_schedule.maker
        fee = value * fee_rate
        liquidity_flag = "taker" if order.is_taker else "maker"
        if order.intent.side == "buy":
            self.position += quantity
            self.cash -= value + fee
        else:
            self.position -= quantity
            self.cash += value - fee
        order.remaining -= quantity
        order.filled += quantity
        if order.remaining <= 1e-9:
            order.status = "filled"
            order.queue_position = 0.0
        else:
            order.status = "partial"
        self.fills.append(
            Fill(
                order_id=order.order_id,
                timestamp=self.current_time,
                price=adjusted_price,
                quantity=quantity,
                fee=fee,
                liquidity_flag=liquidity_flag,
            )
        )
        self.hooks.on_fill(order, self.fills[-1], self)

    def _execution_price(self, side: str) -> float:
        base_price = self.best_ask if side == "buy" else self.best_bid
        if base_price is None:
            raise ValueError("Order book not initialized before market order.")
        spread_adj = base_price * (self.spread_bps / 10_000)
        return base_price + spread_adj if side == "buy" else base_price - spread_adj

    def _apply_slippage(self, price: float, side: str) -> float:
        if self.slippage_bps <= 0:
            return price
        adj = price * (self.slippage_bps / 10_000)
        return price + adj if side == "buy" else price - adj

    def _is_taker(self, intent: OrderIntent, force_taker: bool) -> bool:
        if force_taker or intent.order_type == "market":
            return True
        if intent.price is None:
            return True
        if intent.side == "buy" and self.best_ask is not None:
            return intent.price >= self.best_ask
        if intent.side == "sell" and self.best_bid is not None:
            return intent.price <= self.best_bid
        return False

    def _initial_queue_position(self, intent: OrderIntent, is_taker: bool) -> float:
        if is_taker:
            return 0.0
        if intent.side == "buy":
            return float(self.best_bid_size or 0.0)
        return float(self.best_ask_size or 0.0)

    @staticmethod
    def _trade_crosses_price(order: Order, trade_price: float) -> bool:
        if order.intent.side == "buy":
            return trade_price <= (order.intent.price or trade_price)
        return trade_price >= (order.intent.price or trade_price)


class Policy:
    """Minimal strategy protocol used by the engine."""

    def on_event(self, event: MarketEvent, engine: BacktestEngine) -> Dict:
        raise NotImplementedError
