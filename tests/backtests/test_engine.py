from datetime import datetime, timedelta

import pytest

from backtests.engine import (
    BacktestEngine,
    FeeSchedule,
    MarketEvent,
    OrderIntent,
    Policy,
)
from backtests.strategies.hooks import PolicyDecision


class ScriptedStrategy(Policy):
    def __init__(self, decisions):
        self.decisions = list(decisions)

    def on_event(self, event, engine):
        if not self.decisions:
            return {}
        return self.decisions.pop(0)


def test_fee_calculation_maker_vs_taker():
    start = datetime(2023, 1, 1)
    events = [
        MarketEvent(start, "order_book", {"bid": 100.0, "ask": 101.0}),
        MarketEvent(start + timedelta(seconds=1), "trade", {"price": 100.0, "quantity": 1.0}),
        MarketEvent(start + timedelta(seconds=2), "order_book", {"bid": 100.0, "ask": 101.0}),
        MarketEvent(start + timedelta(seconds=3), "trade", {"price": 100.0, "quantity": 1.0}),
    ]
    decisions = [
        {"orders": [OrderIntent(side="buy", quantity=1.0, price=100.0)]},
        {},
        {"orders": [OrderIntent(side="sell", quantity=1.0, order_type="market")]},
        {},
    ]
    strategy = ScriptedStrategy(decisions)
    engine = BacktestEngine(FeeSchedule(maker=0.001, taker=0.002), events)
    engine.run(strategy)

    assert len(engine.fills) == 2
    maker_fill, taker_fill = engine.fills
    assert maker_fill.liquidity_flag == "maker"
    assert taker_fill.liquidity_flag == "taker"
    assert pytest.approx(maker_fill.fee, rel=1e-6) == 100.0 * 1.0 * 0.001
    assert pytest.approx(taker_fill.fee, rel=1e-6) == 100.0 * 1.0 * 0.002


def test_partial_fill_and_slippage():
    start = datetime(2023, 1, 1)
    events = [
        MarketEvent(start, "order_book", {"bid": 100.0, "ask": 101.0}),
        MarketEvent(start + timedelta(seconds=1), "trade", {"price": 105.0, "quantity": 2.0}),
        MarketEvent(start + timedelta(seconds=2), "trade", {"price": 105.0, "quantity": 3.0}),
    ]
    decisions = [
        {"orders": [OrderIntent(side="sell", quantity=5.0, price=105.0)]},
        {},
        {},
    ]
    strategy = ScriptedStrategy(decisions)
    engine = BacktestEngine(FeeSchedule(maker=0.001, taker=0.002), events, slippage_bps=10)
    engine.run(strategy)

    assert len(engine.fills) == 2
    first_fill, second_fill = engine.fills
    assert first_fill.quantity == 2.0
    assert engine.orders[first_fill.order_id].status in {"partial", "filled"}
    expected_price = 105.0 - (105.0 * 0.001)
    assert pytest.approx(first_fill.price, rel=1e-6) == expected_price
    assert second_fill.quantity == 3.0
    assert pytest.approx(second_fill.price, rel=1e-6) == expected_price
    assert engine.orders[first_fill.order_id].status == "filled"


def test_circuit_breaker_halts_trading():
    start = datetime(2023, 1, 1)
    events = [
        MarketEvent(start, "order_book", {"bid": 100.0, "ask": 101.0}),
        MarketEvent(start + timedelta(seconds=1), "trade", {"price": 100.0, "quantity": 1.0}),
        MarketEvent(start + timedelta(seconds=2), "order_book", {"bid": 101.0, "ask": 102.0}),
    ]
    decisions = [
        PolicyDecision(
            orders=[OrderIntent(side="buy", quantity=1.0, price=101.0)],
            circuit_breaker=True,
        ),
        {"orders": [OrderIntent(side="sell", quantity=1.0, order_type="market")]},
        {"orders": [OrderIntent(side="sell", quantity=1.0, order_type="market")]},
    ]
    strategy = ScriptedStrategy(decisions)
    engine = BacktestEngine(FeeSchedule(maker=0.001, taker=0.002), events)
    engine.run(strategy)

    assert len(engine.fills) == 1
    assert engine.hooks.halted is True
    assert engine.orders[1].status in {"filled", "partial"}
