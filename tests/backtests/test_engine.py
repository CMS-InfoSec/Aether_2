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
        MarketEvent(start, "order_book", {"bid": 100.0, "ask": 101.0, "bid_size": 0.0, "ask_size": 0.0}),
        MarketEvent(start + timedelta(seconds=1), "trade", {"price": 100.0, "quantity": 1.0}),
        MarketEvent(start + timedelta(seconds=2), "order_book", {"bid": 100.0, "ask": 101.0, "bid_size": 0.0, "ask_size": 0.0}),
        MarketEvent(start + timedelta(seconds=3), "trade", {"price": 100.0, "quantity": 1.0}),
    ]
    decisions = [
        PolicyDecision(orders=[OrderIntent(side="buy", quantity=1.0, price=100.0)]),
        {},
        PolicyDecision(orders=[OrderIntent(side="sell", quantity=1.0, order_type="market")]),
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
    trade_log = engine.hooks.get_trade_log()
    assert len(trade_log) == 2
    assert trade_log[0]["liquidity"] == "maker"
    assert trade_log[1]["net_value"] > 0


def test_partial_fill_and_slippage():
    start = datetime(2023, 1, 1)
    events = [
        MarketEvent(start, "order_book", {"bid": 100.0, "ask": 101.0, "ask_size": 0.0}),
        MarketEvent(start + timedelta(seconds=1), "trade", {"price": 105.0, "quantity": 2.0}),
        MarketEvent(start + timedelta(seconds=2), "trade", {"price": 105.0, "quantity": 3.0}),
    ]
    decisions = [
        PolicyDecision(orders=[OrderIntent(side="sell", quantity=5.0, price=105.0)]),
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
    assert engine.orders[first_fill.order_id].queue_position == 0.0


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


def test_flash_crash_triggers_protections():
    start = datetime(2023, 1, 1)
    events = [
        MarketEvent(start, "order_book", {"bid": 100.0, "ask": 101.0, "bid_size": 1.0, "ask_size": 1.0}),
        MarketEvent(start + timedelta(seconds=1), "trade", {"price": 101.0, "quantity": 1.0}),
        MarketEvent(start + timedelta(seconds=2), "order_book", {"bid": 112.0, "ask": 113.0}),
        MarketEvent(start + timedelta(seconds=3), "order_book", {"bid": 90.0, "ask": 91.0}),
    ]
    decisions = [
        PolicyDecision(
            orders=[
                OrderIntent(
                    side="buy",
                    quantity=1.0,
                    order_type="market",
                    stop_loss=None,
                    take_profit=120.0,
                    trailing_offset=5.0,
                )
            ]
        ),
        {},
        {},
        {},
    ]
    engine = BacktestEngine(FeeSchedule(maker=0.0005, taker=0.001), events)
    engine.run(ScriptedStrategy(decisions))

    assert engine.position == pytest.approx(0.0)
    assert len(engine.fills) == 2
    exit_fill = engine.fills[-1]
    assert exit_fill.liquidity_flag == "taker"
    assert engine.hooks.get_trade_log()[-1]["reason"] == "trailing_stop"


def test_latency_gap_delays_execution():
    start = datetime(2023, 1, 1)
    events = [
        MarketEvent(start, "order_book", {"bid": 100.0, "ask": 101.0, "bid_size": 0.0}),
        MarketEvent(start + timedelta(seconds=1), "trade", {"price": 100.0, "quantity": 1.0}),
        MarketEvent(start + timedelta(seconds=3), "trade", {"price": 100.0, "quantity": 1.0}),
    ]
    decisions = [
        PolicyDecision(orders=[OrderIntent(side="buy", quantity=1.0, price=100.0)]),
        {},
        {},
    ]
    engine = BacktestEngine(FeeSchedule(maker=0.0005, taker=0.001), events, latency_seconds=2.0)
    engine.run(ScriptedStrategy(decisions))

    assert len(engine.fills) == 1
    fill = engine.fills[0]
    assert fill.timestamp == events[2].timestamp
    assert engine.orders[1].activation_time == start + timedelta(seconds=2)
