from __future__ import annotations

from decimal import Decimal

import pytest

from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.oms.sim_broker import SimBroker


@pytest.fixture(autouse=True)
def _reset_adapters() -> None:
    KafkaNATSAdapter.reset()
    TimescaleAdapter._events.clear()  # type: ignore[attr-defined]


def test_market_order_immediate_fill() -> None:
    broker = SimBroker("ACC-DEFAULT")

    result = broker.place_order(
        client_order_id="test-market",
        symbol="BTC-USD",
        side="buy",
        quantity=1.0,
        order_type="market",
        market_data={"last_price": 100.0, "spread_bps": 8.0, "volatility_bps": 12.0},
    )

    assert result["status"] == "filled"
    assert pytest.approx(result["filled_qty"], rel=1e-6) == 1.0
    assert result["remaining_qty"] == pytest.approx(0.0)
    assert len(result["fills"]) == 1

    fill_price = result["fills"][0]["price"]
    expected_price = 100.0 * (1 + (5.0 + 4.0 + 12.0) / 10_000.0)
    assert fill_price == pytest.approx(expected_price)

    events = KafkaNATSAdapter(account_id="ACC-DEFAULT").history()
    assert any(event["topic"] == "oms.simulated.orders" for event in events)
    fill_events = [event for event in events if event["topic"] == "oms.simulated.fills"]
    assert fill_events, "expected a fill event to be published"
    assert fill_events[-1]["payload"]["simulated"] is True

    fills = broker._timescale._events["ACC-DEFAULT"]["fills"]  # type: ignore[attr-defined]
    assert fills[-1]["payload"]["simulated"] is True


def test_limit_order_partial_fill() -> None:
    broker = SimBroker("ACC-DEFAULT")

    result = broker.place_order(
        client_order_id="limit-partial",
        symbol="ETH-USD",
        side="sell",
        quantity=Decimal("5"),
        order_type="limit",
        limit_price=Decimal("101"),
        market_data={
            "last_price": 102.0,
            "high": 103.0,
            "best_bid": 101.5,
            "available_liquidity": 2.0,
        },
    )

    assert result["status"] == "partial"
    assert result["filled_qty"] == pytest.approx(2.0)
    assert result["remaining_qty"] == pytest.approx(3.0)
    assert result["fills"][0]["price"] == pytest.approx(101.5)

    open_orders = broker.get_open_orders()
    assert len(open_orders) == 1
    assert open_orders[0]["remaining_qty"] == pytest.approx(3.0)


def test_cancel_open_order() -> None:
    broker = SimBroker("ACC-DEFAULT")

    result = broker.place_order(
        client_order_id="limit-cancel",
        symbol="BTC-USD",
        side="buy",
        quantity=Decimal("1"),
        order_type="limit",
        limit_price=Decimal("99"),
        market_data={"last_price": 102.0, "low": 101.0, "available_liquidity": 0.0},
    )

    assert result["status"] == "open"
    open_orders = broker.get_open_orders()
    assert len(open_orders) == 1

    cancelled = broker.cancel_order(result["order_id"])
    assert cancelled is True
    assert broker.get_open_orders() == []

    events = KafkaNATSAdapter(account_id="ACC-DEFAULT").history()
    statuses = [event["payload"]["status"] for event in events if event["topic"] == "oms.simulated.orders"]
    assert "cancelled" in statuses
