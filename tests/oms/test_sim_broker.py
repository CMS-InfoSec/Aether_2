from __future__ import annotations

from decimal import Decimal

import importlib
import sys
import types

import pytest

from shared.event_bus import KafkaNATSAdapter
import services.oms.sim_broker as sim_broker_module

TimescaleAdapter = sim_broker_module.TimescaleAdapter
SimBroker = sim_broker_module.SimBroker


@pytest.fixture(autouse=True)
def _stub_timescale_adapter(monkeypatch: pytest.MonkeyPatch) -> type[TimescaleAdapter]:
    class _StubTimescaleAdapter:
        _fills: list[dict[str, object]] = []
        _acks: list[dict[str, object]] = []

        def __init__(self, *, account_id: str) -> None:  # type: ignore[no-untyped-def]
            self.account_id = account_id

        @classmethod
        def reset(cls) -> None:
            cls._fills = []
            cls._acks = []

        def record_fill(self, payload: dict[str, object]) -> None:
            type(self)._fills.append(dict(payload))

        def record_ack(self, payload: dict[str, object]) -> None:
            type(self)._acks.append(dict(payload))

        def events(self) -> dict[str, list[dict[str, object]]]:
            return {
                "fills": list(type(self)._fills),
                "acks": list(type(self)._acks),
            }

    monkeypatch.setattr(sim_broker_module, "TimescaleAdapter", _StubTimescaleAdapter)
    monkeypatch.setattr(sys.modules[__name__], "TimescaleAdapter", _StubTimescaleAdapter)
    return _StubTimescaleAdapter


@pytest.fixture(autouse=True)
def _reset_adapters(_stub_timescale_adapter: type[TimescaleAdapter]) -> None:
    KafkaNATSAdapter.reset()
    _stub_timescale_adapter.reset()


def test_market_order_immediate_fill(_stub_timescale_adapter: type[TimescaleAdapter]) -> None:
    broker = SimBroker("ACC-DEFAULT", timescale_factory=_stub_timescale_adapter)

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

    fills = broker._timescale.events()["fills"]
    assert fills[-1]["simulated"] is True
    acks = broker._timescale.events()["acks"]
    assert acks[-1]["simulated"] is True


def test_limit_order_partial_fill(_stub_timescale_adapter: type[TimescaleAdapter]) -> None:
    broker = SimBroker("ACC-DEFAULT", timescale_factory=_stub_timescale_adapter)

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


def test_cancel_open_order(_stub_timescale_adapter: type[TimescaleAdapter]) -> None:
    broker = SimBroker("ACC-DEFAULT", timescale_factory=_stub_timescale_adapter)

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


def test_place_order_normalizes_symbol_variants(
    _stub_timescale_adapter: type[TimescaleAdapter],
) -> None:
    broker = SimBroker("ACC-DEFAULT", timescale_factory=_stub_timescale_adapter)

    result = broker.place_order(
        client_order_id="case-normalize",
        symbol="eth/usd",
        side="buy",
        quantity=1.0,
        order_type="market",
        market_data={"last_price": 1500.0},
    )

    assert result["symbol"] == "ETH-USD"
    events = KafkaNATSAdapter(account_id="ACC-DEFAULT").history()
    symbols = [event["payload"].get("symbol") for event in events]
    assert "ETH-USD" in symbols


def test_place_order_rejects_non_spot_symbol(
    _stub_timescale_adapter: type[TimescaleAdapter],
) -> None:
    broker = SimBroker("ACC-DEFAULT", timescale_factory=_stub_timescale_adapter)

    with pytest.raises(ValueError):
        broker.place_order(
            client_order_id="reject-derivative",
            symbol="BTC-PERP",
            side="buy",
            quantity=1.0,
            order_type="market",
            market_data={"last_price": 100.0},
        )


def test_stop_loss_take_profit_enforced(
    _stub_timescale_adapter: type[TimescaleAdapter],
) -> None:
    broker = SimBroker("ACC-DEFAULT", timescale_factory=_stub_timescale_adapter)

    result = broker.place_order(
        client_order_id="with-brackets",
        symbol="BTC-USD",
        side="buy",
        quantity=Decimal("0.5"),
        order_type="market",
        market_data={"last_price": 100.0, "high": 125.0, "low": 90.0},
        stop_loss=Decimal("95"),
        take_profit=Decimal("115"),
    )

    assert result["stop_loss"] == pytest.approx(95.0)
    assert result["take_profit"] == pytest.approx(115.0)
    assert result["stop_loss_triggered"] is True
    assert result["take_profit_triggered"] is True

    acks = broker._timescale.events()["acks"]
    assert acks[-1]["stop_loss_triggered"] is True
    assert acks[-1]["take_profit_triggered"] is True


def test_invalid_stop_loss_configuration_rejected(
    _stub_timescale_adapter: type[TimescaleAdapter],
) -> None:
    broker = SimBroker("ACC-DEFAULT", timescale_factory=_stub_timescale_adapter)

    with pytest.raises(ValueError):
        broker.place_order(
            client_order_id="bad-stop",
            symbol="BTC-USD",
            side="buy",
            quantity=1.0,
            order_type="market",
            market_data={"last_price": 100.0},
            stop_loss=Decimal("120"),
        )


def test_sim_broker_timescale_fallback_when_adapter_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub_module = types.ModuleType("services.common.adapters")
    monkeypatch.setitem(sys.modules, "services.common.adapters", stub_module)
    monkeypatch.delitem(sys.modules, "services.oms.sim_broker", raising=False)

    module = importlib.import_module("services.oms.sim_broker")

    assert module.TimescaleAdapter.__module__ == "services.oms.sim_broker"

    module.KafkaNATSAdapter.reset()
    module.TimescaleAdapter.reset()

    broker = module.SimBroker("ACC-DEFAULT")

    result = broker.place_order(
        client_order_id="fallback-case",
        symbol="BTC-USD",
        side="buy",
        quantity=0.5,
        order_type="market",
        market_data={"last_price": 100.0},
    )

    assert result["status"] == "filled"

    kafka_history = module.KafkaNATSAdapter(account_id="ACC-DEFAULT").history()
    assert kafka_history, "expected fallback Kafka adapter to capture events"

    events = broker._timescale.events()
    assert events["acks"], "expected fallback Timescale adapter to capture ack payloads"
    assert events["fills"], "expected fallback Timescale adapter to capture fill payloads"

    module.KafkaNATSAdapter.reset()
    module.TimescaleAdapter.reset()
