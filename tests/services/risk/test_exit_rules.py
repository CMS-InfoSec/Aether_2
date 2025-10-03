"""Unit tests for :mod:`services.risk.exit_rules`."""

from __future__ import annotations

import importlib.util
import math
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "services" / "risk" / "exit_rules.py"

services_pkg = types.ModuleType("services")
services_pkg.__path__ = [str(ROOT / "services")]
sys.modules.setdefault("services", services_pkg)

risk_pkg = types.ModuleType("services.risk")
risk_pkg.__path__ = [str(ROOT / "services" / "risk")]
sys.modules.setdefault("services.risk", risk_pkg)

spec = importlib.util.spec_from_file_location("services.risk.exit_rules", MODULE_PATH)
assert spec and spec.loader, "Failed to load exit_rules module specification"
exit_rules = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = exit_rules
spec.loader.exec_module(exit_rules)

ExitRuleEngine = exit_rules.ExitRuleEngine
OrderSnapshot = exit_rules.OrderSnapshot


def _build_order(**overrides: float | str | bool) -> OrderSnapshot:
    payload = {
        "order_id": "O-1",
        "side": "buy",
        "quantity": 5.0,
        "price": 100.0,
        "take_profit": 110.0,
        "stop_loss": 95.0,
        "trailing_offset": None,
        "reduce_only": False,
    }
    payload.update(overrides)
    return OrderSnapshot(**payload)


def test_attach_exits_on_register() -> None:
    engine = ExitRuleEngine()
    order = _build_order()

    actions = engine.register(order)

    assert len(actions) == 2
    tp_action = next(action for action in actions if action.order.exit_type == "take_profit")
    sl_action = next(action for action in actions if action.order.exit_type != "take_profit")

    assert tp_action.action == "create"
    assert tp_action.order.quantity == pytest.approx(order.quantity)
    assert tp_action.order.trigger_price == pytest.approx(order.take_profit)
    assert tp_action.order.reduce_only is True

    assert sl_action.action == "create"
    assert sl_action.order.trigger_price == pytest.approx(order.stop_loss)
    assert sl_action.order.reduce_only is True


def test_trailing_stop_updates_on_improving_price() -> None:
    engine = ExitRuleEngine(price_tolerance=1e-6)
    order = _build_order(stop_loss=None, trailing_offset=2.5)

    actions = engine.register(order)
    assert len(actions) == 2
    trailing = next(action for action in actions if action.order.exit_type == "trailing_stop")
    assert trailing.order.trigger_price == pytest.approx(97.5)

    # Price moves in favour of a long position -> trailing stop ratchets higher
    updates = engine.update_market(best_bid=102.0, best_ask=103.0)
    assert updates, "Trailing stop should be replaced when price improves"
    replace_action = updates[0]
    assert replace_action.action == "replace"
    assert replace_action.cancel_order_id == trailing.order.order_id
    assert replace_action.order.trigger_price > trailing.order.trigger_price

    # Non-improving price should not trigger further replacements
    assert not engine.update_market(best_bid=101.0, best_ask=102.0)

    # Another improvement moves the stop higher again
    updates = engine.update_market(best_bid=105.0, best_ask=106.0)
    assert updates[-1].order.trigger_price > replace_action.order.trigger_price


def test_cancel_entry_cancels_children() -> None:
    engine = ExitRuleEngine()
    engine.register(_build_order())

    cancels = engine.cancel_entry("O-1")
    assert {action.action for action in cancels} == {"cancel"}
    assert {action.cancel_order_id for action in cancels}
    assert not list(engine.active_entries())


def test_reduce_only_orders_do_not_register() -> None:
    engine = ExitRuleEngine()
    actions = engine.register(_build_order(reduce_only=True))

    assert actions == []
    assert not list(engine.active_entries())


@pytest.mark.parametrize("fill_sequence", [[2.0, 4.0], [3.5, 5.0]])
def test_partial_fills_resize_exit_orders(fill_sequence: list[float]) -> None:
    engine = ExitRuleEngine()
    order = _build_order()
    engine.register(order)

    previous_quantity = 0.0
    for filled in fill_sequence:
        actions = engine.record_fill(order.order_id, filled)
        assert actions, "Each partial fill should trigger a resize"
        for action in actions:
            assert action.action == "replace"
            assert action.order is not None
            assert math.isclose(action.order.quantity, filled)
            assert action.order.reduce_only is True
        previous_quantity = filled

    # Replaying the last fill should be a no-op
    assert not engine.record_fill(order.order_id, previous_quantity)

    # When the position fully unwinds we cancel remaining exits
    cancels = engine.record_fill(order.order_id, 0.0)
    if previous_quantity > 0.0:
        assert {action.action for action in cancels} == {"cancel"}
        assert all(action.cancel_order_id for action in cancels)
