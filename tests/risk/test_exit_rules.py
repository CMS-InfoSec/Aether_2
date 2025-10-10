"""Regression coverage for automated exit orchestration."""

from __future__ import annotations

from services.risk.exit_rules import ExitRuleEngine, OrderSnapshot


def _register_order(**overrides) -> tuple[ExitRuleEngine, list]:
    engine = ExitRuleEngine()
    order = OrderSnapshot(
        order_id=overrides.get("order_id", "order-1"),
        side=overrides.get("side", "buy"),
        quantity=overrides.get("quantity", 1.0),
        price=overrides.get("price", 100.0),
        take_profit=overrides.get("take_profit", 110.0),
        stop_loss=overrides.get("stop_loss", 90.0),
        trailing_offset=overrides.get("trailing_offset"),
    )
    actions = engine.register(order)
    return engine, actions


def test_register_emits_bracket_orders() -> None:
    engine, actions = _register_order()

    assert {action.action for action in actions} == {"create"}
    assert len(actions) == 2

    for action in actions:
        assert action.order is not None
        assert action.order.parent_order_id == "order-1"
        assert action.order.reduce_only is True
        assert action.order.side == "sell"

    take_profit = next(action.order for action in actions if action.order.exit_type == "take_profit")
    stop_loss = next(action.order for action in actions if action.order.exit_type in {"stop_loss", "trailing_stop"})

    assert take_profit.trigger_price == 110.0
    assert stop_loss.trigger_price == 90.0


def test_trailing_stop_updates_on_market_improvement() -> None:
    engine, actions = _register_order(stop_loss=None, trailing_offset=5.0, take_profit=None)

    assert len(actions) == 1
    initial = actions[0].order
    assert initial is not None
    assert initial.exit_type == "trailing_stop"
    assert initial.trigger_price == 95.0

    replacements = engine.update_market(best_bid=120.0, best_ask=None)
    assert replacements, "Expected trailing stop to refresh when market improves"

    replacement = replacements[0]
    assert replacement.action == "replace"
    assert replacement.cancel_order_id == initial.order_id
    assert replacement.order is not None
    assert replacement.order.exit_type == "trailing_stop"
    assert replacement.order.trigger_price > initial.trigger_price


def test_cancel_entry_emits_cancel_actions() -> None:
    engine, actions = _register_order()
    child_ids = {action.order.order_id for action in actions if action.order is not None}

    cancel_actions = engine.cancel_entry("order-1")
    assert {action.cancel_order_id for action in cancel_actions} == child_ids
    assert all(action.action == "cancel" for action in cancel_actions)
    assert engine.cancel_entry("order-1") == []

