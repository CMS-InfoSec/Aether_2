"""Strategy evaluation hooks for policy integration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from backtests.engine import BacktestEngine, Fill, Order, OrderIntent


@dataclass
class PolicyDecision:
    """Container emitted by a policy implementation."""

    orders: List[OrderIntent]
    circuit_breaker: bool = False
    force_taker: bool = False


class StrategyHooks:
    """Coordinates policy decisions with the backtest engine."""

    def __init__(self) -> None:
        self.halted: bool = False
        self.pending_tp: Dict[int, float] = {}
        self.pending_sl: Dict[int, float] = {}
        self.trailing_states: Dict[int, Dict[str, Any]] = {}
        self.order_side: Dict[int, str] = {}
        self._order_seq: int = 0
        self.trade_log: List[Dict[str, Any]] = []

    def next_order_id(self) -> int:
        self._order_seq += 1
        return self._order_seq

    def process_decision(
        self,
        decision: Union[PolicyDecision, Dict],
        submit: Callable[[OrderIntent, bool], Order],
    ) -> List[Order]:
        """Convert policy decisions into actual engine orders."""

        if not decision:
            return []
        if isinstance(decision, dict):
            decision = PolicyDecision(**decision)
        created: List[Order] = []
        if self.halted:
            return created
        for intent in decision.orders:
            order = submit(intent, decision.force_taker)
            self._register_protections(order.order_id, intent)
            created.append(order)
        if decision.circuit_breaker:
            self.halted = True
        return created

    def evaluate_tp_sl(self, engine: BacktestEngine, market_state: Dict[str, float]) -> None:
        """Monitor market state for TP/SL triggers and create exit orders."""

        if self.halted:
            return
        triggered_tp: List[int] = []
        bid = market_state.get("bid")
        ask = market_state.get("ask")
        for order_id, target in list(self.pending_tp.items()):
            direction = self.order_side.get(order_id)
            if direction == "buy" and engine.position > 0 and bid is not None and bid >= target:
                engine.place_exit_order(order_id, target, "take_profit")
                triggered_tp.append(order_id)
            elif direction == "sell" and engine.position < 0 and ask is not None and ask <= target:
                engine.place_exit_order(order_id, target, "take_profit")
                triggered_tp.append(order_id)
        for order_id in triggered_tp:
            self._clear_order(order_id)
        triggered_sl: List[int] = []
        for order_id, threshold in list(self.pending_sl.items()):
            direction = self.order_side.get(order_id)
            if direction == "buy" and engine.position > 0 and ask is not None and ask <= threshold:
                engine.place_exit_order(order_id, threshold, "stop_loss")
                triggered_sl.append(order_id)
            elif direction == "sell" and engine.position < 0 and bid is not None and bid >= threshold:
                engine.place_exit_order(order_id, threshold, "stop_loss")
                triggered_sl.append(order_id)
        for order_id in triggered_sl:
            self._clear_order(order_id)
        self._evaluate_trailing(engine, market_state)

    def on_fill(self, order: Order, fill: Fill, engine: BacktestEngine) -> None:
        """Record fills for reporting and update trailing anchors."""

        gross_value = fill.price * fill.quantity
        if order.intent.side == "buy":
            net_value = -(gross_value + fill.fee)
        else:
            net_value = gross_value - fill.fee
        self.trade_log.append(
            {
                "order_id": order.order_id,
                "timestamp": fill.timestamp,
                "side": order.intent.side,
                "quantity": fill.quantity,
                "price": fill.price,
                "fee": fill.fee,
                "liquidity": fill.liquidity_flag,
                "gross_value": gross_value,
                "net_value": net_value,
                "reason": order.exit_reason or order.status,
            }
        )
        state = self.trailing_states.get(order.order_id)
        if state:
            if state["direction"] == "long":
                current = state.get("extreme")
                state["extreme"] = fill.price if current is None else max(current, fill.price)
            else:
                current = state.get("extreme")
                state["extreme"] = fill.price if current is None else min(current, fill.price)

    def on_exit(self, order_id: int) -> None:
        self._clear_order(order_id)

    def get_trade_log(self) -> List[Dict[str, Any]]:
        return list(self.trade_log)

    def _register_protections(self, order_id: int, intent: "OrderIntent") -> None:
        if intent.take_profit is not None:
            self.pending_tp[order_id] = intent.take_profit
        if intent.stop_loss is not None:
            self.pending_sl[order_id] = intent.stop_loss
        if intent.trailing_offset is not None or intent.trailing_percentage is not None:
            self.trailing_states[order_id] = {
                "direction": "long" if intent.side == "buy" else "short",
                "offset": intent.trailing_offset,
                "percentage": intent.trailing_percentage,
                "extreme": None,
            }
        self.order_side[order_id] = intent.side

    def _clear_order(self, order_id: int) -> None:
        self.pending_tp.pop(order_id, None)
        self.pending_sl.pop(order_id, None)
        self.trailing_states.pop(order_id, None)
        self.order_side.pop(order_id, None)

    def _evaluate_trailing(self, engine: BacktestEngine, market_state: Dict[str, float]) -> None:
        if not self.trailing_states:
            return
        bid = market_state.get("bid")
        ask = market_state.get("ask")
        triggered: List[int] = []
        for order_id, state in list(self.trailing_states.items()):
            direction = state["direction"]
            if direction == "long":
                if bid is None or engine.position <= 0:
                    continue
                current = state.get("extreme")
                state["extreme"] = bid if current is None else max(current, bid)
                stop_price = self._compute_trailing_stop(state, state["extreme"], True)
                if stop_price is None:
                    continue
                if bid <= stop_price:
                    engine.place_exit_order(order_id, stop_price, "trailing_stop")
                    triggered.append(order_id)
            else:
                if ask is None or engine.position >= 0:
                    continue
                current = state.get("extreme")
                state["extreme"] = ask if current is None else min(current, ask)
                stop_price = self._compute_trailing_stop(state, state["extreme"], False)
                if stop_price is None:
                    continue
                if ask >= stop_price:
                    engine.place_exit_order(order_id, stop_price, "trailing_stop")
                    triggered.append(order_id)
        for order_id in triggered:
            self._clear_order(order_id)

    @staticmethod
    def _compute_trailing_stop(state: Dict[str, Any], extreme: float, is_long: bool) -> Optional[float]:
        offset = state.get("offset")
        pct = state.get("percentage")
        if offset is None and pct is None:
            return None
        if pct is not None:
            move = extreme * pct
        else:
            move = offset or 0.0
        return extreme - move if is_long else extreme + move
