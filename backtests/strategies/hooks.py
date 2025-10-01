"""Strategy evaluation hooks for policy integration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, TYPE_CHECKING, Union

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from backtests.engine import BacktestEngine, Order, OrderIntent


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
        self._order_seq: int = 0

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
            if intent.take_profit is not None:
                self.pending_tp[order.order_id] = intent.take_profit
            if intent.stop_loss is not None:
                self.pending_sl[order.order_id] = intent.stop_loss
            created.append(order)
        if decision.circuit_breaker:
            self.halted = True
        return created

    def evaluate_tp_sl(self, engine: BacktestEngine, market_state: Dict[str, float]) -> None:
        """Monitor market state for TP/SL triggers and create exit orders."""

        if self.halted:
            return
        triggered_tp: List[int] = []
        for order_id, target in list(self.pending_tp.items()):
            if engine.position <= 0:
                continue
            if market_state.get("bid") and market_state["bid"] >= target:
                engine.place_exit_order(order_id, target, "take_profit")
                triggered_tp.append(order_id)
        for order_id in triggered_tp:
            self.pending_tp.pop(order_id, None)
            self.pending_sl.pop(order_id, None)
        triggered_sl: List[int] = []
        for order_id, threshold in list(self.pending_sl.items()):
            if engine.position >= 0:
                continue
            if market_state.get("ask") and market_state["ask"] <= threshold:
                engine.place_exit_order(order_id, threshold, "stop_loss")
                triggered_sl.append(order_id)
        for order_id in triggered_sl:
            self.pending_sl.pop(order_id, None)
            self.pending_tp.pop(order_id, None)
