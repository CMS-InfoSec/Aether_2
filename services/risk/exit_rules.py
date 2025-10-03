"""Exit order orchestration for bracketed risk management."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Literal, Optional


Side = Literal["buy", "sell"]
ExitType = Literal["take_profit", "stop_loss", "trailing_stop"]
ActionType = Literal["create", "cancel", "replace"]


@dataclass(frozen=True)
class OrderSnapshot:
    """Immutable view of an entry order that may require exit automation."""

    order_id: str
    side: Side
    quantity: float
    price: float
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    trailing_offset: Optional[float] = None
    reduce_only: bool = False

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.price <= 0:
            raise ValueError("price must be positive")
        if self.take_profit is not None and self.take_profit <= 0:
            raise ValueError("take_profit must be positive when provided")
        if self.stop_loss is not None and self.stop_loss <= 0:
            raise ValueError("stop_loss must be positive when provided")
        if self.trailing_offset is not None and self.trailing_offset <= 0:
            raise ValueError("trailing_offset must be positive when provided")
        if self.side not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")


@dataclass(frozen=True)
class ExitOrder:
    """Representation of an automated exit order."""

    order_id: str
    parent_order_id: str
    side: Side
    quantity: float
    trigger_price: float
    exit_type: ExitType
    reduce_only: bool = True

    def __post_init__(self) -> None:
        if self.quantity < 0:
            raise ValueError("quantity must be non-negative")
        if self.trigger_price <= 0:
            raise ValueError("trigger_price must be positive")


@dataclass(frozen=True)
class ExitOrderAction:
    """Action emitted by :class:`ExitRuleEngine` for downstream OMS handling."""

    action: ActionType
    order: Optional[ExitOrder]
    cancel_order_id: Optional[str] = None


@dataclass
class _TrackedEntry:
    order: OrderSnapshot
    exit_orders: Dict[str, ExitOrder] = field(default_factory=dict)
    versions: Dict[str, int] = field(default_factory=dict)
    filled: float = 0.0
    trailing_offset: Optional[float] = None
    trailing_anchor: Optional[float] = None
    trailing_stop_price: Optional[float] = None


class ExitRuleEngine:
    """Manage stop loss, take profit and trailing exit orchestration."""

    def __init__(self, *, quantity_tolerance: float = 1e-9, price_tolerance: float = 1e-9) -> None:
        self._entries: Dict[str, _TrackedEntry] = {}
        self._qty_tol = quantity_tolerance
        self._px_tol = price_tolerance

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------
    def register(self, order: OrderSnapshot) -> List[ExitOrderAction]:
        """Register *order* and emit required exit orders.

        Returns a list of :class:`ExitOrderAction` objects describing the
        stop-loss / take-profit child orders that should be placed.
        """

        if order.reduce_only:
            return []
        if order.order_id in self._entries:
            raise ValueError(f"Order {order.order_id} already registered")

        entry = _TrackedEntry(order=order)
        actions: List[ExitOrderAction] = []

        if order.take_profit is not None:
            exit_tp = self._create_exit_order(entry, "take_profit", order.take_profit, "take_profit")
            entry.exit_orders["take_profit"] = exit_tp
            actions.append(ExitOrderAction(action="create", order=exit_tp))

        if order.stop_loss is not None or order.trailing_offset is not None:
            trailing = order.trailing_offset
            stop_price = order.stop_loss
            exit_type: ExitType = "stop_loss"

            if trailing is not None:
                exit_type = "trailing_stop"
                entry.trailing_offset = trailing
                if stop_price is None:
                    stop_price = self._compute_initial_trailing_stop(order, trailing)
                entry.trailing_anchor = self._initial_anchor(order, stop_price, trailing)
                entry.trailing_stop_price = stop_price
            if stop_price is None:
                raise ValueError("stop_loss or trailing_offset required to compute stop price")

            exit_sl = self._create_exit_order(entry, "stop_loss", stop_price, exit_type)
            entry.exit_orders["stop_loss"] = exit_sl
            actions.append(ExitOrderAction(action="create", order=exit_sl))

        self._entries[order.order_id] = entry
        return actions

    # ------------------------------------------------------------------
    # Market monitoring
    # ------------------------------------------------------------------
    def update_market(self, *, best_bid: Optional[float], best_ask: Optional[float]) -> List[ExitOrderAction]:
        """Update trailing stops given market best bid/ask prices."""

        actions: List[ExitOrderAction] = []
        for entry in self._entries.values():
            if entry.trailing_offset is None:
                continue

            order = entry.order
            reference = best_bid if order.side == "buy" else best_ask
            if reference is None:
                continue

            improved = False
            if entry.trailing_anchor is None:
                entry.trailing_anchor = reference
                improved = True
            elif order.side == "buy" and reference > entry.trailing_anchor + self._px_tol:
                entry.trailing_anchor = reference
                improved = True
            elif order.side == "sell" and reference < entry.trailing_anchor - self._px_tol:
                entry.trailing_anchor = reference
                improved = True

            if not improved:
                continue

            new_stop = (
                entry.trailing_anchor - entry.trailing_offset
                if order.side == "buy"
                else entry.trailing_anchor + entry.trailing_offset
            )
            if entry.trailing_stop_price is not None:
                if order.side == "buy" and new_stop <= entry.trailing_stop_price + self._px_tol:
                    continue
                if order.side == "sell" and new_stop >= entry.trailing_stop_price - self._px_tol:
                    continue

            exit_sl = entry.exit_orders.get("stop_loss")
            if exit_sl is None:
                continue

            replacement = self._create_exit_order(
                entry,
                "stop_loss",
                new_stop,
                exit_sl.exit_type,
                quantity=exit_sl.quantity,
            )
            actions.append(
                ExitOrderAction(
                    action="replace",
                    order=replacement,
                    cancel_order_id=exit_sl.order_id,
                )
            )
            entry.exit_orders["stop_loss"] = replacement
            entry.trailing_stop_price = new_stop

        return actions

    # ------------------------------------------------------------------
    # Fill monitoring
    # ------------------------------------------------------------------
    def record_fill(self, order_id: str, cumulative_quantity: float) -> List[ExitOrderAction]:
        """Update the cumulative filled quantity for *order_id*.

        Exit orders are resized (via cancel/replace) to mirror the filled
        quantity so that reduce-only semantics are respected even on partial
        fills.
        """

        entry = self._entries.get(order_id)
        if entry is None:
            raise KeyError(f"Order {order_id} not registered")
        cumulative_quantity = min(cumulative_quantity, entry.order.quantity)
        if cumulative_quantity < -self._qty_tol:
            raise ValueError("cumulative_quantity cannot be negative")

        if abs(cumulative_quantity - entry.filled) <= self._qty_tol:
            return []

        entry.filled = cumulative_quantity
        actions: List[ExitOrderAction] = []

        if cumulative_quantity <= self._qty_tol:
            for label, exit_order in list(entry.exit_orders.items()):
                actions.append(
                    ExitOrderAction(
                        action="cancel",
                        order=exit_order,
                        cancel_order_id=exit_order.order_id,
                    )
                )
                entry.exit_orders.pop(label, None)
            entry.trailing_stop_price = None
            return actions

        for label, exit_order in list(entry.exit_orders.items()):
            if abs(exit_order.quantity - cumulative_quantity) <= self._qty_tol:
                continue
            replacement = self._create_exit_order(
                entry,
                label,
                exit_order.trigger_price,
                exit_order.exit_type,
                quantity=cumulative_quantity,
            )
            actions.append(
                ExitOrderAction(
                    action="replace",
                    order=replacement,
                    cancel_order_id=exit_order.order_id,
                )
            )
            entry.exit_orders[label] = replacement

        return actions

    # ------------------------------------------------------------------
    # Cancellation lifecycle
    # ------------------------------------------------------------------
    def cancel_entry(self, order_id: str) -> List[ExitOrderAction]:
        """Cancel the parent order and any associated exits."""

        entry = self._entries.pop(order_id, None)
        if entry is None:
            return []

        actions: List[ExitOrderAction] = []
        for exit_order in entry.exit_orders.values():
            actions.append(
                ExitOrderAction(
                    action="cancel",
                    order=exit_order,
                    cancel_order_id=exit_order.order_id,
                )
            )
        entry.exit_orders.clear()
        return actions

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _create_exit_order(
        self,
        entry: _TrackedEntry,
        label: str,
        trigger_price: float,
        exit_type: ExitType,
        *,
        quantity: Optional[float] = None,
    ) -> ExitOrder:
        versions = entry.versions
        version = versions.get(label, 0) + 1
        versions[label] = version

        parent = entry.order
        child_side: Side = "sell" if parent.side == "buy" else "buy"
        child_quantity = quantity if quantity is not None else parent.quantity

        child_id = f"{parent.order_id}-{label}-{version}"
        return ExitOrder(
            order_id=child_id,
            parent_order_id=parent.order_id,
            side=child_side,
            quantity=child_quantity,
            trigger_price=trigger_price,
            exit_type=exit_type,
            reduce_only=True,
        )

    @staticmethod
    def _compute_initial_trailing_stop(order: OrderSnapshot, trailing: float) -> float:
        if order.side == "buy":
            return max(order.price - trailing, 0.0) or trailing
        return order.price + trailing

    @staticmethod
    def _initial_anchor(order: OrderSnapshot, stop_price: float, trailing: float) -> float:
        if order.side == "buy":
            return stop_price + trailing
        return stop_price - trailing

    def active_entries(self) -> Iterable[str]:
        """Expose active entry identifiers for diagnostic purposes."""

        return list(self._entries)
