"""Utilities for deriving protective exit price levels."""

from __future__ import annotations

from typing import Optional, Tuple

_DEFAULT_TAKE_PROFIT_BPS = 250.0
_DEFAULT_STOP_LOSS_BPS = 125.0
_ATR_TAKE_PROFIT_MULTIPLIER = 3.0
_ATR_STOP_LOSS_MULTIPLIER = 2.0


def _normalize_side(side: str) -> str:
    normalized = (side or "").strip().lower()
    if normalized not in {"buy", "sell"}:
        raise ValueError("side must be 'buy' or 'sell'")
    return normalized


def _price_from_bps(price: float, side: str, bps: float, exit_type: str) -> Optional[float]:
    if price <= 0 or bps is None or bps <= 0:
        return None
    normalized_side = _normalize_side(side)
    factor = bps / 10_000.0
    if exit_type == "take_profit":
        adjustment = 1 + factor if normalized_side == "buy" else 1 - factor
    else:
        adjustment = 1 - factor if normalized_side == "buy" else 1 + factor
    candidate = price * adjustment
    if candidate <= 0:
        return None
    return candidate


def compute_exit_targets(
    *,
    price: float,
    side: str,
    take_profit_bps: Optional[float] = None,
    stop_loss_bps: Optional[float] = None,
    atr: Optional[float] = None,
) -> Tuple[Optional[float], Optional[float]]:
    """Return recommended take-profit and stop-loss trigger prices."""

    if price <= 0:
        return None, None

    normalized_side = _normalize_side(side)
    take_profit = _price_from_bps(price, normalized_side, take_profit_bps, "take_profit")
    stop_loss = _price_from_bps(price, normalized_side, stop_loss_bps, "stop_loss")

    if atr is not None and atr > 0:
        atr_take_profit = price + atr * _ATR_TAKE_PROFIT_MULTIPLIER
        atr_stop_loss = price - atr * _ATR_STOP_LOSS_MULTIPLIER
        if normalized_side == "sell":
            atr_take_profit = max(price - atr * _ATR_TAKE_PROFIT_MULTIPLIER, 0.0)
            atr_stop_loss = price + atr * _ATR_STOP_LOSS_MULTIPLIER
        if take_profit is None:
            take_profit = atr_take_profit
        if stop_loss is None:
            stop_loss = atr_stop_loss

    if take_profit is None:
        default_tp_offset = price * (_DEFAULT_TAKE_PROFIT_BPS / 10_000.0)
        take_profit = (
            price + default_tp_offset if normalized_side == "buy" else max(price - default_tp_offset, 0.0)
        )
    if stop_loss is None:
        default_sl_offset = price * (_DEFAULT_STOP_LOSS_BPS / 10_000.0)
        stop_loss = (
            price - default_sl_offset if normalized_side == "buy" else price + default_sl_offset
        )

    take_profit = max(take_profit, 0.0)
    stop_loss = max(stop_loss, 0.0)

    return round(take_profit, 8), round(stop_loss, 8)


__all__ = ["compute_exit_targets"]
