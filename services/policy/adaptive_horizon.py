"""Adaptive horizon selection utilities for the policy service.

This module exposes a small helper that determines the appropriate model
horizon given the latest market features.  The selection is intentionally
simple – the policy service only needs to differentiate between the three
supported market regimes and choose a coarse time horizon for model
inference.  The helper keeps a small amount of state so that horizon changes
can be logged once per transition.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping
import logging


logger = logging.getLogger(__name__)


# Horizon durations expressed in seconds so that they can be consumed directly
# by the model server or any other downstream component that expects SI units.
_TREND_HORIZON = 60 * 60  # 1 hour
_RANGE_HORIZON = 15 * 60  # 15 minutes
_HIGH_VOL_HORIZON = 5 * 60  # 5 minutes
_DEFAULT_HORIZON = _RANGE_HORIZON

# Cache the most recent horizon per symbol so we only emit transition logs
# when the value actually changes.
_LAST_HORIZONS: dict[str, int] = {}


def _extract_regime(features: Mapping[str, Any] | None) -> str:
    """Pull the regime label from the provided feature payload."""

    if not isinstance(features, Mapping):
        return "unknown"

    regime = features.get("regime")
    if isinstance(regime, str) and regime:
        return regime.lower()

    state = features.get("state")
    if isinstance(state, Mapping):
        nested_regime = state.get("regime")
        if isinstance(nested_regime, str) and nested_regime:
            return nested_regime.lower()

    return "unknown"


def _extract_symbol(features: Mapping[str, Any] | None) -> str:
    if not isinstance(features, Mapping):
        return "UNKNOWN"

    symbol = features.get("symbol") or features.get("instrument")
    if isinstance(symbol, str) and symbol:
        return symbol.upper()
    return "UNKNOWN"


def _resolve_horizon(regime: str) -> int:
    if regime == "trend":
        return _TREND_HORIZON
    if regime in {"range", "neutral"}:
        return _RANGE_HORIZON
    if regime in {"high_vol", "high-vol", "highvol"}:
        return _HIGH_VOL_HORIZON
    return _DEFAULT_HORIZON


def horizon_log(symbol: str, regime: str, horizon: int, ts: datetime) -> None:
    """Emit a structured log entry capturing a horizon transition."""

    logger.info(
        "adaptive_horizon_change",
        extra={
            "event": "adaptive_horizon",
            "symbol": symbol,
            "regime": regime,
            "horizon_seconds": int(horizon),
            "timestamp": ts.astimezone(timezone.utc).isoformat(timespec="microseconds"),
        },
    )


def get_horizon(features: Mapping[str, Any] | None) -> int:
    """Return the model prediction horizon given the latest features."""

    regime = _extract_regime(features)
    horizon = _resolve_horizon(regime)
    symbol = _extract_symbol(features)

    previous = _LAST_HORIZONS.get(symbol)
    if previous != horizon:
        horizon_log(symbol, regime, horizon, datetime.now(timezone.utc))
        _LAST_HORIZONS[symbol] = horizon

    return horizon


def _reset_cache() -> None:
    """Clear cached horizon state. Intended for use in tests."""

    _LAST_HORIZONS.clear()


__all__ = ["get_horizon", "horizon_log"]
