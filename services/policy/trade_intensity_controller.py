"""Trade intensity controller applying adaptive multiplier based on market context."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Mapping, MutableMapping, Optional, SupportsFloat, Tuple


@dataclass
class IntensityState:
    """Holds the smoothed multiplier and the latest diagnostics."""

    ema_multiplier: float
    last_updated: float = field(default_factory=time.time)
    diagnostics: Mapping[str, float] = field(default_factory=dict)
    raw_multiplier: float = 1.0


class TradeIntensityController:
    """Computes trade intensity multipliers using adaptive heuristics.

    The controller takes the opportunity and risk inputs listed below and maps
    them to a multiplier that scales the downstream position sizing result.

    Parameters
    ----------
    alpha:
        Smoothing factor for the exponential moving average applied to the raw
        multiplier. Higher values react faster to changes.
    floor:
        Minimum multiplier regardless of inputs.
    ceiling:
        Maximum multiplier regardless of inputs.
    """

    _REGIME_BIASES: Mapping[str, float] = {
        "bull": 0.12,
        "bullish": 0.12,
        "bear": -0.15,
        "bearish": -0.15,
        "volatile": -0.1,
        "trend": 0.06,
        "trending": 0.06,
        "sideways": -0.05,
        "range": -0.05,
    }

    def __init__(self, *, alpha: float = 0.25, floor: float = 0.15, ceiling: float = 1.5) -> None:
        if not 0.0 < alpha <= 1.0:
            raise ValueError("alpha must be between 0 and 1")
        if floor <= 0:
            raise ValueError("floor must be positive")
        if ceiling <= floor:
            raise ValueError("ceiling must be greater than floor")
        self._alpha = float(alpha)
        self._floor = float(floor)
        self._ceiling = float(ceiling)
        self._state: MutableMapping[Tuple[str, str], IntensityState] = {}

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))

    @staticmethod
    def _normalize_score(
        raw: SupportsFloat | str | None, *, neutral: float = 0.5
    ) -> float:
        if raw is None:
            return neutral
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return neutral
        if value != value:  # NaN check
            return neutral
        return value

    def _compute_raw_multiplier(
        self,
        *,
        signal_confidence: float,
        regime: str,
        queue_depth: float,
        win_rate: float,
        drawdown: float,
        fee_pressure: float,
    ) -> Tuple[float, Dict[str, float]]:
        """Return the unsmoothed multiplier and component diagnostics."""

        signal_adj = 0.6 * (signal_confidence - 0.5)
        win_adj = 0.4 * (win_rate - 0.5)
        regime_adj = self._REGIME_BIASES.get(regime.lower(), 0.0) if regime else 0.0

        # Negative adjustments penalize aggressive sizing when constraints tighten.
        backpressure_penalty = 0.6 * queue_depth
        drawdown_penalty = 0.5 * drawdown
        fee_penalty = 0.35 * fee_pressure

        opportunity = signal_adj + win_adj + regime_adj
        suppression = backpressure_penalty + drawdown_penalty + fee_penalty

        raw_multiplier = 1.0 + opportunity - suppression
        raw_clamped = self._clamp(raw_multiplier, self._floor, self._ceiling)

        diagnostics = {
            "signal_adjustment": round(signal_adj, 4),
            "win_rate_adjustment": round(win_adj, 4),
            "regime_adjustment": round(regime_adj, 4),
            "backpressure_penalty": round(-backpressure_penalty, 4),
            "drawdown_penalty": round(-drawdown_penalty, 4),
            "fee_penalty": round(-fee_penalty, 4),
            "opportunity_score": round(opportunity, 4),
            "suppression_score": round(-suppression, 4),
            "raw_multiplier": round(raw_multiplier, 4),
        }
        return raw_clamped, diagnostics

    def evaluate(
        self,
        *,
        account_id: str,
        symbol: str,
        signal_confidence: Optional[float] = None,
        regime: str = "unknown",
        queue_depth: Optional[float] = None,
        win_rate: Optional[float] = None,
        drawdown: Optional[float] = None,
        fee_pressure: Optional[float] = None,
    ) -> Dict[str, object]:
        """Evaluate and persist the smoothed trade intensity multiplier.

        The inputs are expected to be normalised scores in the ``[0, 1]`` range,
        where higher values indicate stronger opportunity except for
        ``queue_depth``, ``drawdown`` and ``fee_pressure`` which increase
        pressure to scale down risk.
        """

        signal_confidence = self._clamp(self._normalize_score(signal_confidence), 0.0, 1.0)
        queue_depth = self._clamp(self._normalize_score(queue_depth), 0.0, 1.0)
        win_rate = self._clamp(self._normalize_score(win_rate), 0.0, 1.0)
        drawdown = self._clamp(self._normalize_score(drawdown, neutral=0.0), 0.0, 1.0)
        fee_pressure = self._clamp(self._normalize_score(fee_pressure), 0.0, 1.0)

        raw_multiplier, diagnostics = self._compute_raw_multiplier(
            signal_confidence=signal_confidence,
            regime=regime,
            queue_depth=queue_depth,
            win_rate=win_rate,
            drawdown=drawdown,
            fee_pressure=fee_pressure,
        )

        key = (account_id, symbol)
        state = self._state.get(key)
        if state is None:
            ema_multiplier = raw_multiplier
        else:
            ema_multiplier = state.ema_multiplier + self._alpha * (raw_multiplier - state.ema_multiplier)

        ema_multiplier = self._clamp(ema_multiplier, self._floor, self._ceiling)
        state = IntensityState(
            ema_multiplier=ema_multiplier,
            last_updated=time.time(),
            diagnostics=diagnostics,
            raw_multiplier=raw_multiplier,
        )
        self._state[key] = state

        return {
            "account_id": account_id,
            "symbol": symbol,
            "multiplier": round(ema_multiplier, 4),
            "raw_multiplier": round(raw_multiplier, 4),
            "alpha": self._alpha,
            "floor": self._floor,
            "ceiling": self._ceiling,
            "diagnostics": diagnostics,
            "last_updated": state.last_updated,
        }

    def snapshot(self, account_id: str, symbol: str) -> Optional[Dict[str, object]]:
        """Return the latest multiplier information without updating it."""

        state = self._state.get((account_id, symbol))
        if state is None:
            return None
        return {
            "account_id": account_id,
            "symbol": symbol,
            "multiplier": round(state.ema_multiplier, 4),
            "raw_multiplier": round(state.raw_multiplier, 4),
            "alpha": self._alpha,
            "floor": self._floor,
            "ceiling": self._ceiling,
            "diagnostics": dict(state.diagnostics),
            "last_updated": state.last_updated,
        }


# Default controller instance used by the service layer.
controller = TradeIntensityController()
