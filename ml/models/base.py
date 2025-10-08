"""Base utilities for model trainers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

try:  # pragma: no cover - import exercised indirectly in tests.
    import numpy as _np
except ModuleNotFoundError:  # pragma: no cover - exercised in optional-deps tests.
    _np = None


class MissingDependencyError(RuntimeError):
    """Error raised when optional scientific dependencies are unavailable."""


def _require_numpy() -> Any:
    """Return the imported numpy module or raise a descriptive error."""

    if _np is None:
        raise MissingDependencyError("numpy is required for reinforcement learning utilities")
    return _np


def require_numpy() -> Any:
    """Public wrapper around :func:`_require_numpy` for dependent modules."""

    return _require_numpy()


@dataclass
class TrainingResult:
    """Structured result for training runs."""

    metrics: Dict[str, float]
    model: Any
    extra: Optional[Dict[str, Any]] = None


class UncertaintyGate:
    """Utility to gate predictions based on uncertainty estimates."""

    def __init__(self, threshold: float) -> None:
        self.threshold = threshold

    def apply(self, predictions: Any, uncertainties: Any) -> Any:
        np = _require_numpy()
        predictions_arr = np.asarray(predictions)
        uncertainties_arr = np.asarray(uncertainties)
        mask = uncertainties_arr <= self.threshold
        gated = np.where(mask, predictions_arr, 0.0)
        return gated


def fee_aware_reward(returns: Any, turnovers: Any, fee_bps: float) -> Any:
    """Calculate fee-aware rewards for reinforcement learning agents."""

    np = _require_numpy()
    returns_arr = np.asarray(returns)
    turnovers_arr = np.asarray(turnovers)
    fees = turnovers_arr * fee_bps / 10_000.0
    return returns_arr - fees


__all__ = [
    "TrainingResult",
    "UncertaintyGate",
    "fee_aware_reward",
    "MissingDependencyError",
    "require_numpy",
]
