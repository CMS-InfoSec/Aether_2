"""Base utilities for model trainers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np


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

    def apply(self, predictions: np.ndarray, uncertainties: np.ndarray) -> np.ndarray:
        mask = uncertainties <= self.threshold
        gated = np.where(mask, predictions, 0.0)
        return gated


def fee_aware_reward(returns: np.ndarray, turnovers: np.ndarray, fee_bps: float) -> np.ndarray:
    """Calculate fee-aware rewards for reinforcement learning agents."""

    fees = turnovers * fee_bps / 10_000.0
    return returns - fees


__all__ = ["TrainingResult", "UncertaintyGate", "fee_aware_reward"]
