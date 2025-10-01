"""Lightweight helpers exposing infrastructure cost efficiency telemetry.

This module intentionally keeps the interface extremely small so that other
services can stub the data source during tests.  The :mod:`cost_throttler`
module depends on :func:`get_cost_metrics` to decide whether operational costs
are out of line with the recent profitability for an account.

The helpers here can easily be swapped out for a real implementation that
queries a data warehouse or metrics backend.  For the test environment we keep
the information entirely in memory and provide convenience setters.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

__all__ = [
    "CostEfficiencyMetrics",
    "get_cost_metrics",
    "set_cost_metrics",
    "clear_cost_metrics",
]


@dataclass
class CostEfficiencyMetrics:
    """Snapshot summarising infra cost against recent profitability."""

    infra_cost: float
    recent_pnl: float
    observed_at: Optional[datetime] = None


_METRICS: Dict[str, CostEfficiencyMetrics] = {}


def set_cost_metrics(account_id: str, metrics: CostEfficiencyMetrics) -> None:
    """Store ``metrics`` for ``account_id`` in the in-memory registry."""

    _METRICS[account_id] = metrics


def get_cost_metrics(account_id: str) -> Optional[CostEfficiencyMetrics]:
    """Return the most recent cost efficiency metrics for ``account_id``."""

    return _METRICS.get(account_id)


def clear_cost_metrics() -> None:
    """Remove all stored cost metrics.

    This is primarily useful for tests where isolated state is expected.
    """

    _METRICS.clear()

