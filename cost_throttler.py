"""Infrastructure cost aware throttling utilities.

The module keeps a lightweight in-memory registry of throttle state per
account.  Decisions are made by comparing infrastructure spend against recent
profit and loss obtained from :mod:`cost_efficiency`.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from cost_efficiency import CostEfficiencyMetrics, get_cost_metrics

logger = logging.getLogger(__name__)

__all__ = [
    "CostThrottler",
    "ThrottleStatus",
    "throttle_log",
    "get_throttle_log",
    "clear_throttle_log",
]


def _env_ratio_threshold() -> float:
    value = os.getenv("COST_THROTTLE_RATIO", "0.6")
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.6


DEFAULT_RATIO_THRESHOLD = _env_ratio_threshold()
_THROTTLE_LOG: list[dict[str, object]] = []
_LOG_LOCK = threading.Lock()


@dataclass(frozen=True)
class ThrottleStatus:
    """Current throttling state for an account."""

    active: bool
    reason: Optional[str] = None
    action: Optional[str] = None
    cost_ratio: Optional[float] = None
    infra_cost: Optional[float] = None
    recent_pnl: Optional[float] = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


def throttle_log(account_id: str, reason: str, ts: datetime) -> None:
    """Persist throttle actions for auditability."""

    entry = {"account_id": account_id, "reason": reason, "ts": ts}
    with _LOG_LOCK:
        _THROTTLE_LOG.append(entry)
    logger.info("Throttle action", extra={"event": "cost_throttle", **entry})


def get_throttle_log() -> list[dict[str, object]]:
    with _LOG_LOCK:
        return list(_THROTTLE_LOG)


def clear_throttle_log() -> None:
    with _LOG_LOCK:
        _THROTTLE_LOG.clear()


class CostThrottler:
    """Simple controller that toggles throttled mode based on cost ratios."""

    def __init__(self, ratio_threshold: Optional[float] = None) -> None:
        if ratio_threshold is None:
            ratio_threshold = DEFAULT_RATIO_THRESHOLD
        self._ratio_threshold = max(float(ratio_threshold), 0.0)
        self._state: Dict[str, ThrottleStatus] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _extract_metrics(metrics: Optional[CostEfficiencyMetrics]) -> Optional[Tuple[float, float]]:
        if metrics is None:
            return None

        cost = getattr(metrics, "infra_cost", None)
        pnl = getattr(metrics, "recent_pnl", None)
        if cost is None and isinstance(metrics, dict):
            cost = metrics.get("infra_cost")
        if pnl is None and isinstance(metrics, dict):
            pnl = metrics.get("recent_pnl")

        if cost is None or pnl is None:
            return None
        try:
            cost_val = float(cost)
        except (TypeError, ValueError):
            return None
        try:
            pnl_val = float(pnl)
        except (TypeError, ValueError):
            return None
        return cost_val, pnl_val

    def _determine_action(self, ratio: float) -> str:
        if ratio >= self._ratio_threshold * 1.5:
            return "reduce_inference_refresh"
        return "throttle_retraining"

    def _update_state(self, account_id: str, status: ThrottleStatus) -> ThrottleStatus:
        with self._lock:
            previous = self._state.get(account_id)
            self._state[account_id] = status

        if previous is None or previous.active != status.active or previous.reason != status.reason:
            reason = status.reason if status.active else "throttle_cleared"
            throttle_log(account_id, reason or "throttle_cleared", status.updated_at)
        return status

    def evaluate(self, account_id: str) -> ThrottleStatus:
        snapshot = get_cost_metrics(account_id)
        if snapshot.metrics is None or snapshot.is_stale():
            return self.get_status(account_id)

        metrics = self._extract_metrics(snapshot.metrics)
        if metrics is None:
            return self.get_status(account_id)

        infra_cost, recent_pnl = metrics
        pnl_reference = abs(recent_pnl)
        if pnl_reference <= 0:
            cost_ratio = float("inf") if infra_cost > 0 else 0.0
        else:
            cost_ratio = infra_cost / pnl_reference

        now = snapshot.retrieved_at
        if cost_ratio >= self._ratio_threshold:
            action = self._determine_action(cost_ratio)
            reason = (
                "Infrastructure cost %.2f exceeds %.0f%% of recent PnL"
                % (infra_cost, self._ratio_threshold * 100)
            )
            status = ThrottleStatus(
                active=True,
                reason=reason,
                action=action,
                cost_ratio=cost_ratio,
                infra_cost=infra_cost,
                recent_pnl=recent_pnl,
                updated_at=now,
            )
        else:
            status = ThrottleStatus(
                active=False,
                reason=None,
                action=None,
                cost_ratio=cost_ratio,
                infra_cost=infra_cost,
                recent_pnl=recent_pnl,
                updated_at=now,
            )

        return self._update_state(account_id, status)

    def get_status(self, account_id: str) -> ThrottleStatus:
        with self._lock:
            status = self._state.get(account_id)
        if status is None:
            return ThrottleStatus(active=False)
        return replace(status)

    def clear(self, account_id: Optional[str] = None) -> None:
        with self._lock:
            if account_id is None:
                self._state.clear()
            else:
                self._state.pop(account_id, None)

