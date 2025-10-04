"""Lightweight helpers exposing infrastructure cost efficiency telemetry.

This module intentionally keeps the interface extremely small so that other
services can stub the data source during tests.  The :mod:`cost_throttler`
module depends on :func:`get_cost_metrics` to decide whether operational costs
are out of line with the recent profitability for an account.

The helpers now persist to the shared Timescale/Postgres repository via
``services.common.cost_efficiency_store`` while still providing an in-memory
fallback for unit tests.
"""

from __future__ import annotations


import os
import importlib.machinery
import importlib.util
import sys
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

_REPO_ROOT = str(Path(__file__).resolve().parent)
if _REPO_ROOT in sys.path:
    sys.path.remove(_REPO_ROOT)
sys.path.insert(0, _REPO_ROOT)

_loaded_services = sys.modules.get("services")
if _loaded_services is not None:
    module_path = getattr(_loaded_services, "__file__", "")
    if module_path and not module_path.startswith(_REPO_ROOT):
        sys.modules.pop("services", None)

try:
    from services.common.cost_efficiency_store import (
        CostEfficiencyRecord,
        InMemoryCostEfficiencyStore,
        configure_cost_efficiency_store,
        get_cost_efficiency_store,
    )
except ModuleNotFoundError:
    base_path = Path(__file__).resolve().parent
    services_path = base_path / "services"
    services_module = sys.modules.get("services")
    module_file = getattr(services_module, "__file__", None)
    if services_module is None or module_file is None or not module_file.startswith(str(services_path)):
        services_spec = importlib.util.spec_from_file_location("services", services_path / "__init__.py")
        if services_spec is None or services_spec.loader is None:  # pragma: no cover - defensive guard
            raise
        services_module = importlib.util.module_from_spec(services_spec)
        services_module.__path__ = [str(services_path)]
        sys.modules["services"] = services_module
        services_spec.loader.exec_module(services_module)

    common_module = sys.modules.get("services.common")
    if common_module is None:
        common_spec = importlib.machinery.ModuleSpec(
            "services.common", loader=None, is_package=True
        )
        common_module = importlib.util.module_from_spec(common_spec)
        common_module.__path__ = [str(services_path / "common")]
        sys.modules["services.common"] = common_module

    module_path = services_path / "common" / "cost_efficiency_store.py"
    spec = importlib.util.spec_from_file_location(
        "services.common.cost_efficiency_store",
        module_path,
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("services.common.cost_efficiency_store", module)
    spec.loader.exec_module(module)
    CostEfficiencyRecord = module.CostEfficiencyRecord  # type: ignore[attr-defined]
    InMemoryCostEfficiencyStore = module.InMemoryCostEfficiencyStore  # type: ignore[attr-defined]
    configure_cost_efficiency_store = module.configure_cost_efficiency_store  # type: ignore[attr-defined]
    get_cost_efficiency_store = module.get_cost_efficiency_store  # type: ignore[attr-defined]

__all__ = [
    "CostEfficiencyMetrics",
    "CostMetricsSnapshot",
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


@dataclass(frozen=True)
class CostMetricsSnapshot:
    """Envelope returned by :func:`get_cost_metrics` providing metadata."""

    account_id: str
    metrics: Optional[CostEfficiencyMetrics]
    retrieved_at: datetime
    stored_at: Optional[datetime]

    @property
    def observed_at(self) -> Optional[datetime]:
        if self.metrics is None:
            return None
        return self.metrics.observed_at

    @property
    def age(self) -> Optional[timedelta]:
        observed = self.observed_at
        if observed is None:
            return None
        return self.retrieved_at - observed

    def is_stale(self, max_age_seconds: Optional[float] = None) -> bool:
        metrics = self.metrics
        if metrics is None:
            return True
        age = self.age
        if age is None:
            return False
        if max_age_seconds is None:
            max_age_seconds = _stale_threshold_seconds()
        try:
            max_age = float(max_age_seconds)
        except (TypeError, ValueError):
            max_age = 0.0
        if max_age <= 0:
            return False
        return age.total_seconds() > max_age


def _stale_threshold_seconds() -> float:
    value = os.getenv("COST_METRICS_MAX_AGE_SECONDS", "900")
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return 900.0


def _ensure_store_initialised() -> None:
    # When running in isolated unit tests ``configure_cost_efficiency_store``
    # may not have been invoked.  Ensure a fallback store exists.
    store = get_cost_efficiency_store()
    if isinstance(store, InMemoryCostEfficiencyStore):
        return
    # The default store is already configured during module import, but a
    # misbehaving test could have replaced it with ``None``. Guard against it.
    if store is None:  # pragma: no cover - defensive guard
        configure_cost_efficiency_store(InMemoryCostEfficiencyStore())


def _record_to_metrics(account_id: str, record: Optional[CostEfficiencyRecord]) -> CostMetricsSnapshot:
    retrieved_at = datetime.now(timezone.utc)
    if record is None:
        return CostMetricsSnapshot(
            account_id=account_id,
            metrics=None,
            retrieved_at=retrieved_at,
            stored_at=None,
        )
    metrics = CostEfficiencyMetrics(
        infra_cost=record.infra_cost,
        recent_pnl=record.recent_pnl,
        observed_at=record.observed_at,
    )
    return CostMetricsSnapshot(
        account_id=account_id,
        metrics=metrics,
        retrieved_at=retrieved_at,
        stored_at=record.stored_at,
    )


def set_cost_metrics(account_id: str, metrics: CostEfficiencyMetrics) -> None:
    """Persist ``metrics`` for ``account_id`` via the configured repository."""

    _ensure_store_initialised()
    store = get_cost_efficiency_store()
    observed_at = metrics.observed_at or datetime.now(timezone.utc)
    store.upsert(
        account_id,
        infra_cost=float(metrics.infra_cost),
        recent_pnl=float(metrics.recent_pnl),
        observed_at=observed_at,
    )


def get_cost_metrics(account_id: str) -> CostMetricsSnapshot:
    """Return the most recent cost efficiency metrics for ``account_id``."""

    _ensure_store_initialised()
    store = get_cost_efficiency_store()
    record = store.fetch(account_id)
    return _record_to_metrics(account_id, record)


def clear_cost_metrics() -> None:
    """Remove all stored cost metrics.

    This is primarily useful for tests where isolated state is expected.
    """

    _ensure_store_initialised()
    get_cost_efficiency_store().clear()


