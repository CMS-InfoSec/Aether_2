"""Regression tests for hedge override persistence."""
from __future__ import annotations

from pathlib import Path

import services.hedge.hedge_service as hedge_service


def _state_path(tmp_path: Path) -> Path:
    return tmp_path / "override_state.json"


def _create_service(tmp_path: Path) -> hedge_service.HedgeService:
    store = hedge_service.HedgeOverrideStateStore(history_limit=5, state_path=_state_path(tmp_path))
    return hedge_service.HedgeService(history_limit=5, state_store=store)


def test_override_persists_across_instances(tmp_path: Path) -> None:
    service = _create_service(tmp_path)

    metrics = hedge_service.HedgeMetricsRequest(volatility=0.8, drawdown=0.2, stablecoin_price=1.0)
    service.evaluate(metrics)
    service.set_override(45.0, reason="manual intervention")

    state_file = _state_path(tmp_path)
    assert state_file.exists()

    reloaded_service = _create_service(tmp_path)
    override = reloaded_service.get_override()
    assert override is not None
    assert override.target_pct == 45.0
    assert override.reason == "manual intervention"

    history = list(reloaded_service.get_history())
    assert history[0].mode == "override"
    assert history[0].reason == "manual intervention"
    assert history[-1].mode == "auto"

    reloaded_service.clear_override()

    cleared_service = _create_service(tmp_path)
    assert cleared_service.get_override() is None
    latest_history = list(cleared_service.get_history())
    assert latest_history[0].mode == "override_cleared"
