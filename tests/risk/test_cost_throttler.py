from datetime import datetime, timezone

import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy import create_engine  # type: ignore[import-untyped]
from sqlalchemy.orm import sessionmaker  # type: ignore[import-untyped]

from cost_efficiency import (
    CostEfficiencyMetrics,
    clear_cost_metrics,
    set_cost_metrics,
)
from cost_throttler import CostThrottler, ThrottleRepository, get_throttle_log


@pytest.fixture()
def throttle_repository(tmp_path):
    database_path = tmp_path / "cost-throttle.db"
    engine = create_engine(f"sqlite:///{database_path}", future=True)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    repository = ThrottleRepository(session_factory)
    repository.clear()
    yield repository
    repository.clear()
    engine.dispose()


def test_cost_throttler_transitions_between_states(throttle_repository: ThrottleRepository) -> None:
    throttler = CostThrottler(ratio_threshold=0.5, repository=throttle_repository)
    clear_cost_metrics()

    set_cost_metrics(
        "acct-1",
        CostEfficiencyMetrics(
            infra_cost=700.0,
            recent_pnl=1000.0,
            observed_at=datetime.now(timezone.utc),
        ),
    )
    status = throttler.evaluate("acct-1")

    assert status.active is True
    assert status.action in {"throttle_retraining", "reduce_inference_refresh"}
    persisted = throttler.get_status("acct-1")
    assert persisted.active is True

    history = throttle_repository.history("acct-1")
    assert len(history) == 1
    assert history[0].active is True
    assert history[0].reason and history[0].reason != "throttle_cleared"

    set_cost_metrics(
        "acct-1",
        CostEfficiencyMetrics(
            infra_cost=100.0,
            recent_pnl=1000.0,
            observed_at=datetime.now(timezone.utc),
        ),
    )
    status = throttler.evaluate("acct-1")
    assert status.active is False
    persisted = throttler.get_status("acct-1")
    assert persisted.active is False

    history = throttle_repository.history("acct-1")
    assert len(history) == 2
    assert history[-1].active is False
    assert history[-1].reason == "throttle_cleared"

    logs = [entry for entry in get_throttle_log() if entry["account_id"] == "acct-1"]
    assert [entry["active"] for entry in logs] == [True, False]


def test_cost_throttler_persists_state_across_restart(throttle_repository: ThrottleRepository) -> None:
    throttler = CostThrottler(ratio_threshold=0.5, repository=throttle_repository)
    clear_cost_metrics()

    set_cost_metrics(
        "acct-1",
        CostEfficiencyMetrics(
            infra_cost=800.0,
            recent_pnl=1000.0,
            observed_at=datetime.now(timezone.utc),
        ),
    )
    throttler.evaluate("acct-1")

    set_cost_metrics(
        "acct-1",
        CostEfficiencyMetrics(
            infra_cost=100.0,
            recent_pnl=1000.0,
            observed_at=datetime.now(timezone.utc),
        ),
    )
    throttler.evaluate("acct-1")

    history_before_restart = throttle_repository.history("acct-1")
    assert [entry.active for entry in history_before_restart] == [True, False]

    restarted_repo = ThrottleRepository(throttle_repository.session_factory)
    restarted_throttler = CostThrottler(ratio_threshold=0.5, repository=restarted_repo)

    persisted_status = restarted_throttler.get_status("acct-1")
    assert persisted_status.active is False
    assert persisted_status.cost_ratio is not None

    # evaluate without metrics should fall back to stored status
    clear_cost_metrics()
    resumed_status = restarted_throttler.evaluate("acct-1")
    assert resumed_status.active is False

    history_after_restart = restarted_repo.history("acct-1")
    assert [entry.active for entry in history_after_restart] == [True, False]
    assert history_after_restart[-1].reason == "throttle_cleared"

