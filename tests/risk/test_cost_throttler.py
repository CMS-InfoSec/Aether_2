import os
from datetime import datetime, timedelta, timezone

from cost_efficiency import (
    CostEfficiencyMetrics,
    clear_cost_metrics,
    set_cost_metrics,
)
from cost_throttler import (
    CostThrottler,
    clear_throttle_log,
    get_throttle_log,
)


def test_cost_throttler_transitions_between_states() -> None:
    throttler = CostThrottler(ratio_threshold=0.5)
    clear_cost_metrics()
    clear_throttle_log()

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
    assert status.action == "throttle_retraining" or status.action == "reduce_inference_refresh"
    assert throttler.get_status("acct-1").active is True

    logs = get_throttle_log()
    assert any(entry["account_id"] == "acct-1" and entry["reason"] != "throttle_cleared" for entry in logs)

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
    assert throttler.get_status("acct-1").active is False

    logs = get_throttle_log()
    assert any(entry["account_id"] == "acct-1" and entry["reason"] == "throttle_cleared" for entry in logs)


def test_cost_throttler_ignores_stale_metrics() -> None:
    throttler = CostThrottler(ratio_threshold=0.5)
    clear_cost_metrics()
    clear_throttle_log()

    original_threshold = os.environ.get("COST_METRICS_MAX_AGE_SECONDS")
    os.environ["COST_METRICS_MAX_AGE_SECONDS"] = "1"
    try:
        set_cost_metrics(
            "acct-2",
            CostEfficiencyMetrics(
                infra_cost=700.0,
                recent_pnl=1000.0,
                observed_at=datetime.now(timezone.utc),
            ),
        )
        status = throttler.evaluate("acct-2")
        assert status.active is True

        set_cost_metrics(
            "acct-2",
            CostEfficiencyMetrics(
                infra_cost=100.0,
                recent_pnl=1000.0,
                observed_at=datetime.now(timezone.utc) - timedelta(seconds=10),
            ),
        )

        status = throttler.evaluate("acct-2")
        assert status.active is True
    finally:
        if original_threshold is None:
            os.environ.pop("COST_METRICS_MAX_AGE_SECONDS", None)
        else:
            os.environ["COST_METRICS_MAX_AGE_SECONDS"] = original_threshold


def test_cost_throttler_survives_restart() -> None:
    throttler = CostThrottler(ratio_threshold=0.5)
    clear_cost_metrics()
    clear_throttle_log()

    set_cost_metrics(
        "acct-3",
        CostEfficiencyMetrics(
            infra_cost=800.0,
            recent_pnl=1000.0,
            observed_at=datetime.now(timezone.utc),
        ),
    )
    status = throttler.evaluate("acct-3")
    assert status.active is True

    # Simulate a process restart by constructing a new throttler without touching the store.
    restarted = CostThrottler(ratio_threshold=0.5)
    status_after_restart = restarted.evaluate("acct-3")
    assert status_after_restart.active is True

