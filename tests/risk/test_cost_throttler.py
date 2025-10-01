from datetime import datetime, timezone

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

