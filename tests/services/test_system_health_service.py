from types import SimpleNamespace

from services.system import health_service


def test_build_health_snapshot(monkeypatch):
    class _Totals:
        instrument_exposure = {"BTC-USD": 1500.0, "ETH-USD": 500.0}
        max_correlation = 0.9

    class _Response:
        totals = _Totals()
        breaches = [
            SimpleNamespace(
                constraint="max_cluster_exposure",
                value=1_200.0,
                limit=1_000.0,
                detail={"cluster": "BTC"},
            )
        ]

    class _Aggregator:
        correlation_limit = 0.8

        def portfolio_status(self):  # pragma: no cover - simple passthrough
            return _Response()

    monkeypatch.setattr(health_service, "portfolio_aggregator", _Aggregator())
    monkeypatch.setattr(health_service, "compute_daily_return_pct", lambda account_id=None: 1.25)
    monkeypatch.setattr(
        health_service,
        "_simulation_status",
        lambda: {"active": True, "reason": "training"},
    )

    snapshot = health_service.build_health_snapshot(account_id="alpha")

    assert snapshot["daily_return_pct"] == 1.25
    diversification = snapshot["diversification"]
    assert diversification["top_assets"][0] == {"instrument": "BTC-USD", "exposure": 1500.0}
    assert diversification["flags"][0]["constraint"] == "max_cluster_exposure"
    assert diversification["correlation_note"] == "elevated"
    assert snapshot["simulation"] == {"active": True, "reason": "training"}
