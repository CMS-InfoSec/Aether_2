import logging
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


def test_build_health_snapshot_filters_non_spot(monkeypatch, caplog):
    class _Totals:
        instrument_exposure = {"btc-usd": 1500.0, "ETH-PERP": 500.0, "eth/usd": 250.0}
        max_correlation = 0.4

    class _Response:
        totals = _Totals()
        breaches = []

    class _Aggregator:
        correlation_limit = 0.8

        def portfolio_status(self):  # pragma: no cover - simple passthrough
            return _Response()

    monkeypatch.setattr(health_service, "portfolio_aggregator", _Aggregator())
    monkeypatch.setattr(health_service, "compute_daily_return_pct", lambda account_id=None: None)
    monkeypatch.setattr(health_service, "_simulation_status", lambda: {"active": False, "reason": None})

    caplog.set_level(logging.WARNING)

    snapshot = health_service.build_health_snapshot(account_id=None)

    diversification = snapshot["diversification"]
    assert diversification["top_assets"] == [
        {"instrument": "BTC-USD", "exposure": 1500.0},
        {"instrument": "ETH-USD", "exposure": 250.0},
    ]
    assert "Dropping non-spot instruments" in caplog.text
