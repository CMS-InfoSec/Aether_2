from __future__ import annotations

import pytest

prometheus_client = pytest.importorskip("prometheus_client")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.alert_manager import (
    AlertManager,
    DriftSignal,
    OMSError,
    RiskEvent,
    alert_fee_spike,
    alert_latency,
    alert_trade_rejections,
    configure_metrics,
    get_alert_metrics,
    setup_alerting,
)


CollectorRegistry = prometheus_client.CollectorRegistry


class RecordingAlertManager(AlertManager):
    """Test helper that records alerts pushed by the manager."""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.pushed_alerts: list[dict[str, object]] = []

    def _push_alert(  # type: ignore[override]
        self,
        alert_name: str,
        severity: str,
        description: str,
        labels: dict[str, str] | None = None,
    ) -> None:
        self.pushed_alerts.append(
            {
                "alert_name": alert_name,
                "severity": severity,
                "description": description,
                "labels": labels or {},
            }
        )


def test_handle_risk_event_increments_metric() -> None:
    registry = CollectorRegistry()
    metrics = configure_metrics(registry)
    manager = AlertManager(metrics=metrics, alertmanager_url=None)

    manager.handle_risk_event(
        RiskEvent(event_type="limit_breach", severity="warning", description="")
    )

    value = registry.get_sample_value(
        "aether_risk_events_total",
        {"event_type": "limit_breach", "severity": "warning"},
    )
    assert value == 1.0


def test_handle_oms_error_increments_metric() -> None:
    registry = CollectorRegistry()
    metrics = configure_metrics(registry)
    manager = AlertManager(metrics=metrics, alertmanager_url=None)

    manager.handle_oms_error(
        OMSError(error_code="REJECT", description="Order rejected", severity="critical")
    )

    value = registry.get_sample_value(
        "aether_oms_errors_total",
        {"error_code": "REJECT", "severity": "critical"},
    )
    assert value == 1.0


def test_handle_drift_signal_sets_gauge() -> None:
    registry = CollectorRegistry()
    metrics = configure_metrics(registry)
    manager = AlertManager(metrics=metrics, alertmanager_url=None)

    manager.handle_drift_signal(
        DriftSignal(detector="ks", model="alpha", score=0.7, threshold=0.6)
    )

    value = registry.get_sample_value(
        "aether_drift_detector_score",
        {"detector": "ks", "model": "alpha"},
    )
    assert value == 0.7


def test_handle_drift_signal_pushes_alert_above_threshold() -> None:
    registry = CollectorRegistry()
    metrics = configure_metrics(registry)
    manager = RecordingAlertManager(metrics=metrics, alertmanager_url=None)

    manager.handle_drift_signal(
        DriftSignal(detector="ks", model="alpha", score=0.9, threshold=0.6)
    )

    assert len(manager.pushed_alerts) == 1
    pushed = manager.pushed_alerts[0]
    assert pushed["alert_name"] == "ModelDriftDetected"
    assert pushed["severity"] == "warning"


def test_helper_functions_record_metrics() -> None:
    registry = CollectorRegistry()
    configure_metrics(registry)

    alert_latency("risk_engine", 125)
    alert_fee_spike("acct-1", 3.5)
    alert_trade_rejections("acct-1")

    metrics = get_alert_metrics()
    # Histogram helper functions expose _count/_sum samples for verification.
    latency_count = registry.get_sample_value(
        "aether_alert_latency_ms_count", {"name": "risk_engine"}
    )
    fee_gauge = registry.get_sample_value(
        "aether_fee_spike_pct", {"account_id": "acct-1"}
    )
    trade_count = registry.get_sample_value(
        "aether_trade_rejections_total", {"account_id": "acct-1"}
    )

    assert metrics is not None
    assert latency_count == 1.0
    assert fee_gauge == 3.5
    assert trade_count == 1.0


def test_setup_alerting_binds_manager_to_app_state() -> None:
    registry = CollectorRegistry()
    configure_metrics(registry)

    app = FastAPI()
    setup_alerting(app)

    with TestClient(app):
        assert hasattr(app.state, "alert_manager")
        assert isinstance(app.state.alert_manager, AlertManager)
