"""Tests for the alert deduplication service."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from prometheus_client import CollectorRegistry

from services.alerts.alert_dedupe import (
    AlertDedupeMetrics,
    AlertDedupeService,
    AlertPolicy,
    HTTPException,
)


def _metric_value(metric: object) -> float:
    value = getattr(metric, "_value", 0.0)
    return value.get() if hasattr(value, "get") else float(value)


def _build_alert(
    *,
    fingerprint: str,
    service: str,
    alert_type: str,
    symbol: str,
    severity: str,
    timestamp: datetime,
) -> dict[str, object]:
    return {
        "fingerprint": fingerprint,
        "labels": {
            "service": service,
            "alert_type": alert_type,
            "symbol": symbol,
            "severity": severity,
        },
        "startsAt": timestamp.isoformat(),
    }


def test_alerts_grouped_and_suppressed_within_window() -> None:
    registry = CollectorRegistry()
    metrics = AlertDedupeMetrics(registry=registry)
    policy = AlertPolicy(suppression_window=timedelta(minutes=10), escalation_threshold=5)
    service = AlertDedupeService(policy=policy, metrics=metrics)

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    alert_one = _build_alert(
        fingerprint="a1",
        service="pricing",
        alert_type="LatencyBreach",
        symbol="BTC-USD",
        severity="warning",
        timestamp=base,
    )
    alert_two = _build_alert(
        fingerprint="a2",
        service="pricing",
        alert_type="LatencyBreach",
        symbol="BTC-USD",
        severity="warning",
        timestamp=base + timedelta(minutes=5),
    )
    alert_other_symbol = _build_alert(
        fingerprint="b1",
        service="pricing",
        alert_type="LatencyBreach",
        symbol="ETH-USD",
        severity="warning",
        timestamp=base + timedelta(minutes=6),
    )

    service.ingest_alerts([alert_one])
    active = service.active_alerts()
    assert len(active) == 1
    assert active[0]["count"] == 1

    service.ingest_alerts([alert_one, alert_two, alert_other_symbol])
    active = sorted(service.active_alerts(), key=lambda item: item["symbol"])
    assert len(active) == 2
    btc_entry = next(item for item in active if item["symbol"] == "BTC-USD")
    eth_entry = next(item for item in active if item["symbol"] == "ETH-USD")

    assert btc_entry["count"] == 2
    assert btc_entry["suppressed"] == 1
    assert btc_entry["severity"] == "warning"
    assert eth_entry["count"] == 1

    suppressed_metric = _metric_value(
        metrics.alerts_suppressed_total.labels(
            service="pricing", alert_type="LatencyBreach", symbol="BTC-USD"
        )
    )
    assert suppressed_metric == 1.0


def test_alerts_escalate_after_threshold() -> None:
    registry = CollectorRegistry()
    metrics = AlertDedupeMetrics(registry=registry)
    policy = AlertPolicy(suppression_window=timedelta(minutes=10), escalation_threshold=3)
    service = AlertDedupeService(policy=policy, metrics=metrics)

    base = datetime(2024, 6, 1, tzinfo=timezone.utc)
    alerts = [
        _build_alert(
            fingerprint=f"f{i}",
            service="risk",
            alert_type="FeeSpike",
            symbol="UNI-USD",
            severity="warning",
            timestamp=base + timedelta(minutes=i * 2),
        )
        for i in range(3)
    ]

    service.ingest_alerts([alerts[0]])
    service.ingest_alerts(alerts[:2])
    service.ingest_alerts(alerts[:3])

    active = service.active_alerts()
    assert len(active) == 1
    entry = active[0]
    assert entry["count"] == 3
    assert entry["severity"] == "high"
    assert entry["escalated"] is True

    suppressed_metric = _metric_value(
        metrics.alerts_suppressed_total.labels(
            service="risk", alert_type="FeeSpike", symbol="UNI-USD"
        )
    )
    assert suppressed_metric == 2.0
    escalated_metric = _metric_value(
        metrics.alerts_escalated_total.labels(
            service="risk", alert_type="FeeSpike", symbol="UNI-USD"
        )
    )
    assert escalated_metric == 1.0

    policies = service.policies()
    assert policies["suppression_window_seconds"] == int(timedelta(minutes=10).total_seconds())
    assert policies["escalation_threshold"] == 3


@pytest.mark.asyncio
async def test_alert_dedupe_uses_configured_http_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    created: dict[str, object] = {}

    class DummyAsyncClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            created["timeout"] = kwargs.get("timeout")

    fake_httpx = SimpleNamespace(AsyncClient=DummyAsyncClient)

    original = sys.modules.get("httpx")
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)

    try:
        service = AlertDedupeService(http_timeout=7.5)
        client = await service._get_client()
    finally:
        if original is None:
            monkeypatch.delitem(sys.modules, "httpx", raising=False)
        else:
            monkeypatch.setitem(sys.modules, "httpx", original)

    assert isinstance(client, DummyAsyncClient)
    assert created["timeout"] == 7.5


@pytest.mark.asyncio
async def test_default_fetch_converts_request_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyRequestError(Exception):
        pass

    class DummyTimeout(DummyRequestError):
        pass

    class DummyHTTPStatusError(Exception):
        pass

    class DummyClient:
        async def get(self, url: str) -> None:  # pragma: no cover - runtime path
            raise DummyRequestError("boom")

    fake_httpx = SimpleNamespace(
        AsyncClient=lambda *args, **kwargs: DummyClient(),
        RequestError=DummyRequestError,
        TimeoutException=DummyTimeout,
        HTTPStatusError=DummyHTTPStatusError,
    )

    original = sys.modules.get("httpx")
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)

    try:
        metrics = AlertDedupeMetrics(registry=CollectorRegistry())
        service = AlertDedupeService(metrics=metrics)
        service._client = DummyClient()

        with pytest.raises(HTTPException) as excinfo:
            await service._default_fetch()
    finally:
        if original is None:
            monkeypatch.delitem(sys.modules, "httpx", raising=False)
        else:
            monkeypatch.setitem(sys.modules, "httpx", original)

    assert excinfo.value.status_code == 502
    assert "Alertmanager request failed" in excinfo.value.detail


@pytest.mark.asyncio
async def test_default_fetch_converts_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyRequestError(Exception):
        pass

    class DummyTimeout(DummyRequestError):
        pass

    class DummyHTTPStatusError(Exception):
        pass

    class DummyClient:
        async def get(self, url: str) -> None:  # pragma: no cover - runtime path
            raise DummyTimeout("boom")

    fake_httpx = SimpleNamespace(
        AsyncClient=lambda *args, **kwargs: DummyClient(),
        RequestError=DummyRequestError,
        TimeoutException=DummyTimeout,
        HTTPStatusError=DummyHTTPStatusError,
    )

    original = sys.modules.get("httpx")
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)

    try:
        metrics = AlertDedupeMetrics(registry=CollectorRegistry())
        service = AlertDedupeService(metrics=metrics)
        service._client = DummyClient()

        with pytest.raises(HTTPException) as excinfo:
            await service._default_fetch()
    finally:
        if original is None:
            monkeypatch.delitem(sys.modules, "httpx", raising=False)
        else:
            monkeypatch.setitem(sys.modules, "httpx", original)

    assert excinfo.value.status_code == 504
    assert "timed out" in excinfo.value.detail
