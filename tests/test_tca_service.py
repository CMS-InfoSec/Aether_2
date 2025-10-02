from __future__ import annotations

import importlib
import sys
from datetime import UTC, date, datetime

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for TCA service tests")

from fastapi.testclient import TestClient


@pytest.fixture()
def tca_client(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Provide a fresh TCA service app and client bound to an isolated database."""

    monkeypatch.setenv("TCA_DATABASE_URL", f"sqlite:///{tmp_path}/tca.db")
    sys.modules.pop("tca_service", None)
    module = importlib.import_module("tca_service")
    client = TestClient(module.app)

    try:
        yield client, module
    finally:
        client.app.dependency_overrides.clear()
        sys.modules.pop("tca_service", None)


def test_trade_report_requires_admin_account(tca_client):
    client, _ = tca_client

    response = client.get("/tca/trade", params={"trade_id": "trade-123"})

    assert response.status_code in {401, 403}


def test_trade_report_allows_admin_and_audits_access(
    tca_client, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, module = tca_client

    filled_at = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    sample_row = {
        "trade_id": "trade-123",
        "account_id": "OPS-1",
        "market": "BTC-USD",
        "submitted_at": filled_at,
        "order_metadata": {"mid_price_at_submit": 100.0},
        "fill_id": "fill-1",
        "fill_time": filled_at,
        "fill_price": 101.0,
        "size": 1.0,
        "fee": 0.25,
        "fill_metadata": {"liquidity": "maker"},
    }

    monkeypatch.setattr(module, "_fetch_trade_rows", lambda session, trade_id: [sample_row])
    monkeypatch.setattr(module, "_persist_result", lambda *args, **kwargs: None)

    audit_calls: list[dict[str, object]] = []
    monkeypatch.setattr(module, "log_audit", lambda **kwargs: audit_calls.append(kwargs))
    monkeypatch.setattr(module, "hash_ip", lambda value: "hashed-ip")

    client.app.dependency_overrides[module.require_admin_account] = lambda: "ops-admin"
    try:
        response = client.get("/tca/trade", params={"trade_id": "trade-123"})
    finally:
        client.app.dependency_overrides.pop(module.require_admin_account, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["trade_id"] == "trade-123"
    assert payload["account_id"] == "OPS-1"
    assert audit_calls and audit_calls[0]["actor"] == "ops-admin"


def test_daily_summary_requires_admin_account(tca_client) -> None:
    client, _ = tca_client

    response = client.get("/tca/summary", params={"account_id": "OPS-1"})

    assert response.status_code in {401, 403}


def test_daily_summary_returns_payload_for_admin(
    tca_client, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, module = tca_client

    summary = module.DailySummaryModel(
        account_id="OPS-1",
        date=date(2024, 1, 2),
        avg_slippage_bps=1.2,
        total_cost_usd=12.5,
        maker_ratio=0.4,
        taker_ratio=0.6,
        fee_attribution={"exchange": 5.0},
        trade_count=3,
    )

    monkeypatch.setattr(module, "_daily_summary", lambda *_, **__: summary)
    audit_calls: list[dict[str, object]] = []
    monkeypatch.setattr(module, "log_audit", lambda **kwargs: audit_calls.append(kwargs))
    monkeypatch.setattr(module, "hash_ip", lambda value: "hashed-ip")

    client.app.dependency_overrides[module.require_admin_account] = lambda: "ops-admin"
    try:
        response = client.get("/tca/summary", params={"account_id": "OPS-1", "date": "2024-01-02"})
    finally:
        client.app.dependency_overrides.pop(module.require_admin_account, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["account_id"] == "OPS-1"
    assert payload["date"] == "2024-01-02"
    assert audit_calls and audit_calls[0]["actor"] == "ops-admin"


def test_tca_report_requires_admin_account(tca_client) -> None:
    client, _ = tca_client

    response = client.get(
        "/tca/report",
        params={"account_id": "OPS-1", "symbol": "BTC-USD"},
    )

    assert response.status_code in {401, 403}


def test_tca_report_returns_payload_for_admin(
    tca_client, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, module = tca_client

    metrics = module.ExpectedVsRealised(
        expected_cost=1000.0,
        realized_cost=995.0,
        slippage_bps=5.0,
        slippage_cost_usd=5.0,
        fill_quality_bps=1.5,
        fee_impact_usd=0.75,
        trade_count=2,
    )

    monkeypatch.setattr(module, "_fetch_execution_rows", lambda *_, **__: [object()])
    monkeypatch.setattr(module, "_compare_expected_realised", lambda *_: metrics)
    monkeypatch.setattr(module, "_persist_report", lambda *args, **kwargs: None)

    audit_calls: list[dict[str, object]] = []
    monkeypatch.setattr(module, "log_audit", lambda **kwargs: audit_calls.append(kwargs))
    monkeypatch.setattr(module, "hash_ip", lambda value: "hashed-ip")

    client.app.dependency_overrides[module.require_admin_account] = lambda: "ops-admin"
    try:
        response = client.get(
            "/tca/report",
            params={
                "account_id": "OPS-1",
                "symbol": "BTC-USD",
                "date": "2024-01-03",
            },
        )
    finally:
        client.app.dependency_overrides.pop(module.require_admin_account, None)

    assert response.status_code == 200
    payload = response.json()
    assert payload["account_id"] == "OPS-1"
    assert payload["symbol"] == "BTC-USD"
    assert payload["trade_count"] == 2
    assert audit_calls and audit_calls[0]["actor"] == "ops-admin"
