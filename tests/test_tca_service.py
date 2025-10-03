from __future__ import annotations

import importlib
import sys
import types
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for TCA service tests")

from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import select

BPS_QUANT = Decimal("0.0001")
EIGHT_DP = Decimal("0.00000001")
TWELVE_DP = Decimal("0.000000000001")


@pytest.fixture()
def tca_client(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Provide a fresh TCA service app and client bound to an isolated database."""

    monkeypatch.setenv("TCA_DATABASE_URL", f"sqlite:///{tmp_path}/tca.db")
    security_module = types.ModuleType("services.common.security")

    def _require_admin_stub() -> None:
        raise HTTPException(status_code=401, detail="admin access required")

    security_module.require_admin_account = _require_admin_stub
    sys.modules.setdefault("services", types.ModuleType("services"))
    sys.modules.setdefault("services.common", types.ModuleType("services.common"))
    sys.modules["services.common.security"] = security_module
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
        avg_slippage_bps=Decimal("1.2000"),
        total_cost_usd=Decimal("12.50000000"),
        maker_ratio=Decimal("0.4000"),
        taker_ratio=Decimal("0.6000"),
        fee_attribution={"exchange": Decimal("5.00000000")},
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
        expected_cost=Decimal("1000.00000000"),
        realized_cost=Decimal("995.00000000"),
        slippage_bps=Decimal("5.0000"),
        slippage_cost_usd=Decimal("5.00000000"),
        fill_quality_bps=Decimal("1.5000"),
        fee_impact_usd=Decimal("0.75000000"),
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


def test_trade_report_persists_high_precision_fills(
    tca_client, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, module = tca_client

    filled_at = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    fill_price = Decimal("35000.12345678")
    size = Decimal("0.00012345")
    fee = Decimal("0.00004567")
    mid_price = Decimal("35000.00000000")

    sample_row = {
        "trade_id": "trade-precision",
        "account_id": "OPS-1",
        "market": "BTC-USD",
        "submitted_at": filled_at,
        "order_metadata": {"mid_price_at_submit": mid_price, "side": "buy"},
        "fill_id": "fill-precision",
        "fill_time": filled_at,
        "fill_price": fill_price,
        "size": size,
        "fee": fee,
        "fill_metadata": {"liquidity": "maker"},
    }

    monkeypatch.setattr(module, "_fetch_trade_rows", lambda session, trade_id: [sample_row])
    monkeypatch.setattr(module, "log_audit", lambda **kwargs: None)
    monkeypatch.setattr(module, "hash_ip", lambda value: "hashed-ip")

    client.app.dependency_overrides[module.require_admin_account] = lambda: "ops-admin"
    try:
        response = client.get("/tca/trade", params={"trade_id": "trade-precision"})
    finally:
        client.app.dependency_overrides.pop(module.require_admin_account, None)

    assert response.status_code == 200
    payload = response.json()
    fill_payload = payload["fills"][0]

    expected_slippage = ((fill_price - mid_price) / mid_price) * Decimal("10000")
    expected_cost = (fill_price - mid_price) * size
    expected_notional = fill_price * size

    assert Decimal(fill_payload["fill_price"]) == fill_price.quantize(EIGHT_DP)
    assert Decimal(fill_payload["size"]) == size.quantize(EIGHT_DP)
    assert Decimal(fill_payload["fee"]) == fee.quantize(EIGHT_DP)
    assert Decimal(fill_payload["mid_price_at_submit"]) == mid_price.quantize(EIGHT_DP)
    assert Decimal(fill_payload["slippage_bps"]) == expected_slippage.quantize(BPS_QUANT)
    assert Decimal(fill_payload["notional_usd"]) == expected_notional.quantize(EIGHT_DP)
    assert fill_payload["liquidity"] == "maker"

    assert Decimal(payload["average_slippage_bps"]) == expected_slippage.quantize(BPS_QUANT)
    assert Decimal(payload["total_slippage_cost_usd"]) == expected_cost.quantize(EIGHT_DP)
    assert Decimal(payload["fees_usd"]) == fee.quantize(EIGHT_DP)
    assert Decimal(payload["maker_ratio"]) == Decimal("1.0000")
    assert Decimal(payload["taker_ratio"]) == Decimal("0.0000")

    with module.SessionLocal() as session:
        record = session.get(module.TCAResult, ("OPS-1", "trade-precision"))

    assert record is not None
    assert record.slippage_bps == expected_slippage.quantize(TWELVE_DP)
    assert record.fees_usd == fee.quantize(TWELVE_DP)


def test_tca_report_preserves_decimal_precision(
    tca_client, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, module = tca_client

    filled_at = datetime(2024, 1, 1, 13, 0, tzinfo=UTC)
    fill_price = Decimal("34999.98765432")
    size = Decimal("0.00054321")
    expected_price = Decimal("35001.12345678")
    expected_fee = Decimal("0.00001234")
    realized_fee = Decimal("0.00005432")

    execution_row = {
        "trade_id": "trade-report",
        "account_id": "OPS-1",
        "market": "BTC-USD",
        "submitted_at": filled_at,
        "order_metadata": {
            "expected_price": expected_price,
            "expected_fee": expected_fee,
            "side": "buy",
        },
        "fill_id": "fill-report",
        "fill_time": filled_at,
        "fill_price": fill_price,
        "size": size,
        "fee": realized_fee,
        "fill_metadata": {},
    }

    monkeypatch.setattr(module, "_fetch_execution_rows", lambda *_, **__: [execution_row])
    monkeypatch.setattr(module, "log_audit", lambda **kwargs: None)
    monkeypatch.setattr(module, "hash_ip", lambda value: "hashed-ip")

    expected_metrics = module._compare_expected_realised([execution_row])

    client.app.dependency_overrides[module.require_admin_account] = lambda: "ops-admin"
    try:
        response = client.get(
            "/tca/report",
            params={
                "account_id": "OPS-1",
                "symbol": "BTC-USD",
                "date": "2024-01-01",
            },
        )
    finally:
        client.app.dependency_overrides.pop(module.require_admin_account, None)

    assert response.status_code == 200
    payload = response.json()

    assert Decimal(payload["expected_cost_usd"]) == expected_metrics.expected_cost.quantize(EIGHT_DP)
    assert Decimal(payload["realized_cost_usd"]) == expected_metrics.realized_cost.quantize(EIGHT_DP)
    assert Decimal(payload["slippage_bps"]) == expected_metrics.slippage_bps.quantize(BPS_QUANT)
    assert Decimal(payload["slippage_cost_usd"]) == expected_metrics.slippage_cost_usd.quantize(EIGHT_DP)
    assert Decimal(payload["fill_quality_bps"]) == expected_metrics.fill_quality_bps.quantize(BPS_QUANT)
    assert Decimal(payload["fee_impact_usd"]) == expected_metrics.fee_impact_usd.quantize(EIGHT_DP)
    assert payload["trade_count"] == expected_metrics.trade_count

    with module.SessionLocal() as session:
        stmt = (
            select(module.TCAReport)
            .where(
                module.TCAReport.account_id == "OPS-1",
                module.TCAReport.symbol == "BTC-USD",
            )
            .order_by(module.TCAReport.ts.desc())
        )
        report_record = session.execute(stmt).scalars().first()

    assert report_record is not None
    assert report_record.expected_cost == expected_metrics.expected_cost.quantize(TWELVE_DP)
    assert report_record.realized_cost == expected_metrics.realized_cost.quantize(TWELVE_DP)
    assert report_record.slippage_bps == expected_metrics.slippage_bps.quantize(TWELVE_DP)
