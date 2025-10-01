import importlib
import importlib.util
import sys
from pathlib import Path
from datetime import datetime, timezone

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
from sqlalchemy import text


def _reload_allocator(monkeypatch: pytest.MonkeyPatch, db_url: str) -> object:
    monkeypatch.setenv("CAPITAL_ALLOCATOR_DB_URL", db_url)
    monkeypatch.setenv("ALLOCATOR_DRAWDOWN_THRESHOLD", "0.8")
    monkeypatch.setenv("ALLOCATOR_MIN_THROTTLE_PCT", "0.05")
    sys.modules.pop("capital_allocator", None)
    module = importlib.import_module("capital_allocator")
    return module


def test_capital_allocator_rebalance_and_status(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_url = "sqlite:///:memory:"
    module = _reload_allocator(monkeypatch, db_url)
    engine = module.ENGINE

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS pnl_curves (
                    account_id TEXT,
                    nav REAL,
                    drawdown REAL,
                    drawdown_limit REAL,
                    curve_ts TEXT
                )
                """
            )
        )
        now = datetime.now(timezone.utc).isoformat()
        rows = [
            ("company", 500_000.0, 20_000.0, 100_000.0, now),
            ("director-1", 200_000.0, 85_000.0, 90_000.0, now),
            ("director-2", 300_000.0, 10_000.0, 90_000.0, now),
        ]
        conn.execute(
            text(
                "INSERT INTO pnl_curves (account_id, nav, drawdown, drawdown_limit, curve_ts) "
                "VALUES (:account_id, :nav, :drawdown, :drawdown_limit, :curve_ts)"
            ),
            [
                {
                    "account_id": account,
                    "nav": nav,
                    "drawdown": drawdown,
                    "drawdown_limit": limit,
                    "curve_ts": ts,
                }
                for account, nav, drawdown, limit, ts in rows
            ],
        )

    with TestClient(module.app) as client:
        payload = {"allocations": {"company": 0.5, "director-1": 0.3, "director-2": 0.2}}
        response = client.post("/allocator/rebalance", json=payload)
        assert response.status_code == 200
        body = response.json()
        assert body["total_nav"] == pytest.approx(1_000_000.0)
        accounts = {entry["account_id"]: entry for entry in body["accounts"]}
        assert accounts["director-1"]["throttled"] is True
        assert accounts["director-1"]["allocation_pct"] == pytest.approx(0.05, rel=1e-6)
        assert accounts["company"]["allocation_pct"] == pytest.approx(0.65625, rel=1e-6)
        assert accounts["director-2"]["allocation_pct"] == pytest.approx(0.29375, rel=1e-6)

        with module.SessionLocal() as session:
            result = session.execute(text("SELECT COUNT(*) FROM capital_allocations"))
            assert result.scalar() == 3

        status = client.get("/allocator/status")
        assert status.status_code == 200
        status_body = status.json()
        status_accounts = {entry["account_id"]: entry for entry in status_body["accounts"]}
        assert status_accounts["director-1"]["throttled"] is True
        assert status_accounts["director-1"]["allocation_pct"] == pytest.approx(0.05, rel=1e-6)

    module.ENGINE.dispose()
    sys.modules.pop("capital_allocator", None)
@pytest.fixture()
def risk_service_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    if importlib.util.find_spec("sqlalchemy") is None:
        pytest.skip("sqlalchemy is required for risk service tests")

    db_path = tmp_path / "risk.db"
    monkeypatch.setenv("RISK_DATABASE_URL", f"sqlite:///{db_path}")
    sys.modules.pop("risk_service", None)
    module = importlib.import_module("risk_service")

    try:
        with TestClient(module.app) as client:
            yield client, module
    finally:
        module.ENGINE.dispose()
        sys.modules.pop("risk_service", None)


def test_risk_engine_blocks_when_allocator_throttles(
    risk_service_app, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, module = risk_service_app

    throttled_state = module.AllocatorAccountState(
        account_id="company",
        allocation_pct=0.25,
        allocated_nav=250_000.0,
        drawdown_ratio=0.9,
        throttled=True,
    )

    monkeypatch.setattr(module, "_query_allocator_state", lambda account_id: throttled_state)

    payload = {
        "account_id": "company",
        "intent": {
            "policy_id": "policy-123",
            "instrument_id": "BTC-USD",
            "side": "buy",
            "quantity": 5.0,
            "price": 10_000.0,
        },
        "portfolio_state": {
            "net_asset_value": 200_000.0,
            "notional_exposure": 50_000.0,
            "realized_daily_loss": 10_000.0,
            "fees_paid": 1_000.0,
            "var_95": 25_000.0,
            "var_99": 45_000.0,
        },
    }

    response = client.post("/risk/validate", json=payload)
    assert response.status_code == 200
    body = response.json()
    assert body["pass"] is False
    assert any("capital allocator" in reason.lower() for reason in body["reasons"])
    assert body["adjusted_qty"] == 0.0
