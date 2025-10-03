import importlib
import importlib.util
import sys
from decimal import Decimal
from pathlib import Path
from datetime import datetime, timezone

import pytest

pytest.importorskip("services.common.security")
pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
from sqlalchemy import text

from tests.helpers.authentication import override_admin_auth
from tests.helpers.risk import risk_service_instance


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
                    net_asset_value REAL,
                    equity REAL,
                    ending_balance REAL,
                    balance REAL,
                    drawdown REAL,
                    realized_drawdown REAL,
                    drawdown_value REAL,
                    max_drawdown REAL,
                    drawdown_limit REAL,
                    max_drawdown_limit REAL,
                    drawdown_cap REAL,
                    drawdown_threshold REAL,
                    curve_ts TEXT,
                    valuation_ts TEXT,
                    ts TEXT,
                    created_at TEXT
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
        assert Decimal(body["total_nav"]) == Decimal("1000000.00")
        accounts = {entry["account_id"]: entry for entry in body["accounts"]}
        assert accounts["director-1"]["throttled"] is True
        assert Decimal(accounts["director-1"]["allocation_pct"]) == Decimal("0.05")
        assert Decimal(accounts["company"]["allocation_pct"]).quantize(Decimal("0.000001")) == Decimal("0.656250")
        assert Decimal(accounts["director-2"]["allocation_pct"]).quantize(Decimal("0.000001")) == Decimal("0.293750")

        with module.SessionLocal() as session:
            result = session.execute(text("SELECT COUNT(*) FROM capital_allocations"))
            assert result.scalar() == 3

        status = client.get("/allocator/status")
        assert status.status_code == 200
        status_body = status.json()
        status_accounts = {entry["account_id"]: entry for entry in status_body["accounts"]}
        assert status_accounts["director-1"]["throttled"] is True
        assert Decimal(status_accounts["director-1"]["allocation_pct"]) == Decimal("0.05")

    module.ENGINE.dispose()
    sys.modules.pop("capital_allocator", None)


def test_allocator_handles_large_navs_precision(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
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
                    net_asset_value REAL,
                    equity REAL,
                    ending_balance REAL,
                    balance REAL,
                    drawdown REAL,
                    realized_drawdown REAL,
                    drawdown_value REAL,
                    max_drawdown REAL,
                    drawdown_limit REAL,
                    max_drawdown_limit REAL,
                    drawdown_cap REAL,
                    drawdown_threshold REAL,
                    curve_ts TEXT,
                    valuation_ts TEXT,
                    ts TEXT,
                    created_at TEXT
                )
                """
            )
        )
        conn.execute(text("DELETE FROM pnl_curves"))
        conn.execute(text("DELETE FROM capital_allocations"))
        now = datetime.now(timezone.utc).isoformat()
        rows = [
            ("mega-1", 125_000_000.12, 5_000_000.01, 12_500_000.00, now),
            ("mega-2", 225_000_000.34, 10_000_000.02, 20_000_000.00, now),
            ("mega-3", 175_000_000.45, 3_500_000.00, 15_000_000.00, now),
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
        payload = {
            "allocations": {
                "mega-1": "0.333333",
                "mega-2": "0.333333",
                "mega-3": "0.333334",
            }
        }
        response = client.post("/allocator/rebalance", json=payload)
        assert response.status_code == 200
        body = response.json()

        total_nav = Decimal(body["total_nav"])
        expected_total = sum(Decimal(str(row[1])).quantize(Decimal("0.01")) for row in rows)
        assert total_nav == expected_total

        account_map = {entry["account_id"]: entry for entry in body["accounts"]}
        allocated_sum = Decimal("0")
        sorted_accounts = sorted(payload["allocations"].items())
        for index, (account_id, expected_pct) in enumerate(sorted_accounts, start=1):
            entry = account_map[account_id]
            allocation_pct = Decimal(entry["allocation_pct"])
            allocated_nav = Decimal(entry["allocated_nav"])
            expected_pct_decimal = Decimal(expected_pct)
            assert allocation_pct == expected_pct_decimal
            if index == len(sorted_accounts):
                expected_nav = (total_nav - allocated_sum).quantize(Decimal("0.01"))
            else:
                expected_nav = (expected_pct_decimal * total_nav).quantize(Decimal("0.01"))
                allocated_sum += expected_nav
            assert allocated_nav == expected_nav
        allocated_sum = sum(Decimal(entry["allocated_nav"]) for entry in body["accounts"])
        assert allocated_sum == total_nav

        status = client.get("/allocator/status")
        assert status.status_code == 200
        status_body = status.json()
        status_total_nav = Decimal(status_body["total_nav"])
        assert status_total_nav == total_nav
        status_accounts = {entry["account_id"]: entry for entry in status_body["accounts"]}
        allocated_status_sum = Decimal("0")
        for index, (account_id, expected_pct) in enumerate(sorted(payload["allocations"].items()), start=1):
            entry = status_accounts[account_id]
            allocation_pct = Decimal(entry["allocation_pct"])
            allocated_nav = Decimal(entry["allocated_nav"])
            expected_pct_decimal = Decimal(expected_pct)
            assert allocation_pct == expected_pct_decimal
            if index == len(status_accounts):
                expected_nav = (status_total_nav - allocated_status_sum).quantize(Decimal("0.01"))
            else:
                expected_nav = (expected_pct_decimal * status_total_nav).quantize(Decimal("0.01"))
                allocated_status_sum += expected_nav
            assert allocated_nav == expected_nav

    module.ENGINE.dispose()
    sys.modules.pop("capital_allocator", None)
@pytest.fixture()
def risk_service_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    if importlib.util.find_spec("sqlalchemy") is None:
        pytest.skip("sqlalchemy is required for risk service tests")

    import shared.graceful_shutdown as graceful_shutdown

    monkeypatch.setattr(graceful_shutdown, "install_sigterm_handler", lambda manager: None)

    with risk_service_instance(tmp_path, monkeypatch) as module:
        with TestClient(module.app) as client:
            yield client, module


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

    with override_admin_auth(
        client.app, module.require_admin_account, payload["account_id"]
    ) as headers:
        response = client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": payload["account_id"]},
        )
    assert response.status_code == 200
    body = response.json()
    assert body["pass"] is False
    assert any("capital allocator" in reason.lower() for reason in body["reasons"])
    assert body["adjusted_qty"] == 0.0
