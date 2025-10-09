"""Risk service unit tests covering schema validation and fee awareness."""

from __future__ import annotations

import sys
import types
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, Mapping

import pytest

from tests.helpers.risk import patch_sqlalchemy_for_risk

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_RESTORE_SQLALCHEMY = patch_sqlalchemy_for_risk(Path(__file__).with_name("risk_service_unit.db"))

pytest.importorskip("fastapi")
pytest.importorskip("services.common.security")
from fastapi import status
from fastapi.testclient import TestClient

import risk_service as risk_module
from risk_service import RiskEvaluationContext, app as risk_app, require_admin_account
from tests.helpers.authentication import override_admin_auth

_RESTORE_SQLALCHEMY()

config = dict(getattr(RiskEvaluationContext, "model_config", {}))
config["arbitrary_types_allowed"] = True
config["from_attributes"] = True
RiskEvaluationContext.model_config = config  # type: ignore[attr-defined]


@pytest.fixture
def risk_client() -> Iterator[TestClient]:
    with TestClient(risk_app) as client:
        snapshot = risk_module.UniverseSnapshot(
            symbols={"BTC-USD", "ETH-USD", "SOL-USD"},
            generated_at=datetime.now(timezone.utc),
            thresholds={},
        )
        risk_module._UNIVERSE_CACHE_SNAPSHOT = snapshot
        risk_module._UNIVERSE_CACHE_EXPIRY = datetime.now(timezone.utc) + timedelta(hours=1)
        yield client


def _base_request() -> Dict[str, object]:
    return {
        "account_id": "company",
        "intent": {
            "policy_id": "policy-1",
            "instrument_id": "BTC-USD",
            "side": "buy",
            "quantity": 0.5,
            "price": 30000.0,
        },
        "portfolio_state": {
            "net_asset_value": 1_000_000.0,
            "notional_exposure": 100_000.0,
            "realized_daily_loss": 1_000.0,
            "fees_paid": 1_000.0,
        },
    }


def test_risk_validation_passes_under_fee_budget(risk_client: TestClient) -> None:
    payload = _base_request()
    with override_admin_auth(
        risk_client.app, require_admin_account, payload["account_id"]
    ) as headers:
        response = risk_client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": payload["account_id"]},
        )
    assert response.status_code == 200
    body = response.json()
    assert body["pass"] is True
    assert body["reasons"] == []
    assert body["take_profit"] > payload["intent"]["price"]
    assert body["stop_loss"] < payload["intent"]["price"]


def test_risk_validation_rejects_when_fee_budget_exhausted(
    risk_client: TestClient,
) -> None:
    payload = _base_request()
    payload["portfolio_state"]["fees_paid"] = 40_000.0
    previous_fills = list(risk_module._STUB_FILLS)
    try:
        risk_module.set_stub_fills(
            [
                {
                    "account_id": "company",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "pnl": 0.0,
                    "fee": 40_000.0,
                }
            ]
        )
        with override_admin_auth(
            risk_client.app, require_admin_account, payload["account_id"]
        ) as headers:
            response = risk_client.post(
                "/risk/validate",
                json=payload,
                headers={**headers, "X-Account-ID": payload["account_id"]},
            )
    finally:
        risk_module.set_stub_fills(previous_fills)
    assert response.status_code == 200
    body = response.json()
    assert body["pass"] is False
    assert any("Fee budget exhausted" in reason for reason in body["reasons"])
    assert body["take_profit"] > payload["intent"]["price"]
    assert body["stop_loss"] < payload["intent"]["price"]


def test_risk_validation_enforces_schema(risk_client: TestClient) -> None:
    payload = _base_request()
    payload["intent"]["side"] = "hold"
    with override_admin_auth(
        risk_client.app, require_admin_account, payload["account_id"]
    ) as headers:
        response = risk_client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": payload["account_id"]},
        )
    assert response.status_code == 422


def test_risk_limits_endpoint_returns_configuration(risk_client: TestClient) -> None:
    with override_admin_auth(
        risk_client.app, require_admin_account, "company"
    ) as headers:
        response = risk_client.get(
            "/risk/limits",
            headers=headers,
        )
    assert response.status_code == 200
    body = response.json()
    assert body["limits"]["account_id"] == "company"
    assert body["usage"]["account_id"] == "company"


def test_risk_validation_rejects_unauthenticated_request(
    risk_client: TestClient,
) -> None:
    payload = _base_request()
    response = risk_client.post("/risk/validate", json=payload)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_risk_validation_rejects_account_mismatch(risk_client: TestClient) -> None:
    payload = _base_request()
    with override_admin_auth(
        risk_client.app, require_admin_account, payload["account_id"]
    ) as headers:
        payload["account_id"] = "director-1"
        response = risk_client.post(
            "/risk/validate",
            json=payload,
            headers=headers,
        )
    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert (
        response.json()["detail"]
        == "Account mismatch between authenticated session and payload."
    )


def test_risk_validation_rejects_non_spot_instrument(risk_client: TestClient) -> None:
    payload = _base_request()
    payload["intent"]["instrument_id"] = "BTC-PERP"
    with override_admin_auth(
        risk_client.app, require_admin_account, payload["account_id"]
    ) as headers:
        response = risk_client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": payload["account_id"]},
        )
    assert response.status_code == 200
    body = response.json()
    assert body["pass"] is False
    assert any(
        reason.startswith("Instrument not eligible for spot trading")
        for reason in body["reasons"]
    )


def test_position_size_rejects_non_spot_symbol(risk_client: TestClient) -> None:
    with override_admin_auth(
        risk_client.app, require_admin_account, "company"
    ) as headers:
        response = risk_client.get(
            "/risk/size",
            params={"symbol": "BTC-PERP"},
            headers={**headers, "X-Account-ID": "company"},
        )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["detail"] == "Only spot market symbols are supported for position sizing."


def test_risk_limits_filters_non_spot_whitelist(risk_client: TestClient) -> None:
    with risk_module.get_session() as session:
        record = session.get(risk_module.AccountRiskLimit, "company")
        assert record is not None
        original_whitelist = record.instrument_whitelist
        record.instrument_whitelist = "BTC-USD,BTC-PERP,ETH-USD,ETH-USDT"

    try:
        with override_admin_auth(
            risk_client.app, require_admin_account, "company"
        ) as headers:
            response = risk_client.get(
                "/risk/limits",
                headers={**headers, "X-Account-ID": "company"},
            )
    finally:
        with risk_module.get_session() as session:
            record = session.get(risk_module.AccountRiskLimit, "company")
            assert record is not None
            record.instrument_whitelist = original_whitelist

    assert response.status_code == 200
    whitelist = response.json()["limits"]["instrument_whitelist"]
    assert whitelist == ["BTC-USD", "ETH-USD"]


def test_risk_validation_blocks_when_available_cash_insufficient(
    risk_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _base_request()
    payload["intent"]["quantity"] = 5.0
    payload["intent"]["price"] = 1000.0
    payload["portfolio_state"]["notional_exposure"] = 0.0
    payload["portfolio_state"]["available_cash"] = 250.0

    class _BalanceStub:
        name = "stub"

        def supports(self, operation: str) -> bool:
            return operation == "get_balance"

        async def get_balance(self, account_id: str) -> Mapping[str, Any]:
            assert account_id == payload["account_id"]
            return {
                "net_asset_value": 10_000.0,
                "balances": {"USD": 250.0},
            }

    monkeypatch.setattr(risk_module, "EXCHANGE_ADAPTER", _BalanceStub())

    with override_admin_auth(
        risk_client.app, require_admin_account, payload["account_id"]
    ) as headers:
        response = risk_client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": payload["account_id"]},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["pass"] is False
    assert any(
        reason.startswith("Insufficient available USD balance") for reason in body["reasons"]
    )
    fee_ratio = getattr(risk_module, "_DEFAULT_TAKER_FEE_BPS") / 10_000.0
    expected_qty = payload["portfolio_state"]["available_cash"] / (
        payload["intent"]["price"] * (1.0 + fee_ratio)
    )
    assert body["adjusted_qty"] == pytest.approx(expected_qty)


def test_risk_validation_reserves_fee_budget_when_cash_equals_notional(
    risk_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _base_request()
    payload["intent"]["quantity"] = 1.0
    payload["intent"]["price"] = 100.0
    payload["portfolio_state"]["notional_exposure"] = 0.0
    payload["portfolio_state"]["available_cash"] = 100.0

    async def _stub_sizing(*args: Any, **kwargs: Any) -> Any:
        return types.SimpleNamespace(
            max_position=1_000_000.0,
            volatility=0.2,
            nav=payload["portfolio_state"]["net_asset_value"],
            fee_bps_estimate=None,
        )

    monkeypatch.setattr(
        risk_module.PositionSizer,
        "suggest_max_position",
        _stub_sizing,
    )

    with override_admin_auth(
        risk_client.app, require_admin_account, payload["account_id"]
    ) as headers:
        response = risk_client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": payload["account_id"]},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["pass"] is False
    assert any(
        reason.startswith("Insufficient available USD balance") for reason in body["reasons"]
    )
    assert body["adjusted_qty"] is not None
    fee_ratio = getattr(risk_module, "_DEFAULT_TAKER_FEE_BPS") / 10_000.0
    expected_quantity = payload["portfolio_state"]["available_cash"] / (
        payload["intent"]["price"] * (1.0 + fee_ratio)
    )
    assert body["adjusted_qty"] == pytest.approx(expected_quantity)
    assert body["adjusted_qty"] < payload["intent"]["quantity"]
