from __future__ import annotations

from copy import deepcopy
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.helpers.risk import patch_sqlalchemy_for_risk

_RESTORE_SQLALCHEMY = patch_sqlalchemy_for_risk(Path(__file__).with_name("risk_endpoints.db"))

import pytest
from fastapi import status
from fastapi.testclient import TestClient

if "prometheus_client" not in sys.modules:
    prometheus_client = types.ModuleType("prometheus_client")

    class _CollectorRegistry:
        def __init__(self) -> None:  # pragma: no cover - simple stub
            self.metrics: list[object] = []

    class _Metric:
        def __init__(self, *args, **kwargs) -> None:  # pragma: no cover - simple stub
            self._args = args
            self._kwargs = kwargs

        def labels(self, **kwargs):  # pragma: no cover - simple stub
            return self

        def set(self, *args, **kwargs) -> None:  # pragma: no cover - simple stub
            return None

        def inc(self, *args, **kwargs) -> None:  # pragma: no cover - simple stub
            return None

    def _generate_latest(_registry: _CollectorRegistry) -> bytes:  # pragma: no cover - stub
        return b""

    prometheus_client.CONTENT_TYPE_LATEST = "text/plain"
    prometheus_client.CollectorRegistry = _CollectorRegistry
    prometheus_client.Counter = _Metric
    prometheus_client.Gauge = _Metric
    prometheus_client.generate_latest = _generate_latest

    sys.modules["prometheus_client"] = prometheus_client

pytest.importorskip("services.common.security")

import risk_service as risk_module
from risk_service import app, require_admin_account
from services.common import security
from tests.helpers.authentication import override_admin_auth

_RESTORE_SQLALCHEMY()


@pytest.fixture(autouse=True)
def allow_stub_accounts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        security,
        "ADMIN_ACCOUNTS",
        set(security.ADMIN_ACCOUNTS) | {"ACC-DEFAULT", "ACC-AGGR"},
    )


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    with TestClient(app) as client:
        snapshot = risk_module.UniverseSnapshot(
            symbols={"BTC-USD", "ETH-USD", "SOL-USD"},
            generated_at=datetime.now(timezone.utc),
            thresholds={},
        )
        risk_module._UNIVERSE_CACHE_SNAPSHOT = snapshot
        risk_module._UNIVERSE_CACHE_EXPIRY = datetime.now(timezone.utc) + timedelta(hours=1)
        yield client


@pytest.fixture(name="risk_payload")
def risk_payload_fixture() -> dict[str, object]:
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
            "notional_exposure": 250_000.0,
            "realized_daily_loss": 5_000.0,
            "fees_paid": 2_500.0,
            "var_95": 20_000.0,
            "var_99": 40_000.0,
        },
    }


def test_validate_requires_admin_header(client: TestClient, risk_payload: dict[str, object]) -> None:
    response = client.post("/risk/validate", json=risk_payload)

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_validate_rejects_account_mismatch(client: TestClient, risk_payload: dict[str, object]) -> None:
    payload = deepcopy(risk_payload)
    payload["account_id"] = "director-1"

    with override_admin_auth(
        client.app, require_admin_account, "company"
    ) as headers:
        response = client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": "company"},
        )

    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert (
        response.json()["detail"]
        == "Account mismatch between authenticated session and payload."
    )


def test_limits_requires_admin_header(client: TestClient) -> None:
    response = client.get("/risk/limits")

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_limits_returns_account_data(client: TestClient) -> None:
    with override_admin_auth(
        client.app, require_admin_account, "company"
    ) as headers:
        response = client.get(
            "/risk/limits",
            headers={**headers, "X-Account-ID": "company"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["account_id"] == "company"
    assert "limits" in body and "usage" in body
