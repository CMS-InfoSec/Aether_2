from __future__ import annotations

import asyncio
import importlib.util
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Tuple

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
import pytest

pytest.importorskip("services.common.security")
pytest.importorskip("fastapi")
from fastapi import status
from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
from tests.helpers.authentication import override_admin_auth
from tests.helpers.risk import risk_service_instance


AccountClient = Tuple[TestClient, object]


@pytest.fixture()
def risk_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[AccountClient]:
    if importlib.util.find_spec("sqlalchemy") is None:
        pytest.skip("sqlalchemy is required for risk service tests")
    with risk_service_instance(tmp_path, monkeypatch) as module:
        with TestClient(module.app) as client:
            yield client, module


def _request_payload(account_id: str, instrument: str) -> dict[str, object]:
    return {
        "account_id": account_id,
        "intent": {
            "policy_id": "policy-123",
            "instrument_id": instrument,
            "side": "buy",
            "quantity": 1.0,
            "price": 1_000.0,
        },
        "portfolio_state": {
            "net_asset_value": 2_000_000.0,
            "notional_exposure": 100_000.0,
            "realized_daily_loss": 5_000.0,
            "fees_paid": 1_000.0,
            "var_95": 50_000.0,
            "var_99": 80_000.0,
        },
    }


@pytest.mark.parametrize(
    "account_id,instrument",
    (
        ("company", "BTC-USD"),
        ("director-1", "SOL-USD"),
        ("director-2", "ETH-USD"),
    ),
)
def test_validate_risk_all_admin_accounts(
    risk_app: AccountClient, account_id: str, instrument: str
) -> None:
    client, module = risk_app
    payload = _request_payload(account_id, instrument)
    with override_admin_auth(
        client.app, module.require_admin_account, account_id
    ) as headers:
        response = client.post(
            "/risk/validate",
            json=payload,
            headers={**headers, "X-Account-ID": account_id},
        )

    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == {"pass", "reasons", "adjusted_qty", "cooldown"}
    assert body["pass"] is True
    assert body["reasons"] == []


def test_get_risk_limits_returns_whitelists(risk_app: AccountClient) -> None:
    client, module = risk_app
    expected = {
        "company": ["BTC-USD", "ETH-USD"],
        "director-1": ["SOL-USD"],
        "director-2": ["BTC-USD", "ETH-USD"],
    }

    for account in sorted(ADMIN_ACCOUNTS):
        with override_admin_auth(
            client.app, module.require_admin_account, account
        ) as headers:
            response = client.get("/risk/limits", headers=headers)
        assert response.status_code == 200
        body = response.json()
        assert body["account_id"] == account
        assert body["limits"]["instrument_whitelist"] == expected[account]
        usage = body["usage"]
        assert usage["account_id"] == account
        for metric in ("realized_daily_loss", "fees_paid", "net_asset_value"):
            assert Decimal(str(usage[metric])) == Decimal("0")
        assert usage["var_95"] is None
        assert usage["var_99"] is None


def test_missing_account_returns_404(risk_app: AccountClient) -> None:
    client, module = risk_app
    missing_payload = _request_payload("shadow", "BTC-USD")
    with override_admin_auth(
        client.app, module.require_admin_account, "shadow"
    ) as headers:
        response = client.post(
            "/risk/validate",
            json=missing_payload,
            headers={**headers, "X-Account-ID": "shadow"},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "No risk limits configured for account 'shadow'."

    with override_admin_auth(
        client.app, module.require_admin_account, "shadow"
    ) as headers:
        limits_response = client.get("/risk/limits", headers=headers)
    assert limits_response.status_code == 404


def test_validate_risk_rejects_mismatched_header(risk_app: AccountClient) -> None:
    client, module = risk_app
    payload = _request_payload("company", "BTC-USD")

    with override_admin_auth(
        client.app, module.require_admin_account, "director-1"
    ) as headers:
        response = client.post(
            "/risk/validate",
            json=payload,
            headers=headers,
        )

    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert (
        response.json()["detail"]
        == "Account mismatch between authenticated session and payload."
    )


def test_limits_preserve_large_decimal_usage(risk_app: AccountClient) -> None:
    client, module = risk_app
    account_id = "company"
    large_nav = Decimal("10000000.55")
    realized_loss = Decimal("1234.56")
    fees_paid = Decimal("789.01")

    module.set_stub_account_usage(
        account_id,
        {
            "net_asset_value": large_nav,
            "realized_daily_loss": realized_loss,
            "fees_paid": fees_paid,
            "var_95": Decimal("4567.89"),
            "var_99": Decimal("5678.90"),
        },
    )

    with override_admin_auth(
        client.app, module.require_admin_account, account_id
    ) as headers:
        response = client.get("/risk/limits", headers=headers)
    assert response.status_code == 200
    usage = response.json()["usage"]
    assert Decimal(str(usage["net_asset_value"])) == large_nav
    assert Decimal(str(usage["realized_daily_loss"])) == realized_loss
    assert Decimal(str(usage["fees_paid"])) == fees_paid
    assert Decimal(str(usage["var_95"])) == Decimal("4567.89")
    assert Decimal(str(usage["var_99"])) == Decimal("5678.90")

    module.set_stub_account_usage(
        account_id,
        {
            "net_asset_value": Decimal("0"),
            "realized_daily_loss": Decimal("0"),
            "fees_paid": Decimal("0"),
            "var_95": None,
            "var_99": None,
        },
    )


@pytest.mark.asyncio
async def test_validate_risk_allocator_latency_does_not_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if importlib.util.find_spec("sqlalchemy") is None:
        pytest.skip("sqlalchemy is required for risk service tests")

    delay = 0.25
    account_id = "company"
    allocator_payload = {
        "total_nav": 1_000_000.0,
        "accounts": [
            {
                "account_id": account_id,
                "allocation_pct": 0.4,
                "allocated_nav": 400_000.0,
                "drawdown_ratio": 0.1,
                "throttled": False,
            }
        ],
    }

    class _AllocatorResponse:
        def __init__(self, data: dict[str, object]) -> None:
            self._data = data

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._data

    class _SlowAllocatorClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self) -> "_SlowAllocatorClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def get(self, _endpoint: str) -> _AllocatorResponse:
            time.sleep(delay)
            return _AllocatorResponse(allocator_payload)

    class _SlowAllocatorAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self) -> "_SlowAllocatorAsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

        async def get(self, _endpoint: str) -> _AllocatorResponse:
            await asyncio.sleep(delay)
            return _AllocatorResponse(allocator_payload)

    with risk_service_instance(tmp_path, monkeypatch) as module:
        monkeypatch.setenv("CAPITAL_ALLOCATOR_URL", "http://allocator.internal")
        monkeypatch.setattr(module.httpx, "Client", _SlowAllocatorClient)
        monkeypatch.setattr(module.httpx, "AsyncClient", _SlowAllocatorAsyncClient)

        app = module.app

        with override_admin_auth(app, module.require_admin_account, account_id) as headers:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:
                payload = _request_payload(account_id, "BTC-USD")
                requests = 3
                start = time.perf_counter()
                responses = await asyncio.gather(
                    *(
                        client.post(
                            "/risk/validate",
                            json=payload,
                            headers={**headers, "X-Account-ID": account_id},
                        )
                        for _ in range(requests)
                    )
                )
                elapsed = time.perf_counter() - start

    assert all(response.status_code == 200 for response in responses)
    assert elapsed < delay * 2
