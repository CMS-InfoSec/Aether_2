from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator
import sys
import types

import pytest
from fastapi.testclient import TestClient

from services.common.security import ADMIN_ACCOUNTS
if "aiohttp" not in sys.modules:
    class _StubSession:
        async def __aenter__(self) -> "_StubSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def close(self) -> None:
            return None

        async def post(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("aiohttp stub invoked")

        async def get(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("aiohttp stub invoked")

    class _ClientTimeout:
        def __init__(self, total: float | None = None) -> None:
            self.total = total

    aiohttp_stub = types.SimpleNamespace(
        ClientSession=lambda *args, **kwargs: _StubSession(),
        ClientTimeout=_ClientTimeout,
        ClientError=Exception,
    )
    sys.modules["aiohttp"] = aiohttp_stub

from services.oms.main import app, KrakenClientBundle
from services.oms.kraken_ws import OrderAck
from shared.k8s import KrakenSecretStore


@pytest.fixture(autouse=True)
def _stub_kraken_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    @asynccontextmanager
    async def _factory(account_id: str):
        async def _credentials() -> Dict[str, Any]:
            return {
                "api_key": f"test-key-{account_id}",
                "api_secret": "secret",
                "metadata": {"rotated_at": datetime.now(timezone.utc).isoformat()},
            }

        class _StubWS:
            async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
                return OrderAck(
                    exchange_order_id="SIM-123",
                    status="ok",
                    filled_qty=None,
                    avg_price=None,
                    errors=None,
                )

            async def fetch_open_orders_snapshot(self) -> list[Dict[str, Any]]:
                return []

            async def fetch_own_trades_snapshot(self) -> list[Dict[str, Any]]:
                return []

            async def close(self) -> None:
                return None

        class _StubREST:
            async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
                return OrderAck(
                    exchange_order_id="SIM-123",
                    status="ok",
                    filled_qty=None,
                    avg_price=None,
                    errors=None,
                )

            async def open_orders(self) -> Dict[str, Any]:
                return {"result": {"open": []}}

            async def own_trades(self) -> Dict[str, Any]:
                return {"result": {"trades": {}}}

            async def close(self) -> None:
                return None

        ws_client = _StubWS()
        rest_client = _StubREST()
        try:
            yield KrakenClientBundle(
                credential_getter=_credentials,
                ws_client=ws_client,  # type: ignore[arg-type]
                rest_client=rest_client,  # type: ignore[arg-type]
            )
        finally:
            await ws_client.close()
            await rest_client.close()

    monkeypatch.setattr(app.state, "kraken_client_factory", _factory)


@pytest.fixture(name="client")
def client_fixture() -> Iterator[TestClient]:
    KrakenSecretStore.reset()
    store = KrakenSecretStore()
    for account in ADMIN_ACCOUNTS:
        store.write_credentials(
            account,
            api_key=f"test-key-{account}",
            api_secret=f"test-secret-{account}",
        )

    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()
        KrakenSecretStore.reset()


def test_oms_place_authorized_accounts(client: TestClient) -> None:
    payload = {
        "account_id": "company",
        "order_id": "1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 10.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }
    for account in ADMIN_ACCOUNTS:
        payload["account_id"] = account
        response = client.post("/oms/place", json=payload, headers={"X-Account-ID": account})
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {
            "accepted",
            "routed_venue",
            "fee",
            "exchange_order_id",
            "kraken_status",
            "errors",
        }
        assert data["fee"] == payload["fee"]
        assert data["exchange_order_id"] == "SIM-123"
        assert data["kraken_status"] == "ok"
        assert data["errors"] is None


def test_oms_place_rejects_non_spot_instrument(client: TestClient) -> None:
    payload = {
        "account_id": ADMIN_ACCOUNTS[0],
        "order_id": "non-spot",
        "instrument": "ETH-PERP",
        "side": "BUY",
        "quantity": 1.0,
        "price": 10.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post(
        "/oms/place",
        json=payload,
        headers={"X-Account-ID": payload["account_id"]},
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    detail = response.json().get("detail", [])
    assert any("Only spot market instruments" in str(item.get("msg", item)) for item in detail)


def test_oms_place_rejects_non_admin_account(client: TestClient) -> None:
    payload = {
        "account_id": "shadow",
        "order_id": "1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 10.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }
    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "shadow"})
    assert response.status_code == 403
