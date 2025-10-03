from __future__ import annotations

from typing import Iterator

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator
import sys
import types

import pytest
from fastapi.testclient import TestClient

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

from services.common import security
import services.oms.main as oms_main
from services.oms.main import app, KrakenClientBundle
from services.oms.kraken_ws import OrderAck
from shared.k8s import KrakenSecretStore

ADMIN_ACCOUNTS = ["admin-alpha", "admin-beta", "admin-gamma"]


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
def client_fixture(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setattr(security, "ADMIN_ACCOUNTS", set(ADMIN_ACCOUNTS))
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


@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_place_order_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    payload = {
        "account_id": account_id,
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": account_id})

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "accepted": True,
        "routed_venue": "kraken",
        "fee": payload["fee"],
    }


def test_place_order_rejects_non_admin(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "trade"})

    assert response.status_code == 403


def test_place_order_rejected_ack_sets_accepted_false(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
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
                    exchange_order_id="SIM-REJECT",
                    status="rejected",
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
                    exchange_order_id="SIM-REJECT",
                    status="rejected",
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
    monkeypatch.setattr(oms_main, "_ensure_ack_success", lambda ack, transport: None)

    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "accepted": False,
        "routed_venue": "kraken",
        "fee": payload["fee"],
    }


def test_place_order_mismatched_account(client: TestClient) -> None:
    payload = {
        "account_id": "admin-beta",
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Account mismatch between header and payload."


def test_place_order_validates_side(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-123",
        "side": "hold",
        "instrument": "BTC-USD",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 422
