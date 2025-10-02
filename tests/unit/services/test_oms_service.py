"""Unit tests for the OMS FastAPI service."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Dict

import os

import pytest
from fastapi.testclient import TestClient

from auth_service import create_jwt

os.environ["AUTH_JWT_SECRET"] = "test-secret"

from services.oms import oms_service


class FakeAccount:
    def __init__(self) -> None:
        self.orders: Dict[str, SimpleNamespace] = {}

    async def place_order(self, request: oms_service.OMSPlaceRequest) -> oms_service.OMSPlaceResponse:
        response = oms_service.OMSPlaceResponse(
            exchange_order_id="EX-1",
            status="placed",
            filled_qty=Decimal("0"),
            avg_price=Decimal("0"),
            errors=None,
            transport="websocket",
            reused=False,
        )
        self.orders[request.client_id] = SimpleNamespace(result=response)
        return response

    async def cancel_order(self, request: oms_service.OMSCancelRequest) -> oms_service.OMSOrderStatusResponse:
        response = oms_service.OMSOrderStatusResponse(
            exchange_order_id=request.exchange_order_id or "EX-1",
            status="cancelled",
            filled_qty=Decimal("0"),
            avg_price=Decimal("0"),
            errors=None,
        )
        self.orders[request.client_id] = SimpleNamespace(result=response)
        return response

    async def lookup(self, client_id: str) -> SimpleNamespace | None:
        return self.orders.get(client_id)

    async def start(self) -> None:  # pragma: no cover - compatibility shim
        return None

    async def close(self) -> None:  # pragma: no cover
        return None

    def routing_status(self) -> Dict[str, float | str | None]:
        return {
            "ws_latency": 5.0,
            "rest_latency": 7.5,
            "preferred_path": "websocket",
        }


class FakeManager:
    def __init__(self) -> None:
        self.accounts: Dict[str, FakeAccount] = {}

    async def get_account(self, account_id: str) -> FakeAccount:
        return self.accounts.setdefault(account_id, FakeAccount())

    async def shutdown(self) -> None:  # pragma: no cover - compatibility shim
        return None


@pytest.fixture
def oms_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setattr(oms_service, "manager", FakeManager())
    return TestClient(oms_service.app)


def _auth_headers(account_id: str) -> Dict[str, str]:
    token, _ = create_jwt(subject=account_id, ttl_seconds=3600)
    return {"Authorization": f"Bearer {token}"}


def test_place_order_requires_auth_header(oms_client: TestClient) -> None:
    payload = {
        "account_id": "ACC1",
        "client_id": "CID-1",
        "symbol": "BTC/USD",
        "side": "buy",
        "type": "limit",
        "qty": "1",
        "limit_px": "50000",
    }
    response = oms_client.post("/oms/place", json=payload)
    assert response.status_code == 401


def test_place_order_succeeds_with_matching_account(oms_client: TestClient) -> None:
    payload = {
        "account_id": "ACC1",
        "client_id": "CID-1",
        "symbol": "BTC/USD",
        "side": "buy",
        "type": "limit",
        "qty": "1",
        "limit_px": "50000",
    }
    headers = _auth_headers("ACC1")
    response = oms_client.post("/oms/place", json=payload, headers=headers)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "placed"
    assert body["reused"] is False


def test_status_requires_matching_account(oms_client: TestClient) -> None:
    response = oms_client.get("/oms/status", params={"account_id": "ACC1", "client_id": "CID-1"})
    assert response.status_code == 401

    response = oms_client.get(
        "/oms/status",
        params={"account_id": "ACC1", "client_id": "CID-1"},
        headers=_auth_headers("ACC2"),
    )
    assert response.status_code == 403


def test_status_returns_last_known_result(oms_client: TestClient) -> None:
    headers = _auth_headers("ACC1")
    payload = {
        "account_id": "ACC1",
        "client_id": "CID-1",
        "symbol": "BTC/USD",
        "side": "buy",
        "type": "limit",
        "qty": "1",
        "limit_px": "50000",
    }
    place_response = oms_client.post("/oms/place", json=payload, headers=headers)
    assert place_response.status_code == 200

    status_response = oms_client.get(
        "/oms/status",
        params={"account_id": "ACC1", "client_id": "CID-1"},
        headers=headers,
    )
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "placed"


def test_routing_status_requires_auth(oms_client: TestClient) -> None:
    response = oms_client.get("/oms/routing/status", params={"account_id": "ACC1"})
    assert response.status_code == 401

    response = oms_client.get(
        "/oms/routing/status",
        params={"account_id": "ACC1"},
        headers=_auth_headers("ACC2"),
    )
    assert response.status_code == 403


def test_routing_status_returns_router_state(oms_client: TestClient) -> None:
    headers = _auth_headers("ACC1")
    response = oms_client.get(
        "/oms/routing/status",
        params={"account_id": "ACC1"},
        headers=headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body == {
        "ws_latency": 5.0,
        "rest_latency": 7.5,
        "preferred_path": "websocket",
    }


def test_warm_start_status_endpoint(oms_client: TestClient) -> None:
    response = oms_client.get("/oms/warm_start/report", headers=_auth_headers("ACC1"))
    assert response.status_code == 200
    body = response.json()
    assert body == {"orders_resynced": 0, "fills_replayed": 0, "latency_ms": 0}
