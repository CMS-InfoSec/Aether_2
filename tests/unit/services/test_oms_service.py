"""Unit tests for the OMS FastAPI service."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Dict

import pytest
from fastapi.testclient import TestClient

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
    headers = {"X-Account-ID": "ACC1"}
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
        headers={"X-Account-ID": "ACC2"},
    )
    assert response.status_code == 403


def test_status_returns_last_known_result(oms_client: TestClient) -> None:
    headers = {"X-Account-ID": "ACC1"}
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
