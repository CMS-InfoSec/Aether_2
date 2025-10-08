from __future__ import annotations

import os
from decimal import Decimal
import importlib
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, Iterator

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
services_init = ROOT / "services" / "__init__.py"
spec = importlib.util.spec_from_file_location(
    "services", services_init, submodule_search_locations=[str(services_init.parent)]
)
if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
    raise ImportError("Unable to load local services package for tests")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
sys.modules["services"] = module

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret")
os.environ.setdefault("AUTH_DATABASE_URL", "sqlite:///./test-auth-service.db")

from auth_service import create_jwt

from services.oms import oms_service


class _StubAccount:
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


class _StubManager:
    def __init__(self) -> None:
        self.accounts: Dict[str, _StubAccount] = {}

    async def get_account(self, account_id: str) -> _StubAccount:
        return self.accounts.setdefault(account_id, _StubAccount())

    async def shutdown(self) -> None:  # pragma: no cover
        return None


@pytest.fixture(name="client")
def client_fixture(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setattr(oms_service, "manager", _StubManager())
    with TestClient(oms_service.app) as client:
        yield client


def _auth_headers(account_id: str) -> Dict[str, str]:
    token, _ = create_jwt(subject=account_id, role="admin", ttl_seconds=3600)
    return {"Authorization": f"Bearer {token}"}


def _place_payload(account_id: str) -> Dict[str, str]:
    return {
        "account_id": account_id,
        "client_id": "CID-1",
        "symbol": "BTC/USD",
        "side": "buy",
        "type": "limit",
        "qty": "1",
        "limit_px": "50000",
    }


def test_spoofed_header_rejected(client: TestClient) -> None:
    payload = _place_payload("ACC1")
    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "ACC1"})
    assert response.status_code == 401
    body = response.json()
    assert body["detail"].lower().startswith("missing")


def test_valid_token_allows_order(client: TestClient) -> None:
    payload = _place_payload("ACC1")
    headers = _auth_headers("ACC1")
    response = client.post("/oms/place", json=payload, headers=headers)
    assert response.status_code == 200
    assert response.json()["status"] == "placed"
