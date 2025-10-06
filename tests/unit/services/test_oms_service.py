"""Unit tests for the OMS FastAPI service."""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Dict

ROOT = Path(__file__).resolve().parents[3]
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
from fastapi import status
from fastapi.testclient import TestClient

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret")
os.environ.setdefault("AUTH_DATABASE_URL", "sqlite:///./test-auth-service.db")

from auth_service import create_jwt

from services.oms import oms_service
from services.risk.stablecoin_monitor import StablecoinMonitor, StablecoinMonitorConfig


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
    token, _ = create_jwt(subject=account_id, role="admin", ttl_seconds=3600)
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


def test_missing_account_header_logs_and_counts_failure(
    oms_client: TestClient,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import metrics as metrics_module

    metrics_module.init_metrics(metrics_module._SERVICE_NAME)
    reason = "missing_account_id"

    metric = metrics_module._METRICS["oms_auth_failures_total"]
    prior_count = getattr(metric, "_value", 0.0)

    recorded_reasons: list[str] = []
    original_increment = metrics_module.increment_oms_auth_failures

    def _tracking_increment(*, reason: str, service: str | None = None) -> None:
        recorded_reasons.append(reason)
        original_increment(reason=reason, service=service)

    monkeypatch.setattr(oms_service, "increment_oms_auth_failures", _tracking_increment)

    payload = {
        "account_id": "ACC1",
        "client_id": "CID-1",
        "symbol": "BTC/USD",
        "side": "buy",
        "type": "limit",
        "qty": "1",
        "limit_px": "50000",
    }

    with caplog.at_level(logging.WARNING):
        response = oms_client.post("/oms/place", json=payload)

    assert response.status_code == status.HTTP_401_UNAUTHORIZED

    updated_count = getattr(metric, "_value", 0.0)

    assert updated_count == pytest.approx(prior_count + 1)
    assert recorded_reasons == [reason]

    relevant_records = [
        record
        for record in caplog.records
        if record.getMessage() == "Unauthorized OMS request missing X-Account-ID header"
    ]
    assert relevant_records, "Expected unauthorized request log entry"

    record = relevant_records[-1]
    assert record.reason == reason
    assert record.status_code == status.HTTP_401_UNAUTHORIZED
    assert record.account_header == "missing"
    assert record.source_ip


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


def test_place_order_rejects_non_spot_symbol(oms_client: TestClient) -> None:
    payload = {
        "account_id": "ACC1",
        "client_id": "CID-spot-guard",
        "symbol": "BTC-PERP",
        "side": "buy",
        "type": "limit",
        "qty": "1",
        "limit_px": "50000",
    }
    headers = _auth_headers("ACC1")

    response = oms_client.post("/oms/place", json=payload, headers=headers)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    detail = response.json().get("detail", [])
    assert any("Only spot market symbols" in str(item) for item in detail)


def test_place_order_blocked_when_stablecoin_depegged(
    monkeypatch: pytest.MonkeyPatch, oms_client: TestClient
) -> None:
    monitor = StablecoinMonitor(
        config=StablecoinMonitorConfig(
            depeg_threshold_bps=50,
            recovery_threshold_bps=10,
            feed_max_age_seconds=60,
            monitored_symbols=("USDC-USD",),
            trusted_feeds=("primary_fx",),
        )
    )
    monitor.update("USDC-USD", 0.9900, feed="primary_fx")
    monkeypatch.setattr(oms_service, "get_global_monitor", lambda: monitor)

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
    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert "Stablecoin deviation" in response.json()["detail"]


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

    headers = {"X-Account-ID": "ACC1"}
    response = oms_client.get("/oms/warm_start/status", headers=headers)
    assert response.status_code == 200
    body = response.json()
    assert body["orders_resynced"] == 0
    assert body["fills_replayed"] == 0

