"""Security regression tests for the execution anomaly service."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

from auth.service import InMemorySessionStore
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

services_init = ROOT / "services" / "__init__.py"
services_spec = importlib.util.spec_from_file_location(
    "services",
    services_init,
    submodule_search_locations=[str(ROOT / "services")],
)
services_module = importlib.util.module_from_spec(services_spec)
services_module.__path__ = [str(ROOT / "services")]
assert services_spec.loader is not None
services_spec.loader.exec_module(services_module)
sys.modules["services"] = services_module

common_init = ROOT / "services" / "common" / "__init__.py"
common_spec = importlib.util.spec_from_file_location(
    "services.common",
    common_init,
    submodule_search_locations=[str(ROOT / "services" / "common")],
)
common_module = importlib.util.module_from_spec(common_spec)
assert common_spec.loader is not None
common_spec.loader.exec_module(common_module)
sys.modules["services.common"] = common_module

os.environ.setdefault("EXECUTION_ANOMALY_DATABASE_URL", "sqlite:///:memory:")

MODULE_NAME = "services.anomaly.execution_anomaly"
if MODULE_NAME in sys.modules:
    del sys.modules[MODULE_NAME]
execution_anomaly = importlib.import_module(MODULE_NAME)


def _make_client() -> tuple[TestClient, InMemorySessionStore]:
    client = TestClient(execution_anomaly.app)
    store = InMemorySessionStore(ttl_minutes=60)
    client.app.state.session_store = store
    return client, store


def _auth_headers(store: InMemorySessionStore, account_id: str) -> dict[str, str]:
    session = store.create(account_id)
    token = session.token
    return {"Authorization": f"Bearer {token}", "X-Account-ID": account_id}


def _reset_monitor() -> None:
    execution_anomaly.monitor = execution_anomaly.ExecutionAnomalyMonitor()


def test_ingest_sample_requires_authentication() -> None:
    _reset_monitor()
    client, _ = _make_client()

    response = client.post(
        "/anomaly/execution/sample",
        json={
            "account_id": "company",
            "symbol": "AAPL",
            "rejection_rate": 0.0,
            "cancel_rate": 0.0,
            "partial_fill_ratio": 1.0,
            "realized_slippage_bps": 0.1,
            "model_slippage_bps": 0.1,
        },
    )

    assert response.status_code == 401


def test_ingest_sample_rejects_mismatched_account() -> None:
    _reset_monitor()
    client, store = _make_client()

    response = client.post(
        "/anomaly/execution/sample",
        headers=_auth_headers(store, "company"),
        json={
            "account_id": "trading-desk",
            "symbol": "AAPL",
            "rejection_rate": 0.0,
            "cancel_rate": 0.0,
            "partial_fill_ratio": 1.0,
            "realized_slippage_bps": 0.1,
            "model_slippage_bps": 0.1,
        },
    )

    assert response.status_code == 403
    assert (
        response.json()["detail"]
        == "Authenticated account is not authorized for the requested account."
    )


def test_ingest_sample_allows_authorized_account() -> None:
    _reset_monitor()
    client, store = _make_client()

    response = client.post(
        "/anomaly/execution/sample",
        headers=_auth_headers(store, "company"),
        json={
            "account_id": "company",
            "symbol": "AAPL",
            "rejection_rate": 0.0,
            "cancel_rate": 0.0,
            "partial_fill_ratio": 1.0,
            "realized_slippage_bps": 0.1,
            "model_slippage_bps": 0.1,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload == {"triggered_incidents": [], "safe_mode_triggered": False}


def test_status_requires_authentication() -> None:
    _reset_monitor()
    client, _ = _make_client()

    response = client.get("/anomaly/execution/status")

    assert response.status_code == 401


def test_status_rejects_mismatched_scope() -> None:
    _reset_monitor()
    client, store = _make_client()

    response = client.get(
        "/anomaly/execution/status",
        headers=_auth_headers(store, "company"),
        params={"account_id": "trading-desk"},
    )

    assert response.status_code == 403
    assert (
        response.json()["detail"]
        == "Authenticated account is not authorized for the requested account."
    )


def test_status_allows_authorized_scope() -> None:
    _reset_monitor()
    client, store = _make_client()

    # Seed some data for the monitor to return.
    client.post(
        "/anomaly/execution/sample",
        headers=_auth_headers(store, "company"),
        json={
            "account_id": "company",
            "symbol": "AAPL",
            "rejection_rate": 0.0,
            "cancel_rate": 0.0,
            "partial_fill_ratio": 1.0,
            "realized_slippage_bps": 0.1,
            "model_slippage_bps": 0.1,
        },
    )

    response = client.get(
        "/anomaly/execution/status",
        headers=_auth_headers(store, "company"),
        params={"account_id": "company"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "flags" in payload
    assert "last_24h" in payload
    assert isinstance(payload["flags"], list)
    assert isinstance(payload["last_24h"], list)

