from __future__ import annotations

from fastapi.testclient import TestClient

from app import create_app
from services.models import model_zoo


def setup_function() -> None:
    model_zoo._ZOO = None  # type: ignore[attr-defined]


def _client() -> TestClient:
    return TestClient(create_app())


def test_list_models_returns_inventory() -> None:
    client = _client()
    response = client.get("/models/list", headers={"X-Account-ID": "company"})
    assert response.status_code == 200

    payload = response.json()
    assert "models" in payload
    inventory = payload["models"]
    assert isinstance(inventory, list)
    assert inventory, "Expected seeded model inventory"

    btc_entry = next((entry for entry in inventory if entry["symbol"] == "BTC-USD"), None)
    assert btc_entry is not None, "BTC-USD inventory missing"
    assert btc_entry["active_model"]["stage"] == "Production"
    versions = btc_entry["versions"]
    assert len(versions) >= 2
    assert {version["stage"] for version in versions} >= {"Production", "Archived"}


def test_switch_model_updates_active_state_and_log() -> None:
    client = _client()

    response = client.post(
        "/models/switch",
        json={
            "symbol": "ETH-USD",
            "model_id": "policy-intent::eth-usd::policy:1",
            "stage": "production",
        },
        headers={"X-Account-ID": "director-1"},
    )
    assert response.status_code == 200
    payload = response.json()
    active = payload["active_model"]
    assert active["symbol"] == "ETH-USD"
    assert active["stage"] == "Production"
    assert active["actor"] == "director-1"

    active_response = client.get("/models/active", headers={"X-Account-ID": "company"})
    assert active_response.status_code == 200
    active_models = active_response.json()["active"]
    eth_entry = next((entry for entry in active_models if entry["symbol"] == "ETH-USD"), None)
    assert eth_entry is not None
    assert eth_entry["stage"] == "Production"

    zoo = model_zoo.get_model_zoo()
    log_entries = zoo.list_switch_log(symbol="ETH-USD")
    assert log_entries
    assert log_entries[0].actor == "director-1"


def test_switch_model_returns_404_for_missing_symbol() -> None:
    client = _client()
    response = client.post(
        "/models/switch",
        json={"symbol": "DOGE-USD", "model_id": "does-not-exist:1", "stage": "prod"},
        headers={"X-Account-ID": "company"},
    )
    assert response.status_code == 404
    detail = response.json()["detail"]
    assert "No models registered" in detail
