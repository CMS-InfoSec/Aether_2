"""Unit tests for the universe service."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Dict

import pytest
from fastapi.testclient import TestClient

from services.universe import universe_service


class FakeSession:
    def __init__(self) -> None:
        self.added: list[object] = []
        self.committed = False
        self.flushed = False

    def add(self, record) -> None:
        self.added.append(record)

    def commit(self) -> None:
        self.committed = True

    def flush(self) -> None:
        self.flushed = True


@pytest.fixture
def universe_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    session = FakeSession()

    def override_session():
        try:
            yield session
        finally:
            session.committed = True

    caps = {
        "BTC/USD": SimpleNamespace(value=1_500_000_000.0, entity_id="BTC/USD"),
        "ETH/USD": SimpleNamespace(value=2_000_000_000.0, entity_id="ETH/USD"),
    }
    vols = {
        "BTC/USD": SimpleNamespace(value=0.5, entity_id="BTC/USD"),
        "ETH/USD": SimpleNamespace(value=0.3, entity_id="ETH/USD"),
    }
    volumes = {"BTC-USD": 150_000_000.0, "ETH-USD": 50_000_000.0}
    overrides = {"ETH-USD": SimpleNamespace(approved=False)}

    def fake_feature_map(_session, feature_names):
        if "volatility" in feature_names[0]:
            return vols
        return caps

    monkeypatch.setattr(universe_service, "_latest_feature_map", fake_feature_map)
    monkeypatch.setattr(universe_service, "_kraken_volume_24h", lambda session: volumes)
    monkeypatch.setattr(universe_service, "_latest_manual_overrides", lambda session, migrate=False: overrides)
    universe_service.app.dependency_overrides[universe_service.get_session] = override_session
    return TestClient(universe_service.app)


def teardown_module(module) -> None:  # type: ignore[override]
    universe_service.app.dependency_overrides.clear()


def test_universe_approved_filters_thresholds(universe_client: TestClient) -> None:
    response = universe_client.get("/universe/approved")
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbols"] == ["BTC-USD"]


def test_override_symbol_validates_reason(universe_client: TestClient) -> None:
    response = universe_client.post(
        "/universe/override",
        json={"symbol": " BTC/USD ", "enabled": True, "reason": "  ", "actor": "ops"},
    )
    assert response.status_code == 400


def test_override_symbol_creates_entry(universe_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: Dict[str, object] = {}

    def fake_latest_overrides(session, migrate=False):
        recorded["session"] = session
        return {}

    monkeypatch.setattr(universe_service, "_latest_manual_overrides", fake_latest_overrides)
    response = universe_client.post(
        "/universe/override",
        json={"symbol": "ETH/USD", "enabled": True, "reason": "Listing", "actor": "ops"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["symbol"] == "ETH-USD"
    assert recorded
