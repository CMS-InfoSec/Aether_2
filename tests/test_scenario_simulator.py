from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone
from typing import Any, Dict

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for Scenario Simulator tests")
pytest.importorskip("pandas", reason="pandas is required for Scenario Simulator tests")

import pandas as pd
from fastapi.testclient import TestClient


class _DummyCursor:
    def __enter__(self) -> "_DummyCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - no-op for tests
        return None

    def fetchall(self) -> list[Dict[str, Any]]:  # pragma: no cover - startup doesn't rely on results
        return []


class _DummyConnection:
    def __enter__(self) -> "_DummyConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - no-op for tests
        return None

    def commit(self) -> None:  # pragma: no cover - no-op for tests
        return None

    def cursor(self) -> _DummyCursor:
        return _DummyCursor()


@pytest.fixture()
def simulator_client(monkeypatch: pytest.MonkeyPatch):
    sys.modules.pop("scenario_simulator", None)
    module = importlib.import_module("scenario_simulator")

    monkeypatch.setattr(module, "_get_conn", lambda: _DummyConnection())

    client = TestClient(module.app)
    try:
        yield client, module
    finally:
        client.app.dependency_overrides.clear()
        sys.modules.pop("scenario_simulator", None)


def test_run_scenario_requires_admin_account(simulator_client) -> None:
    client, _ = simulator_client

    response = client.post(
        "/scenario/run",
        json={"shock_pct": 0.01, "vol_multiplier": 1.2},
    )

    assert response.status_code in {401, 403}


def test_run_scenario_uses_verified_actor(monkeypatch: pytest.MonkeyPatch, simulator_client) -> None:
    client, module = simulator_client

    positions = pd.DataFrame(
        [
            {"market": "BTC-USD", "quantity": 2.0, "entry_price": 25000.0},
            {"market": "ETH-USD", "quantity": 5.0, "entry_price": 1500.0},
        ]
    )
    price_history = pd.DataFrame(
        [
            {"market": "BTC-USD", "bucket_start": datetime(2024, 1, 1, tzinfo=timezone.utc), "close": 100.0},
            {"market": "BTC-USD", "bucket_start": datetime(2024, 1, 2, tzinfo=timezone.utc), "close": 105.0},
            {"market": "ETH-USD", "bucket_start": datetime(2024, 1, 1, tzinfo=timezone.utc), "close": 50.0},
            {"market": "ETH-USD", "bucket_start": datetime(2024, 1, 2, tzinfo=timezone.utc), "close": 52.0},
        ]
    )

    monkeypatch.setattr(module, "_fetch_positions", lambda *_: positions)
    monkeypatch.setattr(module, "_fetch_price_history", lambda *_, **__: price_history)

    stored_records: list[Dict[str, Any]] = []

    def _capture_store(conn: Any, results: Dict[str, float], payload: Any, actor: str) -> None:
        stored_records.append({"actor": actor, "results": results, "payload": payload})

    monkeypatch.setattr(module, "_store_run", _capture_store)

    client.app.dependency_overrides[module.require_admin_account] = lambda: "ops-admin"

    try:
        response = client.post(
            "/scenario/run",
            json={"shock_pct": 0.02, "vol_multiplier": 1.5},
        )
    finally:
        client.app.dependency_overrides.pop(module.require_admin_account, None)

    assert response.status_code == 200
    body = response.json()
    assert "projected_pnl" in body
    assert stored_records and stored_records[0]["actor"] == "ops-admin"
