from __future__ import annotations

from datetime import datetime, timedelta, timezone

import importlib

import pytest

from fastapi.testclient import TestClient

try:
    from services.risk import correlation_service as correlation
except ImportError:  # pragma: no cover - fallback for namespace import issues
    correlation = importlib.import_module("services.risk.correlation_service")


def test_compute_correlation_matrix_expected_values() -> None:
    returns = {
        "BTC-USD": [0.1, 0.2, 0.15],
        "ETH-USD": [0.1, 0.18, 0.12],
        "SOL-USD": [0.05, 0.07, 0.06],
    }

    matrix = correlation._compute_correlation_matrix(returns)

    assert matrix["BTC-USD"]["BTC-USD"] == pytest.approx(1.0)
    assert matrix["ETH-USD"]["ETH-USD"] == pytest.approx(1.0)
    assert matrix["SOL-USD"]["SOL-USD"] == pytest.approx(1.0)
    assert matrix["BTC-USD"]["ETH-USD"] == pytest.approx(0.9607689228)
    assert matrix["ETH-USD"]["BTC-USD"] == pytest.approx(0.9607689228)


def test_correlation_endpoint_uses_in_memory_persistence(monkeypatch: pytest.MonkeyPatch) -> None:
    original_persistence = correlation.correlation_persistence
    correlation.price_store.clear()

    store = correlation._InMemoryPersistence()
    correlation.correlation_persistence = store

    monkeypatch.setenv("RISK_CORRELATION_SYMBOLS", "BTC-USD,ETH-USD")
    monkeypatch.setattr(correlation, "_prime_price_cache", lambda *args, **kwargs: None)

    base = datetime.now(timezone.utc) - timedelta(minutes=3)
    btc_prices = [100.0, 110.0, 105.0, 120.0]
    eth_prices = [200.0, 220.0, 210.0, 240.0]
    timestamps = [base + timedelta(minutes=index) for index in range(len(btc_prices))]

    correlation.price_store.replace(
        "BTC-USD",
        [
            correlation.PricePoint(symbol="BTC-USD", timestamp=ts, price=price)
            for ts, price in zip(timestamps, btc_prices)
        ],
    )
    correlation.price_store.replace(
        "ETH-USD",
        [
            correlation.PricePoint(symbol="ETH-USD", timestamp=ts, price=price)
            for ts, price in zip(timestamps, eth_prices)
        ],
    )

    def _session_override():
        with store.session() as session:
            yield session

    correlation.app.dependency_overrides[correlation.get_session] = _session_override

    try:
        with TestClient(correlation.app) as client:
            response = client.get("/correlations/matrix", params={"window": 3})
        assert response.status_code == 200
        payload = response.json()
        assert payload["symbols"] == ["BTC-USD", "ETH-USD"]
        assert payload["matrix"]["BTC-USD"]["ETH-USD"] == pytest.approx(
            payload["matrix"]["ETH-USD"]["BTC-USD"]
        )
        assert store.snapshot()  # persistence captured the correlation values
    finally:
        correlation.app.dependency_overrides.pop(correlation.get_session, None)
        correlation.correlation_persistence = original_persistence
        correlation.price_store.clear()
