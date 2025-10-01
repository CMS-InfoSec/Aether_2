from __future__ import annotations

from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.oms import main
from services.oms.kraken_client import KrakenWSClient, KrakenWebsocketTimeout
from shared.k8s import KrakenSecretStore


@pytest.fixture(autouse=True)
def reset_state() -> None:
    KafkaNATSAdapter.reset()
    TimescaleAdapter.reset()
    KrakenSecretStore.reset()
    main.CircuitBreaker.reset()
    yield
    KafkaNATSAdapter.reset()
    TimescaleAdapter.reset()
    KrakenSecretStore.reset()
    main.CircuitBreaker.reset()


def _seed_credentials(account_id: str) -> None:
    store = KrakenSecretStore()
    store.write_credentials(account_id, api_key="test-key", api_secret="test-secret")


@pytest.fixture(name="client")
def client_fixture() -> TestClient:
    return TestClient(main.app)


class _RecordingClient:
    def __init__(self, account_id: str, **_: Any) -> None:
        self.account_id = account_id
        self.requests: List[Dict[str, Any]] = []

    def add_order(self, payload: Dict[str, Any], timeout: float | None = None) -> Dict[str, Any]:
        self.requests.append(payload)
        return {"status": "ok", "txid": "ABC123", "transport": "websocket"}

    def open_orders(self) -> Dict[str, Any]:
        return {"open": [{"txid": "ABC123", "volume": "0.2"}]}

    def own_trades(self, txid: str | None = None) -> Dict[str, Any]:
        return {"trades": []}

    def close(self) -> None:  # pragma: no cover - interface shim
        return None


def test_precision_snapping(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    records: List[_RecordingClient] = []

    def factory(**kwargs: Any) -> _RecordingClient:
        inst = _RecordingClient(**kwargs)
        records.append(inst)
        return inst

    monkeypatch.setattr(main, "KrakenWSClient", factory)

    payload = {
        "account_id": "admin-eu",
        "order_id": "snap-1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 0.123456,
        "price": 20100.12345,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        "post_only": True,
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-eu"})
    assert response.status_code == 200

    assert records, "Kraken client was not invoked"
    snapped_payload = records[0].requests[0]
    assert snapped_payload["price"] == pytest.approx(20100.1)
    assert snapped_payload["volume"] == pytest.approx(0.1235)
    assert "post" in snapped_payload["oflags"]

    events = TimescaleAdapter(account_id="admin-eu").events()
    assert events["acks"][0]["price"] == pytest.approx(20100.1)
    assert events["acks"][0]["open_orders"]

    history = KafkaNATSAdapter(account_id="admin-eu").history()
    assert any(record["topic"] == "oms.acks" for record in history)


def test_circuit_breaker_halts(client: TestClient) -> None:
    main.CircuitBreaker.halt("BTC-USD", reason="Limit up")

    payload = {
        "account_id": "admin-eu",
        "order_id": "halt-1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 0.1,
        "price": 25000,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-eu"})
    assert response.status_code == 423
    assert response.json()["detail"] == "Limit up"


def test_rest_fallback_when_ws_stalls(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    class TimeoutSession:
        def request(self, channel: str, payload: Dict[str, Any], timeout: float | None = None) -> Dict[str, Any]:
            if channel == "add_order":
                raise KrakenWebsocketTimeout("stalled")
            if channel == "openOrders":
                return {"open": []}
            if channel == "ownTrades":
                return {"trades": []}
            return {"status": "ok"}

        def close(self) -> None:  # pragma: no cover - interface shim
            return None

    def factory(account_id: str, **_: Any) -> KrakenWSClient:
        _seed_credentials(account_id)
        return KrakenWSClient(
            account_id=account_id,
            session_factory=lambda _creds: TimeoutSession(),
            rest_fallback=lambda payload: {
                "status": "ok",
                "txid": "REST-123",
                "transport": "rest",
                "echo": payload,
            },
        )

    monkeypatch.setattr(main, "KrakenWSClient", factory)

    payload = {
        "account_id": "admin-eu",
        "order_id": "rest-1",
        "instrument": "ETH-USD",
        "side": "SELL",
        "quantity": 1.2345,
        "price": 1500.123,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        "time_in_force": "GTC",
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-eu"})
    assert response.status_code == 200

    events = TimescaleAdapter(account_id="admin-eu").events()
    assert events["acks"][0]["transport"] == "rest"
    history = KafkaNATSAdapter(account_id="admin-eu").history()
    assert any(entry["payload"].get("transport") == "rest" for entry in history if entry["topic"] == "oms.acks")
