from __future__ import annotations

from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from datetime import datetime, timezone

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


@pytest.fixture(autouse=True)
def disable_shadow_fills(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main.shadow_oms, "generate_shadow_fills", lambda *_, **__: [])
    monkeypatch.setattr(main.shadow_oms, "record_real_fill", lambda *_, **__: None)


def _seed_credentials(account_id: str) -> None:
    store = KrakenSecretStore()
    store.write_credentials(account_id, api_key="test-key", api_secret="test-secret")
    TimescaleAdapter(account_id=account_id).record_credential_rotation(
        secret_name=store.secret_name(account_id),
        rotated_at=datetime.now(timezone.utc),
    )


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


def test_precision_snapping_buy_never_exceeds(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    records: List[_RecordingClient] = []

    def factory(**kwargs: Any) -> _RecordingClient:
        inst = _RecordingClient(**kwargs)
        records.append(inst)
        return inst

    monkeypatch.setattr(main, "KrakenWSClient", factory)

    _seed_credentials("company")

    payload = {
        "account_id": "company",
        "order_id": "snap-1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 0.123456,
        "price": 20100.12345,
        "take_profit": 20150.159,
        "stop_loss": 20090.987,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        "post_only": True,
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "company"})
    assert response.status_code == 200
    assert response.json()["accepted"] is True

    assert records, "Kraken client was not invoked"
    snapped_payload = records[0].requests[0]
    assert snapped_payload["price"] == pytest.approx(20100.1)
    assert snapped_payload["volume"] == pytest.approx(0.1234)
    assert snapped_payload["takeProfit"] == pytest.approx(20150.1)
    assert snapped_payload["stopLoss"] == pytest.approx(20090.9)

    assert snapped_payload["price"] <= payload["price"]
    assert snapped_payload["volume"] <= payload["quantity"]
    assert snapped_payload["takeProfit"] <= payload["take_profit"]
    assert snapped_payload["stopLoss"] <= payload["stop_loss"]
    assert "post" in snapped_payload["oflags"]

    events = TimescaleAdapter(account_id="company").events()
    assert events["acks"][0]["price"] == pytest.approx(20100.1)
    assert events["acks"][0]["open_orders"]

    history = KafkaNATSAdapter(account_id="company").history()
    assert any(record["topic"] == "oms.acks" for record in history)


def test_precision_snapping_sell_never_falls_short(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    records: List[_RecordingClient] = []

    def factory(**kwargs: Any) -> _RecordingClient:
        inst = _RecordingClient(**kwargs)
        records.append(inst)
        return inst

    monkeypatch.setattr(main, "KrakenWSClient", factory)

    _seed_credentials("company")

    payload = {
        "account_id": "company",
        "order_id": "snap-sell-1",
        "instrument": "ETH-USD",
        "side": "SELL",
        "quantity": 1.2345,
        "price": 1500.123,
        "take_profit": 1495.777,
        "stop_loss": 1505.345,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "company"})
    assert response.status_code == 200
    assert response.json()["accepted"] is True

    assert records, "Kraken client was not invoked"
    snapped_payload = records[0].requests[0]

    assert snapped_payload["price"] == pytest.approx(1500.13)
    assert snapped_payload["volume"] == pytest.approx(1.234)
    assert snapped_payload["takeProfit"] == pytest.approx(1495.78)
    assert snapped_payload["stopLoss"] == pytest.approx(1505.35)

    assert snapped_payload["price"] >= payload["price"]
    assert snapped_payload["volume"] <= payload["quantity"]
    assert snapped_payload["takeProfit"] >= payload["take_profit"]
    assert snapped_payload["stopLoss"] >= payload["stop_loss"]


def test_gtd_order_includes_expiry(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    records: List[_RecordingClient] = []

    def factory(**kwargs: Any) -> _RecordingClient:
        inst = _RecordingClient(**kwargs)
        records.append(inst)
        return inst

    monkeypatch.setattr(main, "KrakenWSClient", factory)

    _seed_credentials("company")

    expire_at = datetime(2024, 5, 17, 12, 34, 56, tzinfo=timezone.utc)

    payload = {
        "account_id": "company",
        "order_id": "gtd-1",
        "instrument": "ETH-USD",
        "side": "SELL",
        "quantity": 0.5,
        "price": 1500.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        "time_in_force": "GTD",
        "expire_time": expire_at.isoformat().replace("+00:00", "Z"),
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "company"})

    assert response.status_code == 200
    assert records, "Kraken client was not invoked"

    submitted = records[0].requests[0]
    assert submitted["timeInForce"] == "GTD"
    assert submitted["expireTime"] == "2024-05-17T12:34:56Z"


def test_gtd_order_requires_expiry(client: TestClient) -> None:
    _seed_credentials("company")

    payload = {
        "account_id": "company",
        "order_id": "gtd-2",
        "instrument": "ETH-USD",
        "side": "SELL",
        "quantity": 0.5,
        "price": 1500.0,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        "time_in_force": "GTD",
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "company"})

    assert response.status_code == 400
    assert response.json()["detail"] == "expire_time is required when time_in_force is GTD"

def test_circuit_breaker_halts(client: TestClient) -> None:
    main.CircuitBreaker.halt("BTC-USD", reason="Limit up")

    payload = {
        "account_id": "company",
        "order_id": "halt-1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 0.1,
        "price": 25000,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "company"})
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
        "account_id": "company",
        "order_id": "rest-1",
        "instrument": "ETH-USD",
        "side": "SELL",
        "quantity": 1.2345,
        "price": 1500.123,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        "time_in_force": "GTC",
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "company"})
    assert response.status_code == 200

    events = TimescaleAdapter(account_id="company").events()
    assert events["acks"][0]["transport"] == "rest"
    history = KafkaNATSAdapter(account_id="company").history()
    assert any(entry["payload"].get("transport") == "rest" for entry in history if entry["topic"] == "oms.acks")


def test_rejected_orders_surface_errors(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    class RejectingClient:
        def __init__(self, **_: Any) -> None:
            pass

        def add_order(self, payload: Dict[str, Any], timeout: float | None = None) -> Dict[str, Any]:
            return {
                "status": "rejected",
                "error": ["EOrder:Insufficient funds"],
                "transport": "websocket",
            }

        def open_orders(self) -> Dict[str, Any]:
            return {"open": []}

        def own_trades(self, txid: str | None = None) -> Dict[str, Any]:
            return {"trades": []}

        def close(self) -> None:  # pragma: no cover - interface shim
            return None

    monkeypatch.setattr(main, "KrakenWSClient", RejectingClient)

    payload = {
        "account_id": "company",
        "order_id": "reject-1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 0.1,
        "price": 20000,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "company"})
    assert response.status_code == 400
    assert "Insufficient funds" in response.json()["detail"]
