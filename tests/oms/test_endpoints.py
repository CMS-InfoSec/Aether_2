from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Tuple
import sys
import types

import httpx
import pytest
import exchange_adapter
from fastapi import Header, HTTPException, Request, status
from fastapi.testclient import TestClient

if "aiohttp" not in sys.modules:
    class _StubSession:
        async def __aenter__(self) -> "_StubSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def close(self) -> None:
            return None

        async def post(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("aiohttp stub invoked")

        async def get(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("aiohttp stub invoked")

    class _ClientTimeout:
        def __init__(self, total: float | None = None) -> None:
            self.total = total

    aiohttp_stub = types.SimpleNamespace(
        ClientSession=lambda *args, **kwargs: _StubSession(),
        ClientTimeout=_ClientTimeout,
        ClientError=Exception,
    )
    sys.modules["aiohttp"] = aiohttp_stub

from services.common import security
from services.common.schemas import GTD_EXPIRE_TIME_REQUIRED

from services.oms import main

from services.oms.main import app, KrakenClientBundle
from services.oms.kraken_ws import OrderAck
from shared.k8s import KrakenSecretStore

ADMIN_ACCOUNTS = ["admin-alpha", "admin-beta", "admin-gamma"]


@pytest.fixture(autouse=True)
def _stub_kraken_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    @asynccontextmanager
    async def _factory(account_id: str):
        async def _credentials() -> Dict[str, Any]:
            return {
                "api_key": f"test-key-{account_id}",
                "api_secret": "secret",
                "metadata": {"rotated_at": datetime.now(timezone.utc).isoformat()},
            }

        trade_time = datetime.now(timezone.utc)

        class _StubWS:
            def __init__(self) -> None:
                self.cancelled: list[Dict[str, Any]] = []

            async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
                return OrderAck(
                    exchange_order_id="SIM-123",
                    status="ok",
                    filled_qty=None,
                    avg_price=None,
                    errors=None,
                )

            async def cancel_order(self, payload: Dict[str, Any]) -> OrderAck:
                self.cancelled.append(dict(payload))
                txid = payload.get("txid") or "SIM-123"
                return OrderAck(
                    exchange_order_id=str(txid),
                    status="canceled",
                    filled_qty=None,
                    avg_price=None,
                    errors=None,
                )

            async def fetch_open_orders_snapshot(self) -> list[Dict[str, Any]]:
                return []

            async def fetch_own_trades_snapshot(self) -> list[Dict[str, Any]]:
                return [
                    {
                        "ordertxid": "SIM-123",
                        "txid": "TRADE-1",
                        "pair": "BTC/USD",
                        "price": 101.5,
                        "volume": 1.0,
                        "fee": 0.1,
                        "time": trade_time.timestamp(),
                    },
                    {
                        "ordertxid": "SIM-456",
                        "txid": "TRADE-2",
                        "pair": "ETH/USD",
                        "price": 201.25,
                        "volume": 2.0,
                        "fee": 0.2,
                        "time": trade_time.timestamp() - 60,
                    },
                ]

            async def close(self) -> None:
                return None

        class _StubREST:
            def __init__(self) -> None:
                self.cancelled: list[Dict[str, Any]] = []

            async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
                return OrderAck(
                    exchange_order_id="SIM-123",
                    status="ok",
                    filled_qty=None,
                    avg_price=None,
                    errors=None,
                )

            async def cancel_order(self, payload: Dict[str, Any]) -> OrderAck:
                self.cancelled.append(dict(payload))
                txid = payload.get("txid") or "SIM-123"
                return OrderAck(
                    exchange_order_id=str(txid),
                    status="canceled",
                    filled_qty=None,
                    avg_price=None,
                    errors=None,
                )

            async def open_orders(self) -> Dict[str, Any]:
                return {"result": {"open": []}}

            async def own_trades(self) -> Dict[str, Any]:
                return {
                    "result": {
                        "trades": [
                            {
                                "ordertxid": "SIM-123",
                                "txid": "TRADE-REST-1",
                        "pair": "BTC/USD",
                        "price": "102.0",
                        "volume": "1.0",
                        "fee": "0.1",
                        "time": trade_time.timestamp(),
                    }
                ]
            }
                }

            async def balance(self) -> Dict[str, Any]:
                return {
                    "result": {
                        "ZUSD": "1234.56",
                        "XXBT": "0.789",
                        "timestamp": trade_time.isoformat(),
                    }
                }

            async def close(self) -> None:
                return None

        ws_client = _StubWS()
        rest_client = _StubREST()
        try:
            yield KrakenClientBundle(
                credential_getter=_credentials,
                ws_client=ws_client,  # type: ignore[arg-type]
                rest_client=rest_client,  # type: ignore[arg-type]
            )
        finally:
            await ws_client.close()
            await rest_client.close()

    monkeypatch.setattr(app.state, "kraken_client_factory", _factory)


@pytest.fixture(name="client")
def client_fixture(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    sample_pairs = {
        "XBTUSD": {
            "wsname": "XBT/USD",
            "base": "XXBT",
            "quote": "ZUSD",
            "tick_size": "0.1",
            "lot_decimals": 4,
        },
        "ETHUSD": {
            "wsname": "ETH/USD",
            "base": "XETH",
            "quote": "ZUSD",
            "tick_size": "0.01",
            "lot_decimals": 3,
        },
        "ADAUSD": {
            "wsname": "ADA/USD",
            "base": "ADA",
            "quote": "ZUSD",
            "tick_size": "0.000001",
            "lot_decimals": 8,
        },
    }

    async def _stub_asset_pairs() -> Dict[str, Any]:
        return sample_pairs

    cache = main.MarketMetadataCache(refresh_interval=0.0)

    monkeypatch.setattr(main, "market_metadata_cache", cache)
    app.state.market_metadata_cache = cache
    monkeypatch.setattr(main, "_fetch_asset_pairs", _stub_asset_pairs)
    asyncio.run(cache.refresh())

    monkeypatch.setattr(security, "ADMIN_ACCOUNTS", set(ADMIN_ACCOUNTS))
    KrakenSecretStore.reset()

    store = KrakenSecretStore()
    for account in ADMIN_ACCOUNTS:
        store.write_credentials(
            account,
            api_key=f"test-key-{account}",
            api_secret=f"test-secret-{account}",
        )

    allowed_accounts = {account.lower() for account in ADMIN_ACCOUNTS}

    def _allow_admin(
        request: Request,
        authorization: str | None = Header(None, alias="Authorization"),
        x_account_id: str | None = Header(None, alias="X-Account-ID"),
    ) -> str:
        header_account = (x_account_id or "").strip()
        if not header_account:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="X-Account-ID header required",
            )
        if header_account.lower() not in allowed_accounts:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Account is not authorized for administrative access.",
            )
        return header_account

    app.dependency_overrides[main.require_admin_account] = _allow_admin
    app.dependency_overrides[security.require_admin_account] = _allow_admin

    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()
        KrakenSecretStore.reset()
        app.dependency_overrides.pop(main.require_admin_account, None)
        app.dependency_overrides.pop(security.require_admin_account, None)


@pytest.fixture
def kraken_adapter(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> Iterator[exchange_adapter.KrakenAdapter]:
    real_async_client = httpx.AsyncClient

    def _async_client_factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        timeout = kwargs.get("timeout")
        transport = httpx.ASGITransport(app=app)
        return real_async_client(
            transport=transport,
            base_url=str(client.base_url),
            timeout=timeout,
        )

    monkeypatch.setattr(exchange_adapter.httpx, "AsyncClient", _async_client_factory)
    adapter = exchange_adapter.KrakenAdapter(primary_url=str(client.base_url))
    yield adapter

@pytest.mark.parametrize("account_id", ADMIN_ACCOUNTS)
def test_place_order_allows_admin_accounts(client: TestClient, account_id: str) -> None:
    payload = {
        "account_id": account_id,
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": account_id})

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "accepted": True,
        "routed_venue": "kraken",
        "fee": payload["fee"],
        "exchange_order_id": "SIM-123",
        "kraken_status": "ok",
        "errors": None,
    }


def test_place_order_rejects_non_spot_instrument(client: TestClient) -> None:
    account_id = ADMIN_ACCOUNTS[0]
    payload = {
        "account_id": account_id,
        "order_id": "ord-spot-guard",
        "instrument": "BTC-PERP",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": account_id})

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    detail = response.json().get("detail", [])
    assert any("Only spot market instruments" in str(item.get("msg", item)) for item in detail)


def test_place_order_rejects_non_admin(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "trade"})

    assert response.status_code == 403


def test_place_order_rejected_ack_sets_accepted_false(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    @asynccontextmanager
    async def _factory(account_id: str):
        async def _credentials() -> Dict[str, Any]:
            return {
                "api_key": f"test-key-{account_id}",
                "api_secret": "secret",
                "metadata": {"rotated_at": datetime.now(timezone.utc).isoformat()},
            }

        class _StubWS:
            async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
                return OrderAck(
                    exchange_order_id="SIM-REJECT",
                    status="rejected",
                    filled_qty=None,
                    avg_price=None,
                    errors=["EOrder:post only"]
                )

            async def fetch_open_orders_snapshot(self) -> list[Dict[str, Any]]:
                return []

            async def fetch_own_trades_snapshot(self) -> list[Dict[str, Any]]:
                return []

            async def close(self) -> None:
                return None

        class _StubREST:
            async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
                return OrderAck(
                    exchange_order_id="SIM-REJECT",
                    status="rejected",
                    filled_qty=None,
                    avg_price=None,
                    errors=["EOrder:post only"]
                )

            async def open_orders(self) -> Dict[str, Any]:
                return {"result": {"open": []}}

            async def own_trades(self) -> Dict[str, Any]:
                return {"result": {"trades": {}}}

            async def close(self) -> None:
                return None

        ws_client = _StubWS()
        rest_client = _StubREST()
        try:
            yield KrakenClientBundle(
                credential_getter=_credentials,
                ws_client=ws_client,  # type: ignore[arg-type]
                rest_client=rest_client,  # type: ignore[arg-type]
            )
        finally:
            await ws_client.close()
            await rest_client.close()

    monkeypatch.setattr(app.state, "kraken_client_factory", _factory)
    monkeypatch.setattr(main, "_ensure_ack_success", lambda ack, transport: None)

    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "accepted": False,
        "routed_venue": "kraken",
        "fee": payload["fee"],
        "exchange_order_id": "SIM-REJECT",
        "kraken_status": "rejected",
        "errors": ["EOrder:post only"],
    }


def test_place_order_mismatched_account(client: TestClient) -> None:
    payload = {
        "account_id": "admin-beta",
        "order_id": "ord-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Account mismatch between header and payload."


def test_place_order_validates_side(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-123",
        "side": "hold",
        "instrument": "BTC-USD",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 422


def test_place_order_snaps_to_exchange_metadata(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    submissions: list[Dict[str, Any]] = []
    call_state = {"get": 0, "refresh": 0}
    original_get = main.market_metadata_cache.get
    original_refresh = main.market_metadata_cache.refresh

    async def _capture_submit(
        ws_client: Any, rest_client: Any, payload: Dict[str, Any]
    ) -> Tuple[OrderAck, str]:
        submissions.append(payload)
        return (
            OrderAck(
                exchange_order_id="SIM-ADA-123",
                status="ok",
                filled_qty=None,
                avg_price=None,
                errors=None,
            ),
            "websocket",
        )

    monkeypatch.setattr(main, "_submit_order", _capture_submit)

    async def _mock_get(self: Any, instrument: str) -> Dict[str, float] | None:
        call_state["get"] += 1
        if call_state["get"] == 1:
            return None
        return await original_get(instrument)

    async def _mock_refresh(self: Any) -> None:
        call_state["refresh"] += 1
        await original_refresh()

    monkeypatch.setattr(
        main.market_metadata_cache,
        "get",
        types.MethodType(_mock_get, main.market_metadata_cache),
    )
    monkeypatch.setattr(
        main.market_metadata_cache,
        "refresh",
        types.MethodType(_mock_refresh, main.market_metadata_cache),
    )

    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-ada",
        "instrument": "ADA-USD",
        "side": "BUY",
        "quantity": 5.432109876,
        "price": 0.123456789,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 200
    assert submissions, "Expected order submission to be captured"

    submitted = submissions[0]
    assert submitted["price"] == pytest.approx(0.123456, rel=0, abs=1e-9)
    assert submitted["volume"] == pytest.approx(5.43210987, rel=0, abs=1e-9)

    assert call_state == {"get": 2, "refresh": 1}


def test_place_order_includes_expire_time_for_gtd(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    submissions: list[Dict[str, Any]] = []

    async def _capture_submit(
        ws_client: Any, rest_client: Any, payload: Dict[str, Any]
    ) -> Tuple[OrderAck, str]:
        submissions.append(payload)
        return (
            OrderAck(
                exchange_order_id="SIM-GTD-123",
                status="ok",
                filled_qty=None,
                avg_price=None,
                errors=None,
            ),
            "websocket",
        )

    monkeypatch.setattr(main, "_submit_order", _capture_submit)

    expire_at = datetime(2025, 5, 20, 15, 30, 0, tzinfo=timezone.utc)

    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-gtd",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        "time_in_force": "GTD",
        "expire_time": expire_at.isoformat(),
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 200
    assert submissions, "Expected Kraken submission to be captured"

    submitted = submissions[0]
    assert submitted["timeInForce"] == "GTD"
    expected_ts = int(expire_at.timestamp())
    assert submitted["expireTime"] == expected_ts
    assert submitted["expiretm"] == expected_ts


def test_place_order_requires_expire_time_for_gtd(client: TestClient) -> None:
    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-gtd-missing",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        "time_in_force": "GTD",
    }

    response = client.post(
        "/oms/place",
        json=payload,
        headers={"X-Account-ID": "admin-alpha"},
    )

    assert response.status_code == 400
    assert response.json() == {"detail": GTD_EXPIRE_TIME_REQUIRED}


def test_place_order_returns_424_when_metadata_missing(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    call_state = {"get": 0, "refresh": 0}

    async def _mock_get(self: Any, instrument: str) -> Dict[str, float] | None:
        call_state["get"] += 1
        return None

    async def _mock_refresh(self: Any) -> None:
        call_state["refresh"] += 1

    monkeypatch.setattr(
        main.market_metadata_cache,
        "get",
        types.MethodType(_mock_get, main.market_metadata_cache),
    )
    monkeypatch.setattr(
        main.market_metadata_cache,
        "refresh",
        types.MethodType(_mock_refresh, main.market_metadata_cache),
    )

    payload = {
        "account_id": "admin-alpha",
        "order_id": "ord-missing",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 424
    assert response.json() == {
        "detail": "Market metadata unavailable; unable to quantize order safely."
    }
    assert call_state == {"get": 2, "refresh": 1}


def test_place_order_latency_with_retry_backoff(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure slow broker responses do not violate the request latency budget."""

    class _SlowTimescale:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def record_ack(self, payload: Dict[str, Any]) -> None:
            return None

        def record_usage(self, notional: float) -> None:
            return None

        def record_fill(self, payload: Dict[str, Any]) -> None:
            return None

        def record_shadow_fill(self, payload: Dict[str, Any]) -> None:
            return None

    attempt_log: Dict[str, list[int]] = {}
    retry_delay = 0.03
    simulated_retries = 3

    class _SlowKafka:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        async def publish(self, *, topic: str, payload: Dict[str, Any]) -> None:
            attempts = 0
            for _ in range(simulated_retries):
                attempts += 1
                await asyncio.sleep(retry_delay)
            attempt_log.setdefault(topic, []).append(attempts)

    monkeypatch.setattr(main, "KafkaNATSAdapter", _SlowKafka)
    monkeypatch.setattr(main, "TimescaleAdapter", _SlowTimescale)
    monkeypatch.setattr(main, "ACK_CACHE_TTL_SECONDS", 0.0)

    payload = {
        "account_id": "admin-alpha",
        "order_id": "order-latency",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 101.5,
        "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    }

    start = time.perf_counter()
    response = client.post("/oms/place", json=payload, headers={"X-Account-ID": "admin-alpha"})
    duration = time.perf_counter() - start

    assert response.status_code == 200
    assert duration < 0.35, f"request exceeded latency budget: {duration:.3f}s"
    assert attempt_log, "expected publish attempts to be recorded"
    for topic, attempts in attempt_log.items():
        for count in attempts:
            assert count == simulated_retries, f"unexpected retry count for {topic}: {count}"


def test_cancel_order_records_events(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    published: list[Dict[str, Any]] = []
    recorded_events: list[Dict[str, Any]] = []

    class _KafkaStub:
        def __init__(self, account_id: str, **_: Any) -> None:
            self.account_id = account_id

        async def publish(self, *, topic: str, payload: Dict[str, Any]) -> None:
            published.append(
                {"account_id": self.account_id, "topic": topic, "payload": payload}
            )

    class _TimescaleStub:
        def __init__(self, account_id: str, **_: Any) -> None:
            self.account_id = account_id

        def record_event(self, event_type: str, payload: Dict[str, Any]) -> None:
            recorded_events.append(
                {
                    "account_id": self.account_id,
                    "event_type": event_type,
                    "payload": payload,
                }
            )

    monkeypatch.setattr(main, "KafkaNATSAdapter", _KafkaStub)
    monkeypatch.setattr(main, "TimescaleAdapter", _TimescaleStub)

    payload = {"account_id": "admin-alpha", "client_id": "ord-cancel", "txid": "SIM-321"}

    response = client.post("/oms/cancel", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == 200
    body = response.json()
    assert body["exchange_order_id"] == "SIM-321"
    assert body["status"].lower() in {"canceled", "cancelled"}

    assert published, "expected Kafka cancel event"
    assert recorded_events, "expected Timescale cancel event"

    kafka_event = published[0]
    timescale_event = recorded_events[0]

    assert kafka_event["topic"] == "oms.cancels"
    assert kafka_event["account_id"] == "admin-alpha"
    assert timescale_event["event_type"] == "oms.cancel"
    assert timescale_event["account_id"] == "admin-alpha"

    assert kafka_event["payload"] == timescale_event["payload"]

    payload_body = kafka_event["payload"]
    assert payload_body["order_id"] == "ord-cancel"
    assert payload_body["txid"] == "SIM-321"
    assert payload_body["requested_txid"] == "SIM-321"
    assert payload_body["status"].lower() in {"canceled", "cancelled"}
    assert payload_body["transport"] in {"websocket", "rest"}

    timestamp = payload_body["timestamp"]
    assert isinstance(timestamp, str)
    datetime.fromisoformat(timestamp)


def test_cancel_order_rejection_records_events(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    published: list[Dict[str, Any]] = []
    recorded_events: list[Dict[str, Any]] = []
    rejections: list[Tuple[str, str, Any]] = []

    class _KafkaStub:
        def __init__(self, account_id: str, **_: Any) -> None:
            self.account_id = account_id

        async def publish(self, *, topic: str, payload: Dict[str, Any]) -> None:
            published.append(
                {"account_id": self.account_id, "topic": topic, "payload": payload}
            )

    class _TimescaleStub:
        def __init__(self, account_id: str, **_: Any) -> None:
            self.account_id = account_id

        def record_event(self, event_type: str, payload: Dict[str, Any]) -> None:
            recorded_events.append(
                {
                    "account_id": self.account_id,
                    "event_type": event_type,
                    "payload": payload,
                }
            )

    async def _mock_cancel_order(*_: Any, **__: Any) -> Tuple[OrderAck, str]:
        return (
            OrderAck(
                exchange_order_id=None,
                status="error",
                filled_qty=None,
                avg_price=None,
                errors=["Rejected"],
            ),
            "websocket",
        )

    def _record_rejection(account_id: str, symbol: str, *, service: Any = None) -> None:
        rejections.append((account_id, symbol, service))

    monkeypatch.setattr(main, "KafkaNATSAdapter", _KafkaStub)
    monkeypatch.setattr(main, "TimescaleAdapter", _TimescaleStub)
    monkeypatch.setattr(main, "_cancel_order", _mock_cancel_order)
    monkeypatch.setattr(main, "increment_trade_rejection", _record_rejection)

    payload = {"account_id": "admin-alpha", "client_id": "ord-cancel", "txid": "SIM-999"}

    response = client.post("/oms/cancel", json=payload, headers={"X-Account-ID": "admin-alpha"})

    assert response.status_code == status.HTTP_502_BAD_GATEWAY
    assert published, "expected Kafka cancel event on rejection"
    assert recorded_events, "expected Timescale cancel event on rejection"

    kafka_event = published[0]
    timescale_event = recorded_events[0]

    assert kafka_event["topic"] == "oms.cancels"
    assert kafka_event["account_id"] == "admin-alpha"
    assert timescale_event["event_type"] == "oms.cancel"
    assert timescale_event["account_id"] == "admin-alpha"
    assert kafka_event["payload"] == timescale_event["payload"]

    payload_body = kafka_event["payload"]
    assert payload_body["order_id"] == "ord-cancel"
    assert payload_body["txid"] == "SIM-999"
    assert payload_body["status"] == "error"
    assert payload_body["errors"] == ["Rejected"]
    assert payload_body["transport"] == "websocket"
    datetime.fromisoformat(payload_body["timestamp"])

    assert rejections == [("admin-alpha", "unknown", None)]

