from __future__ import annotations

import httpx
import pytest

import exchange_adapter


class _DummyResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.status_code = 200
        self.text = ""

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return dict(self._payload)


@pytest.mark.asyncio
async def test_place_order_includes_authorization_header(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[tuple[str, str, dict[str, object] | None, dict[str, str]]] = []

    class _DummyAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            return None

        async def __aenter__(self) -> "_DummyAsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def post(self, url: str, json=None, headers=None):  # type: ignore[override]
            recorded.append(("POST", url, json, dict(headers or {})))
            return _DummyResponse({"status": "ok"})

        async def get(self, url: str, params=None, headers=None):  # type: ignore[override]
            recorded.append(("GET", url, params, dict(headers or {})))
            return _DummyResponse({})

    monkeypatch.setattr(exchange_adapter.httpx, "AsyncClient", _DummyAsyncClient)

    class _StubSessionManager:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def token_for_account(self, account_id: str) -> str:
            self.calls.append(account_id)
            return "session-token"

    manager = _StubSessionManager()
    adapter = exchange_adapter.KrakenAdapter(primary_url="http://oms", session_manager=manager)

    result = await adapter.place_order("alpha", {"order": "payload", "symbol": "eth/usd"})

    assert result == {"status": "ok"}
    assert manager.calls == ["alpha"]
    assert recorded, "Expected the adapter to perform a POST request"
    method, url, payload, headers = recorded[0]
    assert method == "POST"
    assert url.endswith("/oms/place")
    assert headers["Authorization"] == "Bearer session-token"
    assert headers["X-Account-ID"] == "alpha"
    assert "X-Request-ID" in headers
    assert payload["symbol"] == "ETH-USD"


@pytest.mark.asyncio
async def test_place_order_rejects_non_spot_symbol() -> None:
    adapter = exchange_adapter.KrakenAdapter(primary_url="http://oms")

    with pytest.raises(ValueError, match="spot market"):
        await adapter.place_order("alpha", {"symbol": "BTC-PERP"})


@pytest.mark.asyncio
async def test_get_balance_uses_authorization_header(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[tuple[str, str, dict[str, object] | None, dict[str, str]]] = []

    class _DummyAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            return None

        async def __aenter__(self) -> "_DummyAsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def get(self, url: str, params=None, headers=None):  # type: ignore[override]
            recorded.append(("GET", url, params, dict(headers or {})))
            payload = {"result": {"balances": {"USD": "10"}}}
            return _DummyResponse(payload)

        async def post(self, url: str, json=None, headers=None):  # type: ignore[override]
            recorded.append(("POST", url, json, dict(headers or {})))
            return _DummyResponse({})

    monkeypatch.setattr(exchange_adapter.httpx, "AsyncClient", _DummyAsyncClient)

    class _StubSessionManager:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def token_for_account(self, account_id: str) -> str:
            self.calls.append(account_id)
            return "balance-token"

    manager = _StubSessionManager()
    adapter = exchange_adapter.KrakenAdapter(primary_url="http://oms", session_manager=manager)

    result = await adapter.get_balance("beta")

    assert result["account_id"] == "beta"
    assert manager.calls == ["beta"]
    assert recorded, "Expected the adapter to issue a GET request"
    method, url, params, headers = recorded[0]
    assert method == "GET"
    assert url.endswith("/oms/accounts/beta/balances")
    assert headers["Authorization"] == "Bearer balance-token"
    assert headers["X-Account-ID"] == "beta"


@pytest.mark.asyncio
async def test_place_order_converts_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    class _RateLimitResponse:
        status_code = 429
        text = "rate limited"

        def json(self) -> dict[str, object]:
            return {"error": "rate limited"}

    class _RateLimitClient:
        def __init__(self, *args, **kwargs) -> None:
            return None

        async def __aenter__(self) -> "_RateLimitClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def post(self, url: str, **kwargs):  # type: ignore[override]
            return _RateLimitResponse()

        async def get(self, url: str, **kwargs):  # pragma: no cover - unused here
            return _RateLimitResponse()

    monkeypatch.setattr(exchange_adapter.httpx, "AsyncClient", _RateLimitClient)

    class _StubSessionManager:
        async def token_for_account(self, account_id: str) -> str:
            return "stub-token"

    adapter = exchange_adapter.KrakenAdapter(
        primary_url="http://oms", session_manager=_StubSessionManager()
    )

    with pytest.raises(exchange_adapter.ExchangeRateLimitError) as excinfo:
        await adapter.place_order("alpha", {"symbol": "eth/usd"})

    assert "rate limited" in str(excinfo.value)


@pytest.mark.asyncio
async def test_place_order_converts_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    class _TimeoutClient:
        def __init__(self, *args, **kwargs) -> None:
            return None

        async def __aenter__(self) -> "_TimeoutClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def post(self, url: str, **kwargs):  # type: ignore[override]
            raise httpx.TimeoutException("timeout")

        async def get(self, url: str, **kwargs):  # pragma: no cover - unused here
            raise httpx.TimeoutException("timeout")

    monkeypatch.setattr(exchange_adapter.httpx, "AsyncClient", _TimeoutClient)

    class _StubSessionManager:
        async def token_for_account(self, account_id: str) -> str:
            return "stub-token"

    adapter = exchange_adapter.KrakenAdapter(
        primary_url="http://oms", session_manager=_StubSessionManager()
    )

    with pytest.raises(exchange_adapter.ExchangeTimeoutError):
        await adapter.place_order("alpha", {"symbol": "eth/usd"})


@pytest.mark.asyncio
async def test_place_order_converts_validation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _ValidationResponse:
        status_code = 400
        text = ""

        def json(self) -> dict[str, object]:
            return {"errors": ["invalid precision"]}

    class _ValidationClient:
        def __init__(self, *args, **kwargs) -> None:
            return None

        async def __aenter__(self) -> "_ValidationClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def post(self, url: str, **kwargs):  # type: ignore[override]
            return _ValidationResponse()

        async def get(self, url: str, **kwargs):  # pragma: no cover - unused here
            return _ValidationResponse()

    monkeypatch.setattr(exchange_adapter.httpx, "AsyncClient", _ValidationClient)

    class _StubSessionManager:
        async def token_for_account(self, account_id: str) -> str:
            return "stub-token"

    adapter = exchange_adapter.KrakenAdapter(
        primary_url="http://oms", session_manager=_StubSessionManager()
    )

    with pytest.raises(exchange_adapter.ExchangeValidationError) as excinfo:
        await adapter.place_order("alpha", {"symbol": "eth/usd"})

    assert "invalid precision" in str(excinfo.value)


@pytest.mark.asyncio
async def test_binance_adapter_submit_order_noop_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ENABLE_MULTI_EXCHANGE_ROUTING", raising=False)
    adapter = exchange_adapter.BinanceAdapter()

    ack = await adapter.submit_order("acct-1", {"symbol": "btc/usd"})

    assert ack["status"] == "noop"
    assert ack["reason"] == "multi_exchange_routing_disabled"
    assert ack["symbol"] == "BTC-USD"
    assert not adapter.supports("place_order")


@pytest.mark.asyncio
async def test_binance_adapter_submit_order_stub_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENABLE_MULTI_EXCHANGE_ROUTING", "true")
    adapter = exchange_adapter.BinanceAdapter()

    ack = await adapter.submit_order("acct-2", {"symbol": "eth-usd", "client_id": "cid-123"})

    assert ack["status"] == "accepted"
    assert ack["note"] == "stubbed_execution"
    assert ack["client_order_id"] == "cid-123"
    assert adapter.supports("place_order")


@pytest.mark.asyncio
async def test_coinbase_adapter_fetch_orderbook_normalizes_symbol() -> None:
    adapter = exchange_adapter.CoinbaseAdapter()

    book = await adapter.fetch_orderbook("solusd", depth=5)

    assert book["symbol"] == "SOL-USD"
    assert book["depth"] == 5
    assert book["bids"] == []
    assert book["asks"] == []


@pytest.mark.asyncio
async def test_coinbase_adapter_cancel_noop_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ENABLE_MULTI_EXCHANGE_ROUTING", raising=False)
    adapter = exchange_adapter.CoinbaseAdapter()

    response = await adapter.cancel_order("acct-3", "client-42", exchange_order_id="order-99")

    assert response["status"] == "noop"
    assert response["reason"] == "multi_exchange_routing_disabled"
    assert response["exchange_order_id"] == "order-99"

