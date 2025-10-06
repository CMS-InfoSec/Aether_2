from __future__ import annotations

import pytest

import exchange_adapter


class _DummyResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.status_code = 200

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

