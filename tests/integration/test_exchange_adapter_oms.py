from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
import sys
import types
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

import exchange_adapter
from auth.service import InMemorySessionStore
from services.common import security

if "aiohttp" not in sys.modules:
    class _StubSession:
        async def __aenter__(self) -> "_StubSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def close(self) -> None:
            return None

        async def post(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise RuntimeError("aiohttp stub invoked")

        async def get(self, *args, **kwargs):  # type: ignore[no-untyped-def]
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


@asynccontextmanager
async def _noop_clients(oms_main):
    async def _credentials() -> dict[str, Any]:
        return {
            "api_key": "demo",
            "api_secret": "demo-secret",
            "metadata": {"rotated_at": datetime.now(timezone.utc).isoformat()},
        }

    class _Stub:
        async def close(self) -> None:
            return None

    stub = _Stub()
    yield oms_main.KrakenClientBundle(
        credential_getter=_credentials,
        ws_client=stub,
        rest_client=stub,
    )


class _AdapterResponse:
    def __init__(self, response) -> None:  # type: ignore[no-untyped-def]
        self._response = response
        self.status_code = response.status_code

    def raise_for_status(self) -> None:
        self._response.raise_for_status()

    def json(self) -> Any:
        return self._response.json()


def _extract_path(url: str) -> str:
    parsed = urlsplit(url)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    return path


@pytest.mark.asyncio
async def test_kraken_adapter_round_trip_through_oms(monkeypatch: pytest.MonkeyPatch) -> None:
    oms_main = importlib.reload(importlib.import_module("services.oms.main"))

    class _NoopKafka:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def publish(self, *args, **kwargs) -> None:
            return None

        @classmethod
        def flush_events(cls) -> dict[str, int]:
            return {}

        @classmethod
        def reset(cls, account_id: str | None = None) -> None:
            return None

    class _NoopTimescale:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def record_ack(self, *args, **kwargs) -> None:
            return None

        def record_usage(self, *args, **kwargs) -> None:
            return None

        def record_fill(self, *args, **kwargs) -> None:
            return None

        def record_shadow_fill(self, *args, **kwargs) -> None:
            return None

        @classmethod
        def flush_event_buffers(cls) -> dict[str, int]:
            return {}

    oms_main.MARKET_METADATA = {"BTC-USD": {"tick": 0.01, "lot": 0.0001}}

    monkeypatch.setattr(oms_main, "KafkaNATSAdapter", _NoopKafka)
    monkeypatch.setattr(oms_main, "TimescaleAdapter", _NoopTimescale)
    monkeypatch.setattr(oms_main, "_acquire_kraken_clients", lambda account_id: _noop_clients(oms_main))
    monkeypatch.setattr(oms_main, "_ensure_credentials_valid", lambda credentials: None)

    async def _fake_submit_order(*args, **kwargs):  # type: ignore[no-untyped-def]
        ack = oms_main.OrderAck(
            exchange_order_id="OID-1",
            status="ok",
            filled_qty=None,
            avg_price=None,
            errors=None,
        )
        return ack, "websocket"

    monkeypatch.setattr(oms_main, "_submit_order", _fake_submit_order)
    async def _fake_fetch_open_orders(*args, **kwargs):  # type: ignore[no-untyped-def]
        return []

    async def _fake_fetch_own_trades(*args, **kwargs):  # type: ignore[no-untyped-def]
        return []

    monkeypatch.setattr(oms_main, "_fetch_open_orders", _fake_fetch_open_orders)
    monkeypatch.setattr(oms_main, "_fetch_own_trades", _fake_fetch_own_trades)
    monkeypatch.setattr(oms_main.shadow_oms, "record_real_fill", lambda *a, **k: None)
    monkeypatch.setattr(oms_main.shadow_oms, "generate_shadow_fills", lambda *a, **k: [])

    security.reload_admin_accounts(["company"])
    store = InMemorySessionStore()
    session = store.create("company")
    oms_main.app.state.session_store = store

    import shared.graceful_shutdown as graceful_shutdown

    monkeypatch.setattr(graceful_shutdown, "install_sigterm_handler", lambda manager: None)
    async def _noop_async(*args, **kwargs):  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr(oms_main.market_metadata_cache, "start", _noop_async)
    monkeypatch.setattr(oms_main.market_metadata_cache, "stop", _noop_async)

    with TestClient(oms_main.app) as client:

        class _AdapterClient:
            def __init__(self, *args, **kwargs) -> None:
                self._client = client

            async def __aenter__(self) -> "_AdapterClient":
                return self

            async def __aexit__(self, exc_type, exc, tb) -> None:
                return None

            async def post(self, url: str, json=None, headers=None):  # type: ignore[override]
                response = self._client.post(_extract_path(url), json=json, headers=headers)
                return _AdapterResponse(response)

            async def get(self, url: str, params=None, headers=None):  # type: ignore[override]
                response = self._client.get(_extract_path(url), params=params, headers=headers)
                return _AdapterResponse(response)

        monkeypatch.setattr(exchange_adapter.httpx, "AsyncClient", _AdapterClient)

        class _StubSessionManager:
            def __init__(self, token: str) -> None:
                self.token = token
                self.calls: list[str] = []

            async def token_for_account(self, account_id: str) -> str:
                self.calls.append(account_id)
                return self.token

        manager = _StubSessionManager(session.token)
        adapter = exchange_adapter.KrakenAdapter(
            primary_url=str(client.base_url),
            session_manager=manager,
        )

        payload = {
            "order_id": "CID-1",
            "account_id": "company",
            "instrument": "BTC-USD",
            "side": "BUY",
            "quantity": 1.0,
            "price": 1000.0,
            "fee": {"currency": "USD", "maker": 0.0, "taker": 0.0},
        }

        result = await adapter.place_order("company", payload)

    assert result["accepted"] is True
    assert manager.calls == ["company"]
