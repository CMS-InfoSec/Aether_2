from __future__ import annotations

import sys
import types
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest


_adapters_stub = types.ModuleType("services.common.adapters")


class _InitialKafkaAdapter:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        return None

    def publish(self, *args: Any, **kwargs: Any) -> None:
        return None


class _InitialTimescaleAdapter:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        return None

    def record_ack(self, *args: Any, **kwargs: Any) -> None:
        return None

    def record_usage(self, *args: Any, **kwargs: Any) -> None:
        return None

    def record_fill(self, *args: Any, **kwargs: Any) -> None:
        return None

    def record_shadow_fill(self, *args: Any, **kwargs: Any) -> None:
        return None


class _InitialKrakenSecretManager:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.secret_store = types.SimpleNamespace(core_v1=None, namespace=None)
        self.secret_name = "kraken-secret"

    def get_credentials(self) -> Dict[str, Any]:
        return {
            "api_key": "stub",
            "api_secret": "stub",
            "metadata": {"rotated_at": datetime.now(timezone.utc).isoformat()},
        }


_adapters_stub.KafkaNATSAdapter = _InitialKafkaAdapter
_adapters_stub.TimescaleAdapter = _InitialTimescaleAdapter
_adapters_stub.KrakenSecretManager = _InitialKrakenSecretManager
sys.modules.setdefault("services.common.adapters", _adapters_stub)


_aiohttp_stub = types.ModuleType("aiohttp")


class _StubClientSession:
    async def __aenter__(self) -> "_StubClientSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def close(self) -> None:
        return None

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("aiohttp stub not implemented")

    async def post(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("aiohttp stub not implemented")


class _StubClientTimeout:
    def __init__(self, total: float | None = None) -> None:
        self.total = total


class _StubClientError(Exception):
    pass


_aiohttp_stub.ClientSession = _StubClientSession
_aiohttp_stub.ClientTimeout = _StubClientTimeout
_aiohttp_stub.ClientError = _StubClientError
sys.modules.setdefault("aiohttp", _aiohttp_stub)

from services.common.schemas import FeeBreakdown, OrderPlacementRequest
from services.oms import main
from services.oms.kraken_ws import OrderAck


class _StubKafkaAdapter:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.published: List[Dict[str, Any]] = []

    def publish(self, *, topic: str, payload: Dict[str, Any]) -> None:
        self.published.append({"topic": topic, "payload": dict(payload)})


class _StubTimescaleAdapter:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.acks: List[Dict[str, Any]] = []
        self.usage: List[float] = []
        self.fills: List[Dict[str, Any]] = []
        self.shadow_fills: List[Dict[str, Any]] = []

    def record_ack(self, payload: Dict[str, Any]) -> None:
        self.acks.append(dict(payload))

    def record_usage(self, notional: float) -> None:
        self.usage.append(notional)

    def record_fill(self, payload: Dict[str, Any]) -> None:
        self.fills.append(dict(payload))

    def record_shadow_fill(self, payload: Dict[str, Any]) -> None:
        self.shadow_fills.append(dict(payload))


class _StubShadowOMS:
    def record_real_fill(self, **_: Any) -> None:
        return None

    def generate_shadow_fills(self, **_: Any) -> List[Dict[str, Any]]:
        return []


class _StubMetadataCache:
    def __init__(self, mapping: Dict[str, Dict[str, Any]]) -> None:
        self._mapping = mapping

    async def get(self, instrument: str) -> Dict[str, Any] | None:
        entry = self._mapping.get(instrument)
        return dict(entry) if entry is not None else None

    async def refresh(self) -> None:
        return None


@pytest.mark.asyncio
async def test_place_order_uses_native_pair_names(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_payloads: List[Dict[str, Any]] = []

    async def _stub_submit_order(
        ws_client: Any, rest_client: Any, payload: Dict[str, Any]
    ) -> tuple[OrderAck, str]:
        captured_payloads.append(dict(payload))
        ack = OrderAck(
            exchange_order_id="EX-1",
            status="accepted",
            filled_qty=None,
            avg_price=None,
            errors=None,
        )
        return ack, "websocket"

    async def _stub_fetch_open_orders(*_: Any, **__: Any) -> List[Dict[str, Any]]:
        return []

    async def _stub_fetch_own_trades(*_: Any, **__: Any) -> List[Dict[str, Any]]:
        return []

    metadata = {
        "BTC-USD": {"tick": 0.1, "lot": 0.0001, "native_pair": "XBT/USD"},
        "DOGE-USD": {"tick": 0.0001, "lot": 1.0, "native_pair": "XDG/USD"},
    }

    cache = _StubMetadataCache(metadata)

    @asynccontextmanager
    async def _stub_acquire_clients(account_id: str):
        async def _stub_credential_getter() -> Dict[str, Any]:
            return {
                "api_key": "key",
                "api_secret": "secret",
                "metadata": {"rotated_at": datetime.now(timezone.utc).isoformat()},
            }

        bundle = SimpleNamespace(
            credential_getter=_stub_credential_getter,
            ws_client=object(),
            rest_client=object(),
        )
        yield bundle

    monkeypatch.setattr(main, "KafkaNATSAdapter", _StubKafkaAdapter)
    monkeypatch.setattr(main, "TimescaleAdapter", _StubTimescaleAdapter)
    monkeypatch.setattr(main, "shadow_oms", _StubShadowOMS())
    monkeypatch.setattr(main, "_submit_order", _stub_submit_order)
    monkeypatch.setattr(main, "_fetch_open_orders", _stub_fetch_open_orders)
    monkeypatch.setattr(main, "_fetch_own_trades", _stub_fetch_own_trades)
    monkeypatch.setattr(main, "_acquire_kraken_clients", _stub_acquire_clients)
    monkeypatch.setattr(main, "_ensure_credentials_valid", lambda credentials: None)
    monkeypatch.setattr(main, "market_metadata_cache", cache)
    main.app.state.market_metadata_cache = cache

    fee = FeeBreakdown(currency="USD", maker=0.0, taker=0.0)

    btc_request = OrderPlacementRequest(
        account_id="ACC-1",
        order_id="OID-BTC",
        instrument="BTC-USD",
        side="BUY",
        quantity=0.5,
        price=20000.0,
        fee=fee,
    )
    doge_request = OrderPlacementRequest(
        account_id="ACC-1",
        order_id="OID-DOGE",
        instrument="DOGE-USD",
        side="BUY",
        quantity=1000.0,
        price=0.075,
        fee=fee,
    )

    response_btc = await main.place_order(btc_request, account_id="ACC-1")
    response_doge = await main.place_order(doge_request, account_id="ACC-1")

    assert response_btc.accepted is True
    assert response_doge.accepted is True

    assert captured_payloads[0]["pair"] == "XBT/USD"
    assert captured_payloads[1]["pair"] == "XDG/USD"
