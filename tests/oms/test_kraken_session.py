from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pytest

pytest.importorskip("fastapi")

pytestmark = pytest.mark.anyio("asyncio")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"

import oms_service
from oms_service import KrakenSession, OrderContext


class StubGuard:
    def __init__(self) -> None:
        self.acquire_calls: List[Dict[str, Any]] = []
        self.release_calls: List[Dict[str, Any]] = []

    async def acquire(
        self,
        account_id: str,
        endpoint: str,
        *,
        transport: str,
        urgent: bool = False,
    ) -> None:
        self.acquire_calls.append(
            {
                "account_id": account_id,
                "endpoint": endpoint,
                "transport": transport,
                "urgent": urgent,
            }
        )

    async def release(
        self,
        account_id: str,
        *,
        transport: str,
        successful: bool = True,
        remaining: Optional[int] = None,
    ) -> None:
        self.release_calls.append(
            {
                "account_id": account_id,
                "transport": transport,
                "successful": successful,
                "remaining": remaining,
            }
        )


class DummyCredentialProvider:
    async def get(self, account_id: str) -> Dict[str, str]:
        return {"api_key": "", "api_secret": ""}


@dataclass
class _Ack:
    exchange_order_id: Optional[str] = "order-1"
    status: Optional[str] = "ok"
    filled_qty: Optional[Decimal] = None
    avg_price: Optional[Decimal] = None
    errors: Optional[List[str]] = None


class SuccessfulWSClient:
    def __init__(self, *, credential_getter, stream_update_cb, **_: Any) -> None:
        self._stream_update_cb = stream_update_cb
        self.add_order_calls: List[Dict[str, Any]] = []
        self.cancel_order_calls: List[Dict[str, Any]] = []

    async def ensure_connected(self) -> None:  # pragma: no cover - trivial
        return None

    async def subscribe_private(self, channels: List[str]) -> None:  # pragma: no cover
        return None

    async def stream_handler(self) -> None:  # pragma: no cover - background task
        return None

    async def close(self) -> None:  # pragma: no cover - cleanup helper
        return None

    async def add_order(self, payload: Dict[str, Any]) -> _Ack:
        self.add_order_calls.append(payload)
        return _Ack()

    async def cancel_order(self, payload: Dict[str, Any]) -> _Ack:
        self.cancel_order_calls.append(payload)
        return _Ack(status="canceled")


class SuccessfulRESTClient:
    def __init__(self, *, credential_getter) -> None:
        self.add_order_calls: List[Dict[str, Any]] = []
        self.cancel_order_calls: List[Dict[str, Any]] = []

    async def close(self) -> None:  # pragma: no cover
        return None

    async def add_order(self, payload: Dict[str, Any]) -> _Ack:
        self.add_order_calls.append(payload)
        return _Ack(exchange_order_id="rest-order")

    async def cancel_order(self, payload: Dict[str, Any]) -> _Ack:
        self.cancel_order_calls.append(payload)
        return _Ack(status="canceled", exchange_order_id="rest-cancel")


class FailingWSClient(SuccessfulWSClient):
    async def add_order(self, payload: Dict[str, Any]) -> _Ack:
        raise oms_service.KrakenWSError("boom")

    async def cancel_order(self, payload: Dict[str, Any]) -> _Ack:
        raise oms_service.KrakenWSError("boom")


async def test_place_order_uses_guard_for_websocket(monkeypatch: pytest.MonkeyPatch) -> None:
    guard = StubGuard()
    monkeypatch.setattr(oms_service, "KrakenWSClient", SuccessfulWSClient)
    monkeypatch.setattr(oms_service, "KrakenRESTClient", SuccessfulRESTClient)

    session = KrakenSession("acct", DummyCredentialProvider(), rate_limit_guard=guard)
    context = OrderContext(
        account_id="acct",
        symbol="BTC/USD",
        side="buy",
        qty=1.0,
        client_id="client-1",
        post_only=False,
        reduce_only=False,
        tif=None,
    )

    await session.place_order({"foo": "bar"}, context)

    assert guard.acquire_calls == [
        {
            "account_id": "acct",
            "endpoint": "add_order",
            "transport": "websocket",
            "urgent": False,
        }
    ]
    assert guard.release_calls[0]["successful"] is True
    assert guard.release_calls[0]["transport"] == "websocket"

    await session.close()


async def test_place_order_rest_fallback_releases_websocket(monkeypatch: pytest.MonkeyPatch) -> None:
    guard = StubGuard()
    monkeypatch.setattr(oms_service, "KrakenWSClient", FailingWSClient)
    monkeypatch.setattr(oms_service, "KrakenRESTClient", SuccessfulRESTClient)

    session = KrakenSession("acct", DummyCredentialProvider(), rate_limit_guard=guard)
    context = OrderContext(
        account_id="acct",
        symbol="BTC/USD",
        side="buy",
        qty=1.0,
        client_id="client-1",
        post_only=False,
        reduce_only=False,
        tif=None,
    )

    await session.place_order({"foo": "bar"}, context)

    assert guard.acquire_calls[0]["transport"] == "websocket"
    assert guard.acquire_calls[0]["endpoint"] == "add_order"
    assert guard.release_calls[0]["successful"] is False
    assert guard.release_calls[0]["transport"] == "websocket"
    assert guard.acquire_calls[1]["transport"] == "rest"
    assert guard.release_calls[1]["transport"] == "rest"

    await session.close()


async def test_cancel_order_marks_urgent(monkeypatch: pytest.MonkeyPatch) -> None:
    guard = StubGuard()
    monkeypatch.setattr(oms_service, "KrakenWSClient", SuccessfulWSClient)
    monkeypatch.setattr(oms_service, "KrakenRESTClient", SuccessfulRESTClient)

    session = KrakenSession("acct", DummyCredentialProvider(), rate_limit_guard=guard)

    await session.cancel_order("order-1", "BTC/USD")

    assert guard.acquire_calls[0]["endpoint"] == "cancel_order"
    assert guard.acquire_calls[0]["urgent"] is True
    assert guard.release_calls[0]["transport"] == "websocket"

    await session.close()
