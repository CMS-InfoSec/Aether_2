"""Tests for the Binance and Coinbase spot adapters."""

from __future__ import annotations

from typing import Type

import pytest

from services.adapters.binance_adapter import BinanceAdapter
from services.adapters.coinbase_adapter import CoinbaseAdapter


ADAPTER_CASES = [
    (
        "binance",
        BinanceAdapter,
        "https://api.binance.com",
        "wss://stream.binance.com:9443/ws",
    ),
    (
        "coinbase",
        CoinbaseAdapter,
        "https://api.exchange.coinbase.com",
        "wss://ws-feed.exchange.coinbase.com",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange, adapter_cls, rest_url, websocket_url", ADAPTER_CASES)
async def test_connect_reports_configuration(
    exchange: str,
    adapter_cls: Type,
    rest_url: str,
    websocket_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ENABLE_MULTI_EXCHANGE_ROUTING", raising=False)
    adapter = adapter_cls()

    result = await adapter.connect()

    assert result == {
        "exchange": exchange,
        "status": "connected",
        "rest_url": rest_url,
        "websocket_url": websocket_url,
        "trading_enabled": False,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter_cls", [case[1] for case in ADAPTER_CASES])
async def test_fetch_orderbook_normalizes_symbol(
    adapter_cls: Type,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENABLE_MULTI_EXCHANGE_ROUTING", "1")
    adapter = adapter_cls()

    orderbook = await adapter.fetch_orderbook("btc/usd", depth=5)

    assert orderbook["symbol"] == "BTC-USD"
    assert orderbook["depth"] == 5
    assert orderbook["bids"] == []
    assert orderbook["asks"] == []
    assert orderbook["exchange"] == adapter.name
    assert "as_of" in orderbook


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter_cls", [case[1] for case in ADAPTER_CASES])
async def test_submit_order_respects_multi_exchange_toggle(
    adapter_cls: Type,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = adapter_cls()

    # Routing disabled by default
    ack = await adapter.submit_order("acct", {"symbol": "ETH-USD"})
    assert ack["status"] == "noop"
    assert ack["reason"] == "multi_exchange_routing_disabled"

    # Routing enabled
    monkeypatch.setenv("ENABLE_MULTI_EXCHANGE_ROUTING", "true")
    ack_enabled = await adapter.submit_order("acct", {"symbol": "ETH-USD"})
    assert ack_enabled["status"] == "accepted"
    assert ack_enabled["note"] == "stubbed_execution"
    assert ack_enabled["client_order_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter_cls", [case[1] for case in ADAPTER_CASES])
async def test_cancel_order_respects_multi_exchange_toggle(
    adapter_cls: Type,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = adapter_cls()

    cancel_ack = await adapter.cancel_order("acct", "cid")
    assert cancel_ack["status"] == "noop"
    assert cancel_ack["reason"] == "multi_exchange_routing_disabled"

    monkeypatch.setenv("ENABLE_MULTI_EXCHANGE_ROUTING", "YES")
    cancel_enabled = await adapter.cancel_order("acct", "cid", exchange_order_id="123")
    assert cancel_enabled["status"] == "accepted"
    assert cancel_enabled["note"] == "stubbed_cancellation"
    assert cancel_enabled["exchange_order_id"] == "123"
    assert cancel_enabled["client_order_id"] == "cid"


@pytest.mark.parametrize("adapter_cls", [case[1] for case in ADAPTER_CASES])
def test_supports_tracks_multi_exchange_toggle(
    adapter_cls: Type,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = adapter_cls()
    assert adapter.supports("place_order") is False

    monkeypatch.setenv("ENABLE_MULTI_EXCHANGE_ROUTING", "on")
    adapter_enabled = adapter_cls()
    assert adapter_enabled.supports("place_order") is True
    assert adapter_enabled.supports("cancel_order") is True
    assert adapter_enabled.supports("get_balance") is False
