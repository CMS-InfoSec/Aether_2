from __future__ import annotations

import asyncio
import sys
import types
from decimal import Decimal
from typing import Any, Dict

import pytest

pytest.importorskip("fastapi")
from services.oms import oms_service
from services.oms.kraken_ws import OrderAck, OrderState
from tests.mocks.mock_kraken import (
    MockKrakenError,
    MockKrakenTransportClosed,
)


def _install_cryptography_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    if "cryptography" in sys.modules:
        return

    crypto_mod = types.ModuleType("cryptography")
    hazmat_mod = types.ModuleType("cryptography.hazmat")
    primitives_mod = types.ModuleType("cryptography.hazmat.primitives")
    ciphers_mod = types.ModuleType("cryptography.hazmat.primitives.ciphers")
    aead_mod = types.ModuleType("cryptography.hazmat.primitives.ciphers.aead")

    class AESGCM:  # pragma: no cover - placeholder for optional dependency
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args
            self.kwargs = kwargs

        def encrypt(self, *args: Any, **kwargs: Any) -> bytes:
            raise NotImplementedError("AESGCM.encrypt stub invoked in tests")

        def decrypt(self, *args: Any, **kwargs: Any) -> bytes:
            raise NotImplementedError("AESGCM.decrypt stub invoked in tests")

    aead_mod.AESGCM = AESGCM

    monkeypatch.setitem(sys.modules, "cryptography", crypto_mod)
    monkeypatch.setitem(sys.modules, "cryptography.hazmat", hazmat_mod)
    monkeypatch.setitem(sys.modules, "cryptography.hazmat.primitives", primitives_mod)
    monkeypatch.setitem(sys.modules, "cryptography.hazmat.primitives.ciphers", ciphers_mod)
    monkeypatch.setitem(
        sys.modules,
        "cryptography.hazmat.primitives.ciphers.aead",
        aead_mod,
    )


def _ack_from_exchange_response(payload: Dict[str, Any]) -> OrderAck:
    fills = payload.get("fills") or []
    filled_qty = sum(Decimal(str(fill.get("volume", 0.0))) for fill in fills)
    avg_price = None
    if filled_qty > Decimal("0"):
        total_notional = sum(
            Decimal(str(fill.get("price", 0.0))) * Decimal(str(fill.get("volume", 0.0)))
            for fill in fills
        )
        avg_price = (total_notional / filled_qty) if filled_qty else None
    filled_value = filled_qty if filled_qty != Decimal("0") else None
    return OrderAck(
        exchange_order_id=str(payload.get("txid")) if payload.get("txid") else None,
        status=str(payload.get("status") or "ok"),
        filled_qty=filled_value,
        avg_price=avg_price,
        errors=None,
    )


class _StubWSClient:
    def __init__(
        self,
        *,
        credential_getter,
        stream_update_cb,
        exchange,
    ) -> None:
        del credential_getter
        self._stream_update_cb = stream_update_cb
        self._exchange = exchange

    async def ensure_connected(self) -> None:  # pragma: no cover - no-op
        return None

    async def subscribe_private(self, channels: list[str]) -> None:  # pragma: no cover - no-op
        del channels
        return None

    async def stream_handler(self) -> None:  # pragma: no cover - keep task alive
        while True:
            await asyncio.sleep(3600)

    async def close(self) -> None:  # pragma: no cover - symmetry with real client
        return None

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        try:
            response = self._exchange.place_order_ws(dict(payload))
        except MockKrakenError as exc:  # convert to OMS error
            raise oms_service.KrakenWSError(str(exc)) from exc
        ack = _ack_from_exchange_response(response)
        if self._stream_update_cb is not None:
            state = OrderState(
                client_order_id=str(payload.get("clientOrderId")),
                exchange_order_id=ack.exchange_order_id,
                status=ack.status or "ok",
                filled_qty=ack.filled_qty,
                avg_price=ack.avg_price,
                errors=ack.errors,
                transport="websocket",
            )
            await self._stream_update_cb(state)
        return ack

    def heartbeat_age(self) -> float:
        return 0.0


class _StubRESTClient:
    def __init__(self, *, credential_getter, exchange) -> None:
        del credential_getter
        self._exchange = exchange

    async def close(self) -> None:  # pragma: no cover - symmetry with aiohttp client
        return None

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        response = await self._exchange.place_order_rest(dict(payload))
        return _ack_from_exchange_response(response)

    async def asset_pairs(self) -> Dict[str, Any]:
        return {}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rest_fallback_when_websocket_fails(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    mock_kraken_exchange,
) -> None:
    account_id = "company"
    _install_cryptography_stub(monkeypatch)
    from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter

    KafkaNATSAdapter.reset()
    TimescaleAdapter.reset()
    oms_service._OMS_ACTIVITY_LOG.clear()

    monkeypatch.setenv("KRAKEN_COMPANY_API_KEY", "dummy-key")
    monkeypatch.setenv("KRAKEN_COMPANY_API_SECRET", "dummy-secret")

    def ws_factory(**kwargs: Any) -> _StubWSClient:
        return _StubWSClient(exchange=mock_kraken_exchange, **kwargs)

    def rest_factory(**kwargs: Any) -> _StubRESTClient:
        return _StubRESTClient(exchange=mock_kraken_exchange, **kwargs)

    monkeypatch.setattr(oms_service, "KrakenWSClient", ws_factory)
    monkeypatch.setattr(oms_service, "KrakenRESTClient", rest_factory)

    service = oms_service.OMSService()
    monkeypatch.setattr(oms_service, "oms_service", service)

    caplog.set_level("INFO", logger=oms_service.logger.name)

    mock_kraken_exchange.reset()
    mock_kraken_exchange.schedule_error(
        "place_order", MockKrakenTransportClosed("simulated websocket outage")
    )

    request = oms_service.PlaceOrderRequest(
        account_id=account_id,
        client_id="CID-REST-001",
        symbol="BTC/USD",
        side="buy",
        order_type="limit",
        qty=0.5,
        limit_px=30000.0,
    )

    try:
        response = await service.place_order(request)

        assert response.transport == "rest"
        assert response.status.lower() not in {"error", "failed"}
        assert response.order_id

        history = KafkaNATSAdapter(account_id=account_id).history()
        assert any(
            entry["topic"] == "oms.acks" and entry["payload"].get("transport") == "rest"
            for entry in history
        )

        relevant_logs = [
            record
            for record in caplog.records
            if record.getMessage() == "oms_log"
            and getattr(record, "order_id", None) == response.order_id
        ]
        assert relevant_logs, "Expected oms_log entry for fallback order"

        activity_entries = [
            entry
            for entry in oms_service._OMS_ACTIVITY_LOG
            if entry.get("order_id") == response.order_id
        ]
        assert activity_entries, "Order should be captured in OMS activity log"

        fallback_tagged = any(
            entry.get("fallback_rest") or "fallback_rest=true" in str(entry.get("status", ""))
            for entry in activity_entries
        )
        if not fallback_tagged:
            for record in relevant_logs:
                if getattr(record, "fallback_rest", None) is True:
                    fallback_tagged = True
                    break
                if any(
                    isinstance(value, str) and "fallback_rest=true" in value
                    for value in record.__dict__.values()
                ):
                    fallback_tagged = True
                    break
        assert fallback_tagged, "fallback_rest flag should be present in oms_log"
    finally:
        session = service._sessions.get(account_id)
        if session is not None:
            await session.close()
        KafkaNATSAdapter.reset()
        TimescaleAdapter.reset()
        oms_service._OMS_ACTIVITY_LOG.clear()
