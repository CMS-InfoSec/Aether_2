"""Unit tests covering Sequencer OMS stage behaviours."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

import sequencer


class _StubPublisher:
    def __init__(self) -> None:
        self.events: List[Dict[str, Any]] = []

    async def publish_event(
        self,
        stage: str,
        phase: str,
        *,
        run_id: str,
        intent_id: str,
        account_id: str,
        data: Dict[str, Any],
    ) -> None:
        self.events.append(
            {
                "stage": stage,
                "phase": phase,
                "run_id": run_id,
                "intent_id": intent_id,
                "account_id": account_id,
                "data": data,
            }
        )


@pytest.mark.asyncio
async def test_oms_handler_submits_order_with_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Successful OMS submission should propagate adapter metadata."""

    class _Adapter:
        def __init__(self) -> None:
            self.calls: List[Dict[str, Any]] = []

        def supports(self, operation: str) -> bool:
            return operation in {"place_order", "cancel_order"}

        async def place_order(
            self,
            account_id: str,
            payload: Dict[str, Any],
            *,
            shadow: bool = False,
        ) -> Dict[str, Any]:
            self.calls.append({"account_id": account_id, "payload": payload, "shadow": shadow})
            return {
                "accepted": True,
                "routed_venue": "kraken",
                "exchange_order_id": "EX-123",
                "fills": [{"price": 101.25, "quantity": payload["qty"]}],
                "transport": "test",
            }

        async def cancel_order(  # pragma: no cover - not used in this scenario
            self,
            account_id: str,
            client_id: str,
            *,
            exchange_order_id: Optional[str] = None,
        ) -> Dict[str, Any]:
            return {"status": "cancelled"}

    adapter = _Adapter()
    monkeypatch.setattr(sequencer, "EXCHANGE_ADAPTER", adapter)

    intent = {
        "account_id": "AlphaDesk",
        "order_id": "ORD-1001",
        "instrument": "BTC-USD",
        "side": "buy",
        "quantity": 0.5,
        "price": 101.25,
    }
    payload = {"intent": intent, "risk_validation": {"valid": True}}

    publisher = _StubPublisher()
    ctx = sequencer.PipelineContext(
        run_id="run-1",
        account_id="AlphaDesk",
        intent_id="ORD-1001",
        publisher=publisher,
    )

    result = await sequencer.oms_handler(payload, ctx)

    assert result.artifact["accepted"] is True
    assert result.artifact["exchange_order_id"] == "EX-123"
    assert result.artifact["fills"]
    assert result.artifact["filled_qty"] == pytest.approx(0.5)
    assert result.artifact["avg_price"] == pytest.approx(101.25)
    assert result.artifact["rejection_reasons"] == []

    assert adapter.calls, "Adapter should receive place_order call"
    call = adapter.calls[0]
    assert call["account_id"] == "AlphaDesk"
    assert call["payload"]["client_id"] == "ORD-1001"
    assert call["shadow"] is False


@pytest.mark.asyncio
async def test_oms_handler_raises_on_rejection(monkeypatch: pytest.MonkeyPatch) -> None:
    """OMS rejection should raise StageFailedError with venue reason."""

    class _Adapter:
        def supports(self, operation: str) -> bool:
            return operation == "place_order"

        async def place_order(self, *args, **kwargs) -> Dict[str, Any]:  # type: ignore[no-untyped-def]
            return {
                "accepted": False,
                "errors": ["insufficient funds"],
                "kraken_status": "rejected",
            }

    monkeypatch.setattr(sequencer, "EXCHANGE_ADAPTER", _Adapter())

    intent = {
        "account_id": "AlphaDesk",
        "order_id": "ORD-2002",
        "instrument": "BTC-USD",
        "side": "sell",
        "quantity": 1.0,
        "price": 100.0,
    }
    payload = {"intent": intent, "risk_validation": {"valid": True}}
    ctx = sequencer.PipelineContext(
        run_id="run-2",
        account_id="AlphaDesk",
        intent_id="ORD-2002",
        publisher=_StubPublisher(),
    )

    with pytest.raises(sequencer.StageFailedError) as excinfo:
        await sequencer.oms_handler(payload, ctx)

    message = str(excinfo.value)
    assert "insufficient funds" in message or "rejected" in message


@pytest.mark.asyncio
async def test_oms_handler_raises_on_transport_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Transport failures should surface as StageFailedError."""

    class _Adapter:
        def supports(self, operation: str) -> bool:
            return operation == "place_order"

        async def place_order(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise RuntimeError("network down")

    monkeypatch.setattr(sequencer, "EXCHANGE_ADAPTER", _Adapter())

    intent = {
        "account_id": "AlphaDesk",
        "order_id": "ORD-3003",
        "instrument": "BTC-USD",
        "side": "buy",
        "quantity": 0.25,
        "price": 99.5,
    }
    payload = {"intent": intent, "risk_validation": {"valid": True}}
    ctx = sequencer.PipelineContext(
        run_id="run-3",
        account_id="AlphaDesk",
        intent_id="ORD-3003",
        publisher=_StubPublisher(),
    )

    with pytest.raises(sequencer.StageFailedError) as excinfo:
        await sequencer.oms_handler(payload, ctx)

    assert "Failed to submit order" in str(excinfo.value)


@pytest.mark.asyncio
async def test_oms_rollback_attempts_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rollback should attempt to cancel accepted orders and publish audit event."""

    class _Adapter:
        def __init__(self) -> None:
            self.cancel_calls: List[Dict[str, Any]] = []

        def supports(self, operation: str) -> bool:
            return operation in {"place_order", "cancel_order"}

        async def place_order(self, *args, **kwargs):  # pragma: no cover - unused
            raise AssertionError("place_order should not be invoked in rollback test")

        async def cancel_order(
            self,
            account_id: str,
            client_id: str,
            *,
            exchange_order_id: Optional[str] = None,
        ) -> Dict[str, Any]:
            payload = {
                "account_id": account_id,
                "client_id": client_id,
                "exchange_order_id": exchange_order_id,
            }
            self.cancel_calls.append(payload)
            return {"status": "cancelled"}

    adapter = _Adapter()
    monkeypatch.setattr(sequencer, "EXCHANGE_ADAPTER", adapter)

    publisher = _StubPublisher()
    ctx = sequencer.PipelineContext(
        run_id="run-rollback",
        account_id="alphadesk",
        intent_id="ORD-rollback",
        publisher=publisher,
    )
    artifact = {
        "accepted": True,
        "client_order_id": "ORD-rollback",
        "exchange_order_id": "EX-rollback",
        "account_id": "AlphaDesk",
    }
    result = sequencer.StageResult(payload={}, artifact=artifact)

    await sequencer.oms_rollback(result, ctx)

    assert adapter.cancel_calls == [
        {
            "account_id": "AlphaDesk",
            "client_id": "ORD-rollback",
            "exchange_order_id": "EX-rollback",
        }
    ]

    assert publisher.events, "Rollback should publish audit event"
    event = publisher.events[0]
    assert event["stage"] == "oms"
    assert event["phase"] == "rollback"
    assert event["data"]["cancel_result"]["status"] == "cancelled"
