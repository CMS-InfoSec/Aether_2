"""Integration tests covering Safe Mode orchestration behaviours."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

import safe_mode
from tests.fixtures.backends import MemoryRedis


class _RecordingExchangeAdapter:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.cancelled: list[tuple[str, str, str]] = []

    async def cancel_order(
        self,
        account_id: str,
        client_id: str,
        *,
        exchange_order_id: str | None = None,
    ) -> dict[str, object]:
        self.cancelled.append((account_id, client_id, exchange_order_id or ""))
        return {"status": "ok"}


class _RecordingTimescaleAdapter:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.transitions: list[tuple[bool, str | None, str | None]] = []

    def set_safe_mode(
        self,
        *,
        engaged: bool,
        reason: str | None = None,
        actor: str | None = None,
    ) -> None:
        self.transitions.append((engaged, reason, actor))

    def events(self) -> dict[str, list[dict[str, object]]]:
        return {"acks": []}


@pytest.fixture
def configured_controller(monkeypatch: pytest.MonkeyPatch) -> tuple[
    safe_mode.SafeModeController,
    dict[str, _RecordingExchangeAdapter],
    dict[str, _RecordingTimescaleAdapter],
]:
    backend = MemoryRedis()
    exchange_adapters: dict[str, _RecordingExchangeAdapter] = {}
    timescale_adapters: dict[str, _RecordingTimescaleAdapter] = {}
    open_orders: dict[str, list[dict[str, str]]] = {
        "company": [
            {"client_id": "ORD-1", "txid": "EX-1"},
            {"clientOrderId": "ORD-2", "orderid": "EX-2"},
        ]
    }

    def exchange_factory(account_id: str) -> _RecordingExchangeAdapter:
        adapter = _RecordingExchangeAdapter(account_id)
        exchange_adapters[account_id] = adapter
        return adapter

    def timescale_factory(account_id: str) -> _RecordingTimescaleAdapter:
        adapter = _RecordingTimescaleAdapter(account_id)
        timescale_adapters[account_id] = adapter
        return adapter

    def loader(
        account_id: str, _timescale: _RecordingTimescaleAdapter
    ) -> list[dict[str, str]]:
        return list(open_orders.pop(account_id, []))

    controls = safe_mode.OrderControls(
        account_ids=["company"],
        exchange_factory=exchange_factory,
        timescale_factory=timescale_factory,
        order_snapshot_loader=loader,
    )
    controller = safe_mode.SafeModeController(
        order_controls=controls,
        state_store=safe_mode.SafeModeStateStore(redis_client=backend),
    )
    original_controller = safe_mode.controller
    monkeypatch.setattr(safe_mode, "controller", controller)
    safe_mode.clear_safe_mode_log()

    yield controller, exchange_adapters, timescale_adapters

    controller.reset()
    safe_mode.clear_safe_mode_log()
    monkeypatch.setattr(safe_mode, "controller", original_controller)


class DummyTradingEngine:
    """Minimal trading engine that consults Safe Mode controls."""

    def __init__(self, controller: safe_mode.SafeModeController) -> None:
        self._controller = controller

    @staticmethod
    def _hedging_only(controller: safe_mode.SafeModeController) -> bool:
        controls = controller.order_controls
        if hasattr(controls, "hedging_only"):
            return bool(getattr(controls, "hedging_only"))
        return bool(getattr(controls, "only_hedging"))

    def submit_intent(self) -> str:
        """Simulate submitting a new trading intent."""

        self._controller.guard_new_intent()
        return "intent-submitted"

    def place_order(self, *, hedging: bool) -> str:
        """Simulate routing an order while respecting Safe Mode restrictions."""

        if self._hedging_only(self._controller) and not hedging:
            raise RuntimeError("Only hedging orders are permitted while safe mode is active")
        return "order-placed"


@pytest.mark.integration
def test_safe_mode_blocks_intents_and_cancels_orders(
    configured_controller: tuple[
        safe_mode.SafeModeController,
        dict[str, _RecordingExchangeAdapter],
        dict[str, _RecordingTimescaleAdapter],
    ]
) -> None:
    controller, exchange_adapters, timescale_adapters = configured_controller
    client = TestClient(safe_mode.app)
    engine = DummyTradingEngine(controller)

    # Baseline: trading intents and regular orders succeed when safe mode is inactive.
    assert engine.submit_intent() == "intent-submitted"
    assert engine.place_order(hedging=False) == "order-placed"

    response = client.post(
        "/safe_mode/enter",
        json={"reason": "test"},
        headers={"X-Account-ID": "company"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["active"] is True
    assert payload["reason"] == "test"

    adapter = exchange_adapters["company"]
    assert adapter.cancelled == [
        ("company", "ORD-1", "EX-1"),
        ("company", "ORD-2", "EX-2"),
    ]

    ts_adapter = timescale_adapters["company"]
    assert ts_adapter.transitions[0] == (True, "test", "company")

    # New trading intents should be rejected while safe mode is active.
    with pytest.raises(RuntimeError, match="Safe mode active; new trading intents are blocked"):
        engine.submit_intent()

    # Hedging remains permitted even when regular orders are blocked.
    with pytest.raises(RuntimeError, match="Only hedging orders are permitted"):
        engine.place_order(hedging=False)
    assert engine.place_order(hedging=True) == "order-placed"

    response = client.post(
        "/safe_mode/exit", headers={"X-Account-ID": "company"}
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["active"] is False

    # Timescale safe mode state should have been released.
    assert ts_adapter.transitions[-1] == (False, "test", "company")

    # Once safe mode exits, trading can resume normally.
    assert engine.submit_intent() == "intent-submitted"
    assert engine.place_order(hedging=False) == "order-placed"
