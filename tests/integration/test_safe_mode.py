"""Integration tests covering Safe Mode orchestration behaviours."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

import safe_mode


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
def test_safe_mode_blocks_intents_but_allows_hedging_orders() -> None:
    controller = safe_mode.controller
    controller.reset()
    safe_mode.clear_safe_mode_log()

    client = TestClient(safe_mode.app)
    engine = DummyTradingEngine(controller)

    # Baseline: trading intents and regular orders succeed when safe mode is inactive.
    assert engine.submit_intent() == "intent-submitted"
    assert engine.place_order(hedging=False) == "order-placed"

    response = client.post("/safe_mode/enter", json={"reason": "test"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["active"] is True
    assert payload["reason"] == "test"

    # New trading intents should be rejected while safe mode is active.
    with pytest.raises(RuntimeError, match="Safe mode active; new trading intents are blocked"):
        engine.submit_intent()

    # Hedging remains permitted even when regular orders are blocked.
    with pytest.raises(RuntimeError, match="Only hedging orders are permitted"):
        engine.place_order(hedging=False)
    assert engine.place_order(hedging=True) == "order-placed"

    response = client.post("/safe_mode/exit")
    assert response.status_code == 200
    payload = response.json()
    assert payload["active"] is False

    # Once safe mode exits, trading can resume normally.
    assert engine.submit_intent() == "intent-submitted"
    assert engine.place_order(hedging=False) == "order-placed"

    controller.reset()
    safe_mode.clear_safe_mode_log()
