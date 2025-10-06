import asyncio
from datetime import datetime, timezone

import sys

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

import safe_mode
from tests.fixtures.backends import MemoryRedis


class _StubOrderControls:
    def __init__(self) -> None:
        self.open_orders: list[str] = []
        self.cancelled_orders: list[str] = []
        self.hedging_only = False
        self.only_hedging = False

    def cancel_open_orders(self) -> None:
        self.cancelled_orders.extend(self.open_orders)
        self.open_orders.clear()

    def restrict_to_hedging(self, *, reason: str | None = None, actor: str | None = None) -> None:
        self.hedging_only = True
        self.only_hedging = True

    def lift_restrictions(self, *, reason: str | None = None, actor: str | None = None) -> None:
        self.hedging_only = False
        self.only_hedging = False

    def reset(self) -> None:
        self.open_orders.clear()
        self.cancelled_orders.clear()
        self.hedging_only = False
        self.only_hedging = False


@pytest.fixture(autouse=True)
def reset_state(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = MemoryRedis()
    stub_controls = _StubOrderControls()
    controller = safe_mode.SafeModeController(
        order_controls=stub_controls,
        state_store=safe_mode.SafeModeStateStore(redis_client=backend),
    )
    original_controller = safe_mode.controller
    monkeypatch.setattr(safe_mode, "controller", controller)
    safe_mode.clear_safe_mode_log()
    yield
    controller.reset()
    safe_mode.clear_safe_mode_log()
    monkeypatch.setattr(safe_mode, "controller", original_controller)


def test_enter_safe_mode_enforces_controls() -> None:
    client = TestClient(safe_mode.app)

    controller = safe_mode.controller
    controller.order_controls.open_orders.extend(["ORD-1", "ORD-2"])

    response = client.post(
        "/safe_mode/enter",
        json={"reason": "volatility"},
        headers={"X-Account-ID": "company"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["active"] is True
    assert payload["reason"] == "volatility"

    assert controller.order_controls.open_orders == []
    assert controller.order_controls.cancelled_orders == ["ORD-1", "ORD-2"]
    assert controller.order_controls.only_hedging is True
    assert controller.intent_guard.allow_new_intents is False

    history = safe_mode.controller.kafka_history()
    assert history, "Expected safe mode events to be published"
    kafka_payload = history[-1]["payload"]
    assert kafka_payload["state"] == "entered"
    assert kafka_payload["reason"] == "volatility"

    log_entries = safe_mode.get_safe_mode_log()
    assert log_entries
    assert log_entries[-1]["state"] == "entered"
    assert log_entries[-1]["actor"] == "company"


def test_exit_safe_mode_restores_controls() -> None:
    client = TestClient(safe_mode.app)
    controller = safe_mode.controller

    client.post(
        "/safe_mode/enter",
        json={"reason": "latency"},
        headers={"X-Account-ID": "company"},
    )

    response = client.post("/safe_mode/exit", headers={"X-Account-ID": "company"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["active"] is False

    assert controller.order_controls.only_hedging is False
    assert controller.intent_guard.allow_new_intents is True

    history = safe_mode.controller.kafka_history()
    exit_events = [entry for entry in history if entry["payload"]["state"] == "exited"]
    assert exit_events, "Expected an exit event to be published"

    log_entries = safe_mode.get_safe_mode_log()
    assert log_entries[-1]["state"] == "exited"


def test_status_endpoint_reports_current_state() -> None:
    client = TestClient(safe_mode.app)

    response = client.get("/safe_mode/status")
    assert response.status_code == 200
    assert response.json()["active"] is False

    client.post(
        "/safe_mode/enter",
        json={"reason": "stress"},
        headers={"X-Account-ID": "company"},
    )

    response = client.get("/safe_mode/status")
    assert response.status_code == 200
    payload = response.json()
    assert payload["active"] is True
    assert payload["reason"] == "stress"


def test_safe_mode_endpoints_enforce_authentication() -> None:
    client = TestClient(safe_mode.app)

    unauthenticated_response = client.post(
        "/safe_mode/enter", json={"reason": "volatility"}
    )
    assert unauthenticated_response.status_code == 401

    non_admin_response = client.post(
        "/safe_mode/enter",
        json={"reason": "volatility"},
        headers={"X-Account-ID": "trader"},
    )
    assert non_admin_response.status_code == 403

    unauthenticated_exit = client.post("/safe_mode/exit")
    assert unauthenticated_exit.status_code == 401


def test_safe_mode_state_survives_controller_restart() -> None:
    backend = MemoryRedis()
    store = safe_mode.SafeModeStateStore(redis_client=backend)

    primary = safe_mode.SafeModeController(state_store=store)
    primary.enter(reason="ops_drill", actor="company")

    restarted = safe_mode.SafeModeController(state_store=safe_mode.SafeModeStateStore(redis_client=backend))

    status = restarted.status()
    assert status.active is True
    assert status.reason == "ops_drill"
    assert restarted.intent_guard.allow_new_intents is False
    assert restarted.order_controls.hedging_only is True


def test_kafka_publisher_handles_running_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    published: dict[str, object] = {}

    class _StubAdapter:
        def __init__(self, account_id: str) -> None:
            published["account_id"] = account_id

        async def publish(self, topic: str, payload: dict[str, object]) -> None:
            published["topic"] = topic
            published["payload"] = payload

    import services.common.adapters as adapters

    monkeypatch.setattr(adapters, "KafkaNATSAdapter", _StubAdapter)

    publisher = safe_mode.KafkaSafeModePublisher(account_id="company", topic="ops.safe_mode")
    event = safe_mode.SafeModeEvent(
        reason="drill",
        ts=datetime.now(timezone.utc),
        state="entered",
        actor="ops",
    )

    async def _exercise() -> None:
        publisher.publish(event)
        await asyncio.sleep(0)

    asyncio.run(_exercise())

    assert published["account_id"] == "company"
    assert published["topic"] == "ops.safe_mode"
    assert published["payload"]["state"] == "entered"


def test_state_store_requires_configured_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    original_pytest = sys.modules.get("pytest")
    try:
        monkeypatch.delitem(sys.modules, "pytest", raising=False)
        monkeypatch.delenv("SAFE_MODE_REDIS_URL", raising=False)

        with pytest.raises(RuntimeError, match="SAFE_MODE_REDIS_URL"):
            safe_mode.SafeModeStateStore._create_default_client()
    finally:
        if original_pytest is not None:
            sys.modules["pytest"] = original_pytest


def test_state_store_rejects_stub_without_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    original_pytest = sys.modules.get("pytest")
    try:
        monkeypatch.delitem(sys.modules, "pytest", raising=False)
        monkeypatch.setenv("SAFE_MODE_REDIS_URL", "redis://stub:6379/0")

        def _fake_create(url: str, *, decode_responses: bool, logger: object | None = None) -> tuple[object, bool]:
            return object(), True

        monkeypatch.setattr(safe_mode, "create_redis_from_url", _fake_create)

        with pytest.raises(RuntimeError, match="Failed to connect"):
            safe_mode.SafeModeStateStore._create_default_client()
    finally:
        if original_pytest is not None:
            sys.modules["pytest"] = original_pytest
