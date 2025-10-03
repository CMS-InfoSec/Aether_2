import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

import safe_mode
from tests.fixtures.backends import MemoryRedis


@pytest.fixture(autouse=True)
def reset_state() -> None:
    backend = MemoryRedis()
    safe_mode.controller._state_store = safe_mode.SafeModeStateStore(redis_client=backend)
    safe_mode.controller.reset()
    safe_mode.clear_safe_mode_log()
    yield
    safe_mode.controller.reset()
    safe_mode.clear_safe_mode_log()


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
