from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

import safe_mode
from safe_mode import SafeModeController
from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter


@pytest.fixture(autouse=True)
def reset_state() -> None:
    KafkaNATSAdapter.reset()
    TimescaleAdapter.reset()
    safe_mode.controller.reset()
    yield
    KafkaNATSAdapter.reset()
    TimescaleAdapter.reset()
    safe_mode.controller.reset()


def test_kraken_downtime_triggers_safe_mode() -> None:
    controller = SafeModeController(downtime_threshold_seconds=1.0)

    heartbeat_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    controller.record_heartbeat("Company", timestamp=heartbeat_at)

    events = controller.check_downtime(current_time=heartbeat_at + timedelta(seconds=5))

    assert events, "Safe mode should trigger after extended downtime"
    event = events[0]
    assert event.account_id == "company"
    assert event.reason == "kraken_ws_down"
    assert event.automatic is True

    history = KafkaNATSAdapter(account_id="company").history()
    assert history, "Safe mode event was not published to Kafka"
    payload = history[-1]["payload"]
    assert payload["event"] == "SAFE_MODE_ENTERED"
    assert payload["actions"] == ["CANCEL_OPEN_ORDERS", "HEDGE_TO_USD"]

    config = TimescaleAdapter(account_id="company").load_risk_config()
    assert config["safe_mode"] is True

    events_log = TimescaleAdapter(account_id="company").events()["events"]
    assert any(entry["event_type"] == "safe_mode_engaged" for entry in events_log)


def test_manual_override_endpoints() -> None:
    client = TestClient(safe_mode.app)

    enter_response = client.post(
        "/core/safe_mode/enter",
        params={"account_id": "Company", "reason": "manual_override"},
        headers={"X-Account-ID": "company"},
    )
    assert enter_response.status_code == 200
    enter_payload = enter_response.json()
    assert enter_payload["state"] == "engaged"
    assert enter_payload["reason"] == "manual_override"

    config = TimescaleAdapter(account_id="company").load_risk_config()
    assert config["safe_mode"] is True

    history = KafkaNATSAdapter(account_id="company").history()
    assert any(entry["payload"]["event"] == "SAFE_MODE_ENTERED" for entry in history)

    exit_response = client.post(
        "/core/safe_mode/exit",
        params={"account_id": "Company", "reason": "manual_release"},
        headers={"X-Account-ID": "company"},
    )
    assert exit_response.status_code == 200
    exit_payload = exit_response.json()
    assert exit_payload["state"] == "released"

    config = TimescaleAdapter(account_id="company").load_risk_config()
    assert config["safe_mode"] is False

    history = KafkaNATSAdapter(account_id="company").history()
    assert any(entry["payload"]["event"] == "SAFE_MODE_EXITED" for entry in history)

