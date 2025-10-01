from fastapi.testclient import TestClient

from kill_alerts import email_notifications, reset_notifications, sms_notifications, webhook_notifications
from kill_switch import KillSwitchReason, app
from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter


def setup_function() -> None:
    KafkaNATSAdapter.reset()
    TimescaleAdapter.reset()
    reset_notifications()


def test_trigger_kill_switch_publishes_event_and_sets_state() -> None:
    client = TestClient(app)

    response = client.post(
        "/risk/kill",
        params={"account_id": "Alpha", "reason_code": KillSwitchReason.SPREAD_WIDENING.value},
        headers={"X-Account-ID": "company"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["reason_code"] == KillSwitchReason.SPREAD_WIDENING.value
    assert payload["channels_sent"] == ["email", "sms", "webhook"]
    assert "ts" in payload

    history = KafkaNATSAdapter(account_id="alpha").history()
    assert history, "Kill switch event was not published"
    last_event = history[-1]
    assert last_event["topic"] == "risk.events"
    event_payload = last_event["payload"]
    assert event_payload["event"] == "KILL_SWITCH_TRIGGERED"
    assert event_payload["account_id"] == "alpha"
    assert event_payload["actions"] == ["CANCEL_OPEN_ORDERS", "FLATTEN_POSITIONS"]

    timescale = TimescaleAdapter(account_id="alpha")
    config = timescale.load_risk_config()
    assert config["kill_switch"] is True
    events = timescale.events()["events"]
    assert any(event["type"] == "kill_switch_engaged" for event in events)
    kill_events = timescale.kill_events()
    assert kill_events
    last_event = kill_events[0]
    assert last_event["reason"] == KillSwitchReason.SPREAD_WIDENING.value
    assert last_event["channels_sent"] == ["email", "sms", "webhook"]

    assert email_notifications()
    assert sms_notifications()
    assert webhook_notifications()


def test_list_kill_events_endpoint_returns_recent_events() -> None:
    client = TestClient(app)

    response = client.post(
        "/risk/kill",
        params={"account_id": "Beta", "reason_code": KillSwitchReason.LOSS_CAP_BREACH.value},
        headers={"X-Account-ID": "company"},
    )
    assert response.status_code == 200

    list_response = client.get(
        "/risk/kill_events",
        headers={"X-Account-ID": "company"},
    )

    assert list_response.status_code == 200
    events = list_response.json()
    assert isinstance(events, list)
    assert events
    first_event = events[0]
    assert first_event["account_id"] == "beta"
    assert first_event["reason"] == KillSwitchReason.LOSS_CAP_BREACH.value
    assert first_event["channels_sent"] == ["email", "sms", "webhook"]
