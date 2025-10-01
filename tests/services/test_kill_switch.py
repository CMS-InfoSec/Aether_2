from fastapi.testclient import TestClient

from kill_switch import app
from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter


def setup_function() -> None:
    KafkaNATSAdapter.reset()
    TimescaleAdapter.reset()


def test_trigger_kill_switch_publishes_event_and_sets_state() -> None:
    client = TestClient(app)

    response = client.post(
        "/risk/kill",
        params={"account_id": "Alpha"},
        headers={"X-Account-ID": "company"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
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
