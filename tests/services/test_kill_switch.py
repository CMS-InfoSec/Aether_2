import json
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pytest
from fastapi.testclient import TestClient

from kill_switch import KillSwitchReason, app
from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter


class _DummyResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


class _FakeTimescaleAdapter:
    _configs: Dict[str, Dict[str, Any]] = {}
    _events: Dict[str, List[Dict[str, Any]]] = {}
    _kills: Dict[str, List[Dict[str, Any]]] = {}

    def __init__(self, account_id: str, **_: Any) -> None:
        self.account_id = account_id

    @classmethod
    def reset(cls) -> None:
        cls._configs.clear()
        cls._events.clear()
        cls._kills.clear()

    def _config(self) -> Dict[str, Any]:
        config = self._configs.setdefault(
            self.account_id,
            {
                "kill_switch": False,
                "safe_mode": False,
                "loss_cap": 50_000.0,
            },
        )
        return config

    def set_kill_switch(self, *, engaged: bool, reason: str | None = None, actor: str | None = None) -> None:
        config = self._config()
        config["kill_switch"] = bool(engaged)
        event_payload = {"state": "engaged" if engaged else "released"}
        if reason:
            event_payload["reason"] = reason
        if actor:
            event_payload["actor"] = actor
        events = self._events.setdefault(self.account_id, [])
        events.append({"type": "kill_switch_engaged" if engaged else "kill_switch_released", **event_payload})

    def load_risk_config(self) -> Dict[str, Any]:
        return deepcopy(self._config())

    def events(self) -> Dict[str, List[Dict[str, Any]]]:
        return {"events": [deepcopy(event) for event in self._events.get(self.account_id, [])]}

    def record_kill_event(
        self,
        *,
        reason_code: str,
        triggered_at: datetime,
        channels_sent: List[str],
    ) -> Dict[str, Any]:
        payload = {
            "account_id": self.account_id,
            "reason": reason_code,
            "ts": triggered_at,
            "channels_sent": list(channels_sent),
        }
        entries = self._kills.setdefault(self.account_id, [])
        entries.append(deepcopy(payload))
        return deepcopy(payload)

    def kill_events(self, limit: int | None = None) -> List[Dict[str, Any]]:
        entries = list(self._kills.get(self.account_id, []))
        entries.sort(key=lambda item: item["ts"], reverse=True)
        if limit is not None:
            entries = entries[:limit]
        return [deepcopy(entry) for entry in entries]

    @classmethod
    def all_kill_events(
        cls, *, account_id: str | None = None, limit: int | None = None
    ) -> List[Dict[str, Any]]:
        if account_id is not None:
            entries = list(cls._kills.get(account_id, []))
        else:
            entries = [entry for records in cls._kills.values() for entry in records]
        entries.sort(key=lambda item: item["ts"], reverse=True)
        if limit is not None:
            entries = entries[:limit]
        return [deepcopy(entry) for entry in entries]


@pytest.fixture(autouse=True)
def _patch_timescale(monkeypatch: pytest.MonkeyPatch) -> None:
    KafkaNATSAdapter.reset()
    _FakeTimescaleAdapter.reset()
    monkeypatch.setattr("kill_switch.TimescaleAdapter", _FakeTimescaleAdapter)
    monkeypatch.setattr("services.common.adapters.TimescaleAdapter", _FakeTimescaleAdapter)
    monkeypatch.setitem(globals(), "TimescaleAdapter", _FakeTimescaleAdapter)
    yield
    KafkaNATSAdapter.reset()
    _FakeTimescaleAdapter.reset()


def _configure_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KILL_ALERT_EMAIL_API_KEY", "sendgrid-key")
    monkeypatch.setenv("KILL_ALERT_EMAIL_FROM", "alerts@example.com")
    monkeypatch.setenv("KILL_ALERT_EMAIL_TO", "risk@example.com")
    monkeypatch.setenv("KILL_ALERT_EMAIL_ENDPOINT", "https://sendgrid.test/mail/send")
    monkeypatch.setenv("KILL_ALERT_SMS_ACCOUNT_SID", "AC123")
    monkeypatch.setenv("KILL_ALERT_SMS_AUTH_TOKEN", "token")
    monkeypatch.setenv("KILL_ALERT_SMS_FROM", "+15555550000")
    monkeypatch.setenv("KILL_ALERT_SMS_TO", "+15555550123")
    monkeypatch.setenv("KILL_ALERT_SMS_ENDPOINT", "https://twilio.test/messages")
    monkeypatch.setenv("KILL_ALERT_WEBHOOK_URL", "https://hooks.internal/kill-switch")
    monkeypatch.setenv("KILL_ALERT_WEBHOOK_SECRET", "shhhh")


def test_trigger_kill_switch_publishes_event_and_sets_state(monkeypatch: pytest.MonkeyPatch) -> None:
    client = TestClient(app)

    _configure_env(monkeypatch)

    requests_made: List[Tuple[str, Dict[str, Any]]] = []

    def _fake_post(url: str, **kwargs: Any) -> _DummyResponse:
        requests_made.append((url, kwargs))
        if url == "https://sendgrid.test/mail/send":
            return _DummyResponse(status_code=202)
        if url == "https://twilio.test/messages":
            return _DummyResponse(status_code=201)
        if url == "https://hooks.internal/kill-switch":
            return _DummyResponse(status_code=200)
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("kill_alerts.requests.post", _fake_post)

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
    assert payload["failed_channels"] == []
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

    assert len(requests_made) == 3

    email_request = requests_made[0]
    assert email_request[0] == "https://sendgrid.test/mail/send"
    email_json = email_request[1]["json"]
    assert email_json["from"]["email"] == "alerts@example.com"
    assert email_json["personalizations"][0]["to"] == [{"email": "risk@example.com"}]
    assert email_json["custom_args"]["account_id"] == "alpha"
    assert email_request[1]["headers"]["Authorization"].startswith("Bearer ")

    sms_request = requests_made[1]
    assert sms_request[0] == "https://twilio.test/messages"
    assert sms_request[1]["auth"] == ("AC123", "token")
    assert sms_request[1]["data"]["To"] == "+15555550123"
    assert "Kill switch engaged" in sms_request[1]["data"]["Body"]

    webhook_request = requests_made[2]
    assert webhook_request[0] == "https://hooks.internal/kill-switch"
    headers = webhook_request[1]["headers"]
    body = webhook_request[1]["data"]
    assert headers["Content-Type"] == "application/json"
    parsed_body = json.loads(body.decode("utf-8"))
    assert parsed_body["account_id"] == "alpha"
    assert "X-Signature" in headers


def test_list_kill_events_endpoint_returns_recent_events(monkeypatch: pytest.MonkeyPatch) -> None:
    client = TestClient(app)

    _configure_env(monkeypatch)

    def _fake_post(url: str, **kwargs: Any) -> _DummyResponse:
        if url == "https://sendgrid.test/mail/send":
            return _DummyResponse(status_code=202)
        if url == "https://twilio.test/messages":
            return _DummyResponse(status_code=201)
        if url == "https://hooks.internal/kill-switch":
            return _DummyResponse(status_code=200)
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("kill_alerts.requests.post", _fake_post)

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


def test_partial_notification_failure_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    client = TestClient(app)

    _configure_env(monkeypatch)

    def _fake_post(url: str, **kwargs: Any) -> _DummyResponse:
        if url == "https://sendgrid.test/mail/send":
            return _DummyResponse(status_code=202)
        if url == "https://twilio.test/messages":
            return _DummyResponse(status_code=500, text="twilio down")
        if url == "https://hooks.internal/kill-switch":
            return _DummyResponse(status_code=200)
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("kill_alerts.requests.post", _fake_post)

    response = client.post(
        "/risk/kill",
        params={"account_id": "Gamma", "reason_code": KillSwitchReason.LOSS_CAP_BREACH.value},
        headers={"X-Account-ID": "company"},
    )

    assert response.status_code == 207
    payload = response.json()
    assert payload["status"] == "partial"
    assert payload["channels_sent"] == ["email", "webhook"]
    assert payload["failed_channels"] == ["sms"]
