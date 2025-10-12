"""Unit tests for the Vault health monitoring job."""

from __future__ import annotations

import json

import pytest

from ops.monitoring.vault_health import (
    HttpResponse,
    VaultHealthConfig,
    VaultHealthError,
    check_vault_health,
    parse_health,
)


def _make_response(status_code: int, payload: dict[str, object]) -> HttpResponse:
    return HttpResponse(
        status_code=status_code,
        body=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )


def test_parse_health_marks_active_instance_as_healthy() -> None:
    response = _make_response(200, {"sealed": False, "initialized": True, "message": "active"})

    status = parse_health(response)

    assert status.is_healthy
    assert status.sealed is False
    assert status.status_code == 200


def test_parse_health_considers_standby_healthy_when_unsealed() -> None:
    response = _make_response(429, {"sealed": False, "standby": True})

    status = parse_health(response)

    assert status.is_healthy
    assert status.sealed is False
    assert status.status_code == 429


def test_parse_health_marks_sealed_instance_unhealthy() -> None:
    response = _make_response(503, {"sealed": True, "initialized": True})

    status = parse_health(response)

    assert not status.is_healthy
    assert status.sealed is True
    assert status.status_code == 503


def test_check_vault_health_raises_and_notifies_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    config = VaultHealthConfig(address="https://vault.example.com", slack_webhook="https://hooks.slack")
    response = _make_response(503, {"sealed": True})
    notifications: list[str] = []

    def fake_fetch_health(_: VaultHealthConfig) -> HttpResponse:
        return response

    def fake_notify(webhook: str | None, text: str, emoji: str) -> None:
        notifications.append(text)
        assert webhook == config.slack_webhook
        assert emoji == ":rotating_light:"

    monkeypatch.setattr("ops.monitoring.vault_health.fetch_health", fake_fetch_health)
    monkeypatch.setattr("ops.monitoring.vault_health._send_slack_message", fake_notify)

    with pytest.raises(VaultHealthError):
        check_vault_health(config)

    assert notifications


def test_check_vault_health_raises_on_unreachable_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    config = VaultHealthConfig(address="https://vault.example.com")

    def fake_fetch_health(_: VaultHealthConfig) -> HttpResponse:
        raise VaultHealthError("Vault health endpoint unreachable: timeout")

    monkeypatch.setattr("ops.monitoring.vault_health.fetch_health", fake_fetch_health)
    monkeypatch.setattr("ops.monitoring.vault_health._send_slack_message", lambda *_: None)

    with pytest.raises(VaultHealthError):
        check_vault_health(config)
