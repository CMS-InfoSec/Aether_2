from __future__ import annotations

import pytest

import alerts


def test_push_dependency_fallback_builds_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _capture(payload, alertmanager_url=None):  # type: ignore[no-redef]
        captured["payload"] = payload
        captured["url"] = alertmanager_url

    monkeypatch.setattr(alerts, "_post_alert", _capture)

    alerts.push_dependency_fallback(
        component="auth-service",
        dependency="argon2-cffi",
        fallback="pbkdf2",
        severity="critical",
        environment="staging",
        details={"reason": "module import failed"},
        alertmanager_url="http://example.com",
    )

    payload = captured["payload"]
    assert payload["labels"]["component"] == "auth-service"  # type: ignore[index]
    assert payload["labels"]["dependency"] == "argon2-cffi"  # type: ignore[index]
    assert payload["labels"]["environment"] == "staging"  # type: ignore[index]
    assert payload["annotations"]["detail_reason"] == "module import failed"  # type: ignore[index]
    assert "fallback" in payload["labels"]  # type: ignore[index]


def test_push_dependency_fallback_wraps_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*args, **kwargs):  # type: ignore[no-redef]
        raise RuntimeError("boom")

    monkeypatch.setattr(alerts, "_post_alert", _boom)

    with pytest.raises(alerts.AlertPushError):
        alerts.push_dependency_fallback(
            component="risk-service",
            dependency="psycopg",
            fallback="sqlite",
        )
