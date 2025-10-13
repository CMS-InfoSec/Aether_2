from __future__ import annotations

import pytest

from shared import dependency_alerts


def test_notify_dependency_fallback_skips_in_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def _push(**kwargs):  # type: ignore[no-redef]
        calls.append(kwargs)

    monkeypatch.setattr(dependency_alerts, "push_dependency_fallback", _push)
    monkeypatch.delenv("AETHER_ENABLE_ALERTS_IN_TESTS", raising=False)

    dependency_alerts.notify_dependency_fallback(
        component="auth-service",
        dependency="argon2-cffi",
        fallback="pbkdf2",
        reason="module import failed",
    )

    assert calls == []


def test_notify_dependency_fallback_invokes_push_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def _push(**kwargs):  # type: ignore[no-redef]
        calls.append(kwargs)

    monkeypatch.setenv("AETHER_ENABLE_ALERTS_IN_TESTS", "1")
    monkeypatch.setattr(dependency_alerts, "push_dependency_fallback", _push)

    dependency_alerts.notify_dependency_fallback(
        component="watchdog-service",
        dependency="psycopg",
        fallback="sqlite",
        reason="psycopg import failed",
        metadata={"fallback_url": "sqlite://"},
    )

    assert calls
    payload = calls[0]
    assert payload["component"] == "watchdog-service"
    assert payload["dependency"] == "psycopg"
    assert payload["details"]["fallback_url"] == "sqlite://"
