"""Regression coverage for admin session store fallbacks."""

from __future__ import annotations

from pathlib import Path

import importlib

import pytest

auth_service = importlib.import_module("auth.service")


class _StubRedis:
    """Minimal Redis stub used to trigger fallback paths."""

    def __init__(self, *, decode_responses: bool = True) -> None:
        self.decode_responses = decode_responses

    def setex(self, *args, **kwargs):  # pragma: no cover - not used in tests
        raise AssertionError("Redis stub should not be used when fallback is active")

    def get(self, key: str):  # pragma: no cover - not used in tests
        return None


def _make_stub_client(*args, **kwargs):
    return _StubRedis(), True


def test_build_session_store_rejects_whitespace_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def _recording_stub(*args, **kwargs):
        calls["count"] += 1
        return _StubRedis(), False

    monkeypatch.setattr(auth_service, "create_redis_from_url", _recording_stub)

    with pytest.raises(ValueError, match="whitespace"):
        auth_service.build_session_store_from_url("redis://localhost/0\n")

    assert calls["count"] == 0


def test_build_session_store_rejects_invalid_schemes() -> None:
    with pytest.raises(ValueError, match="redis:// or rediss://"):
        auth_service.build_session_store_from_url("http://localhost:6379/0")


def test_build_session_store_requires_hostname() -> None:
    with pytest.raises(ValueError, match="hostname"):
        auth_service.build_session_store_from_url("redis:///0")


def test_session_store_requires_explicit_insecure_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fallbacks must be explicitly enabled to avoid silent security downgrades."""

    monkeypatch.delenv("AUTH_ALLOW_INSECURE_DEFAULTS", raising=False)
    monkeypatch.delenv("AETHER_ALLOW_INSECURE_TEST_DEFAULTS", raising=False)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(auth_service, "create_redis_from_url", _make_stub_client)
    monkeypatch.setattr(auth_service, "_insecure_defaults_enabled", lambda: False)

    with pytest.raises(RuntimeError, match="requires a reachable Redis instance"):
        auth_service.build_session_store_from_url("redis://localhost/0")


def test_file_backed_session_store_persists_sessions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When insecure defaults are enabled sessions should persist across instances."""

    monkeypatch.setenv("AUTH_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(auth_service, "create_redis_from_url", _make_stub_client)
    monkeypatch.setattr(auth_service, "_insecure_defaults_enabled", lambda: True)

    store = auth_service.build_session_store_from_url("redis://localhost/0", ttl_minutes=10)
    session = store.create("admin-123")

    # A second store should reload the persisted session from disk
    reloaded = auth_service.build_session_store_from_url("redis://localhost/0", ttl_minutes=10)
    restored = reloaded.get(session.token)

    assert restored is not None
    assert restored.token == session.token
    assert restored.admin_id == "admin-123"
