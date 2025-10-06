"""Validate seasonality session store configuration requirements."""

from __future__ import annotations

import sys

import pytest
from fastapi import FastAPI

import services.analytics.seasonality_service as seasonality


def _fresh_app() -> FastAPI:
    application = FastAPI()
    application.state.session_store = None
    return application


def _reset_session_store() -> None:
    seasonality.SESSION_STORE = None
    seasonality.security.set_default_session_store(None)


def test_seasonality_session_store_requires_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SESSION_REDIS_URL", raising=False)
    monkeypatch.delenv("SESSION_STORE_URL", raising=False)
    monkeypatch.delenv("SESSION_BACKEND_DSN", raising=False)

    _reset_session_store()

    with pytest.raises(RuntimeError, match="Session store misconfigured"):
        seasonality._configure_session_store(_fresh_app())


def test_seasonality_session_store_allows_memory_in_pytest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://seasonality-tests")
    _reset_session_store()

    store = seasonality._configure_session_store(_fresh_app())

    from auth.service import InMemorySessionStore

    assert isinstance(store, InMemorySessionStore)


def test_seasonality_session_store_rejects_memory_outside_pytest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://seasonality-prod")
    _reset_session_store()
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    with pytest.raises(RuntimeError, match="memory:// DSNs are only supported"):
        seasonality._configure_session_store(_fresh_app())
