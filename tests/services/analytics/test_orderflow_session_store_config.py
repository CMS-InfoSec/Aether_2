"""Regression tests for the orderflow session store configuration safeguards."""

from __future__ import annotations

import importlib
import sys

import pytest
from fastapi import FastAPI


MODULE_PATH = "services.analytics.orderflow_service"


def _reload_orderflow_module() -> object:
    sys.modules.pop(MODULE_PATH, None)
    return importlib.import_module(MODULE_PATH)


def test_orderflow_session_store_requires_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SESSION_REDIS_URL", raising=False)
    monkeypatch.delenv("SESSION_STORE_URL", raising=False)
    monkeypatch.delenv("SESSION_BACKEND_DSN", raising=False)

    module = _reload_orderflow_module()

    with pytest.raises(RuntimeError, match="Session store misconfigured"):
        module._configure_session_store(FastAPI())


def test_orderflow_session_store_allows_memory_in_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_orderflow_module()
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://orderflow-tests")

    store = module._configure_session_store(FastAPI())

    from auth.service import InMemorySessionStore

    assert isinstance(store, InMemorySessionStore)


def test_orderflow_session_store_rejects_memory_outside_pytest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _reload_orderflow_module()
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://orderflow-prod")
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    with pytest.raises(RuntimeError, match="memory:// DSNs are only permitted"):
        module._configure_session_store(FastAPI())
