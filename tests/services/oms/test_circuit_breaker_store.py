"""Regression tests for the OMS circuit breaker persistence backend."""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any, Tuple

import pytest

from common.utils.redis import InMemoryRedis

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure Redis-related environment variables do not leak across tests."""

    for key in (
        "OMS_CIRCUIT_BREAKER_REDIS_URL",
        "SESSION_REDIS_URL",
        "SESSION_STORE_URL",
        "SESSION_BACKEND_DSN",
    ):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture()
def store_module(monkeypatch: pytest.MonkeyPatch):
    """Import a fresh copy of the circuit breaker store module for each test."""

    monkeypatch.syspath_prepend(str(ROOT))
    for name in (
        "services.oms.circuit_breaker_store",
        "services.oms",
        "services",
    ):
        sys.modules.pop(name, None)
    return importlib.import_module("services.oms.circuit_breaker_store")


def test_create_default_client_requires_configuration(store_module) -> None:
    """The store must fail fast when no Redis DSN is configured."""

    with pytest.raises(RuntimeError, match="requires a Redis URL"):
        store_module.CircuitBreakerStateStore._create_default_client()


def test_create_default_client_rejects_blank_configuration(
    store_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OMS_CIRCUIT_BREAKER_REDIS_URL", "   ")

    with pytest.raises(RuntimeError, match="requires a Redis URL"):
        store_module.CircuitBreakerStateStore._create_default_client()


def test_create_default_client_supports_memory_scheme(
    store_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OMS_CIRCUIT_BREAKER_REDIS_URL", "memory://stub")

    client = store_module.CircuitBreakerStateStore._create_default_client()
    assert isinstance(client, InMemoryRedis)


def test_create_default_client_rejects_stub_when_not_memory(
    store_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OMS_CIRCUIT_BREAKER_REDIS_URL", "redis://cache:6379/0")

    def _fake_create(url: str, *, decode_responses: bool, logger: Any) -> Tuple[object, bool]:
        assert url == "redis://cache:6379/0"
        assert decode_responses is True
        return object(), True

    monkeypatch.setattr(
        store_module, "create_redis_from_url", _fake_create
    )

    with pytest.raises(RuntimeError, match="could not connect to Redis"):
        store_module.CircuitBreakerStateStore._create_default_client()
