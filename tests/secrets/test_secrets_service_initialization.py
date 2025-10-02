from __future__ import annotations

import base64

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

import secrets_service
from secrets_service import ConfigException


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch):
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", base64.b64encode(b"0" * 32).decode())

    secrets_service.SETTINGS = None
    secrets_service.CIPHER = None
    secrets_service.secret_manager = None
    secrets_service.STATE.settings = None
    secrets_service.STATE.cipher = None
    secrets_service.STATE.initialization_error = None


@pytest.fixture(name="stub_core_api")
def _stub_core_api(monkeypatch):
    class _StubCore:
        pass

    monkeypatch.setattr(secrets_service.client, "CoreV1Api", lambda: _StubCore())


async def _immediate_sleep(_: float) -> None:
    return None


@pytest.mark.asyncio
async def test_initialize_dependencies_success(monkeypatch, stub_core_api):
    monkeypatch.setattr(secrets_service.config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(secrets_service.config, "load_kube_config", lambda: None)

    await secrets_service.initialize_dependencies(force=True)

    assert secrets_service.secret_manager is not None
    assert secrets_service.CIPHER is not None
    assert secrets_service.STATE.initialization_error is None


@pytest.mark.asyncio
async def test_initialize_dependencies_retries_then_succeeds(monkeypatch, stub_core_api):
    monkeypatch.setattr(secrets_service.config, "load_incluster_config", lambda: None)

    call_count = {"total": 0}

    def _flaky_kube_config() -> None:
        call_count["total"] += 1
        if call_count["total"] < 2:
            raise ConfigException("temporary failure")

    monkeypatch.setattr(secrets_service.config, "load_kube_config", _flaky_kube_config)
    monkeypatch.setattr(secrets_service.asyncio, "sleep", _immediate_sleep)

    await secrets_service.initialize_dependencies(force=True)

    assert call_count["total"] == 2
    assert secrets_service.secret_manager is not None
    assert secrets_service.STATE.initialization_error is None


def test_requests_return_503_until_config_loaded(monkeypatch, stub_core_api):
    def _always_fail() -> None:
        raise ConfigException("cluster unavailable")

    monkeypatch.setattr(secrets_service.config, "load_incluster_config", _always_fail)
    monkeypatch.setattr(secrets_service.config, "load_kube_config", _always_fail)
    monkeypatch.setattr(secrets_service.asyncio, "sleep", _immediate_sleep)

    with TestClient(secrets_service.app) as client:
        response = client.get("/secrets/kraken/status", params={"account_id": "abc"})

    assert response.status_code == 503
    assert response.json()["detail"] == secrets_service._UNAVAILABLE_MESSAGE
    assert isinstance(secrets_service.STATE.initialization_error, ConfigException)
