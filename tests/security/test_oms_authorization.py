from __future__ import annotations

from typing import Iterator

import sys
import types

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

if "aiohttp" not in sys.modules:
    aiohttp_stub = types.ModuleType("aiohttp")

    class _ClientSession:  # pragma: no cover - stub implementation
        async def __aenter__(self) -> "_ClientSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

        async def close(self) -> None:
            return None

        async def get(self, *args, **kwargs):
            raise RuntimeError("aiohttp.ClientSession stub used during tests")

        async def post(self, *args, **kwargs):
            raise RuntimeError("aiohttp.ClientSession stub used during tests")

    class _ClientTimeout:  # pragma: no cover - stub implementation
        def __init__(self, total: float | None = None) -> None:
            self.total = total

    aiohttp_stub.ClientSession = _ClientSession
    aiohttp_stub.ClientTimeout = _ClientTimeout
    sys.modules["aiohttp"] = aiohttp_stub

from services.oms import oms_service


@pytest.fixture(name="oms_client")
def oms_client_fixture() -> Iterator[TestClient]:
    with TestClient(oms_service.app) as client:
        yield client


def test_warm_start_status_requires_credentials(oms_client: TestClient) -> None:
    response = oms_client.get("/oms/warm_start/status", headers={})

    assert response.status_code == 401


def test_warm_start_status_rejects_non_admin(oms_client: TestClient) -> None:
    response = oms_client.get("/oms/warm_start/status", headers={"X-Account-ID": "guest"})

    assert response.status_code == 403


def test_warm_start_status_allows_admin(oms_client: TestClient) -> None:
    response = oms_client.get("/oms/warm_start/status", headers={"X-Account-ID": "company"})

    assert response.status_code == 200
    assert isinstance(response.json(), dict)
