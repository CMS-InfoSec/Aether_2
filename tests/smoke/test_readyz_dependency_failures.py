from __future__ import annotations

import pytest

pytest.importorskip("fastapi", reason="FastAPI is required for readiness smoke tests")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared import readiness
from shared.readyz_router import ReadyzRouter


class _FailingPostgresReader:
    async def fetchval(self, query: str, *args, **kwargs):  # pragma: no cover - simple stub
        raise RuntimeError("connection refused")


class _ReadOnlyPostgresExecutor:
    async def fetchval(self, query: str, *args, **kwargs):  # pragma: no cover - simple stub
        return True

    async def execute(self, query: str, *args, **kwargs):  # pragma: no cover - unused stub
        return None


class _FailingRedisClient:
    async def ping(self):  # pragma: no cover - simple stub
        raise RuntimeError("redis timeout")


class _FailingKafkaClient:
    async def list_topics(self):  # pragma: no cover - simple stub
        raise RuntimeError("metadata fetch failed")


def _invoke_readyz(check_name: str, probe):
    app = FastAPI()
    router = ReadyzRouter()
    router.register_probe(check_name, probe)
    app.include_router(router.router)

    with TestClient(app) as client:
        return client.get("/readyz")


@pytest.mark.parametrize(
    "check_name, probe, expected_substring",
    [
        (
            "postgres_read",
            lambda: readiness.postgres_read_probe(client=_FailingPostgresReader()),
            "read query failed",
        ),
        (
            "postgres_write",
            lambda: readiness.postgres_write_probe(client=_ReadOnlyPostgresExecutor()),
            "read-only mode",
        ),
        (
            "redis",
            lambda: readiness.redis_ping_probe(client=_FailingRedisClient()),
            "ping failed",
        ),
        (
            "kafka",
            lambda: readiness.kafka_metadata_probe(client=_FailingKafkaClient()),
            "metadata retrieval failed",
        ),
        (
            "optional_provider",
            lambda: readiness.redis_ping_probe(),
            "requires either an explicit client instance or a provider callback",
        ),
    ],
    ids=[
        "postgres-read-failure",
        "postgres-write-readonly",
        "redis-failure",
        "kafka-failure",
        "optional-provider-missing",
    ],
)
def test_readyz_returns_503_when_dependency_fails(check_name, probe, expected_substring) -> None:
    async def _probe_wrapper():
        await probe()

    response = _invoke_readyz(check_name, _probe_wrapper)

    assert response.status_code == 503
    payload = response.json()
    assert payload["status"] == "error"
    assert check_name in payload["checks"]
    check_payload = payload["checks"][check_name]
    assert check_payload["status"] == "error"
    assert expected_substring in check_payload.get("error", "")
