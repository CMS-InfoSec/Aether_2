"""Authorization tests for the benchmark service."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("fastapi")

from fastapi import HTTPException
from fastapi.testclient import TestClient

import benchmark_service


@pytest.fixture()
def benchmark_client() -> TestClient:
    with TestClient(benchmark_service.app) as client:
        yield client


def _sample_payload() -> dict[str, object]:
    return {
        "account_id": "acct-123",
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "aether_return": 0.5,
        "btc_return": 0.4,
    }


def test_upsert_rejects_unauthorized_callers(benchmark_client: TestClient) -> None:
    def deny() -> str:
        raise HTTPException(status_code=403, detail="forbidden")

    benchmark_client.app.dependency_overrides[benchmark_service.require_admin_account] = deny
    try:
        response = benchmark_client.post("/benchmark/curves", json=_sample_payload())
        assert response.status_code == 403
    finally:
        benchmark_client.app.dependency_overrides.pop(
            benchmark_service.require_admin_account, None
        )


def test_benchmark_endpoints_allow_admin_override(benchmark_client: TestClient) -> None:
    benchmark_client.app.dependency_overrides[benchmark_service.require_admin_account] = (
        lambda: "ops-admin"
    )
    try:
        payload = _sample_payload()
        post_response = benchmark_client.post("/benchmark/curves", json=payload)
        assert post_response.status_code == 204

        comparison = benchmark_client.get(
            "/benchmark/compare",
            params={"account_id": payload["account_id"], "date": payload["ts"]},
        )
        assert comparison.status_code == 200
        body = comparison.json()
        assert "aether_return" in body
    finally:
        benchmark_client.app.dependency_overrides.pop(
            benchmark_service.require_admin_account, None
        )
