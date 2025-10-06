"""Authorization tests for the benchmark service."""

from __future__ import annotations

from datetime import datetime, timezone
import sys

import pytest

pytest.importorskip("fastapi")

from fastapi import HTTPException
from fastapi.testclient import TestClient

from tests.helpers.benchmark_service import bootstrap_benchmark_service


@pytest.fixture()
def benchmark_module(tmp_path, monkeypatch):
    module = bootstrap_benchmark_service(tmp_path, monkeypatch, reset=True)
    try:
        yield module
    finally:
        module.ENGINE.dispose()
        sys.modules.pop("benchmark_service", None)


@pytest.fixture()
def benchmark_client(benchmark_module) -> TestClient:
    with TestClient(benchmark_module.app) as client:
        yield client


def _sample_payload() -> dict[str, object]:
    return {
        "account_id": "acct-123",
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "aether_return": 0.5,
        "btc_return": 0.4,
    }


def test_upsert_rejects_unauthorized_callers(
    benchmark_module, benchmark_client: TestClient
) -> None:
    def deny() -> str:
        raise HTTPException(status_code=403, detail="forbidden")

    benchmark_client.app.dependency_overrides[benchmark_module.require_admin_account] = deny
    try:
        response = benchmark_client.post("/benchmark/curves", json=_sample_payload())
        assert response.status_code == 403
    finally:
        benchmark_client.app.dependency_overrides.pop(
            benchmark_module.require_admin_account, None
        )


def test_benchmark_endpoints_allow_admin_override(
    benchmark_module, benchmark_client: TestClient
) -> None:
    benchmark_client.app.dependency_overrides[benchmark_module.require_admin_account] = (
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
            benchmark_module.require_admin_account, None
        )
