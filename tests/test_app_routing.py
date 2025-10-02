from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from app import create_app
import pack_exporter
from policy_service import RegimeSnapshot, _reset_regime_state, regime_classifier
from services.models.meta_learner import get_meta_learner, meta_governance_log


def test_create_app_exposes_knowledge_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    sample = pack_exporter.PackRecord(
        object_key="packs/sample.tar.gz",
        sha256="abc123",
        created_at=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        size=1024,
    )

    class Repository:
        def latest_pack(self) -> pack_exporter.PackRecord | None:
            return sample

    class StubClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def generate_presigned_url(self, operation: str, *, Params: dict, ExpiresIn: int) -> str:
            self.calls.append({"operation": operation, "params": Params, "expires": ExpiresIn})
            return "https://example.com/sample.tar.gz"

    stub_client = StubClient()

    monkeypatch.setattr(pack_exporter, "_require_psycopg", lambda: None)
    monkeypatch.setattr(pack_exporter, "_require_boto3", lambda: None)
    monkeypatch.setattr(pack_exporter, "KnowledgePackRepository", lambda: Repository())
    monkeypatch.setattr(
        pack_exporter,
        "_storage_config_from_env",
        lambda: pack_exporter.ObjectStorageConfig(bucket="bucket", prefix="packs"),
    )
    monkeypatch.setattr(pack_exporter, "_s3_client", lambda config: stub_client)
    monkeypatch.setenv("KNOWLEDGE_PACK_URL_TTL", "600")

    with TestClient(create_app()) as client:
        response = client.get("/knowledge/export/latest")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == sample.object_key
    assert payload["download_url"] == "https://example.com/sample.tar.gz"
    assert stub_client.calls[0]["expires"] == 600


def test_create_app_exposes_meta_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    learner = get_meta_learner()
    learner.reset()
    meta_governance_log.reset()
    _reset_regime_state()

    now = dt.datetime(2024, 6, 1, tzinfo=dt.timezone.utc)
    learner.record_performance(
        symbol="ETH-USD",
        regime="range",
        model="meanrev_model",
        score=0.9,
        ts=now - dt.timedelta(minutes=15),
    )
    learner.record_performance(
        symbol="ETH-USD",
        regime="range",
        model="trend_model",
        score=0.4,
        ts=now - dt.timedelta(minutes=7),
    )

    snapshot = RegimeSnapshot(
        symbol="ETH-USD",
        regime="range",
        volatility=0.01,
        trend_strength=0.2,
        feature_scale=1.0,
        size_scale=0.85,
        sample_count=30,
        updated_at=now,
    )
    with regime_classifier._lock:  # type: ignore[attr-defined]
        regime_classifier._snapshots["ETH-USD"] = snapshot  # type: ignore[attr-defined]

    with TestClient(create_app()) as client:
        response = client.get(
            "/meta/weights",
            params={"symbol": "ETH-USD"},
            headers={"X-Account-ID": "company"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "ETH-USD"
    assert payload["regime"] == "range"
    assert set(payload["weights"]) >= {"meanrev_model", "trend_model"}
