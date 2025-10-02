
"""Integration tests covering router registration on the main FastAPI app."""

from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


import pack_exporter
from app import create_app

from policy_service import RegimeSnapshot, _reset_regime_state, regime_classifier
from services.models.meta_learner import get_meta_learner, meta_governance_log



class _FakeS3Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def generate_presigned_url(self, operation: str, *, Params: dict, ExpiresIn: int) -> str:
        call = {"operation": operation, "params": Params, "expires": ExpiresIn}
        self.calls.append(call)
        return f"https://example.com/{Params['Key']}?ttl={ExpiresIn}"


def test_create_app_registers_knowledge_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    created_at = dt.datetime(2024, 2, 1, tzinfo=dt.timezone.utc)
    sample = pack_exporter.PackRecord(
        object_key="packs/example.tar.gz",
        sha256="deadbeef",
        created_at=created_at,
        size=4096,
    )

    class _Repo:
        def latest_pack(self) -> pack_exporter.PackRecord | None:
            return sample

    fake_repo = _Repo()
    fake_s3 = _FakeS3Client()

    monkeypatch.setattr(pack_exporter, "KnowledgePackRepository", lambda *_, **__: fake_repo)
    monkeypatch.setattr(pack_exporter, "_require_psycopg", lambda: None)
    monkeypatch.setattr(pack_exporter, "_require_boto3", lambda: None)
    monkeypatch.setattr(pack_exporter, "_storage_config_from_env", lambda: pack_exporter.ObjectStorageConfig(bucket="bucket", prefix="packs"))
    monkeypatch.setattr(pack_exporter, "_s3_client", lambda config: fake_s3)
    monkeypatch.setenv("KNOWLEDGE_PACK_URL_TTL", "900")

    app = create_app()
    client = TestClient(app)

    response = client.get("/knowledge/export/latest")
    assert response.status_code == 200

    payload = response.json()
    assert payload["id"] == sample.object_key
    assert payload["hash"] == sample.sha256
    assert payload["download_url"] == "https://example.com/packs/example.tar.gz?ttl=900"
    assert fake_s3.calls and fake_s3.calls[0]["operation"] == "get_object"


def test_create_app_registers_meta_routes(monkeypatch: pytest.MonkeyPatch) -> None:

    learner = get_meta_learner()
    learner.reset()
    meta_governance_log.reset()
    _reset_regime_state()


    now = dt.datetime.now(dt.timezone.utc)

    learner.record_performance(
        symbol="ETH-USD",
        regime="range",
        model="meanrev_model",

        score=0.8,
        ts=now - dt.timedelta(minutes=5),

    )
    learner.record_performance(
        symbol="ETH-USD",
        regime="range",
        model="trend_model",

        score=0.2,
        ts=now - dt.timedelta(minutes=2),

    )

    snapshot = RegimeSnapshot(
        symbol="ETH-USD",
        regime="range",
        volatility=0.01,

        trend_strength=0.3,
        feature_scale=1.0,
        size_scale=1.0,
        sample_count=50,

        updated_at=now,
    )
    with regime_classifier._lock:  # type: ignore[attr-defined]
        regime_classifier._snapshots["ETH-USD"] = snapshot  # type: ignore[attr-defined]


    app = create_app()
    client = TestClient(app)

    response = client.get(
        "/meta/weights",
        params={"symbol": "ETH-USD"},
        headers={"X-Account-ID": "company"},
    )
    assert response.status_code == 200

    payload = response.json()
    assert payload["symbol"] == "ETH-USD"
    assert payload["regime"] == "range"
    assert sum(payload["weights"].values()) > 0

