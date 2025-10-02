from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from app import create_app
import pack_exporter


class _StubS3Client:
    def generate_presigned_url(self, operation: str, *, Params: dict, ExpiresIn: int) -> str:
        return f"https://example.com/{Params['Key']}?ttl={ExpiresIn}"


def test_knowledge_router_is_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    sample = pack_exporter.PackRecord(
        object_key="packs/pack.tar.gz",
        sha256="abc123",
        created_at=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        size=1024,
    )

    class _Repo:
        def latest_pack(self) -> pack_exporter.PackRecord | None:
            return sample

    monkeypatch.setattr(pack_exporter, "_require_psycopg", lambda: None)
    monkeypatch.setattr(pack_exporter, "_require_boto3", lambda: None)
    monkeypatch.setattr(pack_exporter, "KnowledgePackRepository", lambda: _Repo())
    monkeypatch.setattr(
        pack_exporter,
        "_storage_config_from_env",
        lambda: pack_exporter.ObjectStorageConfig(bucket="bucket", prefix="packs"),
    )
    monkeypatch.setattr(pack_exporter, "_s3_client", lambda config: _StubS3Client())
    monkeypatch.setenv("KNOWLEDGE_PACK_URL_TTL", "600")

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/knowledge/export/latest")

    assert response.status_code == 200
    payload = response.json()

    assert payload == {
        "id": sample.object_key,
        "hash": sample.sha256,
        "size": sample.size,
        "ts": sample.created_at.isoformat(),
        "download_url": "https://example.com/packs/pack.tar.gz?ttl=600",
    }
