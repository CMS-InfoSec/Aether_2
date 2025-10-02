
from __future__ import annotations


import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


from app import create_app
import pack_exporter
from services.common.security import require_admin_account


def test_knowledge_router_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pack_exporter, "_require_psycopg", lambda: None)
    monkeypatch.setattr(pack_exporter, "_require_boto3", lambda: None)

    class Repo:
        def latest_pack(self) -> pack_exporter.PackRecord | None:
            return None

    monkeypatch.setattr(pack_exporter, "KnowledgePackRepository", lambda *_, **__: Repo())

    app = create_app()
    app.dependency_overrides[require_admin_account] = lambda: "company"
    with TestClient(app) as client:
        response = client.get("/knowledge/export/latest")

    assert response.status_code == 404
    assert response.json() == {"detail": "No knowledge pack available"}


def test_meta_router_available() -> None:
    app = create_app()
    with TestClient(app) as client:
        response = client.get(
            "/meta/weights",
            params={"symbol": "BTC-USD"},
            headers={"X-Account-ID": "company"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "BTC-USD"
    assert body["regime"] in {"range", "trend", "high_vol"}
    weights = body["weights"]
    assert set(weights.keys()) >= {"trend_model", "meanrev_model", "vol_breakout"}

