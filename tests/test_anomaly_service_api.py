from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Iterator, Tuple

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.pool import StaticPool


@pytest.fixture
def anomaly_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[Tuple[object, TestClient]]:
    monkeypatch.setenv(
        "ANOMALY_DATABASE_URL",
        "postgresql+psycopg://tester:password@localhost:5432/testdb",
    )
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    root_path = Path(__file__).resolve().parents[1]
    for key in list(sys.modules):
        if key == "services" or key.startswith("services."):
            sys.modules.pop(key, None)
    monkeypatch.syspath_prepend(str(root_path))
    import services.alert_manager  # noqa: F401

    def _sqlite_engine(url: str, **kwargs):
        return create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )

    monkeypatch.setattr(sqlalchemy, "create_engine", _sqlite_engine)

    sys.modules.pop("anomaly_service", None)
    module = importlib.import_module("anomaly_service")

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    module.ENGINE = engine
    module.SessionLocal = module.sessionmaker(
        bind=engine, autoflush=False, expire_on_commit=False, future=True
    )
    module.Base.metadata.create_all(bind=engine)

    class _NullDetector:
        def scan_account(self, account_id: str, lookback_minutes: int):
            return []

    class _TestResponseFactory(module.ResponseFactory):
        def __init__(self) -> None:
            super().__init__(
                detector=_NullDetector(),
                repository=module.IncidentRepository(session_factory=module.SessionLocal),
            )

        def _raise_alert(self, incident):
            return None

        def _block_account(self, incident):
            return None

        def _is_blocked(self, account_id: str) -> bool:
            return False

    module._coordinator = _TestResponseFactory()

    client = TestClient(module.app)
    try:
        yield module, client
    finally:
        client.app.dependency_overrides.clear()
        engine.dispose()
        sys.modules.pop("anomaly_service", None)


def test_status_requires_authentication(anomaly_client: Tuple[object, TestClient]) -> None:
    module, client = anomaly_client

    response = client.get("/anomaly/status", params={"account_id": "company"})

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing Authorization header."


def test_status_rejects_account_mismatch(anomaly_client: Tuple[object, TestClient]) -> None:
    module, client = anomaly_client
    client.app.dependency_overrides[module.require_admin_account] = lambda: "company"

    response = client.get("/anomaly/status", params={"account_id": "ops"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Authenticated account does not match requested account."


def test_scan_rejects_payload_mismatch(anomaly_client: Tuple[object, TestClient]) -> None:
    module, client = anomaly_client
    client.app.dependency_overrides[module.require_admin_account] = lambda: "company"

    response = client.post(
        "/anomaly/scan",
        json={"account_id": "ops", "lookback_minutes": 15},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Authenticated account does not match requested account."


def test_scan_allows_matching_account(anomaly_client: Tuple[object, TestClient]) -> None:
    module, client = anomaly_client
    client.app.dependency_overrides[module.require_admin_account] = lambda: "company"

    response = client.post(
        "/anomaly/scan",
        json={"account_id": "company", "lookback_minutes": 15},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["incidents"] == []
    assert body["blocked"] is False
