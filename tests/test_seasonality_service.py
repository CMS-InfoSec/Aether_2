"""Security regression tests for the seasonality analytics service."""

from __future__ import annotations

import importlib
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip(
    "fastapi", reason="FastAPI is required for the seasonality service tests"
)

from fastapi.testclient import TestClient


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture()
def seasonality_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide an isolated seasonality service client with in-memory auth."""

    monkeypatch.syspath_prepend(str(ROOT))
    monkeypatch.setenv(
        "PYTHONPATH", str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")
    )
    monkeypatch.setenv(
        "SEASONALITY_DATABASE_URI", f"sqlite:///{tmp_path}/seasonality.db"
    )
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://seasonality-tests")
    monkeypatch.setenv("SESSION_TTL_MINUTES", "60")

    previous_modules = {
        "services.analytics.seasonality_service": sys.modules.get(
            "services.analytics.seasonality_service"
        ),
        "services.analytics": sys.modules.get("services.analytics"),
        "services.common.security": sys.modules.get("services.common.security"),
        "services.common": sys.modules.get("services.common"),
        "services": sys.modules.get("services"),
    }

    for name in list(previous_modules):
        sys.modules.pop(name, None)

    module = importlib.import_module("services.analytics.seasonality_service")
    security = importlib.import_module("services.common.security")

    module.OhlcvBase.metadata.create_all(bind=module.ENGINE)

    client = TestClient(module.app)
    store = module.SESSION_STORE

    try:
        yield client, module, store
    finally:
        client.app.dependency_overrides.clear()
        security.set_default_session_store(None)
        for name, previous in previous_modules.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


def test_day_of_week_requires_authentication(seasonality_client) -> None:
    client, _module, _store = seasonality_client

    response = client.get(
        "/seasonality/dayofweek",
        params={"symbol": "BTC/USD"},
    )

    assert response.status_code == 401


def test_day_of_week_rejects_non_admin_account(seasonality_client) -> None:
    client, _module, store = seasonality_client
    session = store.create("shadow")

    response = client.get(
        "/seasonality/dayofweek",
        params={"symbol": "BTC/USD"},
        headers=_auth_headers(session.token),
    )

    assert response.status_code == 403


def test_day_of_week_allows_authorised_admin(seasonality_client) -> None:
    client, module, store = seasonality_client
    session = store.create("company")

    with module.SessionLocal() as db:
        base_ts = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        for offset in range(3):
            db.add(
                module.OhlcvBar(
                    market="BTC/USD",
                    bucket_start=base_ts + timedelta(hours=offset),
                    open=100.0 + offset,
                    high=105.0 + offset,
                    low=95.0 + offset,
                    close=100.0 + (offset * 5.0),
                    volume=10.0 + offset,
                )
            )
        db.commit()

    response = client.get(
        "/seasonality/dayofweek",
        params={"symbol": "BTC/USD"},
        headers=_auth_headers(session.token),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTC/USD"
    assert len(payload["metrics"]) == 7
