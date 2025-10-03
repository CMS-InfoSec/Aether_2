import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import importlib
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("fastapi", reason="fastapi is required for training service validation tests")
pytest.importorskip("sqlalchemy", reason="sqlalchemy is required for training service validation tests")

from fastapi import Header, HTTPException, status

from fastapi.testclient import TestClient

try:
    importlib.import_module("services.common.security")
except ModuleNotFoundError:
    services_module = sys.modules.setdefault("services", types.ModuleType("services"))
    common_module = sys.modules.setdefault("services.common", types.ModuleType("services.common"))
    setattr(services_module, "common", common_module)

    security_module = types.ModuleType("services.common.security")

    def require_admin_account(
        authorization: str | None = Header(None, alias="Authorization"),
        x_account_id: str | None = Header(None, alias="X-Account-ID"),
    ) -> str:
        _ = x_account_id
        if authorization is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing Authorization header.",
            )
        return authorization

    security_module.require_admin_account = require_admin_account
    sys.modules["services.common.security"] = security_module
    setattr(common_module, "security", security_module)

try:  # pragma: no cover - skip the suite when dependencies are unavailable.
    import training_service
    _training_service_import_error: Exception | None = None
except Exception as exc:  # noqa: BLE001
    training_service = None  # type: ignore[assignment]
    _training_service_import_error = exc


def _build_training_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    if _training_service_import_error is not None:
        pytest.skip(f"training_service unavailable: {_training_service_import_error}")

    async def _noop_register_job(state):  # type: ignore[override]
        return None

    monkeypatch.setattr(training_service, "_register_job", _noop_register_job)
    return TestClient(training_service.app)


@pytest.fixture
def authorized_training_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    client = _build_training_client(monkeypatch)
    client.app.dependency_overrides[training_service.require_admin_account] = lambda: "ops-admin"
    try:
        yield client
    finally:
        client.app.dependency_overrides.pop(training_service.require_admin_account, None)


@pytest.fixture
def unauthorized_training_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    client = _build_training_client(monkeypatch)
    try:
        yield client
    finally:
        client.app.dependency_overrides.pop(training_service.require_admin_account, None)


def _request_payload(symbol: str) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    return {
        "symbols": [symbol],
        "from": (now - timedelta(days=1)).isoformat(),
        "to": now.isoformat(),
        "gran": ["1h"],
        "feature_version": "v1",
        "model": "lstm",
        "curriculum": False,
        "label_horizon": "15m",
        "run_name": "test-run",
    }


def test_start_training_rejects_non_usd(authorized_training_client: TestClient) -> None:
    response = authorized_training_client.post("/ml/train/start", json=_request_payload("BTC-EUR"))
    assert response.status_code == 400
    assert "Only USD-quoted symbols" in response.json()["detail"]


def test_start_training_requires_admin(unauthorized_training_client: TestClient) -> None:
    response = unauthorized_training_client.post("/ml/train/start", json=_request_payload("BTC-USD"))
    assert response.status_code == 401
    assert "Missing Authorization" in response.json()["detail"]


def test_start_training_allows_admin(authorized_training_client: TestClient) -> None:
    response = authorized_training_client.post("/ml/train/start", json=_request_payload("BTC-USD"))
    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] == "queued"
    assert payload["run_id"]


def test_promote_model_requires_admin(unauthorized_training_client: TestClient) -> None:
    response = unauthorized_training_client.post(
        "/ml/train/promote",
        json={"model_run_id": "run-x", "stage": "Production"},
    )
    assert response.status_code == 401


def test_promote_model_allows_admin(authorized_training_client: TestClient) -> None:
    run_id = f"run-{uuid4()}"
    with training_service.session_scope() as session:
        session.merge(
            training_service.TrainingRunRecord(
                run_id=run_id,
                run_name="audit-test",
                status="completed",
                current_step="finished",
                request_payload={"symbols": ["BTC-USD"]},
                correlation_id=str(uuid4()),
                feature_version="v1",
                label_horizon="15m",
                model_type="lstm",
                curriculum=0,
                metrics={
                    "sharpe": 2.0,
                    "sortino": 2.0,
                    "max_drawdown": -0.1,
                    "hit_rate": 0.75,
                    "fee_aware_pnl": 10.0,
                },
                started_at=training_service._now(),
                finished_at=training_service._now(),
            )
        )

    response = authorized_training_client.post(
        "/ml/train/promote",
        json={"model_run_id": run_id, "stage": "Production"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "promoted"


def test_get_status_requires_admin(unauthorized_training_client: TestClient) -> None:
    response = unauthorized_training_client.get("/ml/train/status", params={"run_id": "missing"})
    assert response.status_code == 401


def test_get_status_allows_admin(authorized_training_client: TestClient) -> None:
    run_id = f"run-{uuid4()}"
    with training_service.session_scope() as session:
        session.merge(
            training_service.TrainingRunRecord(
                run_id=run_id,
                run_name="status-test",
                status="completed",
                current_step="done",
                request_payload={"symbols": ["BTC-USD"]},
                correlation_id=str(uuid4()),
                feature_version="v1",
                label_horizon="15m",
                model_type="lstm",
                curriculum=0,
                metrics={"sharpe": 1.5},
                started_at=training_service._now(),
                finished_at=training_service._now(),
            )
        )

    response = authorized_training_client.get("/ml/train/status", params={"run_id": run_id})
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
