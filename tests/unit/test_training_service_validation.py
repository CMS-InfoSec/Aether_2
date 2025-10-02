from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for training service validation tests")
pytest.importorskip("sqlalchemy", reason="sqlalchemy is required for training service validation tests")

from fastapi.testclient import TestClient

try:  # pragma: no cover - skip the suite when dependencies are unavailable.
    import training_service
    _training_service_import_error: Exception | None = None
except Exception as exc:  # noqa: BLE001
    training_service = None  # type: ignore[assignment]
    _training_service_import_error = exc


@pytest.fixture
def training_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Provide a test client without scheduling background jobs."""

    if _training_service_import_error is not None:
        pytest.skip(f"training_service unavailable: {_training_service_import_error}")

    async def _noop_register_job(state):  # type: ignore[override]
        return None

    monkeypatch.setattr(training_service, "_register_job", _noop_register_job)
    return TestClient(training_service.app)


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


def test_start_training_rejects_non_usd(training_client: TestClient) -> None:
    response = training_client.post("/ml/train/start", json=_request_payload("BTC-EUR"))
    assert response.status_code == 400
    assert "Only USD-quoted symbols" in response.json()["detail"]
