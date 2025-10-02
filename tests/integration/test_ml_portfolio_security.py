"""Integration tests covering ML training bootstrap and portfolio scoping."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

pytest.importorskip("fastapi", reason="fastapi is required for API integration tests")

from fastapi.testclient import TestClient


def _issue_training_request(client: TestClient) -> Dict[str, Any]:
    """Helper to kick off the ML training bootstrap workflow."""

    payload = {
        "pair": "BTC/USD",
        "from": (datetime(2024, 1, 1, tzinfo=timezone.utc)).isoformat(),
        "to": (datetime(2024, 1, 2, tzinfo=timezone.utc)).isoformat(),
        "granularity": "1m",
    }
    response = client.post("/ml/train/start", json=payload)
    try:
        body = response.json() if response.content else None
    except ValueError:
        body = response.text
    return {"status": response.status_code, "payload": body}


@pytest.mark.integration
def test_training_bootstrap_populates_all_backends(monkeypatch: pytest.MonkeyPatch) -> None:
    """Trigger the bootstrap endpoint and assert every backend is touched."""

    from app import create_app
    from ml.training import workflow as training_workflow
    from services import coingecko_ingest

    app = create_app()
    client = TestClient(app)

    loader_calls: List[Dict[str, Any]] = []
    upsert_calls: List[List[Any]] = []
    feast_materializations: List[str] = []
    mlflow_events: Dict[str, Any] = {"runs": 0, "registrations": []}

    async def _fake_fetch_market_chart(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        loader_calls.append({"args": args, "kwargs": kwargs})
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        return {
            "prices": [
                [int(start.timestamp()) * 1000, 42000.0],
                [int((start + timedelta(hours=1)).timestamp()) * 1000, 42100.0],
            ],
            "total_volumes": [
                [int(start.timestamp()) * 1000, 1000.0],
                [int((start + timedelta(hours=1)).timestamp()) * 1000, 1500.0],
            ],
        }

    def _fake_aggregate_daily_rows(symbol: str, payload: Dict[str, Any], *_: Any) -> List[coingecko_ingest.OHLCVRow]:
        return [
            coingecko_ingest.OHLCVRow(
                symbol=symbol,
                ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
                open=42000.0,
                high=42200.0,
                low=41800.0,
                close=42100.0,
                volume=1200.0,
            )
        ]

    async def _fake_upsert(engine: Any, rows: Any, *, batch_size: int = 500) -> None:
        upsert_calls.append([engine, list(rows), batch_size])

    def _fake_materialize(repo_path: str | None = None) -> None:
        feast_materializations.append(repo_path or "default")

    class _DummyRun:
        def __enter__(self) -> "_DummyRun":
            mlflow_events["runs"] += 1
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        @property
        def info(self) -> Any:
            return type("RunInfo", (), {"run_id": "run-1"})()

    class _MLflowClientStub:
        def set_registered_model_alias(self, name: str, alias: str, version: int) -> None:
            mlflow_events.setdefault("aliases", []).append((name, alias, version))

    class _MLflowStub:
        def set_tracking_uri(self, uri: str) -> None:
            mlflow_events["tracking_uri"] = uri

        def set_experiment(self, name: str) -> None:
            mlflow_events["experiment"] = name

        def start_run(self, **_: Any) -> _DummyRun:
            return _DummyRun()

        def log_params(self, params: Dict[str, Any]) -> None:
            mlflow_events.setdefault("params", []).append(params)

        def log_metrics(self, metrics: Dict[str, float]) -> None:
            mlflow_events.setdefault("metrics", []).append(metrics)

        def log_artifacts(self, *_: Any, **__: Any) -> None:
            mlflow_events.setdefault("artifacts", 0)
            mlflow_events["artifacts"] += 1

        def register_model(self, model_uri: str, name: str) -> Any:
            mlflow_events["registrations"].append((model_uri, name))
            return type("RegisteredModel", (), {"version": 1})()

        class tracking:  # noqa: D401 - lightweight namespace stub
            MlflowClient = _MLflowClientStub

    monkeypatch.setattr(coingecko_ingest, "fetch_market_chart", _fake_fetch_market_chart)
    monkeypatch.setattr(coingecko_ingest, "aggregate_daily_rows", _fake_aggregate_daily_rows)
    monkeypatch.setattr(coingecko_ingest, "upsert_ohlcv_rows", _fake_upsert)

    try:
        from data.ingest import feature_jobs as feature_jobs_module
    except ImportError as exc:  # pragma: no cover - highlight missing dependency explicitly
        pytest.fail(f"Feast feature job module unavailable: {exc}")
    monkeypatch.setattr(feature_jobs_module, "materialize_features", _fake_materialize)

    monkeypatch.setattr(training_workflow, "mlflow", _MLflowStub(), raising=False)
    monkeypatch.setattr(training_workflow, "mlflow_pytorch", None, raising=False)

    result = _issue_training_request(client)

    assert result["status"] == 202, f"Unexpected response: {result['payload']}"
    assert loader_calls, "CoinGecko loader was not invoked"
    assert upsert_calls, "Timescale upsert was not triggered"
    assert feast_materializations, "Feast materialization did not run"
    assert mlflow_events["runs"] > 0, "MLflow run was not created"
    assert mlflow_events["registrations"], "Model registry did not receive a new version"


@pytest.mark.integration
def test_portfolio_positions_enforce_account_scopes(monkeypatch: pytest.MonkeyPatch) -> None:
    """JWT account scopes should gate access to portfolio positions."""

    import os

    from auth_service import create_jwt

    os.environ.setdefault("AUTH_JWT_SECRET", "test-secret")

    try:
        import portfolio_service
    except ImportError as exc:  # pragma: no cover - make failure explicit for missing module
        pytest.fail(f"Portfolio service is not available: {exc}")

    client = TestClient(portfolio_service.app)

    token, _ = create_jwt(subject="user_1", role="analyst", claims={"account_scopes": ["company"]})
    headers = {"Authorization": f"Bearer {token}"}

    forbidden = client.get("/portfolio/positions", params={"account_id": "director1"}, headers=headers)
    assert forbidden.status_code == 403

    allowed = client.get("/portfolio/positions", params={"account_id": "company"}, headers=headers)
    assert allowed.status_code == 200

    payload = allowed.json()
    positions = payload.get("positions", []) if isinstance(payload, dict) else []
    assert positions, "Portfolio endpoint returned no rows"
    assert all(entry.get("account_id") == "company" for entry in positions)


@pytest.mark.integration
def test_row_level_security_applies_session_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    """Direct SQL access should respect account scopes via session variables."""

    try:
        import portfolio_service
    except ImportError as exc:  # pragma: no cover - explicit failure
        pytest.fail(f"Portfolio service module missing: {exc}")

    captured_queries: List[str] = []

    class _CursorStub:
        def __init__(self, rows: List[Dict[str, Any]]) -> None:
            self._rows = rows

        def execute(self, query: str, params: Dict[str, Any] | None = None) -> None:
            captured_queries.append(query)

        def fetchall(self) -> List[Dict[str, Any]]:
            return self._rows

        def __enter__(self) -> "_CursorStub":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

    class _ConnectionStub:
        def __init__(self) -> None:
            self.rows = [
                {"account_id": "company", "symbol": "BTC/USD", "notional": 100000.0},
                {"account_id": "director1", "symbol": "ETH/USD", "notional": 50000.0},
            ]

        def cursor(self) -> _CursorStub:
            return _CursorStub(self.rows)

        def close(self) -> None:  # pragma: no cover - compatibility shim
            return None

    def _fake_connect(*_: Any, **__: Any) -> _ConnectionStub:
        return _ConnectionStub()

    if not hasattr(portfolio_service, "query_positions"):
        pytest.fail("Portfolio service does not expose query_positions for direct SQL verification")

    monkeypatch.setattr(portfolio_service, "_connect", _fake_connect, raising=False)

    rows = portfolio_service.query_positions(account_scopes=["company"])
    assert all(row["account_id"] == "company" for row in rows)
    assert any("set_config" in query for query in captured_queries), "Session scope not configured"
