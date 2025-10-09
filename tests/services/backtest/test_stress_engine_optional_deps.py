"""Regression tests for the backtest stress engine optional dependency guards."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import HTTPException, status

from services.backtest import stress_engine
from services.common.config import TimescaleSession


class _StubRepository:
    def __init__(self) -> None:
        self.recorded: list[stress_engine.StressTestResult] = []

    def record(self, result: stress_engine.StressTestResult) -> None:
        self.recorded.append(result)


def test_stress_engine_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    """Running the engine without pandas should surface a clear dependency error."""

    monkeypatch.setattr(stress_engine, "_PANDAS_MODULE", None, raising=False)
    monkeypatch.setattr(stress_engine, "_PANDAS_ERROR", ImportError("pandas missing"), raising=False)
    monkeypatch.setattr(stress_engine, "_NUMPY_MODULE", object(), raising=False)
    monkeypatch.setattr(stress_engine, "_NUMPY_ERROR", None, raising=False)

    engine = stress_engine.PortfolioStressEngine(
        "acct",
        session=TimescaleSession(dsn="memory://", account_schema="acct"),
        repository=_StubRepository(),
    )

    with pytest.raises(stress_engine.MissingDependencyError) as excinfo:
        engine.run(stress_engine.StressScenario.FLASH_CRASH)

    assert "pandas is required" in str(excinfo.value)


def test_repository_falls_back_to_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    """The repository should retain results in memory when SQLAlchemy is unavailable."""

    monkeypatch.setattr(stress_engine, "_SQLALCHEMY_AVAILABLE", False, raising=False)
    monkeypatch.setattr(stress_engine, "_IN_MEMORY_REPOSITORIES", {}, raising=False)

    session = TimescaleSession(dsn="memory://stress", account_schema="acct")
    repository = stress_engine.StressTestRepository(session)

    result = stress_engine.StressTestResult(
        account_id="acct",
        scenario=stress_engine.StressScenario.SPREAD_WIDEN,
        timestamp=datetime.now(timezone.utc),
        base_metrics={"pnl": 5.0},
        stressed_metrics={"pnl": 2.5},
    )

    repository.record(result)

    key = (session.dsn, session.account_schema)
    stored = stress_engine._IN_MEMORY_REPOSITORIES[key]
    assert stored[-1]["account_id"] == "acct"
    assert stored[-1]["scenario"] == stress_engine.StressScenario.SPREAD_WIDEN.value
    assert pytest.approx(stored[-1]["pnl_impact"], rel=1e-6) == result.pnl_impact


def test_run_stress_maps_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing dependency errors should translate into a 503 HTTP response."""

    class _FailingEngine:
        account_id = "acct"

        def run(self, _scenario: stress_engine.StressScenario) -> None:
            raise stress_engine.MissingDependencyError("numpy missing")

    with pytest.raises(HTTPException) as excinfo:
        stress_engine.run_stress(
            scenario=stress_engine.StressScenario.FLASH_CRASH,
            engine=_FailingEngine(),
        )

    assert excinfo.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert "numpy missing" in excinfo.value.detail
