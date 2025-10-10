from __future__ import annotations

from pathlib import Path

from services.backtest import stress_engine
from services.common.config import TimescaleSession


def test_stress_engine_insecure_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("STRESS_ENGINE_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(tmp_path / "state"))

    monkeypatch.setattr(stress_engine, "_NUMPY_MODULE", None)
    monkeypatch.setattr(stress_engine, "_PANDAS_MODULE", None)
    monkeypatch.setattr(stress_engine, "Backtester", None)
    monkeypatch.setattr(stress_engine, "ExamplePolicy", None)
    monkeypatch.setattr(stress_engine, "FeeSchedule", None)
    monkeypatch.setattr(stress_engine, "flash_crash", None)
    monkeypatch.setattr(stress_engine, "spread_widen", None)
    monkeypatch.setattr(stress_engine, "liquidity_halt", None)

    session = TimescaleSession(dsn="sqlite:///local", account_schema="acct_company")
    engine = stress_engine.create_stress_engine("company", session=session)

    result = engine.run(stress_engine.StressScenario.FLASH_CRASH)
    assert result.base_metrics
    assert result.stressed_metrics

    state_path = Path(tmp_path / "state" / "stress_engine" / "company.json")
    assert state_path.exists()
