"""Regression coverage for the CVaR forecaster insecure-default fallback."""

from __future__ import annotations

import importlib
import sys

import pytest


@pytest.fixture(autouse=True)
def _reset_timescale() -> None:
    from services.common.adapters import TimescaleAdapter

    TimescaleAdapter.reset("company")
    yield
    TimescaleAdapter.reset("company")


def _reload_module(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("RISK_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_COMPANY_REDIS_DSN", "memory://local")
    for name in list(sys.modules):
        if name.startswith("numpy"):
            sys.modules.pop(name)
        if name.startswith("services.risk.cvar_forecast"):
            sys.modules.pop(name)
    module = importlib.import_module("services.risk.cvar_forecast")
    return importlib.reload(module)


def test_cvar_forecast_operates_without_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_module(monkeypatch)

    adapter = module.TimescaleAdapter(account_id="company")
    adapter.record_instrument_exposure("BTC-USD", 75_000.0)

    class _FeatureStoreStub:
        def fetch_online_features(self, instrument: str) -> dict[str, dict[str, float]]:
            del instrument
            return {"state": {"volatility": 0.05}}

    forecaster = module.CVaRMonteCarloForecaster(
        account_id="company",
        feature_store=_FeatureStoreStub(),
    )
    response = forecaster.forecast("1d")

    assert response.account_id == "company"
    assert response.simulations == module.DEFAULT_SIMULATIONS
    assert response.positions["BTC-USD"] == pytest.approx(75_000.0)
    assert response.cvar_95 >= response.var_95 >= 0.0
    assert response.loss_cap >= 0.0
    assert set(response.distribution.keys()) == {"nav", "pnl", "loss"}
