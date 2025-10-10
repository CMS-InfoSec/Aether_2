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


def _reload_nav_module(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("RISK_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_COMPANY_REDIS_DSN", "memory://local")
    for name in list(sys.modules):
        if name.startswith("numpy"):
            sys.modules.pop(name)
        if name.startswith("services.risk.nav_forecaster"):
            sys.modules.pop(name)
    module = importlib.import_module("services.risk.nav_forecaster")
    return importlib.reload(module)


def test_nav_forecast_operates_without_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_nav_module(monkeypatch)

    adapter = module.TimescaleAdapter(account_id="company")
    adapter.record_instrument_exposure("BTC-USD", 75_000.0)
    adapter.record_instrument_exposure("ETH-USD", 55_000.0)

    class _FeatureStoreStub:
        def fetch_online_features(self, instrument: str) -> dict[str, dict[str, float]]:
            if instrument == "BTC-USD":
                return {"state": {"volatility": 0.04}}
            if instrument == "ETH-USD":
                return {"state": {"volatility": 0.06}}
            return {"state": {"volatility": 0.05}}

    forecaster = module.NavMonteCarloForecaster(
        account_id="company",
        timescale=adapter,
        feature_store=_FeatureStoreStub(),
        simulations=48,
        seed=11,
    )

    response = forecaster.forecast("2d")

    assert response.account_id == "company"
    assert response.simulations == 48
    assert response.positions["BTC-USD"] == pytest.approx(75_000.0)
    assert response.positions["ETH-USD"] == pytest.approx(55_000.0)
    assert response.metrics.loss_cap >= 0.0
    assert response.metrics.var_95 >= 0.0
    assert response.metrics.cvar_95 >= response.metrics.var_95
    assert 0.0 <= response.metrics.probability_loss_cap_hit <= 1.0

    repeat = forecaster.forecast("2d")
    assert repeat.metrics.mean_nav == pytest.approx(response.metrics.mean_nav)
    assert repeat.metrics.std_nav == pytest.approx(response.metrics.std_nav)


def test_nav_forecast_requires_numpy_without_insecure_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RISK_ALLOW_INSECURE_DEFAULTS", raising=False)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    for name in list(sys.modules):
        if name.startswith("numpy"):
            sys.modules.pop(name)
        if name.startswith("services.risk.nav_forecaster"):
            sys.modules.pop(name)
    module = importlib.import_module("services.risk.nav_forecaster")
    adapter = module.TimescaleAdapter(account_id="company")
    adapter.record_instrument_exposure("BTC-USD", 50_000.0)
    with pytest.raises(ModuleNotFoundError):
        module.NavMonteCarloForecaster(
            account_id="company",
            timescale=adapter,
            feature_store=None,
        )
