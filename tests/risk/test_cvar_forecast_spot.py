"""Regression tests ensuring CVaR forecasting enforces spot-only inputs."""

from __future__ import annotations

import logging

import numpy as np

from services.risk.cvar_forecast import CVaRMonteCarloForecaster


class _StubTimescaleAdapter:
    def __init__(self) -> None:
        self.recorded: list[dict[str, object]] = []

    def load_risk_config(self) -> dict[str, float]:
        return {"nav": 250_000.0, "loss_cap": 10_000.0}

    def open_positions(self) -> dict[str, float]:
        return {
            "btc-usd": 100_000.0,
            "BTC-USD": 25_000.0,
            "ETH/USD": 50_000.0,
            "ETH-PERP": 12_500.0,
            "ADAUP-USD": 5_000.0,
        }

    def record_cvar_result(
        self,
        *,
        horizon: str,
        var95: float,
        cvar95: float,
        prob_cap_hit: float,
        timestamp,
    ) -> None:
        self.recorded.append(
            {
                "horizon": horizon,
                "var95": var95,
                "cvar95": cvar95,
                "prob_cap_hit": prob_cap_hit,
                "timestamp": timestamp,
            }
        )


class _StubFeatureStore:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def fetch_online_features(self, instrument: str) -> dict[str, dict[str, float]]:
        self.queries.append(instrument)
        return {"state": {"volatility": 0.1}}


class _DeterministicForecaster(CVaRMonteCarloForecaster):
    def __init__(self, timescale: _StubTimescaleAdapter, feature_store: _StubFeatureStore) -> None:
        self.account_id = "company"
        self.timescale = timescale
        self.feature_store = feature_store
        self.simulations = 16
        self._rng = _DeterministicRNG()


class _DeterministicRNG:
    def normal(self, *, loc: float, scale: float, size: int):  # type: ignore[override]
        return np.zeros(size, dtype=float)


def test_cvar_forecast_filters_non_spot_positions(caplog) -> None:
    timescale = _StubTimescaleAdapter()
    feature_store = _StubFeatureStore()
    forecaster = _DeterministicForecaster(timescale, feature_store)

    caplog.set_level(logging.WARNING)
    response = forecaster.forecast("1d")

    assert response.positions == {"BTC-USD": 125_000.0, "ETH-USD": 50_000.0}
    assert feature_store.queries == ["BTC-USD", "ETH-USD"]
    assert timescale.recorded, "CVaR results should be persisted"
    assert "Ignoring non-spot instruments" in caplog.text
