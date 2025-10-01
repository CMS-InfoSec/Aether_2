"""Monte Carlo NAV forecasting endpoint for the risk service."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Mapping, Sequence

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.common.security import require_admin_account


TRADING_DAYS_PER_YEAR = 252
DEFAULT_VOLATILITY = 0.25
DEFAULT_CORRELATION = 0.0
DEFAULT_SIMULATIONS = 5000


@dataclass
class _InstrumentContext:
    symbol: str
    notional: float
    volatility: float


class NavForecastMetrics(BaseModel):
    """Summary metrics for the NAV distribution."""

    var_95: float = Field(..., ge=0.0)
    cvar_95: float = Field(..., ge=0.0)
    probability_loss_cap_hit: float = Field(..., ge=0.0, le=1.0)
    mean_nav: float
    std_nav: float
    loss_cap: float = Field(..., ge=0.0)


class NavForecastResponse(BaseModel):
    """Response payload describing the NAV Monte Carlo forecast."""

    account_id: str
    horizon: str
    simulations: int
    timestamp: datetime
    metrics: NavForecastMetrics
    positions: Dict[str, float]


class NavMonteCarloForecaster:
    """Encapsulates Monte Carlo estimation of NAV distributions."""

    def __init__(
        self,
        account_id: str,
        *,
        timescale: TimescaleAdapter | None = None,
        feature_store: RedisFeastAdapter | None = None,
        simulations: int = DEFAULT_SIMULATIONS,
        seed: int | None = 7,
    ) -> None:
        self.account_id = account_id
        self.timescale = timescale or TimescaleAdapter(account_id=account_id)
        self.feature_store = feature_store or RedisFeastAdapter(account_id=account_id)
        self.simulations = int(max(simulations, 1))
        self._rng = np.random.default_rng(seed)

    def forecast(self, horizon: str) -> NavForecastResponse:
        days = self._parse_horizon(horizon)
        config = self.timescale.load_risk_config()
        nav = float(config.get("nav", 0.0))
        if nav <= 0:
            raise ValueError("Account NAV must be positive to run NAV forecast")

        volatility_overrides = self._normalize_mapping(config.get("volatility_overrides", {}))
        correlation_inputs = config.get("correlation_matrix", {})
        loss_cap = float(config.get("loss_cap", 0.0))

        positions = self._load_positions()
        contexts = list(self._instrument_contexts(positions, volatility_overrides))

        nav_paths, _ = self._simulate_paths(
            nav,
            contexts,
            correlation_inputs,
            days,
        )

        losses = nav - nav_paths
        var_threshold = float(np.percentile(losses, 95))
        var_95 = max(0.0, var_threshold)
        tail_mask = losses >= var_threshold
        if np.any(tail_mask):
            tail_losses = losses[tail_mask]
            cvar_95 = max(0.0, float(tail_losses.mean()))
        else:
            cvar_95 = var_95

        if loss_cap > 0.0:
            probability_cap_hit = float(np.mean(losses >= loss_cap))
        else:
            probability_cap_hit = 0.0

        metrics_payload = {
            "var95": var_95,
            "cvar95": cvar_95,
            "prob_loss_cap_hit": probability_cap_hit,
            "mean_nav": float(np.mean(nav_paths)),
            "std_nav": float(np.std(nav_paths)),
            "loss_cap": loss_cap,
        }

        timestamp = datetime.now(timezone.utc)
        self.timescale.record_nav_forecast(
            horizon=horizon,
            metrics=metrics_payload,
            timestamp=timestamp,
        )

        response_metrics = NavForecastMetrics(
            var_95=var_95,
            cvar_95=cvar_95,
            probability_loss_cap_hit=probability_cap_hit,
            mean_nav=metrics_payload["mean_nav"],
            std_nav=metrics_payload["std_nav"],
            loss_cap=loss_cap,
        )

        return NavForecastResponse(
            account_id=self.account_id,
            horizon=horizon,
            simulations=self.simulations,
            timestamp=timestamp,
            metrics=response_metrics,
            positions=positions,
        )

    def _load_positions(self) -> Dict[str, float]:
        positions = self.timescale.open_positions()
        return {
            symbol: float(notional)
            for symbol, notional in positions.items()
            if abs(float(notional)) > 0.0
        }

    def _instrument_contexts(
        self,
        positions: Mapping[str, float],
        volatility_overrides: Mapping[str, float],
    ) -> Iterable[_InstrumentContext]:
        for symbol, notional in positions.items():
            volatility = self._instrument_volatility(symbol, volatility_overrides)
            yield _InstrumentContext(
                symbol=symbol,
                notional=float(notional),
                volatility=volatility,
            )

    def _instrument_volatility(
        self,
        instrument: str,
        volatility_overrides: Mapping[str, float],
    ) -> float:
        if instrument in volatility_overrides:
            try:
                override = float(volatility_overrides[instrument])
                if override > 0:
                    return override
            except (TypeError, ValueError):  # pragma: no cover - defensive casting
                pass

        payload = self.feature_store.fetch_online_features(instrument)
        volatility: float | None = None
        if isinstance(payload, dict):
            state = payload.get("state")
            if isinstance(state, dict):
                raw = state.get("volatility")
                try:
                    if raw is not None:
                        volatility = float(raw)
                except (TypeError, ValueError):  # pragma: no cover - defensive casting
                    volatility = None
        if volatility is None or volatility <= 0:
            volatility = DEFAULT_VOLATILITY
        return float(volatility)

    def _simulate_paths(
        self,
        nav: float,
        contexts: Sequence[_InstrumentContext],
        correlation_inputs: Mapping[str, Mapping[str, float]] | Mapping[str, Dict[str, float]],
        days: float,
    ) -> tuple[np.ndarray, np.ndarray]:
        if not contexts:
            nav_paths = np.full(self.simulations, nav)
            pnl_paths = np.zeros(self.simulations)
            return nav_paths, pnl_paths

        symbols = [context.symbol for context in contexts]
        notionals = np.array([context.notional for context in contexts], dtype=float)
        volatilities = np.array([context.volatility for context in contexts], dtype=float)

        scale = math.sqrt(max(days, 0.0) / TRADING_DAYS_PER_YEAR)
        scaled_vols = volatilities * scale
        correlation_matrix = self._build_correlation_matrix(symbols, correlation_inputs)
        covariance = np.outer(scaled_vols, scaled_vols) * correlation_matrix

        mean = np.zeros(len(contexts))
        try:
            returns = self._rng.multivariate_normal(mean=mean, cov=covariance, size=self.simulations)
        except np.linalg.LinAlgError:
            returns = self._rng.normal(
                loc=0.0,
                scale=scaled_vols,
                size=(self.simulations, len(contexts)),
            )

        pnl_paths = returns @ notionals
        nav_paths = nav + pnl_paths
        return nav_paths, pnl_paths

    def _build_correlation_matrix(
        self,
        symbols: Sequence[str],
        correlation_inputs: Mapping[str, Mapping[str, float]] | Mapping[str, Dict[str, float]],
    ) -> np.ndarray:
        size = len(symbols)
        if size == 0:
            return np.zeros((0, 0))
        matrix = np.eye(size)
        for i, sym_i in enumerate(symbols):
            for j, sym_j in enumerate(symbols):
                if i == j:
                    continue
                matrix[i, j] = self._lookup_correlation(sym_i, sym_j, correlation_inputs)
        matrix = (matrix + matrix.T) / 2.0
        np.fill_diagonal(matrix, 1.0)
        return matrix

    def _lookup_correlation(
        self,
        sym_a: str,
        sym_b: str,
        correlation_inputs: Mapping[str, Mapping[str, float]] | Mapping[str, Dict[str, float]],
    ) -> float:
        row = correlation_inputs.get(sym_a, {})
        value: float | None = None
        if isinstance(row, Mapping):
            candidate = row.get(sym_b)
            if candidate is not None:
                try:
                    value = float(candidate)
                except (TypeError, ValueError):  # pragma: no cover - defensive casting
                    value = None
        if value is None:
            inverse_row = correlation_inputs.get(sym_b, {})
            if isinstance(inverse_row, Mapping):
                candidate = inverse_row.get(sym_a)
                if candidate is not None:
                    try:
                        value = float(candidate)
                    except (TypeError, ValueError):  # pragma: no cover - defensive casting
                        value = None
        if value is None:
            value = DEFAULT_CORRELATION
        return float(np.clip(value, -1.0, 1.0))

    @staticmethod
    def _normalize_mapping(payload: Mapping[str, float] | Dict[str, float]) -> Dict[str, float]:
        normalized: Dict[str, float] = {}
        for key, value in payload.items():
            try:
                normalized[key] = float(value)
            except (TypeError, ValueError):  # pragma: no cover - defensive casting
                continue
        return normalized

    @staticmethod
    def _parse_horizon(horizon: str) -> float:
        if not horizon:
            raise ValueError("Forecast horizon must be provided")
        match = re.fullmatch(r"(?i)(\d+)([hdw])", horizon.strip())
        if match is None:
            raise ValueError("Invalid horizon format. Use values like '24h', '2d', or '1w'.")
        value = int(match.group(1))
        unit = match.group(2).lower()
        if value <= 0:
            raise ValueError("Forecast horizon must be positive")
        if unit == "h":
            return value / 24.0
        if unit == "d":
            return float(value)
        if unit == "w":
            return float(value * 7)
        raise ValueError("Unsupported horizon unit")


router = APIRouter()


@router.get("/risk/nav_forecast", response_model=NavForecastResponse)
def get_nav_forecast(
    account_id: str = Query(..., description="Trading account identifier"),
    horizon: str = Query("24h", description="Forecast horizon such as 24h or 1w"),
    caller: str = Depends(require_admin_account),
) -> NavForecastResponse:
    if caller != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and query parameter.",
        )

    forecaster = NavMonteCarloForecaster(account_id=account_id)
    try:
        return forecaster.forecast(horizon)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

