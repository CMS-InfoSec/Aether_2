"""Monte Carlo CVaR forecasting endpoint for the risk service."""

from __future__ import annotations

import math
import re
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Mapping

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, ConfigDict

from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.common.security import require_admin_account
from shared.spot import is_spot_symbol, normalize_spot_symbol


LOGGER = logging.getLogger(__name__)


TRADING_DAYS_PER_YEAR = 252
DEFAULT_VOLATILITY = 0.25
DEFAULT_SIMULATIONS = 5000
PERCENTILE_BUCKETS: tuple[int, ...] = (1, 5, 25, 50, 75, 95, 99)


class OutcomeDistribution(BaseModel):
    """Summary statistics describing a simulated distribution."""

    mean: float
    std: float
    minimum: float = Field(..., alias="min")
    maximum: float = Field(..., alias="max")
    percentiles: Dict[str, float]

    model_config = ConfigDict(populate_by_name=True)


class CVaRForecastResponse(BaseModel):
    """Response payload describing a CVaR Monte Carlo forecast."""

    account_id: str
    horizon: str
    simulations: int
    timestamp: datetime
    var_95: float = Field(..., ge=0.0)
    cvar_95: float = Field(..., ge=0.0)
    probability_loss_cap_hit: float = Field(..., ge=0.0, le=1.0)
    loss_cap: float = Field(..., ge=0.0)
    positions: Dict[str, float]
    distribution: Dict[str, OutcomeDistribution]


@dataclass
class _InstrumentContext:
    symbol: str
    notional: float
    volatility: float


class CVaRMonteCarloForecaster:
    """Encapsulates Monte Carlo estimation of VaR/CVaR."""

    def __init__(
        self,
        account_id: str,
        *,
        timescale: TimescaleAdapter | None = None,
        feature_store: RedisFeastAdapter | None = None,
        simulations: int = DEFAULT_SIMULATIONS,
        seed: int | None = 42,
    ) -> None:
        self.account_id = account_id
        self.timescale = timescale or TimescaleAdapter(account_id=account_id)
        self.feature_store = feature_store or RedisFeastAdapter(account_id=account_id)
        self.simulations = int(max(simulations, 1))
        self._rng = np.random.default_rng(seed)

    def forecast(self, horizon: str) -> CVaRForecastResponse:
        days = self._parse_horizon(horizon)
        config = self.timescale.load_risk_config()
        nav = float(config.get("nav", 0.0))
        if nav <= 0:
            raise ValueError("Account NAV must be positive to run CVaR forecast")

        positions = self._load_positions()
        instrument_context = list(self._instrument_contexts(positions))
        nav_paths, pnl_paths = self._simulate_paths(nav, instrument_context, days)

        losses = nav - nav_paths
        var_threshold = float(np.percentile(losses, 95))
        var_95 = max(0.0, var_threshold)
        tail_mask = losses >= var_threshold
        if np.any(tail_mask):
            tail_losses = losses[tail_mask]
            cvar_95 = max(0.0, float(tail_losses.mean()))
        else:
            cvar_95 = var_95

        loss_cap = float(config.get("loss_cap", 0.0))
        if loss_cap > 0.0:
            probability_cap_hit = float(np.mean(losses >= loss_cap))
        else:
            probability_cap_hit = 0.0

        distribution_summary = {
            "nav": self._summarize(nav_paths),
            "pnl": self._summarize(pnl_paths),
            "loss": self._summarize(losses),
        }

        timestamp = datetime.now(timezone.utc)
        self.timescale.record_cvar_result(
            horizon=horizon,
            var95=var_95,
            cvar95=cvar_95,
            prob_cap_hit=probability_cap_hit,
            timestamp=timestamp,
        )

        return CVaRForecastResponse(
            account_id=self.account_id,
            horizon=horizon,
            simulations=self.simulations,
            timestamp=timestamp,
            var_95=var_95,
            cvar_95=cvar_95,
            probability_loss_cap_hit=probability_cap_hit,
            loss_cap=loss_cap,
            positions=positions,
            distribution=distribution_summary,
        )

    def _load_positions(self) -> Dict[str, float]:
        positions = self.timescale.open_positions()
        filtered: Dict[str, float] = {}
        dropped: list[str] = []

        for symbol, notional in positions.items():
            normalized = normalize_spot_symbol(symbol)
            if not normalized:
                continue

            if not is_spot_symbol(normalized):
                dropped.append(str(symbol))
                continue

            filtered[normalized] = filtered.get(normalized, 0.0) + float(notional)

        if dropped:
            LOGGER.warning(
                "Ignoring non-spot instruments in CVaR forecast: %s",
                sorted({str(symbol) for symbol in dropped}),
            )

        return {
            symbol: float(notional)
            for symbol, notional in filtered.items()
            if abs(float(notional)) > 0.0
        }

    def _instrument_contexts(self, positions: Mapping[str, float]) -> Iterable[_InstrumentContext]:
        for symbol, notional in positions.items():
            volatility = self._instrument_volatility(symbol)
            yield _InstrumentContext(symbol=symbol, notional=float(notional), volatility=volatility)

    def _instrument_volatility(self, instrument: str) -> float:
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
        contexts: Iterable[_InstrumentContext],
        days: float,
    ) -> tuple[np.ndarray, np.ndarray]:
        pnl = np.zeros(self.simulations)
        scale = math.sqrt(max(days, 0.0) / TRADING_DAYS_PER_YEAR)
        for context in contexts:
            if context.volatility <= 0 or context.notional == 0:
                continue
            sigma = context.volatility * scale
            if sigma <= 0:
                continue
            returns = self._rng.normal(loc=0.0, scale=sigma, size=self.simulations)
            pnl += returns * context.notional
        nav_paths = nav + pnl
        return nav_paths, pnl

    def _summarize(self, sample: np.ndarray) -> OutcomeDistribution:
        percentiles = {
            str(p): float(np.percentile(sample, p)) for p in PERCENTILE_BUCKETS
        }
        return OutcomeDistribution(
            mean=float(np.mean(sample)),
            std=float(np.std(sample)),
            min=float(np.min(sample)),
            max=float(np.max(sample)),
            percentiles=percentiles,
        )

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


@router.get("/risk/cvar", response_model=CVaRForecastResponse)
def get_cvar_forecast(
    account_id: str = Query(..., description="Trading account identifier"),
    horizon: str = Query("24h", description="Forecast horizon such as 24h or 2d"),
    caller: str = Depends(require_admin_account),
) -> CVaRForecastResponse:
    if caller != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and query parameter.",
        )

    forecaster = CVaRMonteCarloForecaster(account_id=account_id)
    try:
        return forecaster.forecast(horizon)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


