"""Monte Carlo CVaR forecasting endpoint for the risk service."""

from __future__ import annotations

import math
import os
import re
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

try:  # pragma: no cover - exercised via tests when numpy is installed
    import numpy as _NUMPY_MODULE  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - handled in insecure-default tests
    _NUMPY_MODULE = None

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import APIRouter, Depends, HTTPException, Query, status
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[assignment]
        APIRouter,
        Depends,
        HTTPException,
        Query,
        status,
    )
from pydantic import BaseModel, Field, ConfigDict

from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.common.security import require_admin_account
from shared.spot import is_spot_symbol, normalize_spot_symbol


LOGGER = logging.getLogger(__name__)


TRADING_DAYS_PER_YEAR = 252
DEFAULT_VOLATILITY = 0.25
DEFAULT_SIMULATIONS = 5000
PERCENTILE_BUCKETS: tuple[int, ...] = (1, 5, 25, 50, 75, 95, 99)
_INSECURE_DEFAULTS_FLAG = "RISK_ALLOW_INSECURE_DEFAULTS"


class MissingDependencyError(RuntimeError):
    """Raised when optional dependencies are unavailable."""


def _insecure_defaults_enabled() -> bool:
    return os.getenv(_INSECURE_DEFAULTS_FLAG) == "1" or bool(os.getenv("PYTEST_CURRENT_TEST"))


class _Array(list[float]):
    """Minimal array implementation supporting arithmetic used in forecasts."""

    def __init__(self, values: Iterable[float]) -> None:
        super().__init__(float(value) for value in values)

    # Arithmetic helpers -------------------------------------------------
    def __add__(self, other: object) -> "_Array":
        if isinstance(other, _Array):
            return _Array(a + b for a, b in zip(self, other))
        if isinstance(other, (int, float)):
            scalar = float(other)
            return _Array(value + scalar for value in self)
        return NotImplemented  # type: ignore[return-value]

    def __radd__(self, other: object) -> "_Array":
        return self.__add__(other)

    def __iadd__(self, other: object) -> "_Array":
        if isinstance(other, _Array):
            for index, value in enumerate(other):
                if index < len(self):
                    self[index] += value
            return self
        if isinstance(other, (int, float)):
            scalar = float(other)
            for index in range(len(self)):
                self[index] += scalar
            return self
        return NotImplemented  # type: ignore[return-value]

    def __mul__(self, other: object) -> "_Array":
        if isinstance(other, _Array):
            return _Array(a * b for a, b in zip(self, other))
        if isinstance(other, (int, float)):
            scalar = float(other)
            return _Array(value * scalar for value in self)
        return NotImplemented  # type: ignore[return-value]

    __rmul__ = __mul__

    def __imul__(self, other: object) -> "_Array":
        if isinstance(other, _Array):
            for index, value in enumerate(other):
                if index < len(self):
                    self[index] *= value
            return self
        if isinstance(other, (int, float)):
            scalar = float(other)
            for index in range(len(self)):
                self[index] *= scalar
            return self
        return NotImplemented  # type: ignore[return-value]

    def __sub__(self, other: object) -> "_Array":
        if isinstance(other, _Array):
            return _Array(a - b for a, b in zip(self, other))
        if isinstance(other, (int, float)):
            scalar = float(other)
            return _Array(value - scalar for value in self)
        return NotImplemented  # type: ignore[return-value]

    def __rsub__(self, other: object) -> "_Array":
        if isinstance(other, _Array):
            return _Array(b - a for a, b in zip(self, other))
        if isinstance(other, (int, float)):
            scalar = float(other)
            return _Array(scalar - value for value in self)
        return NotImplemented  # type: ignore[return-value]

    def __ge__(self, other: object) -> "_Array":
        if isinstance(other, _Array):
            return _Array(1.0 if a >= b else 0.0 for a, b in zip(self, other))
        if isinstance(other, (int, float)):
            scalar = float(other)
            return _Array(1.0 if value >= scalar else 0.0 for value in self)
        return NotImplemented  # type: ignore[return-value]

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, _Array):
            return _Array(value for value, keep in zip(self, key) if bool(keep))
        if isinstance(key, list) and key and all(isinstance(item, bool) for item in key):
            return _Array(value for value, keep in zip(self, key) if keep)
        return super().__getitem__(key)

    # Convenience helpers ------------------------------------------------
    def mean(self) -> float:
        if not self:
            return 0.0
        return sum(float(value) for value in self) / len(self)


class _DeterministicRNG:
    """Deterministic random number generator for insecure-default mode."""

    def __init__(self, seed: int | None = None) -> None:
        self._seed = seed or 0

    def normal(self, *, loc: float, scale: float, size: int) -> _Array:  # type: ignore[override]
        pattern = [0.0, 0.5, -0.25, 0.75, -0.5]
        values = []
        for index in range(int(size)):
            offset = pattern[index % len(pattern)]
            values.append(loc + scale * offset)
        return _Array(values)


class _DeterministicRandom:
    def default_rng(self, seed: int | None = None) -> _DeterministicRNG:
        return _DeterministicRNG(seed)


class _DeterministicNumpy:
    """Subset of numpy functionality used by the CVaR forecaster."""

    def __init__(self) -> None:
        self.random = _DeterministicRandom()

    @staticmethod
    def zeros(size: int) -> _Array:
        return _Array(0.0 for _ in range(int(size)))

    @staticmethod
    def any(values: Iterable[object]) -> bool:
        return any(bool(value) for value in values)

    @staticmethod
    def mean(values: Iterable[object]) -> float:
        series = [float(value) for value in values]
        if not series:
            return 0.0
        return sum(series) / len(series)

    @staticmethod
    def std(values: Iterable[object]) -> float:
        series = [float(value) for value in values]
        if not series:
            return 0.0
        mean_value = _DeterministicNumpy.mean(series)
        return math.sqrt(sum((value - mean_value) ** 2 for value in series) / len(series))

    @staticmethod
    def min(values: Iterable[object]) -> float:
        series = [float(value) for value in values]
        return min(series) if series else 0.0

    @staticmethod
    def max(values: Iterable[object]) -> float:
        series = [float(value) for value in values]
        return max(series) if series else 0.0

    @staticmethod
    def percentile(values: Iterable[object], percentile: float) -> float:
        series = sorted(float(value) for value in values)
        if not series:
            return 0.0
        if percentile <= 0:
            return series[0]
        if percentile >= 100:
            return series[-1]
        position = (percentile / 100.0) * (len(series) - 1)
        lower = int(math.floor(position))
        upper = int(math.ceil(position))
        if lower == upper:
            return series[lower]
        fraction = position - lower
        return series[lower] * (1 - fraction) + series[upper] * fraction


def _resolve_numpy() -> Any:
    if _NUMPY_MODULE is not None:
        return _NUMPY_MODULE
    if _insecure_defaults_enabled():
        LOGGER.warning(
            "numpy is unavailable; activating deterministic CVaR fallback."
        )
        return _DeterministicNumpy()
    raise MissingDependencyError(
        "numpy is required for CVaR Monte Carlo forecasts; set "
        f"{_INSECURE_DEFAULTS_FLAG}=1 to enable deterministic fallbacks."
    )


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
        self._np = _resolve_numpy()
        self._rng = self._np.random.default_rng(seed)

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
        var_threshold = float(self._np.percentile(losses, 95))
        var_95 = max(0.0, var_threshold)
        tail_mask = losses >= var_threshold
        if self._np.any(tail_mask):
            tail_losses = losses[tail_mask]
            mean_value = (
                float(tail_losses.mean())
                if hasattr(tail_losses, "mean")
                else float(self._np.mean(tail_losses))
            )
            cvar_95 = max(0.0, mean_value)
        else:
            cvar_95 = var_95

        loss_cap = float(config.get("loss_cap", 0.0))
        if loss_cap > 0.0:
            probability_cap_hit = float(self._np.mean(losses >= loss_cap))
        else:
            probability_cap_hit = 0.0

        distribution_models = {
            "nav": self._summarize(nav_paths),
            "pnl": self._summarize(pnl_paths),
            "loss": self._summarize(losses),
        }
        distribution_summary = {
            key: value.model_dump() if hasattr(value, "model_dump") else value
            for key, value in distribution_models.items()
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
    ) -> tuple[Any, Any]:
        pnl = self._np.zeros(self.simulations)
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

    def _summarize(self, sample: Any) -> OutcomeDistribution:
        percentiles = {
            str(p): float(self._np.percentile(sample, p)) for p in PERCENTILE_BUCKETS
        }
        return OutcomeDistribution(
            mean=float(self._np.mean(sample)),
            std=float(self._np.std(sample)),
            min=float(self._np.min(sample)),
            max=float(self._np.max(sample)),
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


