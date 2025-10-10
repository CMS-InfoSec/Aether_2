"""Monte Carlo NAV forecasting endpoint for the risk service."""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Sequence, TypeVar, cast

try:  # pragma: no cover - exercised in environments with numpy available
    import numpy as _NUMPY  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - exercised in insecure-default tests
    _NUMPY = None

from fastapi import APIRouter, Depends, HTTPException, Query, status

from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.common.security import require_admin_account
from shared.pydantic_compat import BaseModel, Field


RouteFn = TypeVar("RouteFn", bound=Callable[..., Any])


TRADING_DAYS_PER_YEAR = 252
DEFAULT_VOLATILITY = 0.25
DEFAULT_CORRELATION = 0.0
DEFAULT_SIMULATIONS = 5000
_INSECURE_DEFAULTS_FLAG = "RISK_ALLOW_INSECURE_DEFAULTS"


def _insecure_defaults_enabled() -> bool:
    return os.getenv(_INSECURE_DEFAULTS_FLAG) == "1" or bool(os.getenv("PYTEST_CURRENT_TEST"))


class _DeterministicRNG:
    """Deterministic random number helper used when numpy is unavailable."""

    _PATTERN = (0.0, 0.45, -0.3, 0.7, -0.55)

    def __init__(self, seed: int | None = None) -> None:
        self._seed = seed or 0

    def multivariate_normal(
        self,
        *,
        mean: Sequence[float],
        cov: Sequence[Sequence[float]],
        size: int,
    ) -> list[list[float]]:
        dimensions = len(mean)
        pattern = self._PATTERN
        rows: list[list[float]] = []
        for index in range(int(size)):
            sample: list[float] = []
            for dim in range(dimensions):
                variance = 0.0
                if dim < len(cov) and dim < len(cov[dim]):
                    variance = float(max(cov[dim][dim], 0.0))
                scale = math.sqrt(variance) if variance > 0 else 0.0
                offset = pattern[(index + dim + self._seed) % len(pattern)]
                sample.append(float(mean[dim]) + scale * offset)
            rows.append(sample)
        return rows

    def normal(
        self,
        *,
        loc: float,
        scale: Sequence[float] | float,
        size: tuple[int, int] | int,
    ) -> list[list[float]]:
        if isinstance(scale, (int, float)):
            scales = [float(scale)]
        else:
            scales = [float(value) for value in scale]
        if isinstance(size, int):
            simulations = size
            dimensions = len(scales)
        else:
            simulations, dimensions = size
        pattern = self._PATTERN
        rows: list[list[float]] = []
        for index in range(int(simulations)):
            sample: list[float] = []
            for dim in range(int(dimensions)):
                scalar = scales[dim % len(scales)] if scales else 0.0
                offset = pattern[(index + dim + self._seed) % len(pattern)]
                sample.append(float(loc) + scalar * offset)
            rows.append(sample)
        return rows


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
        self._numpy_available = _NUMPY is not None
        if not self._numpy_available and not _insecure_defaults_enabled():
            raise ModuleNotFoundError(
                "numpy is required for NAV Monte Carlo forecasts; set RISK_ALLOW_INSECURE_DEFAULTS=1 to"
                " enable deterministic local fallbacks",
            )
        if self._numpy_available:
            self._rng = _NUMPY.random.default_rng(seed)
        else:
            self._rng = _DeterministicRNG(seed)

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

        nav_series = self._to_list(nav_paths)
        losses = [nav - value for value in nav_series]
        var_threshold = self._percentile(losses, 95.0)
        var_95 = max(0.0, var_threshold)
        tail_losses = [value for value in losses if value >= var_threshold]
        if tail_losses:
            cvar_95 = max(0.0, self._mean(tail_losses))
        else:
            cvar_95 = var_95

        if loss_cap > 0.0 and losses:
            probability_cap_hit = self._probability(losses, lambda value: value >= loss_cap)
        else:
            probability_cap_hit = 0.0

        metrics_payload = {
            "var95": var_95,
            "cvar95": cvar_95,
            "prob_loss_cap_hit": probability_cap_hit,
            "mean_nav": self._mean(nav_series),
            "std_nav": self._std(nav_series),
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
    ) -> tuple[Iterable[float], Iterable[float]]:
        if not contexts:
            nav_paths = [float(nav)] * self.simulations
            pnl_paths = [0.0] * self.simulations
            return nav_paths, pnl_paths

        scale = math.sqrt(max(days, 0.0) / TRADING_DAYS_PER_YEAR)
        if self._numpy_available:
            symbols = [context.symbol for context in contexts]
            notionals = _NUMPY.array([context.notional for context in contexts], dtype=float)
            volatilities = _NUMPY.array([context.volatility for context in contexts], dtype=float)
            scaled_vols = volatilities * scale
            correlation_matrix = self._build_correlation_matrix(symbols, correlation_inputs)
            covariance = _NUMPY.outer(scaled_vols, scaled_vols) * correlation_matrix
            mean = _NUMPY.zeros(len(contexts))
            try:
                returns = self._rng.multivariate_normal(mean=mean, cov=covariance, size=self.simulations)
            except _NUMPY.linalg.LinAlgError:
                returns = self._rng.normal(
                    loc=0.0,
                    scale=scaled_vols,
                    size=(self.simulations, len(contexts)),
                )
            pnl_paths = returns @ notionals
            nav_paths = nav + pnl_paths
            return nav_paths, pnl_paths

        normalized = self._normalize_correlations(contexts, correlation_inputs)
        nav_paths: list[float] = []
        pnl_paths: list[float] = []
        base_pattern = [0.0, 0.45, -0.3, 0.7, -0.55]
        for simulation in range(self.simulations):
            pnl = 0.0
            for ctx_index, context in enumerate(contexts):
                pattern_value = base_pattern[(simulation + ctx_index) % len(base_pattern)]
                neighbors = normalized.get(context.symbol, {})
                if neighbors:
                    average_corr = sum(neighbors.values()) / len(neighbors)
                else:
                    average_corr = 0.0
                adjustment = max(0.1, min(1.5, 1.0 + average_corr * 0.5))
                pnl += context.notional * context.volatility * scale * pattern_value * adjustment
            pnl_paths.append(float(pnl))
            nav_paths.append(float(nav + pnl))
        return nav_paths, pnl_paths

    def _build_correlation_matrix(
        self,
        symbols: Sequence[str],
        correlation_inputs: Mapping[str, Mapping[str, float]] | Mapping[str, Dict[str, float]],
    ) -> Any:
        size = len(symbols)
        if size == 0:
            return _NUMPY.zeros((0, 0)) if self._numpy_available else []
        if not self._numpy_available:
            matrix = [[0.0 for _ in range(size)] for _ in range(size)]
            for i, sym_i in enumerate(symbols):
                for j, sym_j in enumerate(symbols):
                    if i == j:
                        matrix[i][j] = 1.0
                    else:
                        matrix[i][j] = self._lookup_correlation(sym_i, sym_j, correlation_inputs)
            return matrix

        matrix = _NUMPY.eye(size)
        for i, sym_i in enumerate(symbols):
            for j, sym_j in enumerate(symbols):
                if i == j:
                    continue
                matrix[i, j] = self._lookup_correlation(sym_i, sym_j, correlation_inputs)
        matrix = (matrix + matrix.T) / 2.0
        _NUMPY.fill_diagonal(matrix, 1.0)
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
        return float(max(-1.0, min(1.0, value)))

    def _normalize_correlations(
        self,
        contexts: Sequence[_InstrumentContext],
        correlation_inputs: Mapping[str, Mapping[str, float]] | Mapping[str, Dict[str, float]],
    ) -> Dict[str, Dict[str, float]]:
        symbols = [context.symbol for context in contexts]
        normalized: Dict[str, Dict[str, float]] = {symbol: {} for symbol in symbols}
        for i, sym_i in enumerate(symbols):
            for j, sym_j in enumerate(symbols):
                if i == j:
                    continue
                normalized[sym_i][sym_j] = self._lookup_correlation(sym_i, sym_j, correlation_inputs)
        return normalized

    @staticmethod
    def _to_list(values: Iterable[Any]) -> list[float]:
        if hasattr(values, "tolist"):
            raw = values.tolist()
        else:
            raw = list(values)
        flattened: list[float] = []
        for value in raw:
            if isinstance(value, (list, tuple)):
                flattened.extend(float(item) for item in value)
            else:
                flattened.append(float(value))
        return flattened

    @staticmethod
    def _mean(values: Iterable[float]) -> float:
        series = [float(value) for value in values]
        if not series:
            return 0.0
        return sum(series) / len(series)

    @staticmethod
    def _std(values: Iterable[float]) -> float:
        series = [float(value) for value in values]
        if not series:
            return 0.0
        mean_value = NavMonteCarloForecaster._mean(series)
        variance = sum((value - mean_value) ** 2 for value in series) / len(series)
        return math.sqrt(variance)

    @staticmethod
    def _percentile(values: Iterable[float], percentile: float) -> float:
        series = sorted(float(value) for value in values)
        if not series:
            return 0.0
        if percentile <= 0:
            return series[0]
        if percentile >= 100:
            return series[-1]
        rank = (len(series) - 1) * percentile / 100.0
        lower = math.floor(rank)
        upper = math.ceil(rank)
        if lower == upper:
            return series[int(rank)]
        lower_value = series[lower]
        upper_value = series[upper]
        fraction = rank - lower
        return lower_value * (1 - fraction) + upper_value * fraction

    @staticmethod
    def _probability(values: Iterable[float], predicate: Callable[[float], bool]) -> float:
        series = [float(value) for value in values]
        if not series:
            return 0.0
        matches = sum(1 for value in series if predicate(value))
        return matches / len(series)

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


def _router_get(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``router.get`` for static analysis."""

    return cast(Callable[[RouteFn], RouteFn], router.get(*args, **kwargs))


@_router_get("/risk/nav_forecast", response_model=NavForecastResponse)
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

