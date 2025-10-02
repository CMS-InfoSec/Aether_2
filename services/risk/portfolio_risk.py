"""Portfolio-level risk aggregation and constraint enforcement service."""

from __future__ import annotations

import json
import logging
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from fastapi import FastAPI
from pydantic import BaseModel, Field

from services.common.adapters import TimescaleAdapter


PORTFOLIO_LOGGER = logging.getLogger("portfolio_risk_log")
if not PORTFOLIO_LOGGER.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    handler.setFormatter(formatter)
    PORTFOLIO_LOGGER.addHandler(handler)
PORTFOLIO_LOGGER.setLevel(logging.INFO)


DEFAULT_ACCOUNTS: Tuple[str, ...] = ("alpha", "beta", "gamma")
DEFAULT_CLUSTER_LIMITS: Dict[str, float] = {
    "BTC": 400_000.0,
    "ETH": 300_000.0,
    "ALT": 250_000.0,
}
DEFAULT_CLUSTER_MAP: Dict[str, str] = {
    "BTC-USD": "BTC",
    "BTC-USDT": "BTC",
    "ETH-USD": "ETH",
    "SOL-USD": "ALT",
    "ADA-USD": "ALT",
}
DEFAULT_BETA_MAP: Dict[str, float] = {
    "BTC-USD": 1.0,
    "BTC-USDT": 1.0,
    "ETH-USD": 0.8,
    "SOL-USD": 1.4,
    "ADA-USD": 1.2,
}
DEFAULT_BETA_LIMIT: float = 0.95
DEFAULT_CORRELATION_LIMIT: float = 0.85


def _json_env(name: str, default: Mapping[str, float] | Sequence[str]) -> Mapping[str, float] | Sequence[str]:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        PORTFOLIO_LOGGER.warning("Failed to parse JSON env", extra={"env": name, "value": raw})
        return default
    return parsed


def _accounts_from_env() -> Tuple[str, ...]:
    raw = _json_env("PORTFOLIO_RISK_ACCOUNTS", DEFAULT_ACCOUNTS)
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return tuple(str(entry) for entry in raw)
    return DEFAULT_ACCOUNTS


@dataclass
class Constraint:
    name: str
    limit: float
    value: float
    detail: Dict[str, object]

    def to_payload(self) -> Dict[str, object]:
        return {
            "constraint": self.name,
            "limit": self.limit,
            "value": self.value,
            "detail": self.detail,
        }


class AccountSummary(BaseModel):
    account_id: str = Field(..., description="Account identifier")
    gross_exposure: float = Field(..., ge=0.0)
    instrument_exposure: Dict[str, float] = Field(default_factory=dict)
    var_95: float = Field(..., ge=0.0)
    cvar_95: float = Field(..., ge=0.0)
    snapshot_ts: Optional[datetime] = Field(
        None, description="Timestamp of the latest VaR/CVaR observation"
    )


class PortfolioTotals(BaseModel):
    gross_exposure: float = Field(..., ge=0.0)
    instrument_exposure: Dict[str, float] = Field(default_factory=dict)
    cluster_exposure: Dict[str, float] = Field(default_factory=dict)
    var_95: float = Field(..., ge=0.0)
    cvar_95: float = Field(..., ge=0.0)
    beta_to_btc: float = Field(...)
    max_correlation: float = Field(..., ge=0.0)


class ConstraintBreach(BaseModel):
    constraint: str
    value: float
    limit: float
    detail: Dict[str, object] = Field(default_factory=dict)


class PortfolioStatusResponse(BaseModel):
    totals: PortfolioTotals
    accounts: List[AccountSummary]
    constraints_ok: bool
    breaches: List[ConstraintBreach]
    risk_adjustment: float = Field(..., ge=0.0, le=1.0)


class PortfolioRiskAggregator:
    """Aggregates risk metrics across multiple accounts."""

    def __init__(
        self,
        *,
        accounts: Sequence[str],
        cluster_limits: Mapping[str, float],
        instrument_clusters: Mapping[str, str],
        instrument_betas: Mapping[str, float],
        beta_limit: float,
        correlation_limit: float,
        adapter_factory: Callable[[str], TimescaleAdapter] | None = None,
    ) -> None:
        self.accounts = tuple(accounts)
        self.cluster_limits = dict(cluster_limits)
        self.instrument_clusters = dict(instrument_clusters)
        self.instrument_betas = {symbol: float(beta) for symbol, beta in instrument_betas.items()}
        self.beta_limit = float(beta_limit)
        self.correlation_limit = float(correlation_limit)
        self._adapter_factory = adapter_factory or (lambda account: TimescaleAdapter(account_id=account))

    def _adapter(self, account_id: str) -> TimescaleAdapter:
        return self._adapter_factory(account_id)

    @staticmethod
    def _normalize_exposures(exposures: Mapping[str, float]) -> Dict[str, float]:
        return {
            str(instrument): abs(float(notional))
            for instrument, notional in exposures.items()
            if abs(float(notional)) > 0.0
        }

    def _latest_cvar_observation(
        self, history: Sequence[Mapping[str, object]]
    ) -> Tuple[float, float, Optional[datetime]]:
        if not history:
            return 0.0, 0.0, None
        latest = max(
            history,
            key=lambda entry: entry.get("ts") if isinstance(entry.get("ts"), datetime) else datetime.min,
        )
        var_value = float(latest.get("var95", 0.0))
        cvar_value = float(latest.get("cvar95", 0.0))
        ts = latest.get("ts") if isinstance(latest.get("ts"), datetime) else None
        return var_value, cvar_value, ts

    def portfolio_status(self) -> PortfolioStatusResponse:
        account_summaries: List[AccountSummary] = []
        aggregated_instrument: Dict[str, float] = defaultdict(float)
        aggregated_cluster: Dict[str, float] = defaultdict(float)
        var_values: List[float] = []
        cvar_values: List[float] = []
        correlation_sources: List[Mapping[str, Mapping[str, float]]] = []

        for account_id in self.accounts:
            adapter = self._adapter(account_id)
            exposures = self._normalize_exposures(adapter.open_positions())
            config = adapter.load_risk_config()
            var_val, cvar_val, ts = self._latest_cvar_observation(adapter.cvar_results())
            if var_val <= 0.0:
                var_val = float(config.get("var_limit", 0.0))
            if cvar_val <= 0.0:
                cvar_val = var_val
            correlation_sources.append(config.get("correlation_matrix", {}))

            for instrument, notional in exposures.items():
                aggregated_instrument[instrument] += notional
                cluster = self.instrument_clusters.get(instrument)
                if cluster:
                    aggregated_cluster[cluster] += notional

            account_summary = AccountSummary(
                account_id=account_id,
                gross_exposure=sum(exposures.values()),
                instrument_exposure=dict(exposures),
                var_95=var_val,
                cvar_95=cvar_val,
                snapshot_ts=ts,
            )
            account_summaries.append(account_summary)
            var_values.append(var_val)
            cvar_values.append(cvar_val)

        gross_exposure = sum(aggregated_instrument.values())
        aggregated_var = math.sqrt(sum(value ** 2 for value in var_values)) if var_values else 0.0
        aggregated_cvar = sum(cvar_values)
        beta_to_btc = self._compute_portfolio_beta(aggregated_instrument)
        max_corr = self._compute_max_correlation(aggregated_instrument, correlation_sources)

        totals = PortfolioTotals(
            gross_exposure=gross_exposure,
            instrument_exposure=dict(sorted(aggregated_instrument.items())),
            cluster_exposure=dict(sorted(aggregated_cluster.items())),
            var_95=aggregated_var,
            cvar_95=aggregated_cvar,
            beta_to_btc=beta_to_btc,
            max_correlation=max_corr,
        )

        breaches = self._evaluate_constraints(totals)
        constraints_ok = not breaches
        risk_adjustment = self._risk_adjustment_factor(breaches)

        if breaches:
            for breach in breaches:
                PORTFOLIO_LOGGER.warning(
                    "Portfolio constraint breached",
                    extra={
                        "constraint": breach.name,
                        "value": breach.value,
                        "limit": breach.limit,
                        "detail": breach.detail,
                    },
                )

        return PortfolioStatusResponse(
            totals=totals,
            accounts=account_summaries,
            constraints_ok=constraints_ok,
            breaches=[ConstraintBreach(**breach.to_payload()) for breach in breaches],
            risk_adjustment=risk_adjustment,
        )

    def _compute_portfolio_beta(self, exposures: Mapping[str, float]) -> float:
        if not exposures:
            return 0.0
        weighted_beta = 0.0
        total_exposure = 0.0
        for instrument, notional in exposures.items():
            beta = self.instrument_betas.get(instrument)
            if beta is None:
                continue
            absolute_notional = abs(float(notional))
            if absolute_notional <= 0.0:
                continue
            weighted_beta += absolute_notional * float(beta)
            total_exposure += absolute_notional
        if total_exposure <= 0.0:
            return 0.0
        return weighted_beta / total_exposure

    @staticmethod
    def _combine_correlation_sources(
        sources: Sequence[Mapping[str, Mapping[str, float]]]
    ) -> Dict[Tuple[str, str], float]:
        combined: Dict[Tuple[str, str], float] = {}
        counts: Dict[Tuple[str, str], int] = {}
        for matrix in sources:
            if not isinstance(matrix, Mapping):
                continue
            for instrument, row in matrix.items():
                if not isinstance(row, Mapping):
                    continue
                for other, value in row.items():
                    key = tuple(sorted((str(instrument), str(other))))
                    combined[key] = combined.get(key, 0.0) + float(value)
                    counts[key] = counts.get(key, 0) + 1
        averaged: Dict[Tuple[str, str], float] = {}
        for key, total in combined.items():
            averaged[key] = total / counts[key]
        return averaged

    def _compute_max_correlation(
        self,
        exposures: Mapping[str, float],
        sources: Sequence[Mapping[str, Mapping[str, float]]],
    ) -> float:
        if not exposures:
            return 0.0
        averaged = self._combine_correlation_sources(sources)
        if not averaged:
            return 0.0
        instruments = {instrument for instrument, notional in exposures.items() if abs(float(notional)) > 0}
        max_corr = 0.0
        for (inst_a, inst_b), value in averaged.items():
            if inst_a == inst_b:
                continue
            if inst_a in instruments and inst_b in instruments:
                max_corr = max(max_corr, abs(float(value)))
        return max_corr

    def _evaluate_constraints(self, totals: PortfolioTotals) -> List[Constraint]:
        breaches: List[Constraint] = []
        for cluster, limit in self.cluster_limits.items():
            exposure = totals.cluster_exposure.get(cluster, 0.0)
            if limit > 0 and exposure > limit:
                breaches.append(
                    Constraint(
                        name="max_cluster_exposure",
                        limit=float(limit),
                        value=float(exposure),
                        detail={"cluster": cluster},
                    )
                )
        if self.beta_limit > 0 and totals.beta_to_btc > self.beta_limit:
            breaches.append(
                Constraint(
                    name="max_beta_to_btc",
                    limit=self.beta_limit,
                    value=totals.beta_to_btc,
                    detail={},
                )
            )
        if self.correlation_limit > 0 and totals.max_correlation > self.correlation_limit:
            breaches.append(
                Constraint(
                    name="max_correlation_risk",
                    limit=self.correlation_limit,
                    value=totals.max_correlation,
                    detail={},
                )
            )
        return breaches

    @staticmethod
    def _risk_adjustment_factor(breaches: Sequence[Constraint]) -> float:
        if not breaches:
            return 1.0
        factors: List[float] = []
        for breach in breaches:
            if breach.value <= 0 or breach.limit <= 0:
                continue
            factor = breach.limit / breach.value
            if factor < 1.0:
                factors.append(factor)
        if not factors:
            return 0.0
        return max(0.0, min(factors))


def _load_cluster_limits() -> Dict[str, float]:
    payload = _json_env("PORTFOLIO_CLUSTER_LIMITS", DEFAULT_CLUSTER_LIMITS)
    limits: Dict[str, float] = {}
    if isinstance(payload, Mapping):
        for cluster, value in payload.items():
            try:
                limits[str(cluster)] = float(value)
            except (TypeError, ValueError):
                PORTFOLIO_LOGGER.warning(
                    "Invalid cluster limit entry", extra={"cluster": cluster, "value": value}
                )
    return limits or dict(DEFAULT_CLUSTER_LIMITS)


def _load_cluster_map() -> Dict[str, str]:
    payload = _json_env("PORTFOLIO_CLUSTER_MAP", DEFAULT_CLUSTER_MAP)
    mapping: Dict[str, str] = {}
    if isinstance(payload, Mapping):
        for instrument, cluster in payload.items():
            if cluster:
                mapping[str(instrument)] = str(cluster)
    return mapping or dict(DEFAULT_CLUSTER_MAP)


def _load_beta_map() -> Dict[str, float]:
    payload = _json_env("PORTFOLIO_BETA_MAP", DEFAULT_BETA_MAP)
    betas: Dict[str, float] = {}
    if isinstance(payload, Mapping):
        for instrument, beta in payload.items():
            try:
                betas[str(instrument)] = float(beta)
            except (TypeError, ValueError):
                PORTFOLIO_LOGGER.warning(
                    "Invalid beta entry", extra={"instrument": instrument, "value": beta}
                )
    return betas or dict(DEFAULT_BETA_MAP)


def _load_beta_limit() -> float:
    raw = os.getenv("PORTFOLIO_BETA_LIMIT")
    if not raw:
        return DEFAULT_BETA_LIMIT
    try:
        return float(raw)
    except ValueError:
        PORTFOLIO_LOGGER.warning("Invalid beta limit", extra={"value": raw})
        return DEFAULT_BETA_LIMIT


def _load_correlation_limit() -> float:
    raw = os.getenv("PORTFOLIO_CORRELATION_LIMIT")
    if not raw:
        return DEFAULT_CORRELATION_LIMIT
    try:
        return float(raw)
    except ValueError:
        PORTFOLIO_LOGGER.warning("Invalid correlation limit", extra={"value": raw})
        return DEFAULT_CORRELATION_LIMIT


aggregator = PortfolioRiskAggregator(
    accounts=_accounts_from_env(),
    cluster_limits=_load_cluster_limits(),
    instrument_clusters=_load_cluster_map(),
    instrument_betas=_load_beta_map(),
    beta_limit=_load_beta_limit(),
    correlation_limit=_load_correlation_limit(),
)

app = FastAPI(title="Portfolio Risk Service")


@app.get("/risk/portfolio/status", response_model=PortfolioStatusResponse)
def portfolio_status() -> PortfolioStatusResponse:
    """Return aggregated risk posture across all configured accounts."""

    return aggregator.portfolio_status()


__all__ = [
    "app",
    "aggregator",
    "PortfolioRiskAggregator",
    "PortfolioStatusResponse",
]
