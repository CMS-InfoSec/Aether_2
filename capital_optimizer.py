"""Capital optimization service providing portfolio rebalancing via FastAPI."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Tuple

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from shared.postgres import normalize_sqlalchemy_dsn

DATA_PATH = Path("data/capital_opt_runs.json")
_SQLITE_FALLBACK = "sqlite+pysqlite:///:memory:"


def _database_url() -> str:
    """Return the configured database URL, normalising supported schemes."""

    allow_sqlite = "pytest" in sys.modules
    label = "Capital optimizer database DSN"

    for key in (
        "CAPITAL_OPTIMIZER_DB_URL",
        "CAPITAL_OPTIMIZER_DATABASE_URL",
        "TIMESCALE_DSN",
        "DATABASE_URL",
    ):
        raw = os.getenv(key)
        if raw is None:
            continue
        candidate = raw.strip()
        if not candidate:
            continue
        return normalize_sqlalchemy_dsn(
            candidate,
            allow_sqlite=allow_sqlite,
            label=label,
        )

    if allow_sqlite:
        return normalize_sqlalchemy_dsn(
            _SQLITE_FALLBACK,
            allow_sqlite=True,
            label=label,
        )

    raise RuntimeError(
        "Capital optimizer database DSN is not configured. Set "
        "CAPITAL_OPTIMIZER_DB_URL or TIMESCALE_DSN to a PostgreSQL/Timescale "
        "connection string."
    )


def _engine_options(url: str) -> Dict[str, object]:
    options: Dict[str, object] = {"future": True, "pool_pre_ping": True}
    if url.startswith("sqlite"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if ":memory:" in url:
            options["poolclass"] = StaticPool
    return options


_DB_URL = _database_url()
_ENGINE = create_engine(_DB_URL, **_engine_options(_DB_URL))
SessionFactory = sessionmaker(
    bind=_ENGINE,
    expire_on_commit=False,
    autoflush=False,
    future=True,
)

Base = declarative_base()


class CapitalOptimizationRun(Base):
    __tablename__ = "capital_optimizer_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, nullable=False, unique=True, index=True)
    method = Column(String, nullable=False)
    inputs = Column(JSON, nullable=False)
    outputs = Column(JSON, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)


if _DB_URL.startswith("sqlite"):
    Base.metadata.create_all(bind=_ENGINE)


app = FastAPI(title="Capital Optimizer", version="1.0.0")


class RunRepository:
    """Persistence layer for capital optimizer runs backed by SQLAlchemy."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def latest(self) -> Optional[CapitalOptimizationRun]:
        stmt = (
            select(CapitalOptimizationRun)
            .order_by(CapitalOptimizationRun.ts.desc())
            .limit(1)
        )
        return self._session.execute(stmt).scalar_one_or_none()

    def create(
        self,
        *,
        run_id: str,
        method: str,
        inputs: Mapping[str, object],
        outputs: Mapping[str, object],
        timestamp: datetime,
    ) -> CapitalOptimizationRun:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)
        record = CapitalOptimizationRun(
            run_id=str(run_id),
            method=str(method),
            inputs=json.loads(json.dumps(inputs)),
            outputs=json.loads(json.dumps(outputs)),
            ts=timestamp,
        )
        self._session.add(record)
        try:
            self._session.flush()
        except IntegrityError as exc:
            raise RuntimeError(f"Run with id '{run_id}' already exists") from exc
        return record


def _to_allocation_result(record: CapitalOptimizationRun) -> "AllocationResult":
    return AllocationResult(
        run_id=record.run_id,
        timestamp=record.ts,
        method=record.method,
        inputs=record.inputs,
        account_allocations=record.outputs.get("accounts", {}),
        strategy_allocations=record.outputs.get("strategies", {}),
    )


def migrate_runs_from_file(
    store_path: Path = DATA_PATH,
    *,
    session_factory: Optional[Callable[[], Session]] = None,
) -> int:
    """Migrate historical runs from the legacy JSON store into the database."""

    if not store_path.exists():
        return 0

    with store_path.open("r", encoding="utf-8") as handle:
        try:
            payload = json.load(handle)
        except json.JSONDecodeError as exc:
            raise RuntimeError("capital_opt_runs store is corrupted") from exc

    if not isinstance(payload, list):
        raise RuntimeError("capital_opt_runs store must contain a JSON array")

    factory = session_factory or SessionFactory
    imported = 0

    with factory() as session:
        with session.begin():
            repo = RunRepository(session)
            existing_ids = set(
                session.execute(select(CapitalOptimizationRun.run_id)).scalars()
            )
            for raw in sorted(payload, key=lambda item: item.get("ts", "")):
                run_id = str(raw.get("run_id", "")).strip()
                if not run_id or run_id in existing_ids:
                    continue
                ts_raw = raw.get("ts")
                if isinstance(ts_raw, str):
                    timestamp = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                elif isinstance(ts_raw, datetime):
                    timestamp = ts_raw
                else:
                    timestamp = datetime.now(timezone.utc)
                repo.create(
                    run_id=run_id,
                    method=str(raw.get("method", "unknown")),
                    inputs=raw.get("inputs", {}),
                    outputs=raw.get("outputs", {}),
                    timestamp=timestamp,
                )
                existing_ids.add(run_id)
                imported += 1

    return imported


class StrategyInput(BaseModel):
    """Historical series and constraints for a strategy."""

    strategy_id: str = Field(..., description="Unique identifier of the strategy")
    pnl_curve: List[float] = Field(
        default_factory=list,
        description="Recent profit and loss curve sampled at uniform frequency.",
    )
    compliance_max_allocation: float = Field(
        1.0,
        ge=0.0,
        le=1.0,
        description="Maximum allocation allowed by compliance expressed as a fraction of NAV.",
    )

    @model_validator(mode="after")
    def validate_curve(cls, model: "StrategyInput") -> "StrategyInput":
        if len(model.pnl_curve) < 2:
            raise ValueError(
                "pnl_curve must contain at least two points to estimate risk/return"
            )
        return model


class AccountInput(BaseModel):
    """Account level risk envelope and associated strategies."""

    account_id: str = Field(..., description="Unique identifier for the account")
    drawdown_limit: float = Field(
        0.15,
        gt=0.0,
        description="Maximum tolerated drawdown expressed as a fraction of starting NAV.",
    )
    strategies: List[StrategyInput] = Field(default_factory=list)

    @model_validator(mode="after")
    def non_empty(cls, model: "AccountInput") -> "AccountInput":
        if not model.strategies:
            raise ValueError("each account must include at least one strategy")
        return model


class RebalanceRequest(BaseModel):
    """Request payload for a new optimization run."""

    accounts: List[AccountInput] = Field(default_factory=list)
    risk_aversion: float = Field(
        0.5,
        gt=0.0,
        description="Risk aversion parameter for mean-variance optimization.",
    )
    method: str = Field(
        "mean_variance",
        pattern="^(mean_variance|cvar)$",
        description="Optimization method to use (mean_variance or cvar).",
    )

    @model_validator(mode="after")
    def non_empty_accounts(cls, model: "RebalanceRequest") -> "RebalanceRequest":
        if not model.accounts:
            raise ValueError("accounts must not be empty")
        return model


class AllocationResult(BaseModel):
    """Optimization result detailing allocations by account and strategy."""

    run_id: str
    timestamp: datetime
    method: str
    inputs: Mapping[str, object]
    account_allocations: Mapping[str, float]
    strategy_allocations: Mapping[str, float]


def _compute_returns(pnl_curve: Iterable[float]) -> np.ndarray:
    series = np.asarray(list(pnl_curve), dtype=float)
    diffs = np.diff(series)
    with np.errstate(divide="ignore", invalid="ignore"):
        baseline = series[:-1]
        returns = np.divide(
            diffs,
            np.where(baseline == 0, 1.0, np.abs(baseline)),
            out=np.zeros_like(diffs, dtype=float),
            where=True,
        )
    return returns


def _max_drawdown(curve: np.ndarray) -> float:
    if curve.size == 0:
        return 0.0
    running_max = np.maximum.accumulate(curve)
    peak = np.where(running_max == 0, 1.0, running_max)
    drawdowns = (running_max - curve) / peak
    return float(np.max(drawdowns))


@dataclass(slots=True)
class _StrategyContext:
    account_id: str
    strategy_id: str
    returns: np.ndarray
    compliance_cap: float


def _prepare_context(request: RebalanceRequest) -> Tuple[List[_StrategyContext], np.ndarray]:
    contexts: List[_StrategyContext] = []
    returns_matrix: List[np.ndarray] = []
    for account in request.accounts:
        for strategy in account.strategies:
            returns = _compute_returns(strategy.pnl_curve)
            contexts.append(
                _StrategyContext(
                    account_id=account.account_id,
                    strategy_id=strategy.strategy_id,
                    returns=returns,
                    compliance_cap=float(strategy.compliance_max_allocation),
                )
            )
            returns_matrix.append(returns)
    if not contexts:
        raise ValueError("no strategies available for optimization")
    lengths = {len(ctx.returns) for ctx in contexts}
    if len(lengths) != 1:
        raise ValueError("all pnl_curves must have the same length across strategies")
    matrix = np.vstack(returns_matrix)
    return contexts, matrix


def _mean_variance_optimize(
    contexts: List[_StrategyContext],
    returns_matrix: np.ndarray,
    risk_aversion: float,
    account_limits: Mapping[str, float],
) -> np.ndarray:
    mu = returns_matrix.mean(axis=1)
    cov = np.cov(returns_matrix)
    if cov.ndim == 0:
        cov = np.array([[float(cov) + 1e-6]])
    else:
        cov = np.asarray(cov, dtype=float)
        cov += np.eye(cov.shape[0]) * 1e-6
    scaled_cov = cov * risk_aversion
    try:
        inv_cov = np.linalg.inv(scaled_cov)
    except np.linalg.LinAlgError:
        inv_cov = np.linalg.pinv(scaled_cov)
    raw_weights = inv_cov @ mu
    if np.allclose(raw_weights, 0.0):
        raw_weights = np.ones_like(raw_weights)
    weights = _project_simplex(raw_weights)
    weights = _enforce_compliance_caps(weights, contexts)
    weights = _enforce_account_limits(weights, contexts, returns_matrix, account_limits)
    return weights


def _cvar_optimize(
    contexts: List[_StrategyContext],
    returns_matrix: np.ndarray,
    account_limits: Mapping[str, float],
) -> np.ndarray:
    percentile = 5
    tail_losses = np.percentile(returns_matrix, percentile, axis=1)
    cvar_scores = returns_matrix.mean(axis=1) - np.abs(tail_losses)
    if np.allclose(cvar_scores, 0.0):
        cvar_scores = np.ones_like(cvar_scores)
    weights = _project_simplex(cvar_scores)
    weights = _enforce_compliance_caps(weights, contexts)
    weights = _enforce_account_limits(weights, contexts, returns_matrix, account_limits)
    return weights


def _project_simplex(vector: np.ndarray, z: float = 1.0) -> np.ndarray:
    v = np.asarray(vector, dtype=float)
    if v.ndim != 1:
        v = v.reshape(-1)
    n = v.size
    if n == 0:
        return v
    u = np.sort(v)[::-1]
    cssv = np.cumsum(u)
    rho = np.nonzero(u * np.arange(1, n + 1) > (cssv - z))[0]
    if rho.size == 0:
        theta = 0.0
    else:
        rho_index = rho[-1]
        theta = (cssv[rho_index] - z) / (rho_index + 1)
    w = np.maximum(v - theta, 0.0)
    total = w.sum()
    if total <= 0:
        return np.full_like(w, 1.0 / n)
    return w / total


def _enforce_compliance_caps(weights: np.ndarray, contexts: List[_StrategyContext]) -> np.ndarray:
    adjusted = weights.copy()
    total = 0.0
    for idx, ctx in enumerate(contexts):
        cap = max(0.0, min(1.0, ctx.compliance_cap))
        if adjusted[idx] > cap:
            adjusted[idx] = cap
        total += adjusted[idx]
    if total == 0:
        return _project_simplex(np.ones_like(adjusted))
    return adjusted / total


def _enforce_account_limits(
    weights: np.ndarray,
    contexts: List[_StrategyContext],
    returns_matrix: np.ndarray,
    account_limits: Mapping[str, float],
) -> np.ndarray:
    adjusted = weights.copy()
    updated = False
    for account_id, limit in account_limits.items():
        indices = [idx for idx, ctx in enumerate(contexts) if ctx.account_id == account_id]
        if not indices:
            continue
        account_returns = returns_matrix[indices, :]
        combined_returns = np.sum(adjusted[indices, None] * account_returns, axis=0)
        pnl_curve = np.concatenate(([0.0], np.cumsum(combined_returns)))
        drawdown = _max_drawdown(pnl_curve)
        if drawdown > limit > 0:
            factor = limit / drawdown
            for idx in indices:
                adjusted[idx] *= factor
            updated = True
    if updated:
        adjusted = _project_simplex(adjusted)
    return adjusted


def _aggregate_allocations(
    weights: np.ndarray, contexts: List[_StrategyContext]
) -> Tuple[Dict[str, float], Dict[str, float]]:
    account_allocations: Dict[str, float] = {}
    strategy_allocations: Dict[str, float] = {}
    for weight, ctx in zip(weights.tolist(), contexts):
        strategy_allocations[ctx.strategy_id] = round(float(weight), 6)
        account_allocations.setdefault(ctx.account_id, 0.0)
        account_allocations[ctx.account_id] += float(weight)
    account_allocations = {
        account_id: round(value, 6) for account_id, value in account_allocations.items()
    }
    return account_allocations, strategy_allocations


@app.get("/optimizer/status", response_model=Optional[AllocationResult])
async def optimizer_status() -> Optional[AllocationResult]:
    """Return the allocations from the most recent optimization run."""

    with SessionFactory() as session:
        with session.begin():
            repo = RunRepository(session)
            latest = repo.latest()
            if latest is None:
                return None
            result = _to_allocation_result(latest)
    return result


@app.post("/optimizer/rebalance", response_model=AllocationResult)
async def optimizer_rebalance(payload: RebalanceRequest) -> AllocationResult:
    """Execute a new optimization run and persist the outcome."""

    try:
        contexts, returns_matrix = _prepare_context(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    account_limits = {
        account.account_id: float(account.drawdown_limit)
        for account in payload.accounts
    }

    if payload.method == "mean_variance":
        weights = _mean_variance_optimize(
            contexts, returns_matrix, payload.risk_aversion, account_limits
        )
    else:
        weights = _cvar_optimize(contexts, returns_matrix, account_limits)

    account_allocations, strategy_allocations = _aggregate_allocations(weights, contexts)
    timestamp = datetime.now(timezone.utc)
    run_id = f"run_{timestamp.strftime('%Y%m%dT%H%M%S%f')}"
    run_inputs = payload.model_dump(mode="json")
    run_outputs = {
        "accounts": account_allocations,
        "strategies": strategy_allocations,
    }

    with SessionFactory() as session:
        with session.begin():
            repo = RunRepository(session)
            record = repo.create(
                run_id=run_id,
                method=payload.method,
                inputs=run_inputs,
                outputs=run_outputs,
                timestamp=timestamp,
            )
            result = _to_allocation_result(record)

    return result


__all__ = [
    "app",
    "RunRepository",
    "CapitalOptimizationRun",
    "Base",
    "SessionFactory",
    "migrate_runs_from_file",
]
