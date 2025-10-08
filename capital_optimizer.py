"""Capital optimization service providing portfolio rebalancing via FastAPI."""

from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple
from types import SimpleNamespace

try:  # numpy is optional during local testing environments.
    import numpy as np  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - executed when numpy is unavailable.
    np = None  # type: ignore[assignment]

_SQLALCHEMY_AVAILABLE = True

try:  # pragma: no cover - SQLAlchemy may be absent in lightweight environments.
    from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine, select
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool
except Exception:  # pragma: no cover - executed when SQLAlchemy isn't installed.
    _SQLALCHEMY_AVAILABLE = False
    JSON = Column = DateTime = Integer = String = None  # type: ignore[assignment]
    IntegrityError = RuntimeError  # type: ignore[assignment]
    Session = Any  # type: ignore[assignment]
    StaticPool = type("StaticPool", (), {})  # type: ignore[assignment]

    def create_engine(*args: object, **kwargs: object) -> "_MemoryEngine":  # type: ignore[override]
        return _MemoryEngine(str(args[0]) if args else str(kwargs.get("url", "memory")))

    def declarative_base(*args: object, **kwargs: object) -> type:  # type: ignore[override]
        return _MemoryBase

    def sessionmaker(*, bind: object | None = None, **__: object) -> "_MemorySessionFactory":  # type: ignore[override]
        return _MemorySessionFactory(str(getattr(bind, "_bind_key", bind)))

    def select(*args: object, **kwargs: object) -> None:  # type: ignore[override]
        raise RuntimeError("SQLAlchemy is unavailable in this environment")
else:
    if not getattr(select, "__module__", "").startswith("sqlalchemy"):
        _SQLALCHEMY_AVAILABLE = False

if not _SQLALCHEMY_AVAILABLE:
    JSON = Column = DateTime = Integer = String = None  # type: ignore[assignment]
    IntegrityError = RuntimeError  # type: ignore[assignment]
    Session = Any  # type: ignore[assignment]
    StaticPool = type("StaticPool", (), {})  # type: ignore[assignment]

    def create_engine(*args: object, **kwargs: object) -> "_MemoryEngine":  # type: ignore[override]
        return _MemoryEngine(str(args[0]) if args else str(kwargs.get("url", "memory")))

    def declarative_base(*args: object, **kwargs: object) -> type:  # type: ignore[override]
        return _MemoryBase

    def sessionmaker(*, bind: object | None = None, **__: object) -> "_MemorySessionFactory":  # type: ignore[override]
        return _MemorySessionFactory(str(getattr(bind, "_bind_key", bind)))

    def select(*args: object, **kwargs: object) -> None:  # type: ignore[override]
        raise RuntimeError("SQLAlchemy is unavailable in this environment")

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

from shared.postgres import normalize_sqlalchemy_dsn

DATA_PATH = Path("data/capital_opt_runs.json")
_SQLITE_FALLBACK = "sqlite+pysqlite:///:memory:"


class _MemoryMetadata:
    def create_all(self, bind: object | None = None) -> None:  # pragma: no cover - noop
        del bind


class _MemoryBase:
    metadata = _MemoryMetadata()


class _MemoryEngine:
    """Lightweight engine stand-in used when SQLAlchemy is unavailable."""

    def __init__(self, url: str) -> None:
        self._bind_key = url or "memory"
        self._aether_url = self._bind_key

    def dispose(self) -> None:  # pragma: no cover - noop
        return None


class _MemoryTransaction:
    def __init__(self, session: "_MemorySession") -> None:
        self._session = session

    def __enter__(self) -> "_MemorySession":
        return self._session

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None


class _MemorySession:
    """Minimal session implementation backing the in-memory repository."""

    def __init__(self, store_key: str) -> None:
        self._store_key = store_key or "memory"
        self._bind_identifier = self._store_key

    def __enter__(self) -> "_MemorySession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        self.close()

    def begin(self) -> _MemoryTransaction:
        return _MemoryTransaction(self)

    def close(self) -> None:  # pragma: no cover - noop
        return None

    def commit(self) -> None:  # pragma: no cover - noop
        return None

    def rollback(self) -> None:  # pragma: no cover - noop
        return None

    def add(self, obj: object) -> None:  # pragma: no cover - noop
        del obj

    def flush(self) -> None:  # pragma: no cover - noop
        return None

    def execute(self, *args: object, **kwargs: object) -> SimpleNamespace:  # pragma: no cover - noop
        return SimpleNamespace(
            scalars=lambda: SimpleNamespace(all=lambda: []),
            scalar_one_or_none=lambda: None,
        )

    def get(self, *args: object, **kwargs: object) -> None:  # pragma: no cover - noop
        return None


class _MemorySessionFactory:
    def __init__(self, key: str | None) -> None:
        self._key = str(key or "memory")

    def __call__(self) -> _MemorySession:
        return _MemorySession(self._key)


@dataclass
class _MemoryRunRecord:
    id: int
    run_id: str
    method: str
    inputs: Dict[str, object]
    outputs: Dict[str, object]
    ts: datetime


_MemoryRunRecord.id = "id"  # type: ignore[assignment]
_MemoryRunRecord.run_id = "run_id"  # type: ignore[assignment]
_MemoryRunRecord.method = "method"  # type: ignore[assignment]
_MemoryRunRecord.inputs = "inputs"  # type: ignore[assignment]
_MemoryRunRecord.outputs = "outputs"  # type: ignore[assignment]
_MemoryRunRecord.ts = "ts"  # type: ignore[assignment]


_MEMORY_RUN_STORES: Dict[str, List[_MemoryRunRecord]] = {}
_MEMORY_RUN_COUNTERS: Dict[str, int] = {}


def _memory_store_key(session: Session) -> str:
    identifier = getattr(session, "_bind_identifier", None)
    if not identifier:
        identifier = getattr(session, "_store_key", "memory")
    return str(identifier or "memory")


def _memory_store_for_session(session: Session) -> List[_MemoryRunRecord]:
    key = _memory_store_key(session)
    store = _MEMORY_RUN_STORES.setdefault(key, [])
    setattr(session, "_memory_runs", store)
    return store


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


if _SQLALCHEMY_AVAILABLE:

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
else:
    CapitalOptimizationRun = _MemoryRunRecord  # type: ignore[assignment]


app = FastAPI(title="Capital Optimizer", version="1.0.0")


class RunRepository:
    """Persistence layer for capital optimizer runs backed by SQLAlchemy."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._use_memory = not _SQLALCHEMY_AVAILABLE or not hasattr(session, "execute")

    def latest(self) -> Optional[CapitalOptimizationRun]:
        if self._use_memory:
            store = _memory_store_for_session(self._session)
            if not store:
                return None
            return max(store, key=lambda record: record.ts)
        stmt = (
            select(CapitalOptimizationRun)
            .order_by(CapitalOptimizationRun.ts.desc())
            .limit(1)
        )
        result = self._session.execute(stmt)
        scalar_one_or_none = getattr(result, "scalar_one_or_none", None)
        if callable(scalar_one_or_none):
            return scalar_one_or_none()
        scalars = getattr(result, "scalars", None)
        if callable(scalars):
            return scalars().first()  # type: ignore[return-value]
        return None

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
        serialised_inputs = json.loads(json.dumps(inputs))
        serialised_outputs = json.loads(json.dumps(outputs))
        if self._use_memory:
            store = _memory_store_for_session(self._session)
            if any(existing.run_id == run_id for existing in store):
                raise RuntimeError(f"Run with id '{run_id}' already exists")
            key = _memory_store_key(self._session)
            counter = _MEMORY_RUN_COUNTERS.get(key, 0) + 1
            _MEMORY_RUN_COUNTERS[key] = counter
            record = _MemoryRunRecord(
                id=counter,
                run_id=str(run_id),
                method=str(method),
                inputs=serialised_inputs,
                outputs=serialised_outputs,
                ts=timestamp,
            )
            store.append(record)
            store.sort(key=lambda entry: entry.ts)
            return record

        record = CapitalOptimizationRun(
            run_id=str(run_id),
            method=str(method),
            inputs=serialised_inputs,
            outputs=serialised_outputs,
            ts=timestamp,
        )
        self._session.add(record)
        try:
            self._session.flush()
        except IntegrityError as exc:
            raise RuntimeError(f"Run with id '{run_id}' already exists") from exc
        return record

    def all(self) -> List[CapitalOptimizationRun | _MemoryRunRecord]:
        if self._use_memory:
            return list(_memory_store_for_session(self._session))
        result = self._session.execute(select(CapitalOptimizationRun))
        scalars = getattr(result, "scalars", None)
        if callable(scalars):
            return list(scalars())
        return []

    @property
    def is_memory_backend(self) -> bool:
        return self._use_memory


def _to_allocation_result(record: CapitalOptimizationRun | _MemoryRunRecord) -> "AllocationResult":
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
            if repo.is_memory_backend:
                existing_ids = {record.run_id for record in repo.all()}
            else:
                result = session.execute(select(CapitalOptimizationRun.run_id))
                scalars = getattr(result, "scalars", None)
                existing_ids = set(scalars()) if callable(scalars) else set()
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


def _compute_returns(pnl_curve: Iterable[float]) -> List[float]:
    series = [float(value) for value in pnl_curve]
    if len(series) < 2:
        return []
    returns: List[float] = []
    previous = series[0]
    for value in series[1:]:
        baseline = abs(previous) if previous != 0 else 1.0
        returns.append((value - previous) / baseline)
        previous = value
    return returns


def _max_drawdown(curve: Sequence[float]) -> float:
    values = [float(value) for value in curve]
    if not values:
        return 0.0
    running_max = values[0]
    worst = 0.0
    for value in values:
        running_max = max(running_max, value)
        peak = running_max if running_max != 0 else 1.0
        drawdown = (running_max - value) / peak
        if drawdown > worst:
            worst = drawdown
    return worst


@dataclass(slots=True)
class _StrategyContext:
    account_id: str
    strategy_id: str
    returns: List[float]
    compliance_cap: float


def _prepare_context(
    request: RebalanceRequest,
) -> Tuple[List[_StrategyContext], Sequence[Sequence[float]]]:
    contexts: List[_StrategyContext] = []
    returns_matrix: List[List[float]] = []
    for account in request.accounts:
        for strategy in account.strategies:
            returns = _compute_returns(strategy.pnl_curve)
            contexts.append(
                _StrategyContext(
                    account_id=account.account_id,
                    strategy_id=strategy.strategy_id,
                    returns=list(returns),
                    compliance_cap=float(strategy.compliance_max_allocation),
                )
            )
            returns_matrix.append(list(returns))
    if not contexts:
        raise ValueError("no strategies available for optimization")
    lengths = {len(ctx.returns) for ctx in contexts}
    if len(lengths) != 1:
        raise ValueError("all pnl_curves must have the same length across strategies")
    if np is not None:
        matrix: Sequence[Sequence[float]] = np.asarray(returns_matrix, dtype=float)
    else:
        matrix = returns_matrix
    return contexts, matrix


def _to_list(values: Sequence[float] | Iterable[float]) -> List[float]:
    if np is not None and isinstance(values, np.ndarray):  # type: ignore[arg-type]
        return values.astype(float).tolist()
    return [float(value) for value in values]


def _from_list(values: Sequence[float], template: Sequence[float] | Iterable[float]) -> Sequence[float]:
    if np is not None and isinstance(template, np.ndarray):  # type: ignore[arg-type]
        return np.asarray(values, dtype=float)
    return list(values)


def _matrix_as_list(matrix: Sequence[Sequence[float]]) -> List[List[float]]:
    if np is not None and isinstance(matrix, np.ndarray):  # type: ignore[arg-type]
        return matrix.astype(float).tolist()
    return [list(map(float, row)) for row in matrix]


def _safe_mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _safe_variance(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    average = _safe_mean(values)
    return float(sum((value - average) ** 2 for value in values) / len(values))


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (percentile / 100.0) * (len(ordered) - 1)
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return ordered[lower]
    weight = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def _mean_variance_optimize(
    contexts: List[_StrategyContext],
    returns_matrix: Sequence[Sequence[float]],
    risk_aversion: float,
    account_limits: Mapping[str, float],
) -> Sequence[float]:
    if np is not None and isinstance(returns_matrix, np.ndarray):  # type: ignore[arg-type]
        mu = returns_matrix.mean(axis=1)
        cov = np.cov(returns_matrix)
        if getattr(cov, "ndim", 0) == 0:
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

    rows = _matrix_as_list(returns_matrix)
    means = [_safe_mean(row) for row in rows]
    variances = [_safe_variance(row) for row in rows]
    epsilon = 1e-9
    raw_scores: List[float] = []
    for mean_value, variance in zip(means, variances):
        penalty = variance * max(risk_aversion, epsilon) + epsilon
        raw_scores.append(mean_value / penalty if penalty else mean_value)
    if all(abs(score) < 1e-9 for score in raw_scores):
        raw_scores = [1.0 for _ in raw_scores]
    weights = _project_simplex(raw_scores)
    weights = _enforce_compliance_caps(weights, contexts)
    weights = _enforce_account_limits(weights, contexts, rows, account_limits)
    return weights


def _cvar_optimize(
    contexts: List[_StrategyContext],
    returns_matrix: Sequence[Sequence[float]],
    account_limits: Mapping[str, float],
) -> Sequence[float]:
    percentile = 5
    if np is not None and isinstance(returns_matrix, np.ndarray):  # type: ignore[arg-type]
        tail_losses = np.percentile(returns_matrix, percentile, axis=1)
        cvar_scores = returns_matrix.mean(axis=1) - np.abs(tail_losses)
        if np.allclose(cvar_scores, 0.0):
            cvar_scores = np.ones_like(cvar_scores)
        weights = _project_simplex(cvar_scores)
        weights = _enforce_compliance_caps(weights, contexts)
        weights = _enforce_account_limits(weights, contexts, returns_matrix, account_limits)
        return weights

    rows = _matrix_as_list(returns_matrix)
    tail_losses = [_percentile(row, percentile) for row in rows]
    means = [_safe_mean(row) for row in rows]
    cvar_scores = [mean - abs(tail) for mean, tail in zip(means, tail_losses)]
    if all(abs(score) < 1e-9 for score in cvar_scores):
        cvar_scores = [1.0 for _ in cvar_scores]
    weights = _project_simplex(cvar_scores)
    weights = _enforce_compliance_caps(weights, contexts)
    weights = _enforce_account_limits(weights, contexts, rows, account_limits)
    return weights


def _project_simplex(
    vector: Sequence[float] | Iterable[float], z: float = 1.0
) -> Sequence[float]:
    values = _to_list(vector)
    if not values:
        return _from_list(values, vector)
    sorted_values = sorted(values, reverse=True)
    cumulative: List[float] = []
    running_total = 0.0
    for value in sorted_values:
        running_total += value
        cumulative.append(running_total)
    rho = -1
    for index, value in enumerate(sorted_values):
        if value * (index + 1) > (cumulative[index] - z):
            rho = index
    theta = (cumulative[rho] - z) / (rho + 1) if rho >= 0 else 0.0
    projected = [max(value - theta, 0.0) for value in values]
    total = sum(projected)
    if total <= 0:
        uniform = 1.0 / len(projected) if projected else 0.0
        projected = [uniform for _ in projected]
    else:
        projected = [value / total for value in projected]
    return _from_list(projected, vector)


def _enforce_compliance_caps(
    weights: Sequence[float] | Iterable[float], contexts: List[_StrategyContext]
) -> Sequence[float]:
    adjusted = _to_list(weights)
    total = 0.0
    for idx, ctx in enumerate(contexts):
        cap = max(0.0, min(1.0, ctx.compliance_cap))
        if adjusted[idx] > cap:
            adjusted[idx] = cap
        total += adjusted[idx]
    if total == 0:
        return _from_list(_project_simplex([1.0] * len(adjusted)), weights)
    normalised = [value / total for value in adjusted]
    return _from_list(normalised, weights)


def _enforce_account_limits(
    weights: Sequence[float] | Iterable[float],
    contexts: List[_StrategyContext],
    returns_matrix: Sequence[Sequence[float]],
    account_limits: Mapping[str, float],
) -> Sequence[float]:
    adjusted = _to_list(weights)
    rows = _matrix_as_list(returns_matrix)
    updated = False
    for account_id, limit in account_limits.items():
        indices = [idx for idx, ctx in enumerate(contexts) if ctx.account_id == account_id]
        if not indices:
            continue
        if not rows:
            continue
        periods = len(rows[0])
        combined_returns: List[float] = []
        for column in range(periods):
            total = 0.0
            for idx in indices:
                total += adjusted[idx] * rows[idx][column]
            combined_returns.append(total)
        pnl_curve = [0.0]
        for value in combined_returns:
            pnl_curve.append(pnl_curve[-1] + value)
        drawdown = _max_drawdown(pnl_curve)
        if drawdown > limit > 0:
            factor = limit / drawdown if drawdown else 0.0
            for idx in indices:
                adjusted[idx] *= factor
            updated = True
    if updated:
        adjusted = _to_list(_project_simplex(adjusted))
    return _from_list(adjusted, weights)


def _aggregate_allocations(
    weights: Sequence[float] | Iterable[float], contexts: List[_StrategyContext]
) -> Tuple[Dict[str, float], Dict[str, float]]:
    account_allocations: Dict[str, float] = {}
    strategy_allocations: Dict[str, float] = {}
    for weight, ctx in zip(_to_list(weights), contexts):
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
