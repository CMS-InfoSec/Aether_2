"""FastAPI service exposing a configuration sandbox with automated backtests."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import statistics
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, MutableMapping

import yaml
from fastapi import Depends, FastAPI
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, Integer, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from backtests.reporting import compute_max_drawdown, compute_sharpe

# ---------------------------------------------------------------------------
# Persistent configuration state
# ---------------------------------------------------------------------------


Base = declarative_base()


class SandboxRun(Base):
    """ORM mapping for recorded sandbox evaluation runs."""

    __tablename__ = "sandbox_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    changes_json = Column(Text, nullable=False)
    metrics_json = Column(Text, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


def _database_url() -> str:
    return os.getenv("CONFIG_SANDBOX_DATABASE_URL", "sqlite+pysqlite:////tmp/config_sandbox.db")


def _create_engine(database_url: str):
    connect_args: Dict[str, Any] = {}
    engine_kwargs: Dict[str, Any] = {"future": True}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        engine_kwargs["connect_args"] = connect_args
        if ":memory:" in database_url:
            engine_kwargs["poolclass"] = StaticPool
    return create_engine(database_url, **engine_kwargs)


ENGINE = _create_engine(_database_url())
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, expire_on_commit=False, future=True)
Base.metadata.create_all(bind=ENGINE)


def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


DIRECTOR_ACCOUNTS = {"director-1", "director-2"}
_CONFIG_LOCK = threading.Lock()

_BASE_DIR = Path(__file__).resolve().parent
_DEFAULT_BASELINE_PATH = _BASE_DIR / "config" / "system.yaml"
BASELINE_CONFIG_PATH = Path(os.getenv("SANDBOX_BASELINE_PATH", str(_DEFAULT_BASELINE_PATH)))
LIVE_CONFIG_PATH = Path(os.getenv("SANDBOX_LIVE_PATH", "/tmp/config_live.json"))

STARTING_EQUITY = 1_000_000.0
TRADING_DAYS = 252


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(value)
    return repr(value)


def _json_dumps(payload: Any, *, indent: int | None = None) -> str:
    return json.dumps(payload, default=_json_default, sort_keys=True, indent=indent)


def _load_initial_config() -> Dict[str, Any]:
    if LIVE_CONFIG_PATH.exists():
        try:
            return json.loads(LIVE_CONFIG_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive branch
            raise RuntimeError("Failed to parse live config cache") from exc
    if not BASELINE_CONFIG_PATH.exists():
        raise RuntimeError(f"Baseline configuration file not found: {BASELINE_CONFIG_PATH}")
    with BASELINE_CONFIG_PATH.open("r", encoding="utf-8") as handle:
        baseline = yaml.safe_load(handle) or {}
    LIVE_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LIVE_CONFIG_PATH.write_text(_json_dumps(baseline, indent=2), encoding="utf-8")
    return baseline


_LIVE_CONFIG = _load_initial_config()


def _deep_merge(base: Mapping[str, Any], updates: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a deep merge of ``base`` with ``updates`` without mutating inputs."""

    result = copy.deepcopy(base)
    stack: List[tuple[MutableMapping[str, Any], Mapping[str, Any]]] = [(result, updates)]
    while stack:
        destination, source = stack.pop()
        for key, value in source.items():
            if isinstance(value, Mapping):
                existing = destination.get(key)
                if isinstance(existing, MutableMapping):
                    stack.append((existing, value))
                    continue
            destination[key] = copy.deepcopy(value)
    return result


def _flatten_numeric(payload: Any) -> List[float]:
    values: List[float] = []
    queue: List[Any] = [payload]
    while queue:
        item = queue.pop()
        if isinstance(item, Mapping):
            queue.extend(item.values())
        elif isinstance(item, (list, tuple, set)):
            queue.extend(item)
        elif isinstance(item, (int, float)):
            values.append(float(item))
    return values


def _config_score(config: Mapping[str, Any]) -> float:
    values = _flatten_numeric(config)
    if not values:
        return 0.0
    transformed = [math.tanh(value / 100.0) for value in values]
    return statistics.fmean(transformed)


def _seed_from_config(config: Mapping[str, Any]) -> int:
    payload = _json_dumps(config).encode("utf-8")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=False)


def _simulate_returns(config: Mapping[str, Any]) -> List[float]:
    seed = _seed_from_config(config)
    rng = _DeterministicRandom(seed)
    score = _config_score(config)
    mean = 0.0005 + 0.0003 * score
    volatility = max(0.0005, 0.01 * (1.0 - 0.4 * score))
    returns = [rng.gauss(mean, volatility) for _ in range(TRADING_DAYS)]
    return returns


class _DeterministicRandom:
    """Small wrapper around ``random.Random`` to allow hashing based seeding."""

    def __init__(self, seed: int) -> None:
        import random

        self._rng = random.Random(seed)

    def gauss(self, mean: float, stddev: float) -> float:
        return self._rng.gauss(mean, stddev)


def _equity_curve(returns: Iterable[float]) -> List[float]:
    equity = STARTING_EQUITY
    curve: List[float] = [STARTING_EQUITY]
    for value in returns:
        equity *= 1.0 + value
        curve.append(equity)
    return curve


def _run_backtest(config: Mapping[str, Any]) -> Dict[str, float]:
    returns = _simulate_returns(config)
    equity_curve = _equity_curve(returns)
    pnl = equity_curve[-1] - STARTING_EQUITY
    sharpe = compute_sharpe(returns)
    drawdown = compute_max_drawdown(equity_curve)
    return {
        "pnl": float(pnl),
        "sharpe": float(sharpe),
        "max_drawdown": float(drawdown),
    }


def _current_config() -> Dict[str, Any]:
    with _CONFIG_LOCK:
        return copy.deepcopy(_LIVE_CONFIG)


def _promote_config(new_config: Mapping[str, Any]) -> None:
    serialised = _json_dumps(new_config, indent=2)
    with _CONFIG_LOCK:
        global _LIVE_CONFIG
        LIVE_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        LIVE_CONFIG_PATH.write_text(serialised, encoding="utf-8")
        if isinstance(new_config, Mapping):
            _LIVE_CONFIG = copy.deepcopy(dict(new_config))
        else:  # pragma: no cover - defensive branch
            _LIVE_CONFIG = copy.deepcopy(new_config)


def _has_director_approval(directors: Iterable[str]) -> bool:
    return DIRECTOR_ACCOUNTS.issubset(set(directors))


# ---------------------------------------------------------------------------
# API schemas
# ---------------------------------------------------------------------------


class SandboxTestRequest(BaseModel):
    """Payload for running a sandbox configuration test."""

    config_changes: Dict[str, Any] = Field(..., description="Partial configuration overrides to evaluate")
    directors: List[str] = Field(default_factory=list, description="Directors approving the promotion")


class SandboxTestResponse(BaseModel):
    baseline_pnl: float
    new_pnl: float
    sharpe_diff: float
    drawdown_diff: float


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


app = FastAPI(title="Config Sandbox Service")


@app.post("/sandbox/test_config", response_model=SandboxTestResponse)
def run_sandbox_test(payload: SandboxTestRequest, session: Session = Depends(get_session)) -> SandboxTestResponse:
    baseline_config = _current_config()
    candidate_config = _deep_merge(baseline_config, payload.config_changes)

    baseline_metrics = _run_backtest(baseline_config)
    candidate_metrics = _run_backtest(candidate_config)

    comparison = SandboxTestResponse(
        baseline_pnl=baseline_metrics["pnl"],
        new_pnl=candidate_metrics["pnl"],
        sharpe_diff=candidate_metrics["sharpe"] - baseline_metrics["sharpe"],
        drawdown_diff=candidate_metrics["max_drawdown"] - baseline_metrics["max_drawdown"],
    )

    metrics_record = {
        "baseline": baseline_metrics,
        "candidate": candidate_metrics,
        "comparison": comparison.dict(),
    }

    run_entry = SandboxRun(
        changes_json=_json_dumps(payload.config_changes),
        metrics_json=_json_dumps(metrics_record),
    )
    session.add(run_entry)
    session.commit()

    if payload.directors and _has_director_approval(payload.directors):
        _promote_config(candidate_config)

    return comparison
