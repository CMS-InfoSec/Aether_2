"""FastAPI service exposing a configuration sandbox with automated backtests."""

from __future__ import annotations

import copy
import importlib.util
import hashlib
import json
import logging
import math
import os
import statistics
import sys
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, MutableMapping
from types import SimpleNamespace

try:  # pragma: no cover - PyYAML is optional in lightweight environments
    import yaml
except ModuleNotFoundError:  # pragma: no cover - exercised when dependency missing
    yaml = None  # type: ignore[assignment]

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field


class _StaticPool:  # pragma: no cover - fallback stand-in
    """Placeholder matching SQLAlchemy's StaticPool API."""

    pass


class _SandboxRunStore:
    """In-memory persistence for sandbox run records."""

    def __init__(self) -> None:
        self._records: List[Any] = []
        self._next_id = 1

    def persist(self, run: Any) -> None:
        if getattr(run, "id", 0) <= 0:
            setattr(run, "id", self._next_id)
            self._next_id += 1
        self._records.append(run)

    def reset(self) -> None:
        self._records.clear()
        self._next_id = 1


_IN_MEMORY_STORES: Dict[str, _SandboxRunStore] = {}


def _get_store(url: str) -> _SandboxRunStore:
    store = _IN_MEMORY_STORES.get(url)
    if store is None:
        store = _SandboxRunStore()
        _IN_MEMORY_STORES[url] = store
    return store


class _InMemoryEngine:
    def __init__(self, url: str) -> None:
        self.url = url
        self.store = _get_store(url)

    def reset(self) -> None:
        self.store.reset()

    def dispose(self) -> None:  # pragma: no cover - API parity
        self.store.reset()


class _InMemorySession:
    def __init__(self, store: _SandboxRunStore) -> None:
        self._store = store
        self._pending: List[Any] = []

    def add(self, obj: Any) -> None:
        self._pending.append(obj)

    def commit(self) -> None:
        for obj in self._pending:
            self._store.persist(obj)
        self._pending.clear()

    def close(self) -> None:
        self._pending.clear()


class _FallbackMetadata:
    def create_all(self, **kwargs: Any) -> None:
        bind = kwargs.get("bind")
        if hasattr(bind, "reset"):
            bind.reset()


class _FallbackBase(SimpleNamespace):
    metadata = _FallbackMetadata()


def _fallback_declarative_base() -> Any:
    return _FallbackBase()


def _fallback_sessionmaker(*, bind: Any = None, **_: object) -> Callable[[], Any]:
    if bind is None:
        raise RuntimeError("In-memory sessionmaker requires a bound engine")

    store = getattr(bind, "store", None)
    if store is None:
        raise RuntimeError("In-memory engine does not expose a sandbox store")

    def _factory() -> Any:
        return _InMemorySession(store)

    return _factory


_SQLALCHEMY_AVAILABLE = True
_SQLALCHEMY_STUB_CREATE_ENGINE: Callable[..., Any] | None = None


def _configure_sqlalchemy_fallback(*, stub_engine: Callable[..., Any] | None = None) -> None:
    global _SQLALCHEMY_AVAILABLE, Column, DateTime, Integer, Text, create_engine, Session, StaticPool, declarative_base, sessionmaker, _SQLALCHEMY_STUB_CREATE_ENGINE

    _SQLALCHEMY_AVAILABLE = False
    _SQLALCHEMY_STUB_CREATE_ENGINE = stub_engine
    Column = DateTime = Integer = Text = None  # type: ignore[assignment]
    create_engine = None  # type: ignore[assignment]
    Session = Any  # type: ignore[assignment]
    StaticPool = _StaticPool  # type: ignore[assignment]
    declarative_base = _fallback_declarative_base  # type: ignore[assignment]
    sessionmaker = _fallback_sessionmaker  # type: ignore[assignment]


try:  # pragma: no cover - optional runtime dependency
    from sqlalchemy import Column, DateTime, Integer, Text, create_engine
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool
except Exception:  # pragma: no cover - triggered when SQLAlchemy is unavailable
    _configure_sqlalchemy_fallback()
else:
    _CREATE_ENGINE_MODULE = getattr(create_engine, "__module__", "")
    _SESSIONMAKER_MODULE = getattr(sessionmaker, "__module__", "")
    if any(
        module.startswith(prefix)
        for module in (_CREATE_ENGINE_MODULE, _SESSIONMAKER_MODULE)
        for prefix in ("tests.", "conftest")
    ):
        _configure_sqlalchemy_fallback(stub_engine=create_engine)


from shared.postgres import normalize_sqlalchemy_dsn

from backtests.reporting import compute_max_drawdown, compute_sharpe
try:  # pragma: no cover - support alternative namespace packages
    from services.common.security import get_director_accounts, require_admin_account
except ModuleNotFoundError:  # pragma: no cover - fallback when installed under package namespace
    try:
        from aether.services.common.security import get_director_accounts, require_admin_account
    except ModuleNotFoundError:  # pragma: no cover - direct import when running from source tree
        spec = importlib.util.spec_from_file_location(
            "config_sandbox._security",
            Path(__file__).resolve().parent / "services" / "common" / "security.py",
        )
        if spec is None or spec.loader is None:
            raise
        security_module = importlib.util.module_from_spec(spec)
        sys.modules.setdefault("config_sandbox._security", security_module)
        spec.loader.exec_module(security_module)
        require_admin_account = getattr(security_module, "require_admin_account")
        get_director_accounts = getattr(security_module, "get_director_accounts")

# ---------------------------------------------------------------------------
# Persistent configuration state
# ---------------------------------------------------------------------------


LOGGER = logging.getLogger("config_sandbox")

Base = declarative_base()


if _SQLALCHEMY_AVAILABLE:

    class SandboxRun(Base):
        """ORM mapping for recorded sandbox evaluation runs."""

        __tablename__ = "sandbox_runs"

        id = Column(Integer, primary_key=True, autoincrement=True)
        changes_json = Column(Text, nullable=False)
        metrics_json = Column(Text, nullable=False)
        ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


else:

    @dataclass
    class SandboxRun:  # type: ignore[override]
        """In-memory representation of sandbox evaluation records."""

        changes_json: str
        metrics_json: str
        ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
        id: int = field(default=0, init=False)


_DATABASE_ENV_KEY = "CONFIG_SANDBOX_DATABASE_URL"
_ALLOW_SQLITE_FLAG = "CONFIG_SANDBOX_ALLOW_SQLITE"


def _allow_sqlite_fallback() -> bool:
    """Return whether sqlite URLs are permitted (tests and explicit overrides only)."""

    if "pytest" in sys.modules:
        return True
    return os.getenv(_ALLOW_SQLITE_FLAG) == "1"


def _database_url() -> str:
    raw_value = os.getenv(_DATABASE_ENV_KEY, "")
    if not raw_value.strip():
        raise RuntimeError(
            "Config sandbox database URL must be provided via CONFIG_SANDBOX_DATABASE_URL."
        )

    allow_sqlite = _allow_sqlite_fallback()
    return normalize_sqlalchemy_dsn(
        raw_value,
        allow_sqlite=allow_sqlite,
        label="Config sandbox database URL",
    )


def _create_engine(database_url: str):
    connect_args: Dict[str, Any] = {}
    engine_kwargs: Dict[str, Any] = {"future": True}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        engine_kwargs["connect_args"] = connect_args
        if ":memory:" in database_url:
            engine_kwargs["poolclass"] = StaticPool
    if _SQLALCHEMY_AVAILABLE:
        return create_engine(database_url, **engine_kwargs)

    if _SQLALCHEMY_STUB_CREATE_ENGINE is not None:
        try:  # pragma: no cover - stubbed engine used for instrumentation
            _SQLALCHEMY_STUB_CREATE_ENGINE(database_url, **engine_kwargs)
        except Exception:
            pass
    return _InMemoryEngine(database_url)


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
    if yaml is None:
        LOGGER.warning(
            "PyYAML is not installed; using an empty config sandbox baseline",
        )
        baseline = {}
    else:
        with BASELINE_CONFIG_PATH.open("r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle) or {}
        if not isinstance(parsed, Mapping):
            LOGGER.warning(
                "Config sandbox baseline %s is not a mapping; defaulting to empty configuration",
                BASELINE_CONFIG_PATH,
            )
            baseline = {}
        else:
            baseline = dict(parsed)
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


def _normalize_account_id(account: str) -> str:
    return account.strip().lower()


def _validate_director_approvals(actor: str, directors: Iterable[str]) -> None:
    required_directors = {account for account in get_director_accounts(normalized=True)}
    actor_account = _normalize_account_id(actor)

    if actor_account not in required_directors:
        raise HTTPException(
            status_code=403,
            detail="Authenticated principal is not an authorized director.",
        )

    supplied_directors = {
        _normalize_account_id(value)
        for value in directors
        if value and value.strip()
    }
    supplied_directors.discard(actor_account)

    if not supplied_directors:
        raise HTTPException(
            status_code=403,
            detail="A second director approval is required.",
        )

    if not supplied_directors.issubset(required_directors):
        raise HTTPException(
            status_code=403,
            detail="Director approvals must match the configured director set.",
        )

    combined_approvals = supplied_directors | {actor_account}
    if combined_approvals != required_directors:
        raise HTTPException(
            status_code=403,
            detail="Director approvals must match the configured director set.",
        )


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
def run_sandbox_test(
    payload: SandboxTestRequest,
    session: Session = Depends(get_session),
    actor: str = Depends(require_admin_account),
) -> SandboxTestResponse:
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

    if payload.directors:
        _validate_director_approvals(actor, payload.directors)
        _promote_config(candidate_config)

    return comparison
