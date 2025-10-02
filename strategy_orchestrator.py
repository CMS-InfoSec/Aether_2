"""FastAPI service coordinating strategy registration and routing.

The strategy orchestrator maintains a registry of trading strategies and their
NAV allocations, persists the state to TimescaleDB (or a compatible Postgres
instance), and forwards trade intents to the risk engine with strategy context.

The module provides endpoints for registering strategies, toggling them on or
off, and retrieving the current allocation view that directors consume in the
UI.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Dict, Iterable, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, PositiveFloat, constr
from sqlalchemy import Boolean, Column, DateTime, Float, String, create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import NullPool

from services.common.schemas import RiskValidationRequest, RiskValidationResponse
from strategy_bus import StrategySignalBus, ensure_signal_tables

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


Base = declarative_base()


class StrategyRecord(Base):
    """SQLAlchemy representation of a trading strategy entry."""

    __tablename__ = "strategies"

    name = Column(String, primary_key=True)
    enabled = Column(Boolean, nullable=False, default=True)
    max_nav_pct = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class StrategySnapshot:
    name: str
    description: str
    enabled: bool
    max_nav_pct: float
    allocated_nav_pct: float
    created_at: datetime


class StrategyRegistryError(RuntimeError):
    """Base error for strategy registry failures."""


class StrategyNotFound(StrategyRegistryError):
    """Raised when an operation references a missing strategy."""


class StrategyAllocationError(StrategyRegistryError):
    """Raised when NAV allocations exceed the configured cap."""


class StrategyRegistry:
    """Persistence backed registry for trading strategies."""

    def __init__(
        self,
        session_factory: sessionmaker,
        *,
        risk_engine_url: str,
        default_strategies: Iterable[tuple[str, str, float]],
        http_timeout: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._risk_engine_url = risk_engine_url.rstrip("/")
        self._http_timeout = http_timeout
        self._lock = RLock()
        self._descriptions: Dict[str, str] = {}
        self._bootstrap_defaults(default_strategies)

    @contextmanager
    def _session_scope(self) -> Iterable[Session]:
        session: Session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _bootstrap_defaults(self, defaults: Iterable[tuple[str, str, float]]) -> None:
        with self._lock:
            with self._session_scope() as session:
                for name, description, max_nav_pct in defaults:
                    record = session.get(StrategyRecord, name)
                    if record is None:
                        record = StrategyRecord(name=name, enabled=True, max_nav_pct=max_nav_pct)
                        session.add(record)
                    self._descriptions[name] = description

    def register(self, name: str, description: str, max_nav_pct: float) -> StrategySnapshot:
        with self._lock:
            with self._session_scope() as session:
                existing = session.get(StrategyRecord, name)
                total_allocated = session.execute(select(func.sum(StrategyRecord.max_nav_pct))).scalar() or 0.0
                if existing is not None:
                    total_allocated -= existing.max_nav_pct
                if total_allocated + max_nav_pct > 1.0 + 1e-6:
                    raise StrategyAllocationError(
                        "Total NAV allocation across strategies cannot exceed 100%."
                    )

                if existing is None:
                    record = StrategyRecord(name=name, enabled=True, max_nav_pct=max_nav_pct)
                    session.add(record)
                else:
                    existing.max_nav_pct = max_nav_pct
                    existing.enabled = True
                    record = existing

                self._descriptions[name] = description

            return self.status_for(name)

    def toggle(self, name: str, enabled: bool) -> StrategySnapshot:
        with self._lock:
            with self._session_scope() as session:
                record = session.get(StrategyRecord, name)
                if record is None:
                    raise StrategyNotFound(f"Strategy '{name}' is not registered.")
                record.enabled = enabled

            return self.status_for(name)

    def status(self) -> List[StrategySnapshot]:
        with self._session_scope() as session:
            rows = session.execute(select(StrategyRecord)).scalars().all()

        enabled_total = sum(row.max_nav_pct for row in rows if row.enabled)
        statuses: List[StrategySnapshot] = []
        for row in rows:
            description = self._descriptions.get(row.name, "")
            allocated_nav_pct = (
                (row.max_nav_pct / enabled_total) if row.enabled and enabled_total else 0.0
            )
            statuses.append(
                StrategySnapshot(
                    name=row.name,
                    description=description,
                    enabled=row.enabled,
                    max_nav_pct=row.max_nav_pct,
                    allocated_nav_pct=allocated_nav_pct,
                    created_at=row.created_at,
                )
            )
        statuses.sort(key=lambda snapshot: snapshot.name)
        return statuses

    def status_for(self, name: str) -> StrategySnapshot:
        for snapshot in self.status():
            if snapshot.name == name:
                return snapshot
        raise StrategyNotFound(f"Strategy '{name}' is not registered.")

    async def route_trade_intent(
        self, strategy_name: str, request: RiskValidationRequest
    ) -> RiskValidationResponse:
        with self._lock:
            with self._session_scope() as session:
                record = session.get(StrategyRecord, strategy_name)
                if record is None:
                    raise StrategyNotFound(f"Strategy '{strategy_name}' is not registered.")
                if not record.enabled:
                    raise StrategyAllocationError(
                        f"Strategy '{strategy_name}' is disabled and cannot submit intents."
                    )

        payload = request.model_dump(mode="json")
        portfolio_state = payload.setdefault("portfolio_state", {})
        metadata = portfolio_state.setdefault("metadata", {})
        metadata["strategy_id"] = strategy_name

        url = f"{self._risk_engine_url}/risk/validate"
        headers = {"X-Account-ID": request.account_id}
        try:
            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:  # pragma: no cover - defensive
            raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text)
        except httpx.HTTPError as exc:  # pragma: no cover - network failure
            raise HTTPException(status_code=502, detail=f"Risk engine unreachable: {exc}") from exc

        return RiskValidationResponse.model_validate(response.json())


class StrategyRegisterRequest(BaseModel):
    name: constr(strip_whitespace=True, min_length=1)
    description: constr(strip_whitespace=True, min_length=1)
    max_nav_pct: PositiveFloat = Field(..., le=1.0, description="Maximum NAV percentage allocated to strategy")


class StrategyToggleRequest(BaseModel):
    name: constr(strip_whitespace=True, min_length=1)
    enabled: bool


class StrategyStatusResponse(BaseModel):
    name: str
    description: str
    enabled: bool
    max_nav_pct: float
    allocated_nav_pct: float
    created_at: datetime


class StrategyIntentRequest(BaseModel):
    strategy_name: constr(strip_whitespace=True, min_length=1)
    request: RiskValidationRequest


class StrategySignalResponse(BaseModel):
    name: str
    publisher: str
    schema: Any
    ts: datetime


def _database_url() -> str:
    url = (
        os.getenv("STRATEGY_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or "sqlite:///./strategy.db"
    )
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def _create_engine(url: str) -> Engine:
    kwargs: Dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        kwargs.setdefault("connect_args", {"check_same_thread": False})
        kwargs["poolclass"] = NullPool
    return create_engine(url, **kwargs)


DATABASE_URL = _database_url()
ENGINE = _create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

DEFAULT_STRATEGIES: List[tuple[str, str, float]] = [
    ("breakout", "Breakout strategy capturing range expansions.", 0.25),
    ("meanrev", "Mean reversion strategy targeting short-term pullbacks.", 0.35),
    ("trend", "Trend following momentum strategy across major assets.", 0.40),
]

RISK_ENGINE_URL = os.getenv("RISK_ENGINE_URL", "http://localhost:8000")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

REGISTRY: Optional[StrategyRegistry] = None
SIGNAL_BUS: Optional[StrategySignalBus] = None
INITIALIZATION_ERROR: Optional[Exception] = None

MAX_STARTUP_RETRIES = int(os.getenv("STRATEGY_DB_STARTUP_RETRIES", "5"))
INITIAL_BACKOFF_SECONDS = float(os.getenv("STRATEGY_DB_STARTUP_BACKOFF", "1.0"))
MAX_BACKOFF_SECONDS = float(os.getenv("STRATEGY_DB_STARTUP_BACKOFF_CAP", "30.0"))


def _initialization_message() -> str:
    if INITIALIZATION_ERROR is None:
        return "Strategy orchestrator is initialising dependencies."
    return f"Strategy orchestrator failed to initialise database: {INITIALIZATION_ERROR}"


def _set_components(registry: StrategyRegistry, signal_bus: StrategySignalBus) -> None:
    global REGISTRY, SIGNAL_BUS, INITIALIZATION_ERROR
    REGISTRY = registry
    SIGNAL_BUS = signal_bus
    INITIALIZATION_ERROR = None


def _initialise_components() -> None:
    Base.metadata.create_all(bind=ENGINE)
    ensure_signal_tables(ENGINE)

    registry = StrategyRegistry(
        SessionLocal,
        risk_engine_url=RISK_ENGINE_URL,
        default_strategies=DEFAULT_STRATEGIES,
    )
    signal_bus = StrategySignalBus(
        SessionLocal,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    _set_components(registry, signal_bus)


async def _initialise_with_retry(
    *,
    max_attempts: Optional[int] = None,
    base_delay: Optional[float] = None,
    max_delay: Optional[float] = None,
) -> None:
    global INITIALIZATION_ERROR, REGISTRY, SIGNAL_BUS

    attempts = max_attempts or MAX_STARTUP_RETRIES
    delay = base_delay if base_delay is not None else INITIAL_BACKOFF_SECONDS
    max_backoff = max_delay if max_delay is not None else MAX_BACKOFF_SECONDS

    REGISTRY = None
    SIGNAL_BUS = None
    INITIALIZATION_ERROR = None

    for attempt in range(1, attempts + 1):
        try:
            _initialise_components()
        except (OperationalError, SQLAlchemyError) as exc:
            INITIALIZATION_ERROR = exc
            LOGGER.warning(
                "Database initialisation attempt %s/%s failed: %s",
                attempt,
                attempts,
                exc,
            )
            if attempt == attempts:
                LOGGER.error(
                    "Exhausted database initialisation retries; service will return 503 until the database is reachable."
                )
                return
            sleep_for = max(delay, 0.0)
            if sleep_for:
                await asyncio.sleep(sleep_for)
            delay = min(delay * 2 or INITIAL_BACKOFF_SECONDS, max_backoff)
        except Exception:
            # Bubble unexpected exceptions so FastAPI startup fails loudly.
            INITIALIZATION_ERROR = None
            REGISTRY = None
            SIGNAL_BUS = None
            raise
        else:
            LOGGER.info("Database initialisation succeeded on attempt %s.", attempt)
            return


def _require_registry() -> StrategyRegistry:
    if REGISTRY is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=_initialization_message())
    return REGISTRY


def _require_signal_bus() -> StrategySignalBus:
    if SIGNAL_BUS is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=_initialization_message())
    return SIGNAL_BUS

app = FastAPI(title="Strategy Orchestrator", version="0.1.0")


@app.on_event("startup")
async def _startup_event() -> None:
    await _initialise_with_retry()


@app.post("/strategy/register", response_model=StrategyStatusResponse)
async def register_strategy(payload: StrategyRegisterRequest) -> StrategyStatusResponse:
    registry = _require_registry()
    try:
        snapshot = registry.register(payload.name.lower(), payload.description, payload.max_nav_pct)
    except StrategyAllocationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StrategyStatusResponse(**snapshot.__dict__)


@app.post("/strategy/toggle", response_model=StrategyStatusResponse)
async def toggle_strategy(payload: StrategyToggleRequest) -> StrategyStatusResponse:
    registry = _require_registry()
    try:
        snapshot = registry.toggle(payload.name.lower(), payload.enabled)
    except StrategyNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return StrategyStatusResponse(**snapshot.__dict__)


@app.get("/strategy/status", response_model=List[StrategyStatusResponse])
async def strategy_status() -> List[StrategyStatusResponse]:
    registry = _require_registry()
    snapshots = registry.status()
    return [StrategyStatusResponse(**snapshot.__dict__) for snapshot in snapshots]


@app.post("/strategy/intent", response_model=RiskValidationResponse)
async def route_intent(payload: StrategyIntentRequest) -> RiskValidationResponse:
    registry = _require_registry()
    try:
        return await registry.route_trade_intent(payload.strategy_name.lower(), payload.request)
    except StrategyNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except StrategyAllocationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/strategy/signals", response_model=List[StrategySignalResponse])
async def strategy_signals() -> List[StrategySignalResponse]:
    signal_bus = _require_signal_bus()
    signals = signal_bus.list_signals()
    return [
        StrategySignalResponse(
            name=signal.name,
            publisher=signal.publisher,
            schema=signal.schema,
            ts=signal.ts,
        )
        for signal in signals
    ]

