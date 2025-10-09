"""Stress testing service exposing scenario endpoints for backtests."""

from __future__ import annotations

import logging
import math
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from numbers import Number
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, MutableMapping, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

_NUMPY_ERROR: Exception | None
try:  # pragma: no cover - optional dependency in lightweight environments
    import numpy as _NUMPY_MODULE  # type: ignore[assignment]
except Exception as exc:  # pragma: no cover - exercised only when numpy is missing
    _NUMPY_MODULE = None
    _NUMPY_ERROR = exc
else:  # pragma: no cover - trivial happy path
    _NUMPY_ERROR = None

_PANDAS_ERROR: Exception | None
try:  # pragma: no cover - optional dependency in lightweight environments
    import pandas as _PANDAS_MODULE  # type: ignore[assignment]
except Exception as exc:  # pragma: no cover - exercised only when pandas is missing
    _PANDAS_MODULE = None
    _PANDAS_ERROR = exc
else:  # pragma: no cover - trivial happy path
    _PANDAS_ERROR = None

if TYPE_CHECKING:  # pragma: no cover - used for static analysis only
    import numpy as np  # type: ignore
    import pandas as pd  # type: ignore


class MissingDependencyError(RuntimeError):
    """Raised when a required third-party dependency is unavailable."""


def _require_numpy() -> Any:
    if _NUMPY_MODULE is None:
        raise MissingDependencyError(
            "numpy is required for stress testing functionality"
        ) from _NUMPY_ERROR
    return _NUMPY_MODULE


def _require_pandas() -> Any:
    if _PANDAS_MODULE is None:
        raise MissingDependencyError(
            "pandas is required for stress testing functionality"
        ) from _PANDAS_ERROR
    return _PANDAS_MODULE


def _require_backtest_engine() -> None:
    if Backtester is None or ExamplePolicy is None or FeeSchedule is None:
        message = "backtest engine dependencies are unavailable"
        if _BACKTEST_IMPORT_ERROR is not None:
            message = f"backtest engine is unavailable: {_BACKTEST_IMPORT_ERROR}"
        raise MissingDependencyError(message) from _BACKTEST_IMPORT_ERROR
    if flash_crash is None or spread_widen is None or liquidity_halt is None:
        message = "stress scenario helpers are unavailable"
        raise MissingDependencyError(message) from _BACKTEST_IMPORT_ERROR


_SQLALCHEMY_ERROR: Exception | None = None
_SQLALCHEMY_AVAILABLE = True
try:  # pragma: no cover - exercised only when SQLAlchemy is present
    from sqlalchemy import Column, DateTime, Float, MetaData, String, Table, create_engine, func
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.orm import Session, sessionmaker
except Exception as exc:  # pragma: no cover - executed when SQLAlchemy is missing
    _SQLALCHEMY_AVAILABLE = False
    _SQLALCHEMY_ERROR = exc
    Column = DateTime = Float = MetaData = String = Table = object  # type: ignore[assignment]
    Engine = Any  # type: ignore[assignment]
    Session = Any  # type: ignore[assignment]

    class SQLAlchemyError(Exception):  # type: ignore[override]
        """Placeholder exception used when SQLAlchemy is unavailable."""

    def create_engine(*_: Any, **__: Any) -> Engine:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for stress test persistence") from exc

    def func() -> None:  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for stress test persistence") from exc

from services.common.config import TimescaleSession, get_timescale_session


LOGGER = logging.getLogger(__name__)

_IN_MEMORY_REPOSITORIES: MutableMapping[Tuple[str, str | None], list[Dict[str, Any]]] = {}
_IN_MEMORY_LOCK = RLock()

_BACKTEST_IMPORT_ERROR: Exception | None = None
_generate_synthetic_events = None
try:  # pragma: no cover - optional dependency tree includes numpy/pandas
    from backtest_engine import (
        Backtester,
        ExamplePolicy,
        FeeSchedule,
        flash_crash,
        liquidity_halt,
        spread_widen,
    )
    try:  # pragma: no cover - helper absent in some builds
        from backtest_engine import _generate_synthetic_events as _backtester_generate
    except Exception:  # pragma: no cover - fallback path when helper missing
        _backtester_generate = None
    else:  # pragma: no cover - executed when helper import succeeds
        _generate_synthetic_events = _backtester_generate
except Exception as exc:  # pragma: no cover - exercised when scientific stack is missing
    _BACKTEST_IMPORT_ERROR = exc
    Backtester = None  # type: ignore[assignment]
    ExamplePolicy = None  # type: ignore[assignment]
    FeeSchedule = None  # type: ignore[assignment]
    flash_crash = liquidity_halt = spread_widen = None  # type: ignore[assignment]
else:  # pragma: no cover - trivial happy path
    _BACKTEST_IMPORT_ERROR = None

DEFAULT_SYMBOL = "BTC/USD"
DEFAULT_YEARS = 1
DEFAULT_SLIPPAGE_BPS = 1.0
DEFAULT_INITIAL_CASH = 1_000_000.0


class StressScenario(str, Enum):
    """Supported stress scenarios for the engine."""

    FLASH_CRASH = "flash_crash"
    SPREAD_WIDEN = "spread_widen"
    LIQUIDITY_HALT = "liquidity_halt"


@dataclass(frozen=True)
class StressTestResult:
    """Container encapsulating a stress run and its PnL impact."""

    account_id: str
    scenario: StressScenario
    timestamp: datetime
    base_metrics: Dict[str, float]
    stressed_metrics: Dict[str, float]

    @property
    def pnl_impact(self) -> float:
        base = float(self.base_metrics.get("pnl", 0.0))
        stressed = float(self.stressed_metrics.get("pnl", 0.0))
        return stressed - base


class StressTestRepository:
    """Persistence layer responsible for storing stress test runs."""

    def __init__(self, session: TimescaleSession) -> None:
        self._session_cfg = session
        self._in_memory_store: list[Dict[str, Any]] | None = None

        def _activate_in_memory_store(reason: str) -> None:
            key = (session.dsn, session.account_schema)
            with _IN_MEMORY_LOCK:
                store = _IN_MEMORY_REPOSITORIES.get(key)
                if store is None:
                    store = []
                    _IN_MEMORY_REPOSITORIES[key] = store
            self._in_memory_store = store
            LOGGER.warning("%s; using in-memory stress test repository for %s", reason, session.dsn)

        if not _SQLALCHEMY_AVAILABLE:
            _activate_in_memory_store("SQLAlchemy unavailable")
            return

        try:
            self._engine = create_engine(session.dsn, pool_pre_ping=True, future=True)
        except MissingDependencyError:
            _activate_in_memory_store("SQLAlchemy engine unavailable")
            return
        except Exception:
            LOGGER.warning("Failed to initialise SQLAlchemy engine; falling back to memory store", exc_info=True)
            _activate_in_memory_store("SQLAlchemy engine initialisation failed")
            return

        self._Session = sessionmaker(bind=self._engine, expire_on_commit=False, future=True)
        metadata = MetaData(schema=session.account_schema)
        self._table = Table(
            "stress_tests",
            metadata,
            Column("account_id", String, nullable=False),
            Column("scenario", String, nullable=False),
            Column("pnl_impact", Float, nullable=False),
            Column("ts", DateTime(timezone=True), nullable=False, server_default=func.now()),
            schema=session.account_schema,
        )
        metadata.create_all(self._engine, checkfirst=True)

    def record(self, result: StressTestResult) -> None:
        payload = {
            "account_id": result.account_id,
            "scenario": result.scenario.value,
            "pnl_impact": result.pnl_impact,
            "ts": result.timestamp,
        }

        if self._in_memory_store is not None:
            with _IN_MEMORY_LOCK:
                self._in_memory_store.append(payload)
            return

        try:
            with self._Session() as db:  # type: Session
                db.execute(self._table.insert(), [payload])
                db.commit()
        except SQLAlchemyError:
            LOGGER.exception(
                "Failed to persist stress test for account_id=%s scenario=%s", result.account_id, result.scenario
            )
            raise


class PortfolioStressEngine:
    """Run stress scenarios against a synthetic backtest for an account."""

    def __init__(
        self,
        account_id: str,
        *,
        session: TimescaleSession | None = None,
        repository: StressTestRepository | None = None,
        symbol: str = DEFAULT_SYMBOL,
        years: int = DEFAULT_YEARS,
    ) -> None:
        self.account_id = account_id
        self.symbol = symbol
        self.years = max(int(years), 1)
        self._seed = self._seed_from_account(account_id)
        self._timescale = session or get_timescale_session(account_id)
        self._repository = repository or StressTestRepository(self._timescale)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self, scenario: StressScenario) -> StressTestResult:
        _require_numpy()
        _require_pandas()
        _require_backtest_engine()
        backtester = self._build_backtester()
        base_events = [dict(event) for event in backtester.base_events]
        base_metrics = self._normalise_metrics(backtester.run_with_events(base_events))

        stressed_events = self._apply_scenario(scenario, backtester.base_events)
        stressed_metrics = self._normalise_metrics(backtester.run_with_events(stressed_events))

        timestamp = datetime.now(timezone.utc)
        result = StressTestResult(
            account_id=self.account_id,
            scenario=scenario,
            timestamp=timestamp,
            base_metrics=base_metrics,
            stressed_metrics=stressed_metrics,
        )
        try:
            self._repository.record(result)
        except SQLAlchemyError as exc:  # pragma: no cover - depends on external DB
            raise HTTPException(status_code=500, detail="Failed to persist stress test result") from exc
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _build_backtester(self) -> Backtester:
        _require_backtest_engine()
        bars, books = self._synthetic_events()
        policy = ExamplePolicy(seed=self._seed)
        fee_schedule = FeeSchedule(maker=0.0002, taker=0.0007)
        return Backtester(
            bar_events=bars,
            book_events=books,
            policy=policy,
            fee_schedule=fee_schedule,
            slippage_bps=DEFAULT_SLIPPAGE_BPS,
            initial_cash=DEFAULT_INITIAL_CASH,
            seed=self._seed,
        )

    def _synthetic_events(self) -> tuple[Iterable[Mapping[str, Any]], Iterable[Mapping[str, Any]]]:
        if _generate_synthetic_events is None:
            return self._fallback_events()
        return _generate_synthetic_events(self.symbol, self.years, seed=self._seed)

    def _fallback_events(self) -> tuple[Iterable[Mapping[str, Any]], Iterable[Mapping[str, Any]]]:
        pandas = _require_pandas()
        numpy = _require_numpy()
        timeline = pandas.date_range(
            end=pandas.Timestamp.utcnow(), periods=max(self.years * 365 * 24, 48), freq="h"
        )
        price = 20_000.0
        rng = numpy.random.default_rng(self._seed)
        bars = []
        books = []
        for ts in timeline:
            drift = float(rng.normal(0, price * 0.002))
            price = max(50.0, price + drift)
            spread = max(price * 0.0005, float(rng.normal(price * 0.0008, price * 0.0002)))
            bid = price - spread / 2.0
            ask = price + spread / 2.0
            bid_size = float(max(0.5, rng.lognormal(mean=0.0, sigma=0.4)))
            ask_size = float(max(0.5, rng.lognormal(mean=0.0, sigma=0.4)))
            books.append(
                {
                    "timestamp": ts,
                    "type": "book",
                    "symbol": self.symbol,
                    "bid": bid,
                    "ask": ask,
                    "bid_size": bid_size,
                    "ask_size": ask_size,
                    "halted": False,
                }
            )
            high = max(price, ask) + abs(float(rng.normal(0, spread * 0.5)))
            low = min(price, bid) - abs(float(rng.normal(0, spread * 0.5)))
            close = price + float(rng.normal(0, spread * 0.25))
            volume = float(max(0.1, rng.lognormal(mean=0.0, sigma=0.3)))
            bars.append(
                {
                    "timestamp": ts,
                    "type": "bar",
                    "open": price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                }
            )
        return bars, books

    def _apply_scenario(self, scenario: StressScenario, events: Iterable[Mapping[str, Any]]) -> Iterable[Mapping[str, Any]]:
        _require_backtest_engine()
        mapping = {
            StressScenario.FLASH_CRASH: lambda payload: flash_crash(payload, drop=0.20, depth_factor=0.5),
            StressScenario.SPREAD_WIDEN: lambda payload: spread_widen(payload, widen_bps=125.0),
            StressScenario.LIQUIDITY_HALT: lambda payload: liquidity_halt(payload, gap_events=5),
        }
        base = [dict(event) for event in events]
        injector = mapping.get(scenario)
        if injector is None:  # pragma: no cover - exhaustive enum but defensive
            raise ValueError(f"Unsupported scenario: {scenario}")
        return injector(base)

    @staticmethod
    def _normalise_metrics(metrics: Mapping[str, Any]) -> Dict[str, float]:
        normalised: Dict[str, float] = {}
        for key, value in metrics.items():
            if isinstance(value, Number):
                normalised[key] = float(value)
        return normalised

    @staticmethod
    def _seed_from_account(account_id: str) -> int:
        checksum = zlib.crc32(account_id.encode("utf-8")) & 0xFFFFFFFF
        return int(math.fmod(checksum, 2**31))


class StressTestResponse(BaseModel):
    """Response model returned by the stress testing endpoint."""

    account_id: str = Field(..., description="Account identifier")
    scenario: StressScenario = Field(..., description="Stress scenario that was executed")
    timestamp: datetime = Field(..., description="Timestamp the scenario was executed")
    base_metrics: Dict[str, float] = Field(..., description="Portfolio metrics from the unstressed run")
    stressed_metrics: Dict[str, float] = Field(..., description="Portfolio metrics after applying the scenario")
    pnl_impact: float = Field(..., description="Difference between stressed and base PnL")


def get_engine(account_id: str = Query(..., description="Account identifier to stress")) -> PortfolioStressEngine:
    try:
        session = get_timescale_session(account_id)
    except Exception as exc:  # pragma: no cover - configuration errors are runtime only
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to load account config") from exc
    return PortfolioStressEngine(account_id=account_id, session=session)


router = APIRouter(prefix="/stress", tags=["stress"])


@router.get("/run", response_model=StressTestResponse)
def run_stress(
    scenario: StressScenario = Query(..., description="Scenario to execute"),
    engine: PortfolioStressEngine = Depends(get_engine),
) -> StressTestResponse:
    try:
        result = engine.run(scenario)
    except MissingDependencyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime errors depend on Backtester data
        LOGGER.exception("Failed to run stress scenario %s for %s", scenario, engine.account_id)
        raise HTTPException(status_code=500, detail="Failed to execute stress scenario") from exc
    return StressTestResponse(
        account_id=result.account_id,
        scenario=result.scenario,
        timestamp=result.timestamp,
        base_metrics=result.base_metrics,
        stressed_metrics=result.stressed_metrics,
        pnl_impact=result.pnl_impact,
    )


__all__ = [
    "PortfolioStressEngine",
    "StressScenario",
    "StressTestRepository",
    "StressTestResponse",
    "StressTestResult",
    "router",
]
