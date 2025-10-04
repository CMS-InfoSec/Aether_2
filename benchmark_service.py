"""FastAPI service for comparing account performance against simple benchmarks."""

from __future__ import annotations

import enum
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import Column, DateTime, Float, String, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from services.common.security import require_admin_account

LOGGER = logging.getLogger(__name__)


Base = declarative_base()


class BenchmarkName(str, enum.Enum):
    """Enumerates supported benchmark strategies."""

    AETHER = "aether"
    BTC_HOLD = "btc_usd_hold"
    ETH_HOLD = "eth_usd_hold"
    EQUAL_WEIGHT = "equal_weight_basket"


class BenchmarkCurve(Base):
    """SQLAlchemy model for the ``benchmark_curves`` hypertable."""

    __tablename__ = "benchmark_curves"

    account_id = Column(String, primary_key=True)
    benchmark = Column(String, primary_key=True)
    ts = Column(DateTime(timezone=True), primary_key=True)
    pnl = Column(Float, nullable=False, default=0.0)


@dataclass
class BenchmarkSnapshot:
    """Container for benchmark performance snapshots."""

    benchmark: BenchmarkName
    pnl: float


class BenchmarkComparison(BaseModel):
    """Response payload for benchmark comparison requests."""

    aether_return: float = Field(..., description="Account performance for the requested date")
    btc_return: float = Field(..., description="BTC/USD buy-and-hold benchmark return")
    eth_return: float = Field(..., description="ETH/USD buy-and-hold benchmark return")
    basket_return: float = Field(..., description="Equal-weight basket return of BTC and ETH")
    excess_return: float = Field(..., description="Account return minus the basket benchmark")


class BenchmarkCurveRepository:
    """Data access helpers for benchmark curve persistence."""

    def __init__(self, session: Session):
        self._session = session

    def record_snapshot(self, account_id: str, snapshot: BenchmarkSnapshot, ts: datetime) -> None:
        """Persist or update a benchmark snapshot for an account."""

        record = BenchmarkCurve(
            account_id=account_id,
            benchmark=snapshot.benchmark.value,
            ts=ts,
            pnl=snapshot.pnl,
        )
        self._session.merge(record)

    def record_many(self, account_id: str, snapshots: Iterable[BenchmarkSnapshot], ts: datetime) -> None:
        """Persist multiple benchmark snapshots in a single transaction."""

        for snapshot in snapshots:
            self.record_snapshot(account_id, snapshot, ts)

    def latest_pnl(
        self, account_id: str, benchmark: BenchmarkName, as_of: datetime
    ) -> Optional[float]:
        """Return the latest PnL value prior to ``as_of`` for ``benchmark``."""

        statement = (
            select(BenchmarkCurve.pnl)
            .where(
                BenchmarkCurve.account_id == account_id,
                BenchmarkCurve.benchmark == benchmark.value,
                BenchmarkCurve.ts <= as_of,
            )
            .order_by(BenchmarkCurve.ts.desc())
            .limit(1)
        )
        result = self._session.execute(statement).scalar_one_or_none()
        return float(result) if result is not None else None


_DATABASE_ENV_VARS: tuple[str, ...] = (
    "BENCHMARK_DATABASE_URL",
    "TIMESCALE_DSN",
    "DATABASE_URL",
)
_SSL_MODE_ENV = "BENCHMARK_DB_SSLMODE"
_POOL_SIZE_ENV = "BENCHMARK_DB_POOL_SIZE"
_MAX_OVERFLOW_ENV = "BENCHMARK_DB_MAX_OVERFLOW"
_POOL_TIMEOUT_ENV = "BENCHMARK_DB_POOL_TIMEOUT"
_POOL_RECYCLE_ENV = "BENCHMARK_DB_POOL_RECYCLE"


def _database_url() -> str:
    """Return the configured PostgreSQL/Timescale connection string."""

    raw_url = next((os.getenv(name) for name in _DATABASE_ENV_VARS if os.getenv(name)), None)
    if not raw_url:
        raise RuntimeError(
            "BENCHMARK_DATABASE_URL (or TIMESCALE_DSN/DATABASE_URL) must be set to a "
            "PostgreSQL/Timescale DSN"
        )

    url = raw_url.strip()
    if url.startswith("postgres://"):
        url = "postgresql://" + url.split("://", 1)[1]

    normalized = url.lower()
    if normalized.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        normalized = url.lower()

    allowed_prefixes = ("postgresql+psycopg://", "postgresql+psycopg2://")
    if not normalized.startswith(allowed_prefixes):
        raise RuntimeError(
            "Benchmark service requires a PostgreSQL/Timescale DSN via BENCHMARK_DATABASE_URL "
            "(or TIMESCALE_DSN/DATABASE_URL)."
        )

    return url


def _engine_options(url: str) -> Mapping[str, object]:
    options: dict[str, object] = {
        "future": True,
        "pool_pre_ping": True,
    }

    connect_args: dict[str, object] = {}
    if url.startswith("postgresql"):
        connect_args["sslmode"] = os.getenv(_SSL_MODE_ENV, "require")
        options.update(
            connect_args=connect_args,
            pool_size=int(os.getenv(_POOL_SIZE_ENV, "10")),
            max_overflow=int(os.getenv(_MAX_OVERFLOW_ENV, "5")),
            pool_timeout=int(os.getenv(_POOL_TIMEOUT_ENV, "30")),
            pool_recycle=int(os.getenv(_POOL_RECYCLE_ENV, "1800")),
        )

    return options


DATABASE_URL = _database_url()
ENGINE: Engine = create_engine(DATABASE_URL, **_engine_options(DATABASE_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

app = FastAPI(title="Benchmark Service", version="1.0.0")
app.state.db_sessionmaker = SessionLocal


@app.on_event("startup")
def _create_tables() -> None:
    """Ensure the benchmark table exists before serving requests."""

    try:
        Base.metadata.create_all(ENGINE)
    except SQLAlchemyError as exc:  # pragma: no cover - defensive logging
        LOGGER.exception("Failed to initialise benchmark tables: %s", exc)
        raise


def _parse_date(date_str: str | None) -> datetime:
    if date_str:
        try:
            parsed = datetime.fromisoformat(date_str)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Invalid date format") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    return datetime.now(tz=timezone.utc)


def _basket_return(btc: Optional[float], eth: Optional[float]) -> Optional[float]:
    values = [value for value in (btc, eth) if value is not None]
    if not values:
        return None
    return sum(values) / len(values)


class BenchmarkCurvePayload(BaseModel):
    """Request payload for persisting benchmark snapshots."""

    account_id: str = Field(..., description="Account identifier")
    timestamp: datetime = Field(..., alias="ts", description="Timestamp for the snapshot")
    aether_return: Optional[float] = Field(
        None, description="Account performance captured for the timestamp"
    )
    btc_return: Optional[float] = Field(
        None, description="BTC/USD buy-and-hold benchmark return"
    )
    eth_return: Optional[float] = Field(
        None, description="ETH/USD buy-and-hold benchmark return"
    )
    basket_return: Optional[float] = Field(
        None, description="Equal-weight basket benchmark return"
    )

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def _check_payload(self) -> "BenchmarkCurvePayload":
        if not any(
            value is not None
            for value in (
                self.aether_return,
                self.btc_return,
                self.eth_return,
                self.basket_return,
            )
        ):
            raise ValueError("At least one return value must be provided")
        return self


@app.post("/benchmark/curves", status_code=204)
def upsert_benchmark_curves(
    payload: BenchmarkCurvePayload,
    actor_account: str = Depends(require_admin_account),
) -> None:
    """Persist benchmark returns for an account at a point in time."""

    ts = payload.timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    snapshots: list[BenchmarkSnapshot] = []

    LOGGER.info(
        "Benchmark curve upsert requested by %s for account %s at %s",
        actor_account,
        payload.account_id,
        ts.isoformat(),
    )

    if payload.aether_return is not None:
        snapshots.append(
            BenchmarkSnapshot(benchmark=BenchmarkName.AETHER, pnl=payload.aether_return)
        )
    if payload.btc_return is not None:
        snapshots.append(
            BenchmarkSnapshot(benchmark=BenchmarkName.BTC_HOLD, pnl=payload.btc_return)
        )
    if payload.eth_return is not None:
        snapshots.append(
            BenchmarkSnapshot(benchmark=BenchmarkName.ETH_HOLD, pnl=payload.eth_return)
        )

    basket_value = payload.basket_return
    if basket_value is None:
        basket_value = _basket_return(payload.btc_return, payload.eth_return)
    if basket_value is not None:
        snapshots.append(
            BenchmarkSnapshot(benchmark=BenchmarkName.EQUAL_WEIGHT, pnl=basket_value)
        )

    if not snapshots:
        raise HTTPException(status_code=422, detail="No benchmark data provided")

    with SessionLocal() as session:
        repo = BenchmarkCurveRepository(session)
        try:
            repo.record_many(payload.account_id, snapshots, ts)
            session.commit()
        except SQLAlchemyError as exc:  # pragma: no cover - defensive guard
            session.rollback()
            LOGGER.exception("Failed to persist benchmark snapshots: %s", exc)
            raise HTTPException(status_code=500, detail="Failed to persist benchmark data")


@app.get("/benchmark/compare", response_model=BenchmarkComparison)
def compare_benchmarks(
    account_id: str = Query(..., description="Account identifier"),
    date: Optional[str] = Query(None, description="ISO8601 date or timestamp"),
    actor_account: str = Depends(require_admin_account),
) -> BenchmarkComparison:
    """Compare account performance to BTC, ETH and an equal-weight basket."""

    as_of = _parse_date(date)

    LOGGER.info(
        "Benchmark comparison requested by %s for account %s at %s",
        actor_account,
        account_id,
        as_of.isoformat(),
    )

    with SessionLocal() as session:
        repo = BenchmarkCurveRepository(session)
        aether = repo.latest_pnl(account_id, BenchmarkName.AETHER, as_of) or 0.0
        btc = repo.latest_pnl(account_id, BenchmarkName.BTC_HOLD, as_of) or 0.0
        eth = repo.latest_pnl(account_id, BenchmarkName.ETH_HOLD, as_of) or 0.0
        basket = repo.latest_pnl(account_id, BenchmarkName.EQUAL_WEIGHT, as_of)

        if basket is None:
            basket = _basket_return(btc, eth) or 0.0

    excess = aether - basket
    return BenchmarkComparison(
        aether_return=aether,
        btc_return=btc,
        eth_return=eth,
        basket_return=basket,
        excess_return=excess,
    )
