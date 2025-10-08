"""FastAPI service for comparing account performance against simple benchmarks."""

from __future__ import annotations

import enum
import logging
import os
import sqlite3
import sys
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import AsyncIterator, Iterable, Mapping, Optional
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator

_SQLALCHEMY_AVAILABLE = True

try:  # pragma: no cover - optional dependency in production
    from sqlalchemy import Column, DateTime, Float, String, create_engine, select
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool
except ImportError:  # pragma: no cover - exercised in lightweight environments
    _SQLALCHEMY_AVAILABLE = False
    Column = DateTime = Float = String = object  # type: ignore[assignment]
    Engine = object  # type: ignore[assignment]
    SQLAlchemyError = Exception  # type: ignore[assignment]
    Session = object  # type: ignore[assignment]
    StaticPool = object  # type: ignore[assignment]

    def declarative_base() -> object:  # type: ignore[override]
        return object()

from services.common.security import require_admin_account
from shared.postgres import normalize_sqlalchemy_dsn

LOGGER = logging.getLogger(__name__)


if _SQLALCHEMY_AVAILABLE:
    Base = declarative_base()
    metadata = getattr(Base, "metadata", None)
    if metadata is None or not hasattr(metadata, "drop_all"):
        _SQLALCHEMY_AVAILABLE = False

if not _SQLALCHEMY_AVAILABLE:

    class _FallbackMetadata:
        def __init__(self) -> None:
            self._store: "_SQLiteBenchmarkStore | None" = None

        def bind(self, store: "_SQLiteBenchmarkStore") -> None:
            self._store = store

        def create_all(self, bind: object | None = None, **__: object) -> None:
            if self._store is not None:
                LOGGER.debug("benchmark metadata create_all using fallback store")
                self._store.create_all()

        def drop_all(self, bind: object | None = None, **__: object) -> None:
            if self._store is not None:
                LOGGER.debug("benchmark metadata drop_all using fallback store")
                self._store.drop_all()

    Base = SimpleNamespace(metadata=_FallbackMetadata())


class BenchmarkName(str, enum.Enum):
    """Enumerates supported benchmark strategies."""

    AETHER = "aether"
    BTC_HOLD = "btc_usd_hold"
    ETH_HOLD = "eth_usd_hold"
    EQUAL_WEIGHT = "equal_weight_basket"


if _SQLALCHEMY_AVAILABLE:

    class BenchmarkCurve(Base):
        """SQLAlchemy model for the ``benchmark_curves`` hypertable."""

        __tablename__ = "benchmark_curves"

        account_id = Column(String, primary_key=True)
        benchmark = Column(String, primary_key=True)
        ts = Column(DateTime(timezone=True), primary_key=True)
        pnl = Column(Float, nullable=False, default=0.0)

else:

    @dataclass
    class BenchmarkCurve:
        """Lightweight stand-in used when SQLAlchemy is unavailable."""

        account_id: str
        benchmark: str
        ts: datetime
        pnl: float


if not _SQLALCHEMY_AVAILABLE:

    class _SQLiteBenchmarkStore:
        """SQLite-backed persistence used when SQLAlchemy is not installed."""

        def __init__(self, url: str) -> None:
            self._dsn = url
            self._path = self._resolve_path(url)
            self._lock = threading.RLock()
            self.create_all()

        @property
        def path(self) -> str:
            return self._path

        def _resolve_path(self, url: str) -> str:
            parsed = urlparse(url)
            base, _, _driver = parsed.scheme.partition("+")
            if base and base != "sqlite":
                LOGGER.warning(
                    "SQLAlchemy unavailable; using in-memory SQLite fallback for benchmark DSN %s",
                    url,
                )
                return ":memory:"

            netloc_path = f"{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path
            if netloc_path.startswith("//"):
                netloc_path = netloc_path[1:]
            if netloc_path in {"", "/"}:
                return ":memory:"

            candidate = Path(netloc_path)
            if not candidate.is_absolute():
                candidate = Path.cwd() / candidate
            if str(candidate) != ":memory:":
                candidate.parent.mkdir(parents=True, exist_ok=True)
            return str(candidate)

        def _connect(self) -> sqlite3.Connection:
            conn = sqlite3.connect(self._path, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn

        def create_all(self) -> None:
            with self._lock, self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS benchmark_curves (
                        account_id TEXT NOT NULL,
                        benchmark TEXT NOT NULL,
                        ts REAL NOT NULL,
                        pnl REAL NOT NULL,
                        PRIMARY KEY (account_id, benchmark, ts)
                    )
                    """.strip()
                )
                conn.commit()

        def drop_all(self) -> None:
            with self._lock, self._connect() as conn:
                conn.execute("DROP TABLE IF EXISTS benchmark_curves")
                conn.commit()

        def open(self) -> sqlite3.Connection:
            return self._connect()


    class _SQLiteEngine:
        def __init__(self, store: _SQLiteBenchmarkStore) -> None:
            self.url = store._dsn  # pragma: no cover - simple data attribute
            self._store = store

        def dispose(self) -> None:  # pragma: no cover - API parity
            return None


    class _SQLiteSession:
        def __init__(self, store: _SQLiteBenchmarkStore) -> None:
            self._store = store
            self._conn = store.open()
            self._closed = False

        def _ensure_open(self) -> None:
            if self._closed:
                raise RuntimeError("SQLite benchmark session has been closed")

        def upsert(self, record: BenchmarkCurve) -> None:
            self._ensure_open()
            cutoff = record.ts.astimezone(timezone.utc).timestamp()
            with self._store._lock:
                self._conn.execute(
                    """
                    INSERT INTO benchmark_curves (account_id, benchmark, ts, pnl)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(account_id, benchmark, ts)
                    DO UPDATE SET pnl = excluded.pnl
                    """.strip(),
                    (record.account_id, record.benchmark, cutoff, float(record.pnl)),
                )

        def latest_pnl(self, account_id: str, benchmark: str, as_of: datetime) -> Optional[float]:
            self._ensure_open()
            cutoff = as_of.astimezone(timezone.utc).timestamp()
            with self._store._lock:
                cursor = self._conn.execute(
                    """
                    SELECT pnl
                    FROM benchmark_curves
                    WHERE account_id = ? AND benchmark = ? AND ts <= ?
                    ORDER BY ts DESC
                    LIMIT 1
                    """.strip(),
                    (account_id, benchmark, cutoff),
                )
                row = cursor.fetchone()
            return float(row[0]) if row else None

        def commit(self) -> None:
            self._ensure_open()
            with self._store._lock:
                self._conn.commit()

        def rollback(self) -> None:
            if self._closed:
                return
            with self._store._lock:
                self._conn.rollback()

        def close(self) -> None:
            if self._closed:
                return
            self._conn.close()
            self._closed = True

        # Context manager protocol for ``with SessionLocal() as session`` usage.
        def __enter__(self) -> "_SQLiteSession":
            self._ensure_open()
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            try:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
            finally:
                self.close()


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
        if _SQLALCHEMY_AVAILABLE:
            self._session.merge(record)
        else:
            self._session.upsert(record)  # type: ignore[attr-defined]

    def record_many(self, account_id: str, snapshots: Iterable[BenchmarkSnapshot], ts: datetime) -> None:
        """Persist multiple benchmark snapshots in a single transaction."""

        for snapshot in snapshots:
            self.record_snapshot(account_id, snapshot, ts)

    def latest_pnl(
        self, account_id: str, benchmark: BenchmarkName, as_of: datetime
    ) -> Optional[float]:
        """Return the latest PnL value prior to ``as_of`` for ``benchmark``."""

        if _SQLALCHEMY_AVAILABLE:
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

        return self._session.latest_pnl(  # type: ignore[attr-defined]
            account_id,
            benchmark.value,
            as_of,
        )


_DATABASE_ENV_VARS: tuple[str, ...] = (
    "BENCHMARK_DATABASE_URL",
    "TIMESCALE_DSN",
    "DATABASE_URL",
)
_ALLOW_SQLITE_ENV = "BENCHMARK_ALLOW_SQLITE"
_SSL_MODE_ENV = "BENCHMARK_DB_SSLMODE"
_POOL_SIZE_ENV = "BENCHMARK_DB_POOL_SIZE"
_MAX_OVERFLOW_ENV = "BENCHMARK_DB_MAX_OVERFLOW"
_POOL_TIMEOUT_ENV = "BENCHMARK_DB_POOL_TIMEOUT"
_POOL_RECYCLE_ENV = "BENCHMARK_DB_POOL_RECYCLE"


def _database_url() -> str:
    """Return the configured PostgreSQL/Timescale connection string."""

    raw_url = next((os.getenv(name) for name in _DATABASE_ENV_VARS if os.getenv(name)), "")
    stripped = raw_url.strip()
    if not stripped:
        raise RuntimeError(
            "Benchmark service requires BENCHMARK_DATABASE_URL (or TIMESCALE_DSN/DATABASE_URL) "
            "to be set."
        )

    allow_sqlite = "pytest" in sys.modules or os.getenv(_ALLOW_SQLITE_ENV) == "1"
    return normalize_sqlalchemy_dsn(
        stripped,
        driver="psycopg",
        allow_sqlite=allow_sqlite,
        label="Benchmark database URL",
    )


def _engine_options(url: str) -> Mapping[str, object]:
    options: dict[str, object] = {
        "future": True,
        "pool_pre_ping": True,
    }

    connect_args: dict[str, object] = {}
    if url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        options["connect_args"] = connect_args
        if ":memory:" in url:
            options["poolclass"] = StaticPool
        return options

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

if _SQLALCHEMY_AVAILABLE:
    ENGINE: Engine = create_engine(DATABASE_URL, **_engine_options(DATABASE_URL))
    SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)
else:
    _SQLITE_STORE = _SQLiteBenchmarkStore(DATABASE_URL)
    Base.metadata.bind(_SQLITE_STORE)
    ENGINE = _SQLiteEngine(_SQLITE_STORE)  # type: ignore[assignment]

    def SessionLocal() -> _SQLiteSession:  # type: ignore[override]
        return _SQLiteSession(_SQLITE_STORE)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    try:
        Base.metadata.create_all(ENGINE)
    except SQLAlchemyError as exc:  # pragma: no cover - defensive logging
        LOGGER.exception("Failed to initialise benchmark tables: %s", exc)
        raise
    try:
        yield
    finally:
        pass


app = FastAPI(title="Benchmark Service", version="1.0.0", lifespan=_lifespan)
app.state.db_sessionmaker = SessionLocal



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
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Invalid date format") from exc
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

    return Response(status_code=204)


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
