"""FastAPI service exposing the approved trading universe.

The service periodically gathers market data in order to produce a whitelist
of tradeable symbols.  Operators can manually override the computed
eligibility of any symbol and the action is recorded in an audit log for
traceability.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any, Dict, Iterable, Iterator, List, MutableMapping, Optional, Tuple

try:  # pragma: no cover - requests is optional during local testing
    import requests  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - degrade when requests missing
    requests = None  # type: ignore[assignment]

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field

try:  # pragma: no cover - SQLAlchemy is optional in lightweight environments
    from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String, create_engine, select
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.orm import Session, declarative_base, sessionmaker
    from sqlalchemy.pool import StaticPool
    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - allow import without SQLAlchemy
    JSON = Boolean = Column = DateTime = Integer = String = object  # type: ignore[assignment]

    class Engine:  # type: ignore[override]
        """Placeholder to satisfy type annotations when SQLAlchemy is absent."""

    class SQLAlchemyError(Exception):  # type: ignore[override]
        pass

    class StaticPool:  # type: ignore[override]
        pass

    Session = "InMemorySession"  # type: ignore[assignment]

    def declarative_base() -> type:  # type: ignore[override]
        class _Metadata:
            def create_all(self, bind: Any = None) -> None:  # pragma: no cover - noop fallback
                del bind

            def drop_all(self, bind: Any = None) -> None:  # pragma: no cover - noop fallback
                del bind

        return type("Base", (), {"metadata": _Metadata()})

    create_engine = None  # type: ignore[assignment]
    sessionmaker = None  # type: ignore[assignment]
    select = None  # type: ignore[assignment]
    SQLALCHEMY_AVAILABLE = False


if SQLALCHEMY_AVAILABLE:
    USE_SQLALCHEMY = bool(create_engine and sessionmaker and select)
    if USE_SQLALCHEMY:
        try:  # pragma: no cover - safety when stubs are installed
            _probe = select(1)
            USE_SQLALCHEMY = hasattr(_probe, "where")
        except Exception:
            USE_SQLALCHEMY = False
else:
    USE_SQLALCHEMY = False

if not USE_SQLALCHEMY:
    SQLALCHEMY_AVAILABLE = False

from services.common.security import require_admin_account


LOGGER = logging.getLogger("universe.service")


class _RequestsResponse(Protocol):
    """Subset of the requests.Response API used by the service."""

    def raise_for_status(self) -> None:
        ...

    def json(self) -> Any:
        ...


class _RequestsModule(Protocol):
    """Requests module functionality consumed by the universe service."""

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, Any],
        timeout: float,
    ) -> _RequestsResponse:
        ...


def _load_requests() -> Optional[_RequestsModule]:
    """Dynamically import requests to avoid mandatory dependency on mypy."""

    try:
        module = importlib.import_module("requests")
    except Exception:  # pragma: no cover - requests is optional for offline use
        return None
    return cast(_RequestsModule, module)


REQUESTS: Optional[_RequestsModule] = _load_requests()


def _default_metrics() -> Dict[str, float]:
    """Provide a fresh metrics mapping for ORM defaults."""

    return {}


DEFAULT_DATABASE_URL = "sqlite:///./universe.db"


def _database_url() -> str:
    """Resolve the Timescale/Postgres connection string."""

    allow_sqlite = "pytest" in sys.modules
    raw = (
        os.getenv("UNIVERSE_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or (DEFAULT_DATABASE_URL if allow_sqlite else None)
    )

    if raw is None:
        raise RuntimeError(
            "Universe database DSN is not configured. Set UNIVERSE_DATABASE_URL or "
            "TIMESCALE_DSN to a PostgreSQL/Timescale connection string.",
        )

    candidate = raw.strip()
    if not candidate:
        raise RuntimeError("Universe database DSN cannot be empty once configured.")

    normalized = cast(
        str,
        normalize_sqlalchemy_dsn(
            candidate,
            allow_sqlite=allow_sqlite,
            label="Universe database DSN",
        ),
    )
    return normalized


_DB_URL = _database_url()


_DB_URL = _database_url()


def _engine_options(url: str) -> dict[str, object]:
    options: dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


if TYPE_CHECKING:
    class Base:
        """Static typing stub for the local universe service declarative base."""

        metadata: Any  # pragma: no cover - provided by SQLAlchemy
        registry: Any  # pragma: no cover - provided by SQLAlchemy
else:  # pragma: no cover - runtime base when SQLAlchemy is available
    try:
        from sqlalchemy.orm import declarative_base

if SQLALCHEMY_AVAILABLE:

    class UniverseWhitelist(Base):
        """SQLAlchemy model storing the computed trading universe."""

        __tablename__ = "universe_whitelist"

        symbol: str = Column(String, primary_key=True)
        enabled: bool = Column(Boolean, nullable=False, default=True)
        metrics_json: Dict[str, float] = Column(JSON, nullable=False, default=dict)
        ts: datetime = Column(
            DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
        )


    class AuditLog(Base):
        """Audit log entry capturing manual overrides."""

        __tablename__ = "audit_log"

        id: int = Column(Integer, primary_key=True, autoincrement=True)
        symbol: str = Column(String, nullable=False)
        enabled: bool = Column(Boolean, nullable=False)
        reason: str = Column(String, nullable=False)
        created_at: datetime = Column(
            DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
        )


    def _create_engine() -> Engine:
        return create_engine(_DB_URL, **_engine_options(_DB_URL))


    ENGINE = _create_engine()
    SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

else:

    @dataclass
    class UniverseWhitelist:  # type: ignore[no-redef]
        """In-memory representation of the computed trading universe."""

        symbol: str
        enabled: bool = True
        metrics_json: Dict[str, float] = field(default_factory=dict)
        ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


    @dataclass
    class AuditLog:  # type: ignore[no-redef]
        """In-memory audit record for manual overrides."""

        symbol: str
        enabled: bool
        reason: str
        id: Optional[int] = None
        created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


    _IN_MEMORY_UNIVERSE: MutableMapping[str, UniverseWhitelist] = {}
    _IN_MEMORY_AUDIT_LOG: List[AuditLog] = []
    _IN_MEMORY_LOCK = RLock()
    _NEXT_AUDIT_ID = 1

    class InMemorySession:
        """Tiny transactional layer mimicking the SQLAlchemy session API."""

        def __init__(self) -> None:
            self._closed = False
            self._lock = _IN_MEMORY_LOCK
            self._lock.acquire()

        # ------------------------------------------------------------------
        # Context manager support
        # ------------------------------------------------------------------
        def __enter__(self) -> "InMemorySession":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - no exceptions expected
            self.close()

        # ------------------------------------------------------------------
        # Session API
        # ------------------------------------------------------------------
        def close(self) -> None:
            if not self._closed:
                self._closed = True
                self._lock.release()

        def get(self, model: Any, key: Any) -> Optional[Any]:
            if model is UniverseWhitelist:
                return _IN_MEMORY_UNIVERSE.get(str(key).upper())
            return None

        def add(self, instance: Any) -> None:
            global _NEXT_AUDIT_ID
            if isinstance(instance, UniverseWhitelist):
                _IN_MEMORY_UNIVERSE[instance.symbol.upper()] = instance
            elif isinstance(instance, AuditLog):
                if instance.id is None:  # pragma: no cover - defensive
                    instance.id = _NEXT_AUDIT_ID
                    _NEXT_AUDIT_ID += 1
                else:
                    _NEXT_AUDIT_ID = max(_NEXT_AUDIT_ID, instance.id + 1)
                _IN_MEMORY_AUDIT_LOG.append(instance)

        def commit(self) -> None:  # pragma: no cover - commits are implicit
            return None

        def __del__(self) -> None:  # pragma: no cover - ensure locks are released
            self.close()

    Engine = Engine  # type: ignore[assignment]
    Session = InMemorySession  # type: ignore[assignment]

    def SessionLocal() -> InMemorySession:  # type: ignore[override]
        return InMemorySession()

    ENGINE = None


    _IN_MEMORY_UNIVERSE: MutableMapping[str, UniverseWhitelist] = {}
    _IN_MEMORY_AUDIT_LOG: List[AuditLog] = []
    _IN_MEMORY_LOCK = RLock()
    _NEXT_AUDIT_ID = 1

    class InMemorySession:
        """Tiny transactional layer mimicking the SQLAlchemy session API."""

        def __init__(self) -> None:
            self._closed = False
            self._lock = _IN_MEMORY_LOCK
            self._lock.acquire()

        # ------------------------------------------------------------------
        # Context manager support
        # ------------------------------------------------------------------
        def __enter__(self) -> "InMemorySession":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - no exceptions expected
            self.close()

        # ------------------------------------------------------------------
        # Session API
        # ------------------------------------------------------------------
        def close(self) -> None:
            if not self._closed:
                self._closed = True
                self._lock.release()

        def get(self, model: Any, key: Any) -> Optional[Any]:
            if model is UniverseWhitelist:
                return _IN_MEMORY_UNIVERSE.get(str(key).upper())
            return None

        def add(self, instance: Any) -> None:
            global _NEXT_AUDIT_ID
            if isinstance(instance, UniverseWhitelist):
                _IN_MEMORY_UNIVERSE[instance.symbol.upper()] = instance
            elif isinstance(instance, AuditLog):
                if instance.id is None:  # pragma: no cover - defensive
                    instance.id = _NEXT_AUDIT_ID
                    _NEXT_AUDIT_ID += 1
                else:
                    _NEXT_AUDIT_ID = max(_NEXT_AUDIT_ID, instance.id + 1)
                _IN_MEMORY_AUDIT_LOG.append(instance)

        def commit(self) -> None:  # pragma: no cover - commits are implicit
            return None

        def __del__(self) -> None:  # pragma: no cover - ensure locks are released
            self.close()

    Engine = Engine  # type: ignore[assignment]
    Session = InMemorySession  # type: ignore[assignment]

    def SessionLocal() -> InMemorySession:  # type: ignore[override]
        return InMemorySession()

    ENGINE = None


def get_session() -> Iterator[Session]:
    """Provide a SQLAlchemy session scoped to the request lifecycle."""

    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


app = FastAPI(title="Universe Service")

RouteFn = TypeVar("RouteFn", bound=Callable[..., Any])


def _app_get(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``FastAPI.get`` to satisfy strict checking."""

    return cast(Callable[[RouteFn], RouteFn], app.get(*args, **kwargs))


def _app_post(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``FastAPI.post`` to satisfy strict checking."""

    return cast(Callable[[RouteFn], RouteFn], app.post(*args, **kwargs))


def _app_on_event(event: str) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``FastAPI.on_event`` for coroutine registration."""

    return cast(Callable[[RouteFn], RouteFn], app.on_event(event))


MARKET_CAP_THRESHOLD = 1_000_000_000.0
GLOBAL_VOLUME_THRESHOLD = 100_000_000.0
KRAKEN_VOLUME_THRESHOLD = 10_000_000.0
ANNUALISED_VOL_THRESHOLD = 0.40


class UniverseThresholds(BaseModel):
    """Threshold configuration applied when computing the trading universe."""

    cap: float = Field(..., description="Minimum required market capitalisation in USD.")
    volume_global: float = Field(..., description="Minimum required global trading volume in USD.")
    volume_kraken: float = Field(..., description="Minimum required Kraken specific volume in USD.")
    ann_vol: float = Field(..., description="Minimum annualised volatility required to participate.")


class UniverseResponse(BaseModel):
    """Response payload returned by ``GET /universe/approved``."""

    symbols: List[str] = Field(..., description="Alphabetically sorted list of approved symbols.")
    generated_at: datetime = Field(..., description="Timestamp when the universe was generated.")
    thresholds: UniverseThresholds


class OverrideRequest(BaseModel):
    """Payload required to override a symbol's eligibility."""

    symbol: str = Field(..., description="Symbol that should be toggled", example="BTC")
    enabled: bool = Field(..., description="Whether the symbol should be part of the universe.")
    reason: str = Field(..., description="Explanation for the manual intervention.")


def _initialise_database() -> None:
    """Ensure persistence backend is ready for use."""

    if SQLALCHEMY_AVAILABLE:
        if ENGINE is None:
            raise RuntimeError("Universe database engine has not been initialised")
        Base.metadata.create_all(bind=ENGINE)
        return

    # Reset the in-memory store for deterministic tests when SQLAlchemy is absent.
    with _IN_MEMORY_LOCK:
        _IN_MEMORY_UNIVERSE.clear()
        _IN_MEMORY_AUDIT_LOG.clear()
        global _NEXT_AUDIT_ID
        _NEXT_AUDIT_ID = 1


_STATIC_COINGECKO_DATA: Dict[str, Dict[str, float]] = {
    "BTC": {"market_cap": 400_000_000_000.0, "global_volume": 150_000_000_000.0},
    "ETH": {"market_cap": 200_000_000_000.0, "global_volume": 80_000_000_000.0},
    "DOGE": {"market_cap": 12_000_000_000.0, "global_volume": 5_000_000_000.0},
}


def fetch_coingecko_market_data() -> Dict[str, Dict[str, float]]:
    """Fetch market capitalisation and global volume from CoinGecko.

    The function attempts to retrieve live data and falls back to a small static
    sample in the event of network failures.  The resulting mapping is keyed by
    the uppercase asset symbol.
    """

    if requests is None:  # pragma: no cover - exercised when dependency missing
        LOGGER.info("requests is unavailable; using static CoinGecko sample data")
        return dict(_STATIC_COINGECKO_DATA)

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "price_change_percentage": "24h",
    }

        try:
            response = REQUESTS.get(url, params=params, timeout=15)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # pragma: no cover - network fallback
            LOGGER.warning("Failed to fetch CoinGecko data: %s", exc)
        else:
            result: Dict[str, Dict[str, float]] = {}
            if isinstance(payload, Sequence):
                for raw_entry in payload:
                    if not isinstance(raw_entry, Mapping):
                        continue
                    symbol = str(raw_entry.get("symbol", "")).upper()
                    if not symbol:
                        continue
                    result[symbol] = {
                        "market_cap": float(raw_entry.get("market_cap") or 0.0),
                        "global_volume": float(raw_entry.get("total_volume") or 0.0),
                    }
            if result:
                return result
    else:  # pragma: no cover - requests missing during offline usage
        LOGGER.debug("requests module unavailable; using fallback CoinGecko sample")

    # deterministic fallback ensures the service remains functional without
    # external connectivity.
    return dict(_STATIC_COINGECKO_DATA)


def fetch_kraken_volume(symbols: Iterable[str]) -> Dict[str, float]:
    """Stub for fetching Kraken specific traded volume.

    An integration with the internal market data warehouse can replace this
    implementation at a later time.
    """

    return {symbol: 20_000_000.0 for symbol in symbols}


def fetch_annualised_volatility(symbols: Iterable[str]) -> Dict[str, float]:
    """Stub for computing annualised volatility for each symbol."""

    return {symbol: 0.45 for symbol in symbols}


def _latest_manual_overrides(
    session: Session, symbols: Iterable[str]
) -> Dict[str, Tuple[bool, str, datetime]]:
    """Return the most recent manual override per symbol."""

    symbol_list = sorted({symbol.upper() for symbol in symbols})
    if not symbol_list:
        return {}

    overrides: Dict[str, Tuple[bool, str, datetime]] = {}

    if SQLALCHEMY_AVAILABLE and select is not None:
        try:
            stmt = (
                select(AuditLog.symbol, AuditLog.enabled, AuditLog.reason, AuditLog.created_at)
                .where(AuditLog.symbol.in_(symbol_list))
                .order_by(AuditLog.symbol, AuditLog.created_at.desc())
            )

            for symbol, enabled, reason, created_at in session.execute(stmt):
                if symbol not in overrides:
                    overrides[symbol] = (enabled, reason, created_at)
            return overrides
        except AttributeError:
            overrides.clear()

    seen: set[str] = set()
    for entry in reversed(_IN_MEMORY_AUDIT_LOG):
        canonical_symbol = entry.symbol.upper()
        if canonical_symbol in symbol_list and canonical_symbol not in seen:
            overrides[canonical_symbol] = (entry.enabled, entry.reason, entry.created_at)
            seen.add(canonical_symbol)

    return overrides


def _compute_universe(session: Session) -> datetime:
    """Compute and persist the approved trading universe.

    Returns the timestamp representing when the universe was generated.
    """

    market_data = fetch_coingecko_market_data()
    symbols = list(market_data.keys())
    kraken_volume = fetch_kraken_volume(symbols)
    annualised_vol = fetch_annualised_volatility(symbols)
    overrides = _latest_manual_overrides(session, symbols)
    generated_at = datetime.now(timezone.utc)

    for symbol, metrics in market_data.items():
        metrics_blob: Dict[str, Any] = {
            "cap": metrics.get("market_cap", 0.0),
            "volume_global": metrics.get("global_volume", 0.0),
            "volume_kraken": kraken_volume.get(symbol, 0.0),
            "ann_vol": annualised_vol.get(symbol, 0.0),
        }

        passes_thresholds = (
            metrics_blob["cap"] >= MARKET_CAP_THRESHOLD
            and metrics_blob["volume_global"] >= GLOBAL_VOLUME_THRESHOLD
            and metrics_blob["volume_kraken"] >= KRAKEN_VOLUME_THRESHOLD
            and metrics_blob["ann_vol"] >= ANNUALISED_VOL_THRESHOLD
        )

        entry = session.get(UniverseWhitelist, symbol)
        if entry is None:
            entry = UniverseWhitelist(symbol=symbol)
            session.add(entry)

        override_details = overrides.get(symbol)
        override_enabled: Optional[bool] = None
        if override_details is not None:
            override_enabled, override_reason, override_ts = override_details
            metrics_blob["override_reason"] = override_reason
            metrics_blob["override_at"] = override_ts.isoformat()
        metrics_blob["computed_enabled"] = passes_thresholds
        metrics_blob["override_applied"] = override_enabled is not None

        entry.metrics_json = metrics_blob
        entry.ts = generated_at
        entry.enabled = override_enabled if override_enabled is not None else passes_thresholds

    session.commit()
    return generated_at


def _refresh_universe_periodically() -> None:
    """Background task that recomputes the universe every 24 hours."""

    async def _run() -> None:
        while True:
            try:
                session = SessionLocal()
                try:
                    _compute_universe(session)
                finally:
                    session.close()
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.exception("Failed to refresh trading universe: %s", exc)
            await asyncio.sleep(timedelta(days=1).total_seconds())

    asyncio.create_task(_run())


@_app_on_event("startup")
async def _startup_event() -> None:
    """Initialise the database schema and compute the first universe."""

    _initialise_database()
    session = SessionLocal()
    try:
        _compute_universe(session)
    finally:
        session.close()
    _refresh_universe_periodically()


@_app_get("/universe/approved", response_model=UniverseResponse)
def get_universe(
    session: Session = Depends(get_session),
    _: str = Depends(require_admin_account),
) -> UniverseResponse:
    """Return the currently approved trading universe."""

    if SQLALCHEMY_AVAILABLE and select is not None:
        try:
            entries = session.execute(
                select(UniverseWhitelist).where(UniverseWhitelist.enabled.is_(True))
            ).scalars()
            symbols = sorted({entry.symbol for entry in entries})

            latest_generated = session.execute(
                select(UniverseWhitelist.ts).order_by(UniverseWhitelist.ts.desc())
            ).scalars().first()
        except AttributeError:
            symbols = []
            latest_generated = None
    else:
        symbols = sorted(
            symbol
            for symbol, entry in _IN_MEMORY_UNIVERSE.items()
            if entry.enabled
        )
        latest_generated = max(
            (entry.ts for entry in _IN_MEMORY_UNIVERSE.values()),
            default=None,
        )

    if latest_generated is None:
        raise HTTPException(status_code=404, detail="Universe has not been generated yet.")

    thresholds = UniverseThresholds(
        cap=MARKET_CAP_THRESHOLD,
        volume_global=GLOBAL_VOLUME_THRESHOLD,
        volume_kraken=KRAKEN_VOLUME_THRESHOLD,
        ann_vol=ANNUALISED_VOL_THRESHOLD,
    )

    return UniverseResponse(symbols=symbols, generated_at=latest_generated, thresholds=thresholds)


@_app_post("/universe/override", status_code=204)
def override_symbol(
    payload: OverrideRequest,
    request: Request,
    session: Session = Depends(get_session),
    actor: str = Depends(require_admin_account),
) -> Response:
    """Manually toggle a symbol's eligibility and record the action."""

    symbol = payload.symbol.upper()
    entry = session.get(UniverseWhitelist, symbol)
    if entry is None:
        entry = UniverseWhitelist(symbol=symbol, metrics_json={})
        session.add(entry)

    header_account = (request.headers.get("X-Account-ID") or "").strip().lower()
    normalized_actor = actor.strip().lower()
    if header_account and header_account != normalized_actor:
        raise HTTPException(
            status_code=403,
            detail="Account scope does not match authenticated administrator.",
        )

    request_scopes = getattr(request.state, "account_scopes", None)
    if request_scopes:
        normalized_scopes = {
            str(scope).strip().lower()
            for scope in request_scopes
            if str(scope or "").strip()
        }
        if normalized_scopes and normalized_actor not in normalized_scopes:
            raise HTTPException(
                status_code=403,
                detail="Authenticated administrator lacks access to the requested account scope.",
            )

    entry.enabled = payload.enabled
    entry.ts = datetime.now(timezone.utc)

    audit_row = AuditLog(symbol=symbol, enabled=payload.enabled, reason=payload.reason)
    session.add(audit_row)
    session.commit()

    return Response(status_code=204)


__all__ = ["app"]

