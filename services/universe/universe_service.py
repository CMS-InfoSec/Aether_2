"""FastAPI microservice exposing the approved trading universe.

The service combines market capitalisation data sourced from CoinGecko with
Kraken venue liquidity in TimescaleDB to determine which symbols are eligible
for trading.  Operators can manually override the computed universe while all
changes are captured in the audit log for traceability.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence
from uuid import UUID, uuid4

try:  # pragma: no cover - alembic is optional during tests
    from alembic import command
    from alembic.config import Config
except ModuleNotFoundError:  # pragma: no cover - allow import without alembic
    command = None  # type: ignore[assignment]
    Config = None  # type: ignore[assignment]
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, Column, DateTime, Float, String, create_engine, func, select
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.engine import Engine, URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from services.common.security import require_admin_account
from shared.spot import filter_spot_symbols, is_spot_symbol, normalize_spot_symbol

LOGGER = logging.getLogger(__name__)


def _normalize_database_url(url: str) -> str:
    """Ensure SQLAlchemy uses the psycopg2 dialect when possible."""

    if url.startswith("postgresql+psycopg://"):
        return "postgresql+psycopg2://" + url[len("postgresql+psycopg://") :]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg2://" + url[len("postgresql://") :]
    if url.startswith("postgres://"):
        return "postgresql+psycopg2://" + url[len("postgres://") :]
    return url


def _require_database_url() -> URL:
    """Return the configured Timescale/PostgreSQL DSN for the universe service."""

    primary = os.getenv("UNIVERSE_DATABASE_URL")
    fallback = os.getenv("TIMESCALE_DATABASE_URI")
    legacy = os.getenv("DATABASE_URL")
    raw_url = primary or fallback or legacy

    if not raw_url:
        raise RuntimeError(
            "UNIVERSE_DATABASE_URL (or legacy DATABASE_URL/TIMESCALE_DATABASE_URI) must point to a managed Timescale/PostgreSQL DSN."
        )

    normalised = _normalize_database_url(raw_url)

    try:
        url = make_url(normalised)
    except ArgumentError as exc:  # pragma: no cover - configuration error
        raise RuntimeError(f"Invalid universe database URL '{raw_url}': {exc}") from exc

    if not url.drivername.lower().startswith("postgresql"):
        raise RuntimeError(
            "Universe service requires a PostgreSQL/TimescaleDSN; "
            f"received driver '{url.drivername}'."
        )

    return url


def _engine_options(url: URL) -> dict[str, Any]:
    options: dict[str, Any] = {
        "future": True,
        "pool_pre_ping": True,
        "pool_size": int(os.getenv("UNIVERSE_DB_POOL_SIZE", "15")),
        "max_overflow": int(os.getenv("UNIVERSE_DB_MAX_OVERFLOW", "10")),
        "pool_timeout": int(os.getenv("UNIVERSE_DB_POOL_TIMEOUT", "30")),
        "pool_recycle": int(os.getenv("UNIVERSE_DB_POOL_RECYCLE", "1800")),
    }

    connect_args: dict[str, Any] = {}

    forced_sslmode = os.getenv("UNIVERSE_DB_SSLMODE")
    if forced_sslmode:
        connect_args["sslmode"] = forced_sslmode
    elif "sslmode" not in url.query and url.host not in {None, "localhost", "127.0.0.1"}:
        connect_args["sslmode"] = "require"

    if connect_args:
        options["connect_args"] = connect_args

    return options


DATABASE_URL: Optional[URL] = None
ENGINE: Optional[Engine] = None
SessionLocal: Optional[sessionmaker] = None


Base = declarative_base()


class Feature(Base):
    """TimescaleDB feature storage used by ingestion pipelines."""

    __tablename__ = "features"

    feature_name = Column(String, primary_key=True)
    entity_id = Column(String, primary_key=True)
    event_timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float)
    attributes = Column("metadata", JSONB)


class OhlcvBar(Base):
    """Kraken OHLCV candles captured in TimescaleDB."""

    __tablename__ = "ohlcv_bars"

    market = Column(String, primary_key=True)
    bucket_start = Column(DateTime(timezone=True), primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)


class UniverseWhitelist(Base):
    """Manual override table persisted in TimescaleDB."""

    __tablename__ = "universe_whitelist"

    asset_id = Column(String, primary_key=True)
    as_of = Column(DateTime(timezone=True), primary_key=True)
    source = Column(String, nullable=False)
    approved = Column(Boolean, nullable=False)
    details = Column("metadata", JSONB, default=dict)


class AuditLog(Base):
    """Audit events recorded for manual overrides."""

    __tablename__ = "audit_log"

    event_id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    entity_type = Column(String, nullable=False)
    entity_id = Column(String, nullable=False)
    actor = Column(String, nullable=False)
    action = Column(String, nullable=False)
    event_time = Column(DateTime(timezone=True), nullable=False)
    attributes = Column("metadata", JSONB, default=dict)


def _create_engine(url: URL) -> Engine:
    return create_engine(
        url.render_as_string(hide_password=False),
        **_engine_options(url),
    )


def ensure_database(app: Optional[FastAPI] = None) -> None:
    """Initialise database connectivity lazily when required."""

    global DATABASE_URL, ENGINE, SessionLocal

    if ENGINE is not None and SessionLocal is not None:
        if app is not None:
            app.state.universe_engine = ENGINE
            app.state.db_sessionmaker = SessionLocal
            if DATABASE_URL is not None:
                app.state.universe_database_url = DATABASE_URL
        return

    if app is not None:
        existing_sessionmaker = getattr(app.state, "db_sessionmaker", None)
        existing_engine = getattr(app.state, "universe_engine", None)
        existing_url = getattr(app.state, "universe_database_url", None)

        if existing_sessionmaker is not None and existing_engine is not None:
            SessionLocal = existing_sessionmaker
            ENGINE = existing_engine
            if isinstance(existing_url, URL):
                DATABASE_URL = existing_url
            elif isinstance(existing_url, str):
                try:
                    DATABASE_URL = make_url(existing_url)
                except ArgumentError:  # pragma: no cover - defensive guard
                    DATABASE_URL = None
            return

    url = _require_database_url()
    engine = _create_engine(url)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    DATABASE_URL = url
    ENGINE = engine
    SessionLocal = session_factory

    if app is not None:
        app.state.universe_database_url = url
        app.state.universe_engine = engine
        app.state.db_sessionmaker = session_factory

_MIGRATIONS_PATH = Path(__file__).resolve().parents[2] / "data" / "migrations"


def run_migrations(url: Optional[URL] = None) -> None:
    """Apply outstanding database migrations for the universe service."""

    ensure_database()

    target_url = url or DATABASE_URL
    if target_url is None:
        raise RuntimeError("Universe database is not configured; cannot run migrations.")

    if command is None or Config is None:
        raise RuntimeError(
            "Universe migrations require alembic; install the optional dependency or stub run_migrations in tests."
        )

    config = Config()
    config.set_main_option("script_location", str(_MIGRATIONS_PATH))
    config.set_main_option("sqlalchemy.url", target_url.render_as_string(hide_password=False))
    config.attributes["configure_logger"] = False

    LOGGER.info("Applying universe service migrations")
    command.upgrade(config, "head")


def get_session() -> Iterator[Session]:
    ensure_database()

    if SessionLocal is None:
        raise RuntimeError(
            "Universe database session factory is not initialised. Ensure service startup has executed or configure SessionLocal manually."
        )

    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


app = FastAPI(title="Universe Selection Service")
app.state.db_sessionmaker = None
app.state.universe_engine = None
app.state.universe_database_url = None


@app.on_event("startup")
def _on_startup() -> None:
    ensure_database(app)

    if DATABASE_URL is not None:
        run_migrations(DATABASE_URL)


class UniverseResponse(BaseModel):
    symbols: List[str]
    generated_at: datetime


class OverrideRequest(BaseModel):
    symbol: str = Field(..., description="Base asset symbol to override", example="BTC")
    enabled: bool = Field(..., description="Whether the symbol should be allowed for trading")
    reason: Optional[str] = Field(None, description="Reason for the manual override")


class OverrideResponse(BaseModel):
    symbol: str
    enabled: bool
    reason: Optional[str]
    actor: str
    updated_at: datetime
    audit_event_id: UUID


MARKET_CAP_THRESHOLD = 1_000_000_000.0
VOLUME_THRESHOLD = 100_000_000.0
VOLATILITY_THRESHOLD = 0.40
MANUAL_OVERRIDE_SOURCE = "manual_override"


KRAKEN_BASE_ALIASES = {
    "XBT": "BTC",
    "XXBT": "BTC",
    "XXBTZ": "BTC",
    "XDG": "DOGE",
    "XXDG": "DOGE",
    "XETH": "ETH",
    "XETC": "ETC",
}

KRAKEN_QUOTE_ALIASES = {
    "USD": "USD",
    "ZUSD": "USD",
}


_UNSUPPORTED_QUOTE_SUFFIXES = (
    "EUR",
    "GBP",
    "CAD",
    "AUD",
    "JPY",
    "CHF",
    "NZD",
    "SGD",
    "HKD",
    "CNY",
    "KRW",
    "TRY",
    "USDT",
)


def _latest_feature_map(session: Session, feature_names: Sequence[str]) -> Dict[str, Feature]:
    """Return latest feature rows keyed by entity id with priority order."""

    results: Dict[str, Feature] = {}
    for feature_name in feature_names:
        subquery = (
            select(Feature.entity_id, func.max(Feature.event_timestamp).label("latest"))
            .where(Feature.feature_name == feature_name)
            .group_by(Feature.entity_id)
            .subquery()
        )

        rows = (
            session.execute(
                select(Feature)
                .where(Feature.feature_name == feature_name)
                .join(
                    subquery,
                    (Feature.entity_id == subquery.c.entity_id)
                    & (Feature.event_timestamp == subquery.c.latest),
                )
            )
            .scalars()
            .all()
        )

        for feature in rows:
            canonical = _normalize_market(feature.entity_id)
            if canonical is None:
                continue
            results.setdefault(canonical, feature)

    return results


def _kraken_volume_24h(session: Session) -> Dict[str, float]:
    """Aggregate the past 24 hours of Kraken volume per USD-quoted market."""

    since = datetime.now(timezone.utc) - timedelta(days=1)
    rows = session.execute(
        select(OhlcvBar.market, func.sum(OhlcvBar.volume).label("volume"))
        .where(OhlcvBar.bucket_start >= since)
        .group_by(OhlcvBar.market)
    )

    volumes: Dict[str, float] = {}
    for market, volume in rows:
        if volume is None:
            continue

        normalized = _normalize_market(market)
        if normalized is None:
            continue

        volumes[normalized] = max(volumes.get(normalized, 0.0), float(volume))
    return volumes


def _normalize_market(market: str) -> Optional[str]:
    """Return a canonical ``"BASE-USD"`` identifier for a Kraken market."""

    if not market:
        return None

    token = market.strip().upper()
    if not token:
        return None

    compact = token.replace("/", "").replace("-", "")
    if not compact:
        return None

    base_token: Optional[str] = None
    quote_token: Optional[str] = None

    for alias in sorted(KRAKEN_QUOTE_ALIASES.keys(), key=len, reverse=True):
        if compact.endswith(alias):
            base_token = compact[: -len(alias)]
            quote_token = alias
            break

    if not base_token or not quote_token:
        if "/" in token or "-" in token:
            return None
        base = _normalize_asset_symbol(compact, is_quote=False)
        if not base:
            return None
        if base.endswith(_UNSUPPORTED_QUOTE_SUFFIXES):
            return None
        return _finalize_spot_symbol(base)

    base = _normalize_asset_symbol(base_token, is_quote=False)
    quote = _normalize_asset_symbol(quote_token, is_quote=True)

    if not base or quote != "USD":
        return None

    return _finalize_spot_symbol(base)


def _normalize_asset_symbol(symbol: str, *, is_quote: bool) -> str:
    """Normalise Kraken specific asset aliases to canonical symbols."""

    token = symbol.strip()
    if not token:
        return ""

    aliases = KRAKEN_QUOTE_ALIASES if is_quote else KRAKEN_BASE_ALIASES

    direct = aliases.get(token)
    if direct:
        token = direct

    # Trim Kraken specific leading/trailing characters before retrying.
    trimmed = token
    while trimmed.endswith(("X", "Z")) and len(trimmed) > 3:
        trimmed = trimmed[:-1]
    while trimmed.startswith(("X", "Z")) and len(trimmed) > 3:
        trimmed = trimmed[1:]

    return aliases.get(trimmed, trimmed)


def _finalize_spot_symbol(base_asset: str) -> Optional[str]:
    """Return a canonical USD spot symbol for *base_asset* if eligible."""

    candidate = f"{base_asset}-USD"
    normalized = normalize_spot_symbol(candidate)
    if not normalized or not normalized.endswith("-USD"):
        return None

    if not is_spot_symbol(normalized):
        LOGGER.warning(
            "Dropping non-spot instrument from universe evaluation",
            extra={"symbol": candidate},
        )
        return None

    return normalized


def _latest_manual_overrides(session: Session, *, migrate: bool = False) -> Dict[str, UniverseWhitelist]:
    """Return the most recent manual override per asset."""

    overrides: Dict[str, UniverseWhitelist] = {}
    migrated = False
    rows = session.execute(
        select(UniverseWhitelist)
        .where(UniverseWhitelist.source == MANUAL_OVERRIDE_SOURCE)
        .order_by(UniverseWhitelist.asset_id, UniverseWhitelist.as_of.desc())
    ).scalars()

    for record in rows:
        canonical = _normalize_market(record.asset_id)
        if canonical is None:
            continue
        if migrate and record.asset_id != canonical:
            record.asset_id = canonical
            migrated = True
        overrides.setdefault(canonical, record)

    if migrate and migrated:
        session.flush()
    return overrides


def _evaluate_universe(session: Session) -> List[str]:
    caps = _latest_feature_map(session, ["coingecko.market_cap", "coingecko_market_cap"])
    vols = _latest_feature_map(
        session,
        ["coingecko.volatility_30d", "coingecko_volatility_30d", "coingecko.volatility"],
    )
    volumes = _kraken_volume_24h(session)
    overrides = _latest_manual_overrides(session)

    approved: set[str] = set()

    for pair, cap_feature in caps.items():
        canonical_pair = _normalize_market(pair)
        if canonical_pair is None:
            continue

        market_cap = float(cap_feature.value or 0.0)
        vol_feature = vols.get(canonical_pair)
        if vol_feature is None and pair != canonical_pair:
            vol_feature = vols.get(pair)
        volatility = float(vol_feature.value) if vol_feature and vol_feature.value is not None else 0.0
        kraken_volume = volumes.get(canonical_pair, 0.0)

        if (
            market_cap >= MARKET_CAP_THRESHOLD
            and kraken_volume >= VOLUME_THRESHOLD
            and volatility >= VOLATILITY_THRESHOLD
        ):
            approved.add(canonical_pair)

    for pair, override in overrides.items():
        canonical_pair = _normalize_market(pair)
        if canonical_pair is None:
            continue

        if override.approved:
            approved.add(canonical_pair)
        else:
            approved.discard(canonical_pair)

    spot_symbols = filter_spot_symbols(sorted(approved), logger=LOGGER)
    return [symbol for symbol in spot_symbols if symbol.endswith("-USD")]


@app.get("/universe/approved", response_model=UniverseResponse)
def approved_universe(session: Session = Depends(get_session)) -> UniverseResponse:
    symbols = _evaluate_universe(session)
    return UniverseResponse(symbols=symbols, generated_at=datetime.now(timezone.utc))


@app.post("/universe/override", response_model=OverrideResponse, status_code=201)
def override_symbol(
    request: OverrideRequest,
    session: Session = Depends(get_session),
    actor_account: str = Depends(require_admin_account),
) -> OverrideResponse:
    raw_symbol = request.symbol.strip().upper()
    if not raw_symbol:
        raise HTTPException(status_code=400, detail="Symbol must not be empty")

    if request.reason is not None and not request.reason.strip():
        raise HTTPException(status_code=400, detail="Override reason must not be empty when provided")

    canonical_symbol = _normalize_market(raw_symbol)
    if canonical_symbol is None:
        raise HTTPException(status_code=400, detail="Symbol must be a USD-quoted spot market")

    latest_overrides = _latest_manual_overrides(session, migrate=True)
    previous = latest_overrides.get(canonical_symbol)

    now = datetime.now(timezone.utc)
    details: Dict[str, object] = {"actor": actor_account}
    if request.reason:
        details["reason"] = request.reason

    override_record = UniverseWhitelist(
        asset_id=canonical_symbol,
        as_of=now,
        source=MANUAL_OVERRIDE_SOURCE,
        approved=request.enabled,
        details=details,
    )
    session.add(override_record)

    audit_payload = {
        "previous": {
            "approved": previous.approved if previous else None,
            "reason": (previous.details or {}).get("reason") if previous else None,
        },
        "current": {
            "approved": request.enabled,
            "reason": request.reason,
        },
        "actor": actor_account,
    }

    audit_entry = AuditLog(
        event_id=uuid4(),
        entity_type="universe.symbol",
        entity_id=canonical_symbol,
        actor=actor_account,
        action="universe.override.enabled" if request.enabled else "universe.override.disabled",
        event_time=now,
        attributes=audit_payload,
    )
    session.add(audit_entry)

    session.commit()

    return OverrideResponse(
        symbol=canonical_symbol,
        enabled=request.enabled,
        reason=request.reason,
        actor=actor_account,
        updated_at=now,
        audit_event_id=audit_entry.event_id,
    )


__all__ = [
    "app",
    "approved_universe",
    "override_symbol",
    "run_migrations",
]

