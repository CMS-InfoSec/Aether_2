"""FastAPI microservice exposing account level risk validation."""

from __future__ import annotations

import asyncio
import json
import logging
import os

import math

import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from decimal import Decimal

from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Set, Union


import httpx

from fastapi import Depends, FastAPI, HTTPException, Query, status

from pydantic import BaseModel, ConfigDict, Field, PositiveFloat, model_validator
from sqlalchemy import Column, DateTime, Float, Integer, Numeric, String, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from statistics import mean


from alerts import push_exchange_adapter_failure
from exchange_adapter import get_exchange_adapter
from metrics import (
    increment_trade_rejection,
    metric_context,
    observe_risk_validation_latency,
    record_fees_nav_pct,
    setup_metrics,
    traced_span,
)
from esg_filter import ESG_REASON, esg_filter
from services.common.compliance import (
    SQLALCHEMY_AVAILABLE as SANCTIONS_SQLALCHEMY_AVAILABLE,
    SanctionRecord,
    ensure_sanctions_schema,
    is_blocking_status,
)
from services.common.security import require_admin_account
from services.common.spot import require_spot_http
from battle_mode import BattleModeController, create_battle_mode_tables

from cost_throttler import CostThrottler
from services.risk.position_sizer import PositionSizer
from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from shared.exits import compute_exit_targets
from shared.graceful_shutdown import flush_logging_handlers, setup_graceful_shutdown
from shared.spot import filter_spot_symbols, is_spot_symbol, normalize_spot_symbol


logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("risk.audit")
logging.basicConfig(level=logging.INFO)


NUMERIC_PRECISION = 28
NUMERIC_SCALE = 8
DECIMAL_ZERO = Decimal("0")


def _as_decimal(
    value: Optional[Union[Decimal, float, int, str]], *, default: Decimal = DECIMAL_ZERO
) -> Decimal:
    """Normalize numeric inputs to :class:`~decimal.Decimal` instances."""

    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, str)):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    return Decimal(str(value))


def _maybe_decimal(value: Optional[Union[Decimal, float, int, str]]) -> Optional[Decimal]:
    """Return ``None`` or the normalized decimal representation of *value*."""

    if value is None:
        return None
    return _as_decimal(value)


SHUTDOWN_TIMEOUT = float(os.getenv("RISK_SHUTDOWN_TIMEOUT", os.getenv("SERVICE_SHUTDOWN_TIMEOUT", "75.0")))

_CAPITAL_ALLOCATOR_URL = os.getenv("CAPITAL_ALLOCATOR_URL")
_CAPITAL_ALLOCATOR_TIMEOUT = float(os.getenv("CAPITAL_ALLOCATOR_TIMEOUT", "1.5"))
_CAPITAL_ALLOCATOR_TOLERANCE = float(os.getenv("CAPITAL_ALLOCATOR_NAV_TOLERANCE", "0.02"))
_BATTLE_MODE_VOL_THRESHOLD = float(os.getenv("BATTLE_MODE_VOL_THRESHOLD", "1.0"))
DEFAULT_EXCHANGE = os.getenv("PRIMARY_EXCHANGE", "kraken")

_UNIVERSE_SERVICE_URL = os.getenv("UNIVERSE_SERVICE_URL", "http://localhost:9000")
_UNIVERSE_SERVICE_TIMEOUT = float(os.getenv("UNIVERSE_SERVICE_TIMEOUT", "5.0"))
_UNIVERSE_CACHE_TTL = int(os.getenv("UNIVERSE_CACHE_TTL", "300"))


Base = declarative_base()


class AccountRiskUsage(Base):
    """Persisted account level usage metrics backing the service responses."""

    __tablename__ = "account_risk_usage"

    account_id = Column(String, primary_key=True)
    realized_daily_loss = Column(
        Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
        nullable=False,
        default=DECIMAL_ZERO,
    )
    fees_paid = Column(
        Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
        nullable=False,
        default=DECIMAL_ZERO,
    )
    net_asset_value = Column(
        Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
        nullable=False,
        default=DECIMAL_ZERO,
    )
    var_95 = Column(Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE), nullable=True)
    var_99 = Column(Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)


class PositionSizeLog(Base):
    """Record of suggested position sizes for auditability."""

    __tablename__ = "position_size_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False, index=True)
    symbol = Column(String, nullable=False)
    volatility = Column(Float, nullable=False)
    size = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc))


class ConfigError(RuntimeError):
    """Raised when account specific configuration cannot be loaded."""


class UniverseServiceError(RuntimeError):
    """Raised when the trading universe service cannot be queried."""


@dataclass(frozen=True)
class UniverseSnapshot:
    symbols: Set[str]
    generated_at: datetime
    thresholds: Dict[str, Any]


class AccountRiskLimit(Base):
    """SQLAlchemy representation of account risk configuration."""

    __tablename__ = "account_risk_limits"

    account_id = Column(String, primary_key=True)
    max_daily_loss = Column(Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE), nullable=False)
    fee_budget = Column(Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE), nullable=False)
    max_nav_pct_per_trade = Column(
        Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
        nullable=False,
    )
    notional_cap = Column(Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE), nullable=False)
    cooldown_minutes = Column(Integer, nullable=False, default=0)
    instrument_whitelist = Column(String, nullable=True)
    var_95_limit = Column(Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE), nullable=True)
    var_99_limit = Column(Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE), nullable=True)
    spread_threshold_bps = Column(
        Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
        nullable=True,
    )
    latency_stall_seconds = Column(
        Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
        nullable=True,
    )
    exchange_outage_block = Column(Integer, nullable=False, default=0)

    correlation_threshold = Column(
        Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
        nullable=True,
    )
    correlation_lookback = Column(Integer, nullable=True)
    diversification_cluster_limits = Column(String, nullable=True)


    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            "AccountRiskLimit("
            f"account_id={self.account_id!r}, "
            f"max_daily_loss={self.max_daily_loss}, "
            f"fee_budget={self.fee_budget}, "
            f"max_nav_pct_per_trade={self.max_nav_pct_per_trade}, "
            f"notional_cap={self.notional_cap}, "
            f"cooldown_minutes={self.cooldown_minutes}, "
            f"instrument_whitelist={self.instrument_whitelist}, "
            f"var_95_limit={self.var_95_limit}, "
            f"var_99_limit={self.var_99_limit}, "
            f"spread_threshold_bps={self.spread_threshold_bps}, "
            f"latency_stall_seconds={self.latency_stall_seconds}, "
            f"exchange_outage_block={self.exchange_outage_block}, "

            f"correlation_threshold={self.correlation_threshold}, "
            f"correlation_lookback={self.correlation_lookback}, "
            f"diversification_cluster_limits={self.diversification_cluster_limits}"

            ")"
        )

    @property
    def whitelist(self) -> List[str]:
        if not self.instrument_whitelist:
            return []
        return filter_spot_symbols(self.instrument_whitelist.split(","), logger=logger)

    @property
    def cluster_limits(self) -> Dict[str, float]:
        return _parse_cluster_limits(self.diversification_cluster_limits)


@dataclass
class AllocatorAccountState:
    account_id: str
    allocation_pct: float
    allocated_nav: float
    drawdown_ratio: float
    throttled: bool


def _database_url() -> str:
    """Return the configured managed Timescale/Postgres database URL."""

    candidates = (
        os.getenv("RISK_DATABASE_URL"),
        os.getenv("TIMESCALE_DSN"),
        os.getenv("DATABASE_URL"),
    )

    url: Optional[str] = next((value.strip() for value in candidates if value), None)
    if not url:
        raise RuntimeError(
            "RISK_DATABASE_URL must be defined with a PostgreSQL/Timescale connection string."
        )

    normalized = url.lower()
    if normalized.startswith("postgres://"):
        url = "postgresql://" + url.split("://", 1)[1]
        normalized = url.lower()

    allowed_prefixes = (
        "postgresql://",
        "postgresql+psycopg://",
        "postgresql+psycopg2://",
    )
    if not normalized.startswith(allowed_prefixes):
        raise RuntimeError(
            "Risk service requires a PostgreSQL/Timescale DSN; "
            f"received '{url}'."
        )

    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)

    return url


def _engine_options(url: str) -> Dict[str, object]:
    options: Dict[str, object] = {
        "future": True,
        "pool_pre_ping": True,
        "pool_size": int(os.getenv("RISK_DB_POOL_SIZE", "10")),
        "max_overflow": int(os.getenv("RISK_DB_MAX_OVERFLOW", "20")),
        "pool_timeout": int(os.getenv("RISK_DB_POOL_TIMEOUT", "30")),
        "pool_recycle": int(os.getenv("RISK_DB_POOL_RECYCLE", "1800")),
    }

    sslmode = os.getenv("RISK_DB_SSLMODE", "require").strip()
    connect_args: Dict[str, object] = {}
    if sslmode:
        connect_args["sslmode"] = sslmode
    options["connect_args"] = connect_args
    return options


_DB_URL = _database_url()
ENGINE: Engine = create_engine(_DB_URL, **_engine_options(_DB_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)


_DEFAULT_LIMITS: List[Dict[str, object]] = [
    {
        "account_id": "company",
        "max_daily_loss": 150_000.0,
        "fee_budget": 35_000.0,
        "max_nav_pct_per_trade": 0.35,
        "notional_cap": 7_500_000.0,
        "cooldown_minutes": 60,
        "instrument_whitelist": ["BTC-USD", "ETH-USD"],
        "var_95_limit": 250_000.0,
        "var_99_limit": 450_000.0,
        "spread_threshold_bps": 15.0,
        "latency_stall_seconds": 2.0,
        "exchange_outage_block": 0,
    },
    {
        "account_id": "director-1",
        "max_daily_loss": 50_000.0,
        "fee_budget": 10_000.0,
        "max_nav_pct_per_trade": 0.25,
        "notional_cap": 1_500_000.0,
        "cooldown_minutes": 120,
        "instrument_whitelist": ["SOL-USD"],
        "var_95_limit": 100_000.0,
        "var_99_limit": 175_000.0,
        "spread_threshold_bps": 20.0,
        "latency_stall_seconds": 3.0,
        "exchange_outage_block": 1,

        "correlation_threshold": 0.85,
        "correlation_lookback": 20,
        "diversification_cluster_limits": json.dumps({"BTC": 0.4}),

    },
    {
        "account_id": "director-2",
        "max_daily_loss": 100_000.0,
        "fee_budget": 20_000.0,
        "max_nav_pct_per_trade": 0.3,
        "notional_cap": 3_500_000.0,
        "cooldown_minutes": 90,
        "instrument_whitelist": ["BTC-USD", "ETH-USD"],
        "var_95_limit": 180_000.0,
        "var_99_limit": 320_000.0,
        "spread_threshold_bps": 18.0,
        "latency_stall_seconds": 2.5,
        "exchange_outage_block": 0,

        "correlation_threshold": 0.9,
        "correlation_lookback": 30,
        "diversification_cluster_limits": json.dumps({"ENERGY": 0.5}),

    },
]


def _seed_default_limits(session: Session) -> None:
    for payload in _DEFAULT_LIMITS:
        record = session.get(AccountRiskLimit, payload["account_id"])
        whitelist = filter_spot_symbols(
            payload.get("instrument_whitelist", []), logger=logger
        )
        whitelist_blob = ",".join(sorted(whitelist)) if whitelist else None
        if record is None:
            record = AccountRiskLimit(
                account_id=payload["account_id"],
                max_daily_loss=_as_decimal(payload["max_daily_loss"]),
                fee_budget=_as_decimal(payload["fee_budget"]),
                max_nav_pct_per_trade=_as_decimal(payload["max_nav_pct_per_trade"]),
                notional_cap=_as_decimal(payload["notional_cap"]),
                cooldown_minutes=int(payload["cooldown_minutes"]),
                instrument_whitelist=whitelist_blob,
                var_95_limit=_maybe_decimal(payload.get("var_95_limit")),
                var_99_limit=_maybe_decimal(payload.get("var_99_limit")),
                spread_threshold_bps=_maybe_decimal(payload.get("spread_threshold_bps")),
                latency_stall_seconds=_maybe_decimal(payload.get("latency_stall_seconds")),
                exchange_outage_block=int(payload["exchange_outage_block"]),
            )
            session.add(record)
        else:
            record.max_daily_loss = _as_decimal(payload["max_daily_loss"])
            record.fee_budget = _as_decimal(payload["fee_budget"])
            record.max_nav_pct_per_trade = _as_decimal(payload["max_nav_pct_per_trade"])
            record.notional_cap = _as_decimal(payload["notional_cap"])
            record.cooldown_minutes = int(payload["cooldown_minutes"])
            record.instrument_whitelist = whitelist_blob
            record.var_95_limit = _maybe_decimal(payload.get("var_95_limit"))
            record.var_99_limit = _maybe_decimal(payload.get("var_99_limit"))
            record.spread_threshold_bps = _maybe_decimal(payload.get("spread_threshold_bps"))
            record.latency_stall_seconds = _maybe_decimal(payload.get("latency_stall_seconds"))
            record.exchange_outage_block = int(payload["exchange_outage_block"])


def _bootstrap_storage() -> None:
    Base.metadata.create_all(bind=ENGINE)

    create_battle_mode_tables(ENGINE)

    with get_session() as session:
        _seed_default_limits(session)


@contextmanager
def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _log_position_size(
    account_id: str,
    symbol: str,
    volatility: float,
    size: float,
    timestamp: datetime,
) -> None:
    normalized_symbol = symbol.upper()
    record = PositionSizeLog(
        account_id=account_id,
        symbol=normalized_symbol,
        volatility=float(volatility),
        size=float(size),
        created_at=timestamp,
    )
    with get_session() as session:
        session.add(record)


_STUB_MARKET_TELEMETRY: Dict[str, Dict[str, float]] = {
    "BTC-USD": {
        "spread_bps": 4.0,
        "latency_seconds": 0.6,
        "exchange_outage": 0,
    }
}


_STUB_PROM_METRICS: Dict[str, float] = {
    "latency_seconds": 0.8,
}


_STUB_PRICE_HISTORY: Dict[str, List[Dict[str, float]]] = {
    "BTC-USD": [
        {"high": 30250.0, "low": 29780.0, "close": 30010.0},
        {"high": 30310.0, "low": 29820.0, "close": 30055.0},
        {"high": 30440.0, "low": 29960.0, "close": 30180.0},
        {"high": 30510.0, "low": 30010.0, "close": 30245.0},
        {"high": 30620.0, "low": 30120.0, "close": 30360.0},
        {"high": 30740.0, "low": 30200.0, "close": 30480.0},
        {"high": 30830.0, "low": 30290.0, "close": 30570.0},
        {"high": 30920.0, "low": 30380.0, "close": 30640.0},
        {"high": 31010.0, "low": 30460.0, "close": 30735.0},
        {"high": 31120.0, "low": 30540.0, "close": 30810.0},
        {"high": 31200.0, "low": 30600.0, "close": 30900.0},
        {"high": 31310.0, "low": 30690.0, "close": 30980.0},
        {"high": 31420.0, "low": 30780.0, "close": 31070.0},
        {"high": 31510.0, "low": 30860.0, "close": 31140.0},
        {"high": 31600.0, "low": 30930.0, "close": 31220.0},
    ]
}


_STUB_ACCOUNT_RETURNS: Dict[str, List[float]] = {
    "company": [-1500.0 + (i % 5) * 100 for i in range(260)],
    "director-1": [-4500.0 + (i % 7) * 250 for i in range(260)],
}


_STUB_RISK_LIMITS: Dict[str, Dict[str, Any]] = {}


_STUB_ACCOUNT_USAGE: Dict[str, Dict[str, Decimal]] = {}


_STUB_FILLS: List[Dict[str, object]] = [
    {
        "account_id": "company",
        "timestamp": datetime.utcnow().isoformat(),
        "pnl": -2500.0,
        "fee": 125.0,
    }
]


def _filter_spot_instruments(symbols: Iterable[str]) -> List[str]:
    """Return normalized USD spot symbols, discarding non-compliant entries."""

    return filter_spot_symbols(symbols, logger=logger)


class TradeIntent(BaseModel):
    """Represents the incoming trading intent from the policy layer."""

    policy_id: str = Field(..., description="Identifier for the originating policy or strategy")
    instrument_id: str = Field(..., description="Instrument or symbol to be traded")
    side: str = Field(..., pattern="^(buy|sell)$", description="Trade direction")
    quantity: PositiveFloat = Field(..., description="Requested trade quantity")
    price: PositiveFloat = Field(..., description="Reference price used for risk checks")
    notional: Optional[PositiveFloat] = Field(
        None, description="Explicit notional value, overrides quantity * price when provided"
    )

    @model_validator(mode="after")
    def compute_notional(cls, model: "TradeIntent") -> "TradeIntent":
        quantity = model.quantity
        price = model.price
        notional = model.notional
        if notional is None and quantity is not None and price is not None:
            model.notional = quantity * price
        return model

    @property
    def notional_per_unit(self) -> float:
        return self.price


class AccountPortfolioState(BaseModel):
    """Snapshot of the account state used for risk validation."""

    net_asset_value: PositiveFloat = Field(..., description="Account NAV in quote currency")
    notional_exposure: float = Field(
        0.0,
        ge=0.0,
        description="Current total notional exposure across open positions",
    )
    realized_daily_loss: float = Field(
        0.0,
        ge=0.0,
        description="Realized trading loss accumulated during the current session",
    )
    fees_paid: float = Field(0.0, ge=0.0, description="Fees accumulated during the current session")
    var_95: Optional[float] = Field(
        None, ge=0.0, description="Current 1-day 95% Value-at-Risk for the portfolio"
    )
    var_99: Optional[float] = Field(
        None, ge=0.0, description="Current 1-day 99% Value-at-Risk for the portfolio"
    )
    instrument_exposure: Dict[str, float] = Field(
        default_factory=dict,
        description="Current absolute notional exposure per instrument",
    )
    historical_returns: Dict[str, List[float]] = Field(
        default_factory=dict,
        description="Historical return series used for correlation analysis",
    )
    instrument_clusters: Dict[str, str] = Field(
        default_factory=dict,
        description="Mapping from instrument to diversification cluster",
    )
    cluster_exposure: Dict[str, float] = Field(
        default_factory=dict,
        description="Current absolute exposure per diversification cluster",
    )


class RiskValidationRequest(BaseModel):
    account_id: str = Field(..., description="Unique account identifier")
    intent: TradeIntent
    portfolio_state: AccountPortfolioState


class RiskValidationResponse(BaseModel):
    """Decision returned by the risk service."""

    pass_: bool = Field(..., alias="pass", description="Indicates whether the trade was approved")
    reasons: List[str] = Field(default_factory=list, description="Detailed failure reasons, if any")
    adjusted_qty: Optional[float] = Field(
        None,
        description="Suggested adjusted quantity when the full request cannot be honored",
    )
    cooldown: Optional[datetime] = Field(
        None,
        description="Timestamp until which trading is halted due to hard limits",
    )
    take_profit: Optional[float] = Field(
        None,
        description="Recommended take-profit trigger price derived from policy and volatility",
    )
    stop_loss: Optional[float] = Field(
        None,
        description="Recommended stop-loss trigger price derived from policy and volatility",
    )

    model_config = ConfigDict(populate_by_name=True)


class RiskEvaluationContext(BaseModel):
    request: RiskValidationRequest
    limits: AccountRiskLimit

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def current_notional(self) -> float:
        return self.request.portfolio_state.notional_exposure

    @property
    def intended_notional(self) -> float:
        return float(self.request.intent.notional)

class AccountUsage(BaseModel):
    account_id: str
    realized_daily_loss: Decimal = Field(..., ge=DECIMAL_ZERO)
    fees_paid: Decimal = Field(..., ge=DECIMAL_ZERO)
    net_asset_value: Decimal = Field(..., ge=DECIMAL_ZERO)
    var_95: Optional[Decimal] = Field(None, ge=DECIMAL_ZERO)
    var_99: Optional[Decimal] = Field(None, ge=DECIMAL_ZERO)


class AccountRiskLimitModel(BaseModel):
    account_id: str
    max_daily_loss: float
    fee_budget: float
    max_nav_pct_per_trade: float
    notional_cap: float
    cooldown_minutes: int
    instrument_whitelist: List[str] = Field(default_factory=list)
    var_95_limit: Optional[float] = None
    var_99_limit: Optional[float] = None
    spread_threshold_bps: Optional[float] = None
    latency_stall_seconds: Optional[float] = None
    exchange_outage_block: bool = False

    correlation_threshold: Optional[float] = None
    correlation_lookback: Optional[int] = None
    diversification_cluster_limits: Dict[str, float] = Field(default_factory=dict)


    model_config = ConfigDict(from_attributes=True)


class RiskLimitsResponse(BaseModel):
    account_id: str
    limits: AccountRiskLimitModel
    usage: AccountUsage


class BattleModeToggleRequest(BaseModel):
    account_id: str = Field(..., description="Account identifier to toggle battle mode for")
    engage: Optional[bool] = Field(
        None,
        description="Explicit state to set. When omitted the state will be toggled.",
    )
    reason: str = Field(..., min_length=1, description="Audit reason for the toggle request")


app = FastAPI(title="Risk Validation Service", version="1.0.0")
setup_metrics(app, service_name="risk-service")
COST_THROTTLER = CostThrottler()
EXCHANGE_ADAPTER = get_exchange_adapter(DEFAULT_EXCHANGE)

shutdown_manager = setup_graceful_shutdown(
    app,
    service_name="risk-service",
    allowed_paths={"/", "/docs", "/openapi.json"},
    shutdown_timeout=SHUTDOWN_TIMEOUT,
    logger_instance=logger,
)


def _flush_risk_event_buffers() -> None:
    """Flush buffered risk service artifacts before shutdown."""

    flush_logging_handlers("", __name__, "risk.audit")
    loop = asyncio.new_event_loop()
    try:
        summary = loop.run_until_complete(TimescaleAdapter.flush_event_buffers())
    finally:
        loop.close()
    if summary:
        logger.info("Flushed Timescale event buffers", extra={"buckets": summary})


shutdown_manager.register_flush_callback(_flush_risk_event_buffers)


_UNIVERSE_CACHE_LOCK = asyncio.Lock()
_UNIVERSE_CACHE_SNAPSHOT: Optional[UniverseSnapshot] = None
_UNIVERSE_CACHE_EXPIRY: Optional[datetime] = None



@app.on_event("startup")
def _on_startup() -> None:
    _bootstrap_storage()


battle_mode_controller = BattleModeController(
    session_factory=get_session, threshold=_BATTLE_MODE_VOL_THRESHOLD
)


@app.post("/risk/validate", response_model=RiskValidationResponse)
async def validate_risk(
    request: RiskValidationRequest,
    account_id: str = Depends(require_admin_account),
) -> RiskValidationResponse:

    """Validate a trading intent against account level risk limits."""

    if request.account_id != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between authenticated session and payload.",
        )

    logger.info("Received risk validation request for account %s", account_id)

    try:
        limits = _load_account_limits(account_id)
    except ConfigError as exc:

        logger.exception("Unable to load risk limits for account %s", request.account_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc


    context = RiskEvaluationContext(request=request, limits=limits)
    symbol = str(context.request.intent.instrument_id)

    try:
        with traced_span(
            "risk.evaluate",
            account_id=account_id,
            symbol=symbol,
        ):
            evaluation_start = time.perf_counter()
            decision = await _evaluate(context)
        observe_risk_validation_latency((time.perf_counter() - evaluation_start) * 1000.0)
    except Exception as exc:  # pragma: no cover - defensive programming
        logger.exception("Risk evaluation failed for account %s", account_id)
        raise HTTPException(status_code=500, detail="Internal risk evaluation failure") from exc

    logger.info(
        "Risk evaluation completed for account %s: passed=%s",
        account_id,
        decision.pass_,
    )

    metrics_ctx = metric_context(account_id=account_id, symbol=symbol)

    if not decision.pass_:
        _audit_failure(context, decision)
        increment_trade_rejection(
            account_id,
            symbol,
            context=metrics_ctx,
        )

    state = context.request.portfolio_state
    nav = float(state.net_asset_value) if state.net_asset_value else 0.0
    fees = float(state.fees_paid) if state.fees_paid else 0.0
    fees_pct = (fees / nav * 100.0) if nav else 0.0
    record_fees_nav_pct(
        account_id,
        symbol,
        fees_pct,
        context=metrics_ctx,
    )

    return decision.dict(by_alias=True)


@app.get("/risk/limits")
async def get_risk_limits(
    account_id: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    try:
        limits = _load_account_limits(account_id)
    except ConfigError as exc:
        logger.exception("Unable to load risk limits for account %s", account_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    await _refresh_usage_from_balance(account_id)
    usage = _load_account_usage(account_id)
    whitelist = limits.whitelist

    limit_model = AccountRiskLimitModel(
        account_id=limits.account_id,
        max_daily_loss=limits.max_daily_loss,
        fee_budget=limits.fee_budget,
        max_nav_pct_per_trade=limits.max_nav_pct_per_trade,
        notional_cap=limits.notional_cap,
        cooldown_minutes=limits.cooldown_minutes,
        instrument_whitelist=whitelist,
        var_95_limit=limits.var_95_limit,
        var_99_limit=limits.var_99_limit,
        spread_threshold_bps=limits.spread_threshold_bps,
        latency_stall_seconds=limits.latency_stall_seconds,
        exchange_outage_block=bool(limits.exchange_outage_block),

        correlation_threshold=limits.correlation_threshold,
        correlation_lookback=limits.correlation_lookback,
        diversification_cluster_limits=limits.cluster_limits,

    )

    response = RiskLimitsResponse(account_id=account_id, limits=limit_model, usage=usage)
    logger.info("Retrieved risk limits for account %s", account_id)
    return response.dict()



@app.get("/risk/size")
async def get_position_size(
    symbol: str = Query(..., min_length=1, description="Instrument symbol to size"),
    account_id: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    symbol = require_spot_http(symbol, logger=logger)

    try:
        limits = _load_account_limits(account_id)
    except ConfigError as exc:
        logger.exception("Unable to load risk limits for account %s", account_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    await _refresh_usage_from_balance(account_id)
    usage = _load_account_usage(account_id)

    nav_value = float(usage.net_asset_value) if usage.net_asset_value else 0.0
    available_balance: Optional[float] = None

    if EXCHANGE_ADAPTER.supports("get_balance"):
        try:
            snapshot = await EXCHANGE_ADAPTER.get_balance(account_id)
        except NotImplementedError:
            snapshot = None
        except Exception as exc:  # pragma: no cover - best effort diagnostics
            logger.debug(
                "Balance snapshot unavailable for account %s: %s",
                account_id,
                exc,
            )
            snapshot = None
        if isinstance(snapshot, Mapping):
            nav_override = _coerce_float(
                snapshot.get("net_asset_value")
                or snapshot.get("nav")
                or snapshot.get("total_value")
            )
            if nav_override is not None and nav_override > 0:
                nav_value = nav_override
            available_balance = _extract_available_usd(snapshot)

    timescale_adapter = TimescaleAdapter(account_id=account_id)
    feature_store = RedisFeastAdapter(account_id=account_id)
    sizer = PositionSizer(
        account_id,
        limits=limits,
        timescale=timescale_adapter,
        feature_store=feature_store,
        log_callback=_log_position_size,
    )

    try:
        result = await sizer.suggest_max_position(
            symbol,
            nav=nav_value,
            available_balance=available_balance,
        )
    except Exception as exc:  # pragma: no cover - defensive programming
        logger.exception(
            "Position sizing failed for account %s symbol %s", account_id, symbol
        )
        raise HTTPException(status_code=500, detail="Unable to compute position size") from exc

    payload = {
        "account_id": result.account_id,
        "symbol": result.symbol,
        "max_size_usd": result.max_size_usd,
        "size_units": result.size_units,
        "reason": result.reason,
        "nav": result.nav,
        "available_balance": result.available_balance,
        "volatility": result.volatility,
        "risk_budget": result.risk_budget,
        "base_size_usd": result.base_size_usd,
        "expected_edge_bps": result.expected_edge_bps,
        "fee_bps_estimate": result.fee_bps_estimate,
        "slippage_bps": result.slippage_bps,
        "safety_margin_bps": result.safety_margin_bps,
        "regime": result.regime,
        "price": result.price,
        "timestamp": result.timestamp,
        "diagnostics": result.diagnostics,
    }
    payload["max_position"] = result.max_size_usd  # Backwards compatibility
    logger.info(
        "Computed position size for account %s symbol %s: %.2f USD (reason=%s)",
        account_id,
        symbol,
        result.max_size_usd,
        result.reason,
    )
    return payload



@app.get("/risk/throttle/status")
async def get_throttle_status(
    account_id: str = Depends(require_admin_account),
) -> Dict[str, Optional[str]]:
    """Expose the current cost based throttling status for ``account_id``."""

    status = COST_THROTTLER.evaluate(account_id)
    return {"active": status.active, "reason": status.reason}



def _load_account_limits(account_id: str) -> AccountRiskLimit:
    stub_limits = _STUB_RISK_LIMITS.get(account_id)
    if stub_limits is not None:
        return AccountRiskLimit(**dict(stub_limits))

    with get_session() as session:
        limits = session.get(AccountRiskLimit, account_id)
        if not limits:
            raise ConfigError(f"No risk limits configured for account '{account_id}'.")
        return limits


def _load_sanction_hits(symbol: str) -> List[SanctionRecord]:
    """Return active sanction records for the provided symbol."""

    normalized_symbol = symbol.upper()
    if not SANCTIONS_SQLALCHEMY_AVAILABLE:
        return []
    with get_session() as session:
        stmt = select(SanctionRecord).where(SanctionRecord.symbol == normalized_symbol)
        records = session.execute(stmt).scalars().all()
    return [record for record in records if is_blocking_status(record.status)]


async def _get_approved_universe() -> UniverseSnapshot:
    """Fetch the approved trading universe from the dedicated service."""

    global _UNIVERSE_CACHE_SNAPSHOT, _UNIVERSE_CACHE_EXPIRY

    now = datetime.now(timezone.utc)
    async with _UNIVERSE_CACHE_LOCK:
        if (
            _UNIVERSE_CACHE_SNAPSHOT is not None
            and _UNIVERSE_CACHE_EXPIRY is not None
            and now < _UNIVERSE_CACHE_EXPIRY
        ):
            return _UNIVERSE_CACHE_SNAPSHOT

    url = f"{_UNIVERSE_SERVICE_URL.rstrip('/')}/universe/approved"
    try:
        async with httpx.AsyncClient(timeout=_UNIVERSE_SERVICE_TIMEOUT) as client:
            response = await client.get(url)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise UniverseServiceError(
            f"Universe service returned status {exc.response.status_code}"
        ) from exc
    except httpx.RequestError as exc:
        raise UniverseServiceError("Unable to reach universe service") from exc

    try:
        payload = response.json()
    except ValueError as exc:  # pragma: no cover - defensive
        raise UniverseServiceError("Universe service returned invalid JSON") from exc

    raw_symbols: List[str] = []
    for symbol in payload.get("symbols", []):
        normalized = normalize_spot_symbol(symbol)
        if normalized:
            raw_symbols.append(normalized)

    symbols = {symbol for symbol in raw_symbols if is_spot_symbol(symbol)}
    dropped = set(raw_symbols) - symbols
    if dropped:
        logger.warning(
            "Dropping non-spot instruments from approved universe", extra={"symbols": sorted(dropped)}
        )
    if not symbols:
        raise UniverseServiceError("Universe service did not return any spot instruments")

    generated_raw = payload.get("generated_at")
    generated_at = now
    if isinstance(generated_raw, str):
        try:
            generated_at = datetime.fromisoformat(generated_raw)
        except ValueError:
            generated_at = now
    elif isinstance(generated_raw, (int, float)):
        generated_at = datetime.fromtimestamp(float(generated_raw), tz=timezone.utc)

    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)

    thresholds = payload.get("thresholds") or {}
    snapshot = UniverseSnapshot(symbols=symbols, generated_at=generated_at, thresholds=thresholds)

    async with _UNIVERSE_CACHE_LOCK:
        _UNIVERSE_CACHE_SNAPSHOT = snapshot
        _UNIVERSE_CACHE_EXPIRY = datetime.now(timezone.utc) + timedelta(seconds=_UNIVERSE_CACHE_TTL)

    return snapshot


async def _evaluate(context: RiskEvaluationContext) -> RiskValidationResponse:
    reasons: List[str] = []
    suggested_quantities: List[float] = []
    cooldown_until: Optional[datetime] = None

    limits = context.limits
    state = context.request.portfolio_state
    await _refresh_usage_from_fills(context.request.account_id, state)
    intent = context.request.intent
    trade_notional = context.intended_notional
    normalized_instrument = normalize_spot_symbol(intent.instrument_id)
    base_price = float(intent.price)
    exit_take_profit, exit_stop_loss = compute_exit_targets(
        price=base_price,
        side=intent.side,
    )

    def _register_violation(
        message: str, *, cooldown: bool = False, details: Optional[Dict[str, object]] = None
    ) -> None:
        reasons.append(message)
        logger.warning(
            "Risk check failed for account=%s instrument=%s reason=%s",
            context.request.account_id,
            intent.instrument_id,
            message,
            extra={
                "event": "risk_limit_violation",
                "account_id": context.request.account_id,
                "instrument_id": intent.instrument_id,
                "reason": message,
                **(details or {}),
            },
        )
        if cooldown:
            nonlocal cooldown_until
            cooldown_until = cooldown_until or _determine_cooldown(limits)
        _audit_violation(context, message, details)

    if not is_spot_symbol(normalized_instrument):
        _register_violation(
            "Instrument not eligible for spot trading",
            cooldown=True,
            details={"instrument": normalized_instrument},
        )

    universe_snapshot: Optional[UniverseSnapshot]
    try:
        universe_snapshot = await _get_approved_universe()
    except UniverseServiceError as exc:
        _register_violation(
            "Unable to confirm instrument approval with universe service",
            cooldown=True,
            details={"error": str(exc)},
        )
        universe_snapshot = None
    else:
        if normalized_instrument not in universe_snapshot.symbols:
            _register_violation(
                "Instrument not approved by trading universe service",
                cooldown=True,
                details={
                    "instrument": normalized_instrument,
                    "universe_generated_at": universe_snapshot.generated_at.isoformat(),
                },
            )

    esg_allowed, esg_entry = esg_filter.evaluate(str(intent.instrument_id))
    if not esg_allowed:
        esg_details: Dict[str, object] = {"reason": ESG_REASON}
        if esg_entry is not None:
            esg_details["esg_flag"] = esg_entry.flag
            esg_details["esg_score"] = esg_entry.score
            if esg_entry.reason:
                esg_details["esg_note"] = esg_entry.reason
        _register_violation(
            "Instrument blocked by ESG compliance policy",
            cooldown=True,
            details=esg_details,
        )
        esg_filter.log_rejection(
            context.request.account_id,
            str(intent.instrument_id),
            esg_entry,
        )

    throttle_status = COST_THROTTLER.evaluate(context.request.account_id)
    if throttle_status.active:
        details = {
            "action": throttle_status.action,
            "cost_ratio": throttle_status.cost_ratio,
            "infra_cost": throttle_status.infra_cost,
            "recent_pnl": throttle_status.recent_pnl,
        }
        _register_violation(
            throttle_status.reason
            or "Account operations throttled due to infrastructure cost overruns",
            cooldown=True,
            details=details,
        )
        if 0.0 not in suggested_quantities:
            suggested_quantities.append(0.0)

    sizing_result = None
    try:
        sizer = PositionSizer(
            context.request.account_id,
            limits=limits,
            timescale=TimescaleAdapter(account_id=context.request.account_id),
            feature_store=RedisFeastAdapter(account_id=context.request.account_id),
            log_callback=_log_position_size,
        )
        nav_value = float(state.net_asset_value)
        exposure = float(state.notional_exposure or 0.0)
        available_balance = max(nav_value - exposure, 0.0)
        sizing_result = await sizer.suggest_max_position(
            normalized_instrument,
            nav=nav_value,
            available_balance=available_balance,
        )
    except Exception:  # pragma: no cover - defensive programming
        logger.exception(
            "Failed to compute position sizing for account %s instrument %s",
            context.request.account_id,
            normalized_instrument,
        )

    allocator_state = await _query_allocator_state(context.request.account_id)
    if allocator_state:
        _apply_allocator_guard(
            allocator_state,
            context,
            trade_notional,
            lambda message, cooldown=False, details=None: _register_violation(
                message, cooldown=cooldown, details=details
            ),
            suggested_quantities,
        )

    whitelist = limits.whitelist
    whitelist_set = {symbol.upper() for symbol in whitelist}
    if whitelist_set and normalized_instrument not in whitelist_set:
        _register_violation(
            "Instrument not whitelisted for account",
            cooldown=True,
            details={
                "instrument": normalized_instrument,
                "whitelist": sorted(whitelist_set),
            },
        )

    if sizing_result is not None and trade_notional > sizing_result.max_position:
        message = (
            "Trade notional exceeds volatility adjusted position cap: "
            f"trade={trade_notional:.2f} cap={sizing_result.max_position:.2f}"
        )
        _register_violation(
            message,
            details={
                "trade_notional": trade_notional,
                "position_cap": sizing_result.max_position,
                "volatility": sizing_result.volatility,
                "nav": sizing_result.nav,
            },
        )
        if (
            intent.side == "buy"
            and intent.notional_per_unit
            and sizing_result.max_position > 0.0
        ):
            suggested_quantities.append(
                sizing_result.max_position / float(intent.notional_per_unit)
            )

    # 1. Daily loss cap check
    if state.realized_daily_loss >= limits.max_daily_loss:
        _register_violation(
            "Daily loss limit exceeded: "
            f"loss={state.realized_daily_loss:.2f} limit={limits.max_daily_loss:.2f}",
            cooldown=True,
            details={"loss": state.realized_daily_loss, "limit": limits.max_daily_loss},
        )

    # 2. Fee budget check
    if state.fees_paid >= limits.fee_budget:
        _register_violation(
            "Fee budget exhausted: "
            f"fees={state.fees_paid:.2f} limit={limits.fee_budget:.2f}",
            cooldown=True,
            details={"fees": state.fees_paid, "limit": limits.fee_budget},
        )

    nav_value = float(state.net_asset_value)
    nav_pct_limit = float(limits.max_nav_pct_per_trade)
    nav_cap_for_trade = nav_pct_limit * nav_value

    scaled_notional_cap = None
    target_vol = getattr(limits, "target_volatility", None)
    atr_value: Optional[float] = None
    if target_vol:
        atr_value = _compute_atr(intent.instrument_id)
        if atr_value:
            base_size = nav_cap_for_trade or trade_notional
            current_vol = max(atr_value, 1e-8)
            scaled_notional_cap = base_size * float(target_vol) / current_vol
        elif nav_cap_for_trade:
            scaled_notional_cap = nav_cap_for_trade

    if atr_value is not None and atr_value > 0:
        exit_take_profit, exit_stop_loss = compute_exit_targets(
            price=base_price,
            side=intent.side,
            atr=atr_value,
        )

    volatility_metric: Optional[float] = None
    volatility_source: Optional[str] = None
    if atr_value is not None:
        volatility_metric = atr_value
        volatility_source = "atr"
    else:
        realized_vol = _compute_realized_volatility(state)
        if realized_vol is not None:
            volatility_metric = realized_vol
            volatility_source = "realized_vol"

    if volatility_metric is not None and volatility_source:
        battle_mode_controller.auto_update(
            context.request.account_id,
            volatility_metric,
            metric=volatility_source,
            reason=f"{volatility_source}={volatility_metric:.6f}",
        )

    battle_status = battle_mode_controller.status(context.request.account_id)
    if battle_status.engaged and not _is_position_reduction(intent, state, trade_notional):
        _register_violation(
            "Battle mode active: speculative trades are temporarily suspended",
            cooldown=True,
            details={
                "threshold": battle_status.threshold,
                "metric": battle_status.metric,
                "volatility": battle_status.volatility,
            },
        )
        suggested_quantities.append(0.0)

    # 3. Max NAV percentage per trade
    if trade_notional > nav_cap_for_trade:
        _register_violation(
            "Trade notional exceeds NAV percentage limit: "
            f"trade={trade_notional:.2f} limit={nav_cap_for_trade:.2f}",
            details={"trade_notional": trade_notional, "nav_cap": nav_cap_for_trade},
        )
        if intent.side == "buy":
            suggested_quantities.append(nav_cap_for_trade / intent.notional_per_unit)

    if scaled_notional_cap is not None and trade_notional > scaled_notional_cap:
        _register_violation(
            "Trade notional exceeds volatility adjusted cap: "
            f"trade={trade_notional:.2f} cap={scaled_notional_cap:.2f}",
            details={
                "trade_notional": trade_notional,
                "volatility_cap": scaled_notional_cap,
                "target_volatility": target_vol,
                "atr": atr_value,
            },
        )
        if intent.side == "buy" and scaled_notional_cap > 0:
            suggested_quantities.append(scaled_notional_cap / intent.notional_per_unit)

    # 4. Notional cap on projected exposure
    current_exposure = state.notional_exposure
    if intent.side == "buy":
        projected_exposure = current_exposure + trade_notional
    else:
        projected_exposure = max(current_exposure - trade_notional, 0.0)

    if projected_exposure > limits.notional_cap:
        _register_violation(
            "Projected notional exposure exceeds cap: "
            f"projected={projected_exposure:.2f} cap={limits.notional_cap:.2f}",
            details={"projected": projected_exposure, "cap": limits.notional_cap},
        )
        if intent.side == "buy":
            allowable_increment = max(limits.notional_cap - current_exposure, 0.0)
            if allowable_increment > 0:
                suggested_quantities.append(allowable_increment / intent.notional_per_unit)
            else:
                suggested_quantities.append(0.0)

    nav = state.net_asset_value
    if nav:
        fee_ratio = (state.fees_paid / nav) if nav else 0.0
        if fee_ratio > 0.0015:
            _register_violation(
                "Fee usage exceeds 0.15% of NAV",
                cooldown=True,
                details={"fees_paid": state.fees_paid, "nav": nav, "fee_ratio": fee_ratio},
            )

    historical_var = _compute_historical_var(context.request.account_id, nav)
    if historical_var is not None:
        state.var_95 = historical_var
        loss_threshold = nav * 0.05 if nav else None
        if loss_threshold is not None and historical_var > loss_threshold:
            _register_violation(
                "Historical VaR exceeds 5% of NAV",
                cooldown=True,
                details={
                    "historical_var": historical_var,
                    "nav": nav,
                    "threshold": loss_threshold,
                },
            )

    # 5. VaR checks
    if limits.var_95_limit is not None and state.var_95 is not None:
        if state.var_95 > limits.var_95_limit:
            _register_violation(
                "Portfolio VaR(95) exceeds limit: "
                f"var95={state.var_95:.2f} limit={limits.var_95_limit:.2f}",
                cooldown=True,
                details={"var95": state.var_95, "limit": limits.var_95_limit},
            )
    if limits.var_99_limit is not None and state.var_99 is not None:
        if state.var_99 > limits.var_99_limit:
            _register_violation(
                "Portfolio VaR(99) exceeds limit: "
                f"var99={state.var_99:.2f} limit={limits.var_99_limit:.2f}",
                cooldown=True,
                details={"var99": state.var_99, "limit": limits.var_99_limit},
            )

    correlation_threshold = getattr(limits, "correlation_threshold", None)
    correlation_matrix = _compute_correlation_matrix(
        state.historical_returns,
        getattr(limits, "correlation_lookback", None),
    )
    if (
        correlation_threshold is not None
        and intent.side == "buy"
        and correlation_matrix
    ):
        base_exposures = state.instrument_exposure or {}
        current_correlation = _weighted_portfolio_correlation(
            base_exposures,
            intent.instrument_id,
            correlation_matrix,
        )
        projected_exposures = _apply_trade_to_exposures(
            base_exposures,
            intent.instrument_id,
            trade_notional,
            intent.side,
        )
        projected_correlation = _weighted_portfolio_correlation(
            projected_exposures,
            intent.instrument_id,
            correlation_matrix,
        )
        if projected_correlation > correlation_threshold and projected_correlation > current_correlation:
            allowed_notional = _max_trade_notional_for_correlation(
                base_exposures,
                intent.instrument_id,
                trade_notional,
                intent.side,
                correlation_matrix,
                correlation_threshold,
            )
            correlation_details = {
                "current_correlation": current_correlation,
                "projected_correlation": projected_correlation,
                "threshold": correlation_threshold,
                "allowed_notional": allowed_notional,
            }
            if allowed_notional <= 0:
                _register_violation(
                    "Projected correlation breach: trade blocked",
                    details=correlation_details,
                )
                suggested_quantities.append(0.0)
            elif allowed_notional < trade_notional:
                suggested_quantities.append(allowed_notional / intent.notional_per_unit)
                _register_violation(
                    "Trade size reduced to maintain correlation threshold",
                    details=correlation_details,
                )

    cluster_limits = getattr(limits, "cluster_limits", {})
    if cluster_limits:
        instrument_cluster = state.instrument_clusters.get(intent.instrument_id)
        if instrument_cluster and instrument_cluster in cluster_limits:
            nav = float(state.net_asset_value) if state.net_asset_value else 0.0
            current_cluster_exposure = abs(state.cluster_exposure.get(instrument_cluster, 0.0))
            if intent.side == "buy":
                projected_cluster_exposure = current_cluster_exposure + trade_notional
            else:
                projected_cluster_exposure = max(current_cluster_exposure - trade_notional, 0.0)

            exposure_ratio = (projected_cluster_exposure / nav) if nav else 0.0
            limit_ratio = float(cluster_limits[instrument_cluster])
            if exposure_ratio > limit_ratio:
                diversification_details = {
                    "cluster": instrument_cluster,
                    "projected_exposure": projected_cluster_exposure,
                    "current_exposure": current_cluster_exposure,
                    "nav": nav,
                    "threshold_ratio": limit_ratio,
                    "projected_ratio": exposure_ratio,
                }
                if intent.side == "buy":
                    allowable_increment = max(limit_ratio * nav - current_cluster_exposure, 0.0)
                    if allowable_increment <= 0:
                        suggested_quantities.append(0.0)
                    else:
                        suggested_quantities.append(allowable_increment / intent.notional_per_unit)
                _register_violation(
                    "Diversification constraint breached for cluster",
                    details=diversification_details,
                )

    telemetry = _load_market_telemetry(intent.instrument_id)
    latency_seconds = _read_prometheus_latency(intent.instrument_id)
    if telemetry and limits.spread_threshold_bps is not None:
        spread_bps = telemetry.get("spread_bps")
        if spread_bps is not None and spread_bps > limits.spread_threshold_bps:
            _register_violation(
                "Market spread wider than allowed threshold",
                details={"spread_bps": spread_bps, "threshold": limits.spread_threshold_bps},
            )

    if limits.latency_stall_seconds is not None and latency_seconds is not None:
        if latency_seconds > limits.latency_stall_seconds:
            _register_violation(
                "Order routing latency above stall threshold",
                details={"latency": latency_seconds, "threshold": limits.latency_stall_seconds},
            )

    if limits.exchange_outage_block and telemetry:
        if telemetry.get("exchange_outage", 0):
            _register_violation(
                "Exchange outage flag active",
                cooldown=True,
                details={"exchange_outage": telemetry.get("exchange_outage")},
            )

    adjusted_quantity: Optional[float] = None
    if suggested_quantities:
        adjusted_quantity = round(min(q for q in suggested_quantities if q is not None), 8)

    passed = len(reasons) == 0
    return RiskValidationResponse(
        pass_=passed,
        reasons=reasons,
        adjusted_qty=adjusted_quantity,
        cooldown=cooldown_until,
        take_profit=exit_take_profit,
        stop_loss=exit_stop_loss,
    )


def _determine_cooldown(limits: AccountRiskLimit) -> datetime:
    minutes = getattr(limits, "cooldown_minutes", 0) or 0
    return datetime.utcnow() + timedelta(minutes=minutes)


def _audit_violation(
    context: RiskEvaluationContext, reason: str, details: Optional[Dict[str, object]] = None
) -> None:
    intent = context.request.intent
    state = context.request.portfolio_state
    payload = {
        "event": "risk_limit_violation",
        "account_id": context.request.account_id,
        "policy_id": intent.policy_id,
        "instrument_id": intent.instrument_id,
        "reason": reason,
        "current_notional": state.notional_exposure,
        "requested_notional": context.intended_notional,
    }
    if details:
        payload.update(details)
    audit_logger.warning(
        "risk_limit_violation account=%s policy=%s instrument=%s reason=%s",
        context.request.account_id,
        intent.policy_id,
        intent.instrument_id,
        reason,
        extra=payload,
    )


def _audit_failure(context: RiskEvaluationContext, decision: RiskValidationResponse) -> None:
    intent = context.request.intent
    state = context.request.portfolio_state
    audit_logger.warning(
        "risk_validation_failed account=%s policy=%s instrument=%s reasons=%s",
        context.request.account_id,
        intent.policy_id,
        intent.instrument_id,
        decision.reasons,
        extra={
            "event": "risk_validation_failed",
            "account_id": context.request.account_id,
            "policy_id": intent.policy_id,
            "instrument_id": intent.instrument_id,
            "reasons": decision.reasons,
            "current_notional": state.notional_exposure,
            "requested_notional": context.intended_notional,
            "cooldown": decision.cooldown,
        },
    )


def set_stub_limits(records: Iterable[AccountRiskLimit]) -> None:

    """Utility used in testing to override the in-memory limits table."""

    _STUB_RISK_LIMITS.clear()
    for record in records:
        cluster_limits = getattr(record, "cluster_limits", {}) or {}
        cluster_payload = json.dumps(cluster_limits) if cluster_limits else None
        _STUB_RISK_LIMITS[record.account_id] = {
            "account_id": record.account_id,
            "max_daily_loss": record.max_daily_loss,
            "fee_budget": record.fee_budget,
            "max_nav_pct_per_trade": record.max_nav_pct_per_trade,
            "notional_cap": record.notional_cap,
            "cooldown_minutes": record.cooldown_minutes,
            "instrument_whitelist": ",".join(record.whitelist),
            "var_95_limit": record.var_95_limit,
            "var_99_limit": record.var_99_limit,
            "spread_threshold_bps": record.spread_threshold_bps,
            "latency_stall_seconds": record.latency_stall_seconds,
            "exchange_outage_block": 1 if record.exchange_outage_block else 0,
            "correlation_threshold": getattr(record, "correlation_threshold", None),
            "correlation_lookback": getattr(record, "correlation_lookback", None),
            "diversification_cluster_limits": cluster_payload,
        }



def _load_account_usage(account_id: str) -> AccountUsage:
    with get_session() as session:
        record = session.get(AccountRiskUsage, account_id)

    if record is None:
        return AccountUsage(
            account_id=account_id,
            realized_daily_loss=DECIMAL_ZERO,
            fees_paid=DECIMAL_ZERO,
            net_asset_value=DECIMAL_ZERO,
            var_95=None,
            var_99=None,
        )

    return AccountUsage(
        account_id=account_id,
        realized_daily_loss=_as_decimal(record.realized_daily_loss),
        fees_paid=_as_decimal(record.fees_paid),
        net_asset_value=_as_decimal(record.net_asset_value),
        var_95=_maybe_decimal(record.var_95),
        var_99=_maybe_decimal(record.var_99),
    )


def _load_market_telemetry(instrument_id: str) -> Optional[Dict[str, float]]:
    telemetry = _STUB_MARKET_TELEMETRY.get(instrument_id)
    if telemetry is None:
        logger.debug("No market telemetry found for instrument %s", instrument_id)
    return telemetry


def _read_prometheus_latency(instrument_id: str) -> Optional[float]:
    key = f"latency_seconds:{instrument_id}"
    if key in _STUB_PROM_METRICS:
        return _STUB_PROM_METRICS[key]
    return _STUB_PROM_METRICS.get("latency_seconds")


def _parse_cluster_limits(raw_value: Optional[Any]) -> Dict[str, float]:
    if not raw_value:
        return {}
    if isinstance(raw_value, dict):
        parsed: Dict[str, float] = {}
        for key, value in raw_value.items():
            try:
                parsed[str(key)] = float(value)
            except (TypeError, ValueError):
                logger.debug("Skipping invalid cluster limit entry", extra={"cluster": key, "value": value})
        return parsed
    if isinstance(raw_value, str):
        try:
            data = json.loads(raw_value)
        except (json.JSONDecodeError, TypeError, ValueError):
            logger.warning("Failed to parse diversification cluster limits", extra={"value": raw_value})
            return {}
        return _parse_cluster_limits(data)
    logger.debug("Unsupported cluster limit format", extra={"value": raw_value})
    return {}


def _compute_correlation_matrix(
    return_series: Dict[str, List[float]], window: Optional[int]
) -> Dict[str, Dict[str, float]]:
    instruments = sorted(return_series)
    matrix: Dict[str, Dict[str, float]] = {}
    if not instruments:
        return matrix

    for index, instrument in enumerate(instruments):
        series_i = list(return_series.get(instrument, []))
        if window and window > 0:
            series_i = series_i[-window:]
        if len(series_i) < 2:
            continue
        matrix.setdefault(instrument, {})[instrument] = 1.0
        for other in instruments[index + 1 :]:
            series_j = list(return_series.get(other, []))
            if window and window > 0:
                series_j = series_j[-window:]
            corr = _pearson_correlation(series_i, series_j)
            if corr is None:
                continue
            matrix.setdefault(instrument, {})[other] = corr
            matrix.setdefault(other, {})[instrument] = corr

    return matrix


def _pearson_correlation(series_a: List[float], series_b: List[float]) -> Optional[float]:
    if not series_a or not series_b:
        return None
    length = min(len(series_a), len(series_b))
    if length < 2:
        return None
    trimmed_a = series_a[-length:]
    trimmed_b = series_b[-length:]
    mean_a = sum(trimmed_a) / length
    mean_b = sum(trimmed_b) / length
    numerator = sum((a - mean_a) * (b - mean_b) for a, b in zip(trimmed_a, trimmed_b))
    denom_a = math.sqrt(sum((a - mean_a) ** 2 for a in trimmed_a))
    denom_b = math.sqrt(sum((b - mean_b) ** 2 for b in trimmed_b))
    if denom_a == 0 or denom_b == 0:
        return 0.0
    return numerator / (denom_a * denom_b)


def _lookup_correlation(
    matrix: Dict[str, Dict[str, float]], instrument_a: str, instrument_b: str
) -> Optional[float]:
    if instrument_a in matrix and instrument_b in matrix[instrument_a]:
        return matrix[instrument_a][instrument_b]
    if instrument_b in matrix and instrument_a in matrix[instrument_b]:
        return matrix[instrument_b][instrument_a]
    return None


def _apply_trade_to_exposures(
    exposures: Dict[str, float], instrument: str, trade_notional: float, side: str
) -> Dict[str, float]:
    updated = {key: abs(float(value)) for key, value in exposures.items() if abs(float(value)) > 0}
    current = updated.get(instrument, 0.0)
    if side == "buy":
        updated[instrument] = current + trade_notional
    else:
        updated[instrument] = max(current - trade_notional, 0.0)
        if updated[instrument] <= 0:
            updated.pop(instrument, None)
    return updated


def _weighted_portfolio_correlation(
    exposures: Dict[str, float], instrument: str, matrix: Dict[str, Dict[str, float]]
) -> float:
    if not exposures:
        return 0.0
    weighted_correlations: List[float] = []
    weights: List[float] = []
    for other, exposure in exposures.items():
        if other == instrument:
            continue
        weight = abs(float(exposure))
        if weight <= 0:
            continue
        correlation = _lookup_correlation(matrix, instrument, other)
        if correlation is None:
            continue
        weights.append(weight)
        weighted_correlations.append(weight * correlation)
    if not weights:
        return 0.0
    total_weight = sum(weights)
    if total_weight <= 0:
        return 0.0
    return sum(weighted_correlations) / total_weight


def _max_trade_notional_for_correlation(
    exposures: Dict[str, float],
    instrument: str,
    requested_notional: float,
    side: str,
    matrix: Dict[str, Dict[str, float]],
    threshold: float,
) -> float:
    if requested_notional <= 0 or side != "buy":
        return requested_notional

    low = 0.0
    high = requested_notional
    allowed = 0.0
    for _ in range(25):
        mid = (low + high) / 2.0
        projected = _apply_trade_to_exposures(exposures, instrument, mid, side)
        correlation = _weighted_portfolio_correlation(projected, instrument, matrix)
        if correlation <= threshold or math.isclose(correlation, threshold, rel_tol=1e-5, abs_tol=1e-5):
            allowed = mid
            low = mid
        else:
            high = mid
    return max(0.0, min(allowed, requested_notional))


def set_stub_account_usage(
    account_id: str, usage: Mapping[str, Union[Decimal, float, int, str]]
) -> None:
    with get_session() as session:
        record = session.get(AccountRiskUsage, account_id)
        if record is None:
            record = AccountRiskUsage(account_id=account_id)
        record.realized_daily_loss = _as_decimal(usage.get("realized_daily_loss"))
        record.fees_paid = _as_decimal(usage.get("fees_paid"))
        record.net_asset_value = _as_decimal(usage.get("net_asset_value"))
        record.var_95 = _maybe_decimal(usage.get("var_95"))
        record.var_99 = _maybe_decimal(usage.get("var_99"))
        session.add(record)

    stored = _STUB_ACCOUNT_USAGE.setdefault(account_id, {})
    stored["realized_daily_loss"] = _as_decimal(usage.get("realized_daily_loss"))
    stored["fees_paid"] = _as_decimal(usage.get("fees_paid"))
    stored["net_asset_value"] = _as_decimal(usage.get("net_asset_value"))
    stored["var_95"] = _maybe_decimal(usage.get("var_95"))
    stored["var_99"] = _maybe_decimal(usage.get("var_99"))


def set_stub_market_telemetry(instrument_id: str, telemetry: Dict[str, float]) -> None:
    _STUB_MARKET_TELEMETRY[instrument_id] = telemetry


def set_stub_prometheus_metric(name: str, value: float, instrument_id: Optional[str] = None) -> None:
    if instrument_id:
        key = f"{name}:{instrument_id}"
        _STUB_PROM_METRICS[key] = value
    else:
        _STUB_PROM_METRICS[name] = value


def _compute_atr(instrument_id: str, window: int = 14) -> Optional[float]:
    history = _STUB_PRICE_HISTORY.get(instrument_id)
    if not history or len(history) < window + 1:
        return None

    recent = history[-(window + 1) :]
    true_ranges: List[float] = []
    prev_close: Optional[float] = None
    for bar in recent:
        high = bar.get("high")
        low = bar.get("low")
        close = bar.get("close")
        if high is None or low is None or close is None:
            continue
        if prev_close is None:
            tr = float(high) - float(low)
        else:
            tr = max(
                float(high) - float(low),
                abs(float(high) - prev_close),
                abs(float(low) - prev_close),
            )
        true_ranges.append(tr)
        prev_close = float(close)

    if len(true_ranges) < window:
        return None

    return mean(true_ranges[-window:])


def _compute_realized_volatility(state: AccountPortfolioState, window: int = 30) -> Optional[float]:
    returns: List[float] = []
    for series in (state.historical_returns or {}).values():
        if not series:
            continue
        tail = series[-window:]
        for value in tail:
            if value is None:
                continue
            returns.append(float(value))

    if len(returns) < 2:
        return None

    mean_return = sum(returns) / len(returns)
    variance = sum((value - mean_return) ** 2 for value in returns) / (len(returns) - 1)
    if variance < 0:
        return 0.0
    return math.sqrt(variance)


def _is_position_reduction(
    intent: TradeIntent, state: AccountPortfolioState, trade_notional: float
) -> bool:
    current_total = float(state.notional_exposure or 0.0)
    tolerance = max(current_total * 1e-6, 1e-6)
    if intent.side == "sell":
        projected_total = max(current_total - trade_notional, 0.0)
    else:
        projected_total = current_total + trade_notional

    if projected_total + tolerance < current_total:
        return True

    instrument_exposure = (state.instrument_exposure or {}).get(intent.instrument_id, 0.0)
    if intent.side == "sell" and instrument_exposure > 0:
        return True

    return False


def _compute_historical_var(account_id: str, nav: float, window: int = 250) -> Optional[float]:
    pnl_history = _STUB_ACCOUNT_RETURNS.get(account_id)
    if not pnl_history or len(pnl_history) < window:
        return None

    sample = pnl_history[-window:]
    sorted_pnl = sorted(sample)
    index = max(int(0.05 * len(sorted_pnl)) - 1, 0)
    loss_quantile = sorted_pnl[index]
    projected_loss = max(-loss_quantile, 0.0)

    if nav:
        projected_loss = min(projected_loss, float(nav))

    return projected_loss


async def _load_recent_fills(account_id: str) -> List[Dict[str, object]]:
    today = datetime.utcnow().date()
    fallback: List[Dict[str, object]] = []
    for record in _STUB_FILLS:
        if record.get("account_id") != account_id:
            continue
        timestamp = record.get("timestamp")
        if isinstance(timestamp, str):
            try:
                timestamp_dt = datetime.fromisoformat(timestamp)
            except ValueError:
                continue
        elif isinstance(timestamp, datetime):
            timestamp_dt = timestamp
        else:
            continue
        if timestamp_dt.date() == today:
            fallback.append(record)

    if not EXCHANGE_ADAPTER.supports("get_trades"):
        return fallback

    try:
        trades = await EXCHANGE_ADAPTER.get_trades(account_id=account_id, limit=100)
    except NotImplementedError:
        return fallback
    except Exception as exc:  # pragma: no cover - defensive guard for optional integrations
        _handle_exchange_adapter_failure(account_id, "get_trades", exc)
        return fallback

    normalized: List[Dict[str, object]] = []
    for entry in trades:
        if not isinstance(entry, Mapping):
            continue
        trade_account = entry.get("account_id")
        if trade_account is not None and str(trade_account) != account_id:
            continue
        normalized.append(dict(entry))

    return normalized or fallback


async def _refresh_usage_from_fills(account_id: str, state: AccountPortfolioState) -> None:
    fills = await _load_recent_fills(account_id)
    if not fills:
        return

    total_pnl = DECIMAL_ZERO
    total_fees = DECIMAL_ZERO
    for record in fills:
        pnl = _as_decimal(record.get("pnl"))
        fee = _as_decimal(record.get("fee"))
        total_pnl += pnl
        total_fees += fee

    realized_loss = max(-total_pnl, DECIMAL_ZERO)
    state.realized_daily_loss = float(realized_loss)
    state.fees_paid = float(total_fees)
    usage = _STUB_ACCOUNT_USAGE.setdefault(account_id, {})
    usage["realized_daily_loss"] = realized_loss
    usage["fees_paid"] = total_fees


def _handle_exchange_adapter_failure(account_id: str, operation: str, exc: Exception) -> None:
    logger.error(
        "Exchange adapter %s operation %s failed for account %s: %s",
        EXCHANGE_ADAPTER.name,
        operation,
        account_id,
        exc,
        exc_info=exc,
    )
    try:
        push_exchange_adapter_failure(
            adapter=EXCHANGE_ADAPTER.name,
            account_id=account_id,
            operation=operation,
            error=str(exc),
        )
    except Exception:  # pragma: no cover - best effort alerting
        logger.debug("Failed to emit exchange adapter alert", exc_info=True)


async def _refresh_usage_from_balance(account_id: str) -> None:
    if not EXCHANGE_ADAPTER.supports("get_balance"):
        return

    try:
        snapshot = await EXCHANGE_ADAPTER.get_balance(account_id)
    except NotImplementedError:
        return
    except Exception as exc:  # pragma: no cover - defensive guard for optional integrations
        _handle_exchange_adapter_failure(account_id, "get_balance", exc)
        return

    if not isinstance(snapshot, Mapping):
        return

    nav_value = snapshot.get("net_asset_value") or snapshot.get("nav") or snapshot.get("total_value")
    nav = _coerce_float(nav_value)
    if nav is None:
        return

    nav_decimal = max(_as_decimal(nav), DECIMAL_ZERO)

    with get_session() as session:
        record = session.get(AccountRiskUsage, account_id)
        if record is None:
            record = AccountRiskUsage(account_id=account_id)
        record.net_asset_value = nav_decimal
        record.updated_at = datetime.now(timezone.utc)
        session.add(record)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_available_usd(snapshot: Mapping[str, Any]) -> Optional[float]:
    balances = snapshot.get("balances") if isinstance(snapshot, Mapping) else None
    candidates: List[float] = []
    if isinstance(balances, Mapping):
        for key in ("USD", "ZUSD", "USDC", "USDT"):
            candidate = _coerce_float(balances.get(key)) if key in balances else None
            if candidate is not None:
                candidates.append(candidate)
    if candidates:
        return max(candidates)

    fallback: Any = None
    if isinstance(snapshot, Mapping):
        fallback = snapshot.get("available_balance")
        if fallback is None:
            fallback = snapshot.get("available_cash")
    return _coerce_float(fallback)


def set_stub_price_history(instrument_id: str, history: List[Dict[str, float]]) -> None:
    _STUB_PRICE_HISTORY[instrument_id] = history


def set_stub_account_returns(account_id: str, pnl_history: List[float]) -> None:
    _STUB_ACCOUNT_RETURNS[account_id] = pnl_history


def set_stub_fills(records: List[Dict[str, object]]) -> None:
    _STUB_FILLS.clear()
    _STUB_FILLS.extend(records)
async def _query_allocator_state(account_id: str) -> Optional[AllocatorAccountState]:
    url = os.getenv("CAPITAL_ALLOCATOR_URL", _CAPITAL_ALLOCATOR_URL)
    if not url:
        return None

    endpoint = url.rstrip("/") + "/allocator/status"
    try:
        async with httpx.AsyncClient(timeout=_CAPITAL_ALLOCATOR_TIMEOUT) as client:
            response = await client.get(endpoint)
            response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("Capital allocator query failed for account %s: %s", account_id, exc)
        return None

    try:
        payload = response.json()
    except ValueError as exc:  # pragma: no cover - defensive guard
        logger.warning("Allocator response is not valid JSON: %s", exc)
        return None

    total_nav = float(payload.get("total_nav") or 0.0)
    accounts = payload.get("accounts", [])
    for entry in accounts:
        if str(entry.get("account_id")) != account_id:
            continue

        def _float(value: object, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        allocation_pct = _float(entry.get("allocation_pct", entry.get("allocation")))
        allocated_nav = entry.get("allocated_nav")
        allocated_value = _float(allocated_nav, allocation_pct * total_nav)
        drawdown_ratio = _float(entry.get("drawdown_ratio"))
        throttled = bool(entry.get("throttled"))
        return AllocatorAccountState(
            account_id=account_id,
            allocation_pct=allocation_pct,
            allocated_nav=allocated_value,
            drawdown_ratio=drawdown_ratio,
            throttled=throttled,
        )
    return None


def _apply_allocator_guard(
    allocator_state: AllocatorAccountState,
    context: RiskEvaluationContext,
    trade_notional: float,
    register_violation_with_context: Callable[[str, bool, Optional[Dict[str, object]]], None],
    suggested_quantities: List[float],
) -> None:
    state = context.request.portfolio_state
    intent = context.request.intent
    current_nav = float(state.net_asset_value or 0.0)
    trade_amount = float(trade_notional or 0.0)
    tolerance = max(_CAPITAL_ALLOCATOR_TOLERANCE, 0.0)

    if allocator_state.throttled and intent.side == "buy":
        register_violation_with_context(
            "Account trading disabled by capital allocator",  # message
            True,
            {
                "allocation_pct": allocator_state.allocation_pct,
                "drawdown_ratio": allocator_state.drawdown_ratio,
            },
        )
        suggested_quantities.append(0.0)
        return

    allocation_nav = max(allocator_state.allocated_nav, 0.0)
    if allocation_nav <= 0:
        return

    projected_nav = current_nav
    if intent.side == "buy":
        projected_nav += trade_amount
    else:
        projected_nav = max(current_nav - trade_amount, 0.0)

    allowed_nav = allocation_nav * (1.0 + tolerance)
    if projected_nav <= allowed_nav:
        return

    register_violation_with_context(
        "Trade would exceed capital allocation limit",
        False,
        {
            "projected_nav": projected_nav,
            "allocation_nav": allocation_nav,
            "allocation_pct": allocator_state.allocation_pct,
        },
    )

    if intent.side == "buy":
        headroom = max(allowed_nav - current_nav, 0.0)
        if headroom > 0 and intent.notional_per_unit > 0:
            suggested_quantities.append(headroom / intent.notional_per_unit)
        else:
            suggested_quantities.append(0.0)
