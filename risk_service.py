"""FastAPI microservice exposing account level risk validation."""

from __future__ import annotations

import json
import logging
import os

import math

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta

from typing import Callable, Dict, Iterable, Iterator, List, Optional


import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, PositiveFloat, model_validator
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool


from metrics import (
    increment_trade_rejection,
    record_fees_nav_pct,
    setup_metrics,
)
from services.common.compliance import (
    SanctionRecord,
    ensure_sanctions_schema,
    is_blocking_status,
)
from services.common.security import require_admin_account


logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("risk.audit")
logging.basicConfig(level=logging.INFO)


_CAPITAL_ALLOCATOR_URL = os.getenv("CAPITAL_ALLOCATOR_URL")
_CAPITAL_ALLOCATOR_TIMEOUT = float(os.getenv("CAPITAL_ALLOCATOR_TIMEOUT", "1.5"))
_CAPITAL_ALLOCATOR_TOLERANCE = float(os.getenv("CAPITAL_ALLOCATOR_NAV_TOLERANCE", "0.02"))


Base = declarative_base()


class AccountRiskUsage(Base):
    """Persisted account level usage metrics backing the service responses."""

    __tablename__ = "account_risk_usage"

    account_id = Column(String, primary_key=True)
    realized_daily_loss = Column(Float, nullable=False, default=0.0)
    fees_paid = Column(Float, nullable=False, default=0.0)
    net_asset_value = Column(Float, nullable=False, default=0.0)
    var_95 = Column(Float, nullable=True)
    var_99 = Column(Float, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)


class ConfigError(RuntimeError):
    """Raised when account specific configuration cannot be loaded."""


class AccountRiskLimit(Base):
    """SQLAlchemy representation of account risk configuration."""

    __tablename__ = "account_risk_limits"

    account_id = Column(String, primary_key=True)
    max_daily_loss = Column(Float, nullable=False)
    fee_budget = Column(Float, nullable=False)
    max_nav_pct_per_trade = Column(Float, nullable=False)
    notional_cap = Column(Float, nullable=False)
    cooldown_minutes = Column(Integer, nullable=False, default=0)
    instrument_whitelist = Column(String, nullable=True)
    var_95_limit = Column(Float, nullable=True)
    var_99_limit = Column(Float, nullable=True)
    spread_threshold_bps = Column(Float, nullable=True)
    latency_stall_seconds = Column(Float, nullable=True)
    exchange_outage_block = Column(Integer, nullable=False, default=0)

    correlation_threshold = Column(Float, nullable=True)
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
        return [token.strip() for token in self.instrument_whitelist.split(",") if token.strip()]

    @property
    def cluster_limits(self) -> Dict[str, float]:
        return _parse_cluster_limits(self.diversification_cluster_limits)


DEFAULT_DATABASE_URL = "sqlite:///./risk.db"


@dataclass
class AllocatorAccountState:
    account_id: str
    allocation_pct: float
    allocated_nav: float
    drawdown_ratio: float
    throttled: bool


def _database_url() -> str:
    url = os.getenv("RISK_DATABASE_URL") or os.getenv("TIMESCALE_DSN") or os.getenv(
        "DATABASE_URL", DEFAULT_DATABASE_URL
    )
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def _engine_options(url: str) -> Dict[str, object]:
    options: Dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url == "sqlite:///:memory:" or url.endswith(":memory:"):
            options["poolclass"] = StaticPool
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
        whitelist = payload.get("instrument_whitelist", [])
        whitelist_blob = ",".join(sorted(whitelist)) if whitelist else None
        if record is None:
            record = AccountRiskLimit(
                account_id=payload["account_id"],
                max_daily_loss=float(payload["max_daily_loss"]),
                fee_budget=float(payload["fee_budget"]),
                max_nav_pct_per_trade=float(payload["max_nav_pct_per_trade"]),
                notional_cap=float(payload["notional_cap"]),
                cooldown_minutes=int(payload["cooldown_minutes"]),
                instrument_whitelist=whitelist_blob,
                var_95_limit=float(payload["var_95_limit"])
                if payload.get("var_95_limit") is not None
                else None,
                var_99_limit=float(payload["var_99_limit"])
                if payload.get("var_99_limit") is not None
                else None,
                spread_threshold_bps=float(payload["spread_threshold_bps"])
                if payload.get("spread_threshold_bps") is not None
                else None,
                latency_stall_seconds=float(payload["latency_stall_seconds"])
                if payload.get("latency_stall_seconds") is not None
                else None,
                exchange_outage_block=int(payload["exchange_outage_block"]),
            )
            session.add(record)
        else:
            record.max_daily_loss = float(payload["max_daily_loss"])
            record.fee_budget = float(payload["fee_budget"])
            record.max_nav_pct_per_trade = float(payload["max_nav_pct_per_trade"])
            record.notional_cap = float(payload["notional_cap"])
            record.cooldown_minutes = int(payload["cooldown_minutes"])
            record.instrument_whitelist = whitelist_blob
            record.var_95_limit = (
                float(payload["var_95_limit"])
                if payload.get("var_95_limit") is not None
                else None
            )
            record.var_99_limit = (
                float(payload["var_99_limit"])
                if payload.get("var_99_limit") is not None
                else None
            )
            record.spread_threshold_bps = (
                float(payload["spread_threshold_bps"])
                if payload.get("spread_threshold_bps") is not None
                else None
            )
            record.latency_stall_seconds = (
                float(payload["latency_stall_seconds"])
                if payload.get("latency_stall_seconds") is not None
                else None
            )
            record.exchange_outage_block = int(payload["exchange_outage_block"])


def _bootstrap_storage() -> None:
    Base.metadata.create_all(bind=ENGINE)
    ensure_sanctions_schema(ENGINE)
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


_STUB_MARKET_TELEMETRY: Dict[str, Dict[str, float]] = {
    "AAPL": {
        "spread_bps": 5.0,
        "latency_seconds": 0.8,
        "exchange_outage": 0,
    }
}


_STUB_PROM_METRICS: Dict[str, float] = {
    "latency_seconds": 0.8,
}


_STUB_PRICE_HISTORY: Dict[str, List[Dict[str, float]]] = {
    "AAPL": [
        {"high": 178.5, "low": 174.4, "close": 176.3},
        {"high": 179.1, "low": 175.2, "close": 178.7},
        {"high": 181.2, "low": 176.8, "close": 180.3},
        {"high": 180.9, "low": 177.6, "close": 179.4},
        {"high": 182.4, "low": 178.9, "close": 181.7},
        {"high": 183.2, "low": 179.7, "close": 182.1},
        {"high": 184.0, "low": 180.1, "close": 183.5},
        {"high": 185.6, "low": 181.4, "close": 184.2},
        {"high": 186.2, "low": 182.7, "close": 185.3},
        {"high": 187.3, "low": 183.5, "close": 186.8},
        {"high": 188.8, "low": 184.6, "close": 187.1},
        {"high": 189.5, "low": 185.2, "close": 188.6},
        {"high": 190.7, "low": 186.1, "close": 189.4},
        {"high": 191.2, "low": 187.5, "close": 190.8},
        {"high": 192.3, "low": 188.4, "close": 191.5},
    ]
}


_STUB_ACCOUNT_RETURNS: Dict[str, List[float]] = {
    "ACC-DEFAULT": [-1500.0 + (i % 5) * 100 for i in range(260)],
    "ACC-AGGR": [-4500.0 + (i % 7) * 250 for i in range(260)],
}


_STUB_FILLS: List[Dict[str, object]] = [
    {
        "account_id": "ACC-DEFAULT",
        "timestamp": datetime.utcnow().isoformat(),
        "pnl": -2500.0,
        "fee": 125.0,
    }
]


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

    class Config:
        arbitrary_types_allowed = True


class AccountUsage(BaseModel):
    account_id: str
    realized_daily_loss: float = Field(..., ge=0.0)
    fees_paid: float = Field(..., ge=0.0)
    net_asset_value: float = Field(..., ge=0.0)
    var_95: Optional[float] = Field(None, ge=0.0)
    var_99: Optional[float] = Field(None, ge=0.0)


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


app = FastAPI(title="Risk Validation Service", version="1.0.0")
setup_metrics(app)



@app.on_event("startup")
def _on_startup() -> None:
    _bootstrap_storage()


_bootstrap_storage()


@app.post("/risk/validate", response_model=RiskValidationResponse)
async def validate_risk(request: RiskValidationRequest) -> RiskValidationResponse:

    """Validate a trading intent against account level risk limits."""

    try:
        request = RiskValidationRequest.parse_obj(payload)
    except ValidationError as exc:  # pragma: no cover - FastAPI handles validation
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    if request.account_id != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )

    logger.info("Received risk validation request for account %s", account_id)

    try:
        limits = _load_account_limits(account_id)
    except ConfigError as exc:

        logger.exception("Unable to load risk limits for account %s", request.account_id)
        raise HTTPException(status_code=404, detail=str(exc)) from exc


    context = RiskEvaluationContext(request=request, limits=limits)

    try:
        decision = _evaluate(context)
    except Exception as exc:  # pragma: no cover - defensive programming
        logger.exception("Risk evaluation failed for account %s", account_id)
        raise HTTPException(status_code=500, detail="Internal risk evaluation failure") from exc

    logger.info(
        "Risk evaluation completed for account %s: passed=%s",
        account_id,
        decision.pass_,
    )
    symbol = str(context.request.intent.instrument_id)

    if not decision.pass_:
        _audit_failure(context, decision)
        increment_trade_rejection(
            account_id,
            symbol,
        )

    state = context.request.portfolio_state
    nav = float(state.net_asset_value) if state.net_asset_value else 0.0
    fees = float(state.fees_paid) if state.fees_paid else 0.0
    fees_pct = (fees / nav * 100.0) if nav else 0.0
    record_fees_nav_pct(
        account_id,
        symbol,
        fees_pct,
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

    usage = _load_account_usage(account_id)
    limit_model = AccountRiskLimitModel(
        account_id=limits.account_id,
        max_daily_loss=limits.max_daily_loss,
        fee_budget=limits.fee_budget,
        max_nav_pct_per_trade=limits.max_nav_pct_per_trade,
        notional_cap=limits.notional_cap,
        cooldown_minutes=limits.cooldown_minutes,
        instrument_whitelist=limits.whitelist,
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


def _load_account_limits(account_id: str) -> AccountRiskLimit:
    with get_session() as session:
        limits = session.get(AccountRiskLimit, account_id)
        if not limits:
            raise ConfigError(f"No risk limits configured for account '{account_id}'.")
        return limits


def _load_sanction_hits(symbol: str) -> List[SanctionRecord]:
    """Return active sanction records for the provided symbol."""

    normalized_symbol = symbol.upper()
    with get_session() as session:
        stmt = select(SanctionRecord).where(SanctionRecord.symbol == normalized_symbol)
        records = session.execute(stmt).scalars().all()
    return [record for record in records if is_blocking_status(record.status)]


def _evaluate(context: RiskEvaluationContext) -> RiskValidationResponse:
    reasons: List[str] = []
    suggested_quantities: List[float] = []
    cooldown_until: Optional[datetime] = None

    limits = context.limits
    state = context.request.portfolio_state
    _refresh_usage_from_fills(context.request.account_id, state)
    intent = context.request.intent
    trade_notional = context.intended_notional

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

    sanction_hits = _load_sanction_hits(intent.instrument_id)
    if sanction_hits:
        sources = sorted({record.source for record in sanction_hits})
        statuses = sorted({record.status.lower() for record in sanction_hits})
        _register_violation(
            "Instrument is present on compliance sanctions list",
            cooldown=True,
            details={"sources": sources, "statuses": statuses},
        )
        return RiskValidationResponse(
            pass_=False,
            reasons=reasons,
            adjusted_qty=0.0,
            cooldown=cooldown_until,
        )

    allocator_state = _query_allocator_state(context.request.account_id)
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
    if whitelist and intent.instrument_id not in whitelist:
        _register_violation(
            "Instrument not whitelisted for account",
            cooldown=True,
            details={"instrument": intent.instrument_id, "whitelist": whitelist},
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

    nav_cap_for_trade = limits.max_nav_pct_per_trade * state.net_asset_value

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
            realized_daily_loss=0.0,
            fees_paid=0.0,
            net_asset_value=0.0,
            var_95=None,
            var_99=None,
        )

    return AccountUsage(
        account_id=account_id,
        realized_daily_loss=float(record.realized_daily_loss or 0.0),
        fees_paid=float(record.fees_paid or 0.0),
        net_asset_value=float(record.net_asset_value or 0.0),
        var_95=float(record.var_95) if record.var_95 is not None else None,
        var_99=float(record.var_99) if record.var_99 is not None else None,
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


def set_stub_account_usage(account_id: str, usage: Dict[str, float]) -> None:
    with get_session() as session:
        record = session.get(AccountRiskUsage, account_id)
        if record is None:
            record = AccountRiskUsage(account_id=account_id)
        record.realized_daily_loss = float(usage.get("realized_daily_loss", 0.0) or 0.0)
        record.fees_paid = float(usage.get("fees_paid", 0.0) or 0.0)
        record.net_asset_value = float(usage.get("net_asset_value", 0.0) or 0.0)
        record.var_95 = float(usage["var_95"]) if usage.get("var_95") is not None else None
        record.var_99 = float(usage["var_99"]) if usage.get("var_99") is not None else None
        session.add(record)


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


def _load_recent_fills(account_id: str) -> List[Dict[str, object]]:
    today = datetime.utcnow().date()
    fills: List[Dict[str, object]] = []
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
            fills.append(record)
    return fills


def _refresh_usage_from_fills(account_id: str, state: AccountPortfolioState) -> None:
    fills = _load_recent_fills(account_id)
    if not fills:
        return

    total_pnl = 0.0
    total_fees = 0.0
    for record in fills:
        pnl = float(record.get("pnl", 0.0))
        fee = float(record.get("fee", 0.0))
        total_pnl += pnl
        total_fees += fee

    realized_loss = max(-total_pnl, 0.0)
    state.realized_daily_loss = realized_loss
    state.fees_paid = total_fees
    usage = _STUB_ACCOUNT_USAGE.setdefault(account_id, {})
    usage["realized_daily_loss"] = realized_loss
    usage["fees_paid"] = total_fees


def set_stub_price_history(instrument_id: str, history: List[Dict[str, float]]) -> None:
    _STUB_PRICE_HISTORY[instrument_id] = history


def set_stub_account_returns(account_id: str, pnl_history: List[float]) -> None:
    _STUB_ACCOUNT_RETURNS[account_id] = pnl_history


def set_stub_fills(records: List[Dict[str, object]]) -> None:
    _STUB_FILLS.clear()
    _STUB_FILLS.extend(records)
def _query_allocator_state(account_id: str) -> Optional[AllocatorAccountState]:
    url = os.getenv("CAPITAL_ALLOCATOR_URL", _CAPITAL_ALLOCATOR_URL)
    if not url:
        return None

    endpoint = url.rstrip("/") + "/allocator/status"
    try:
        with httpx.Client(timeout=_CAPITAL_ALLOCATOR_TIMEOUT) as client:
            response = client.get(endpoint)
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
