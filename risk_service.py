"""FastAPI microservice exposing account level risk validation."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
from statistics import mean
from typing import Dict, Iterable, List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, PositiveFloat, root_validator
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.orm import declarative_base

from metrics import (
    increment_trade_rejection,
    record_fees_nav_pct,
    setup_metrics,
)


logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("risk.audit")
logging.basicConfig(level=logging.INFO)


Base = declarative_base()


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
    target_volatility = Column(Float, nullable=True)

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
            f"target_volatility={self.target_volatility}"
            ")"
        )

    @property
    def whitelist(self) -> List[str]:
        if not self.instrument_whitelist:
            return []
        return [token.strip() for token in self.instrument_whitelist.split(",") if token.strip()]


class StubSession:
    """Minimal stand-in for a SQLAlchemy session used in tests."""

    def __enter__(self) -> "StubSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def get(self, model: Base, pk: str) -> Optional[AccountRiskLimit]:
        row = _STUB_RISK_LIMITS.get(pk)
        if not row:
            return None
        return model(**row)


@contextmanager
def get_session():
    yield StubSession()


_STUB_RISK_LIMITS: Dict[str, Dict[str, float]] = {
    "ACC-DEFAULT": {
        "account_id": "ACC-DEFAULT",
        "max_daily_loss": 50_000.0,
        "fee_budget": 10_000.0,
        "max_nav_pct_per_trade": 0.25,
        "notional_cap": 1_000_000.0,
        "cooldown_minutes": 120,
        "instrument_whitelist": "AAPL,MSFT,GOOG",
        "var_95_limit": 35_000.0,
        "var_99_limit": 65_000.0,
        "spread_threshold_bps": 15.0,
        "latency_stall_seconds": 2.5,
        "exchange_outage_block": 1,
        "target_volatility": 3.0,
    },
    "ACC-AGGR": {
        "account_id": "ACC-AGGR",
        "max_daily_loss": 150_000.0,
        "fee_budget": 25_000.0,
        "max_nav_pct_per_trade": 0.4,
        "notional_cap": 5_000_000.0,
        "cooldown_minutes": 60,
        "instrument_whitelist": "ES_F,CL_F",
        "var_95_limit": 100_000.0,
        "var_99_limit": 180_000.0,
        "spread_threshold_bps": 10.0,
        "latency_stall_seconds": 1.5,
        "exchange_outage_block": 0,
        "target_volatility": 4.5,
    },
}


_STUB_ACCOUNT_USAGE: Dict[str, Dict[str, float]] = {
    "ACC-DEFAULT": {
        "realized_daily_loss": 15_000.0,
        "fees_paid": 4_000.0,
        "net_asset_value": 350_000.0,
        "var_95": 30_000.0,
        "var_99": 55_000.0,
    }
}


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
    side: str = Field(..., regex="^(buy|sell)$", description="Trade direction")
    quantity: PositiveFloat = Field(..., description="Requested trade quantity")
    price: PositiveFloat = Field(..., description="Reference price used for risk checks")
    notional: Optional[PositiveFloat] = Field(
        None, description="Explicit notional value, overrides quantity * price when provided"
    )

    @root_validator
    def compute_notional(cls, values):
        quantity = values.get("quantity")
        price = values.get("price")
        notional = values.get("notional")
        if notional is None and quantity is not None and price is not None:
            values["notional"] = quantity * price
        return values

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

    class Config:
        allow_population_by_field_name = True


class RiskEvaluationContext(BaseModel):
    request: RiskValidationRequest
    limits: AccountRiskLimit

    @property
    def current_notional(self) -> float:
        return self.request.portfolio_state.notional_exposure

    @property
    def intended_notional(self) -> float:
        return float(self.request.intent.notional)


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
    target_volatility: Optional[float] = None

    class Config:
        orm_mode = True


class RiskLimitsResponse(BaseModel):
    account_id: str
    limits: AccountRiskLimitModel
    usage: AccountUsage


app = FastAPI(title="Risk Validation Service", version="1.0.0")
setup_metrics(app)


@app.post("/risk/validate", response_model=RiskValidationResponse)
async def validate_risk(request: RiskValidationRequest) -> RiskValidationResponse:
    """Validate a trading intent against account level risk limits."""

    logger.info("Received risk validation request for account %s", request.account_id)

    try:
        limits = _load_account_limits(request.account_id)
    except ConfigError as exc:
        logger.exception("Unable to load risk limits for account %s", request.account_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    context = RiskEvaluationContext(request=request, limits=limits)

    try:
        decision = _evaluate(context)
    except Exception as exc:  # pragma: no cover - defensive programming
        logger.exception("Risk evaluation failed for account %s", request.account_id)
        raise HTTPException(status_code=500, detail="Internal risk evaluation failure") from exc

    logger.info(
        "Risk evaluation completed for account %s: passed=%s",
        request.account_id,
        decision.pass_,
    )
    symbol = str(context.request.intent.instrument_id)

    if not decision.pass_:
        _audit_failure(context, decision)
        increment_trade_rejection(
            request.account_id,
            symbol,
        )

    state = context.request.portfolio_state
    nav = float(state.net_asset_value) if state.net_asset_value else 0.0
    fees = float(state.fees_paid) if state.fees_paid else 0.0
    fees_pct = (fees / nav * 100.0) if nav else 0.0
    record_fees_nav_pct(
        request.account_id,
        symbol,
        fees_pct,
    )

    return decision


@app.get("/risk/limits", response_model=RiskLimitsResponse)
async def get_risk_limits(account_id: str = Query(..., description="Account identifier")) -> RiskLimitsResponse:
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
        target_volatility=limits.target_volatility,
    )

    response = RiskLimitsResponse(account_id=account_id, limits=limit_model, usage=usage)
    logger.info("Retrieved risk limits for account %s", account_id)
    return response


def _load_account_limits(account_id: str) -> AccountRiskLimit:
    with get_session() as session:
        limits = session.get(AccountRiskLimit, account_id)
        if not limits:
            raise ConfigError(f"No risk limits configured for account '{account_id}'.")
        return limits


def _evaluate(context: RiskEvaluationContext) -> RiskValidationResponse:
    reasons: List[str] = []
    suggested_quantities: List[float] = []
    cooldown_until: Optional[datetime] = None

    limits = context.limits
    state = context.request.portfolio_state
    _refresh_usage_from_fills(context.request.account_id, state)
    intent = context.request.intent

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

    trade_notional = context.intended_notional
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
            "target_volatility": record.target_volatility,
        }


def _load_account_usage(account_id: str) -> AccountUsage:
    usage = _STUB_ACCOUNT_USAGE.get(account_id, {})
    nav = usage.get("net_asset_value", 0.0)
    if nav <= 0:
        nav = _STUB_ACCOUNT_USAGE.setdefault(account_id, {}).get("net_asset_value", 0.0)
    return AccountUsage(
        account_id=account_id,
        realized_daily_loss=usage.get("realized_daily_loss", 0.0),
        fees_paid=usage.get("fees_paid", 0.0),
        net_asset_value=nav,
        var_95=usage.get("var_95"),
        var_99=usage.get("var_99"),
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


def set_stub_account_usage(account_id: str, usage: Dict[str, float]) -> None:
    _STUB_ACCOUNT_USAGE[account_id] = usage


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
