"""FastAPI microservice exposing account level risk validation."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from fastapi import FastAPI, HTTPException
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

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            "AccountRiskLimit("
            f"account_id={self.account_id!r}, "
            f"max_daily_loss={self.max_daily_loss}, "
            f"fee_budget={self.fee_budget}, "
            f"max_nav_pct_per_trade={self.max_nav_pct_per_trade}, "
            f"notional_cap={self.notional_cap}, "
            f"cooldown_minutes={self.cooldown_minutes}"
            ")"
        )


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
    },
    "ACC-AGGR": {
        "account_id": "ACC-AGGR",
        "max_daily_loss": 150_000.0,
        "fee_budget": 25_000.0,
        "max_nav_pct_per_trade": 0.4,
        "notional_cap": 5_000_000.0,
        "cooldown_minutes": 60,
    },
}


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
    intent = context.request.intent

    # 1. Daily loss cap check
    if state.realized_daily_loss >= limits.max_daily_loss:
        reasons.append(
            "Daily loss limit exceeded: "
            f"loss={state.realized_daily_loss:.2f} limit={limits.max_daily_loss:.2f}"
        )
        cooldown_until = _determine_cooldown(limits)

    # 2. Fee budget check
    if state.fees_paid >= limits.fee_budget:
        reasons.append(
            "Fee budget exhausted: "
            f"fees={state.fees_paid:.2f} limit={limits.fee_budget:.2f}"
        )
        cooldown_until = cooldown_until or _determine_cooldown(limits)

    trade_notional = context.intended_notional
    nav_cap_for_trade = limits.max_nav_pct_per_trade * state.net_asset_value

    # 3. Max NAV percentage per trade
    if trade_notional > nav_cap_for_trade:
        reasons.append(
            "Trade notional exceeds NAV percentage limit: "
            f"trade={trade_notional:.2f} limit={nav_cap_for_trade:.2f}"
        )
        if intent.side == "buy":
            suggested_quantities.append(nav_cap_for_trade / intent.notional_per_unit)

    # 4. Notional cap on projected exposure
    current_exposure = state.notional_exposure
    if intent.side == "buy":
        projected_exposure = current_exposure + trade_notional
    else:
        projected_exposure = max(current_exposure - trade_notional, 0.0)

    if projected_exposure > limits.notional_cap:
        reasons.append(
            "Projected notional exposure exceeds cap: "
            f"projected={projected_exposure:.2f} cap={limits.notional_cap:.2f}"
        )
        if intent.side == "buy":
            allowable_increment = max(limits.notional_cap - current_exposure, 0.0)
            if allowable_increment > 0:
                suggested_quantities.append(allowable_increment / intent.notional_per_unit)
            else:
                suggested_quantities.append(0.0)

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
        }
