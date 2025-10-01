"""FastAPI service exposing risk validation logic."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, PositiveFloat, root_validator

from config import AccountRiskLimits, ConfigError, get_account_limits

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Risk Validation Service", version="1.0.0")


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
    account_portfolio_state: AccountPortfolioState


class RiskValidationResponse(BaseModel):
    """Decision returned by the risk service."""

    pass_: bool = Field(
        ..., alias="pass", description="Indicates whether the trade was approved"
    )
    reasons: List[str] = Field(default_factory=list, description="Detailed failure reasons, if any")
    adjusted_qty: Optional[float] = Field(
        None,
        description="Suggested adjusted quantity when the full request cannot be honored",
    )
    cooldown_until: Optional[datetime] = Field(
        None,
        description="Timestamp until which trading is halted due to hard limits",
    )

    class Config:
        allow_population_by_field_name = True


class RiskEvaluationContext(BaseModel):
    request: RiskValidationRequest
    limits: AccountRiskLimits

    @property
    def current_notional(self) -> float:
        return self.request.account_portfolio_state.notional_exposure

    @property
    def intended_notional(self) -> float:
        return float(self.request.intent.notional)


@app.post("/risk/validate", response_model=RiskValidationResponse)
async def validate_risk(request: RiskValidationRequest) -> RiskValidationResponse:
    """Validate a trading intent against account level risk limits."""

    logger.info("Received risk validation request for account %s", request.account_id)

    try:
        limits = get_account_limits(request.account_id)
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
    return decision


def _evaluate(context: RiskEvaluationContext) -> RiskValidationResponse:
    reasons: List[str] = []
    adjusted_quantity: Optional[float] = None
    cooldown_until: Optional[datetime] = None

    limits = context.limits
    state = context.request.account_portfolio_state
    intent = context.request.intent

    # 1. Daily loss cap check
    if state.realized_daily_loss >= limits.daily_loss_limit:
        reasons.append(
            "Daily loss limit exceeded: "
            f"loss={state.realized_daily_loss:.2f} limit={limits.daily_loss_limit:.2f}"
        )
        cooldown_until = _determine_cooldown(limits)

    # 2. Fee budget check
    if state.fees_paid >= limits.fee_budget:
        reasons.append(
            "Fee budget exhausted: "
            f"fees={state.fees_paid:.2f} limit={limits.fee_budget:.2f}"
        )
        cooldown_until = cooldown_until or _determine_cooldown(limits)

    intended_notional = context.intended_notional
    current_exposure = state.notional_exposure
    requested_delta = intended_notional if intent.side == "buy" else -intended_notional
    projected_exposure = max(current_exposure + requested_delta, 0.0)
    nav_cap = limits.max_nav_pct * state.net_asset_value
    notional_cap = limits.notional_cap

    breached_limits: List[str] = []
    if projected_exposure > nav_cap:
        breached_limits.append(f"nav_cap={nav_cap:.2f}")
    if projected_exposure > notional_cap:
        breached_limits.append(f"notional_cap={notional_cap:.2f}")

    if breached_limits:
        limits_clause = " ".join(breached_limits)
        reasons.append(
            "Projected notional exposure exceeds limits: "
            f"projected={projected_exposure:.2f} current={current_exposure:.2f} {limits_clause}"
        )
        if intent.side == "buy":
            allowable_target = min(nav_cap, notional_cap)
            allowable_increment = max(allowable_target - current_exposure, 0.0)
            if allowable_increment > 0 and intent.price > 0:
                adjusted_quantity = round(
                    allowable_increment / intent.notional_per_unit,
                    8,
                )
            else:
                adjusted_quantity = 0.0

    passed = len(reasons) == 0
    return RiskValidationResponse(
        pass_=passed,
        reasons=reasons,
        adjusted_qty=adjusted_quantity,
        cooldown_until=cooldown_until,
    )


def _determine_cooldown(limits: AccountRiskLimits) -> datetime:
    return datetime.utcnow() + timedelta(minutes=limits.cooldown_minutes)
