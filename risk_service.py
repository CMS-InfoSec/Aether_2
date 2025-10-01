"""FastAPI microservice exposing account level risk validation."""

from __future__ import annotations

import json
import logging
import math
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from fastapi import Body, Depends, FastAPI, HTTPException, status
try:  # pragma: no cover - support Pydantic v2 compatibility mode
    from pydantic.v1 import BaseModel, Field, PositiveFloat, ValidationError, root_validator
except ImportError:  # pragma: no cover - fallback for Pydantic v1
    from pydantic import BaseModel, Field, PositiveFloat, ValidationError, root_validator
try:  # pragma: no cover - optional dependency for lightweight testing
    from sqlalchemy import Column, Float, Integer, String
    from sqlalchemy.orm import declarative_base
except ModuleNotFoundError:  # pragma: no cover
    class Column:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    Float = float  # type: ignore[assignment]
    Integer = int  # type: ignore[assignment]
    String = str  # type: ignore[assignment]

    def declarative_base():  # type: ignore[override]
        class Base:  # minimal stub
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)

        return Base

from metrics import (
    increment_trade_rejection,
    record_fees_nav_pct,
    setup_metrics,
)
from services.common.security import require_admin_account


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
        "correlation_threshold": 0.85,
        "correlation_lookback": 20,
        "diversification_cluster_limits": json.dumps({"BTC": 0.4}),
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
        "correlation_threshold": 0.9,
        "correlation_lookback": 30,
        "diversification_cluster_limits": json.dumps({"ENERGY": 0.5}),
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

    class Config:
        orm_mode = True


class RiskLimitsResponse(BaseModel):
    account_id: str
    limits: AccountRiskLimitModel
    usage: AccountUsage


app = FastAPI(title="Risk Validation Service", version="1.0.0")
setup_metrics(app)


@app.post("/risk/validate")
async def validate_risk(
    payload: Dict[str, Any] = Body(...),
    account_id: str = Depends(require_admin_account),
) -> Dict[str, Any]:
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
        logger.exception("Unable to load risk limits for account %s", account_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

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


def _evaluate(context: RiskEvaluationContext) -> RiskValidationResponse:
    reasons: List[str] = []
    suggested_quantities: List[float] = []
    cooldown_until: Optional[datetime] = None

    limits = context.limits
    state = context.request.portfolio_state
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

    # 3. Max NAV percentage per trade
    if trade_notional > nav_cap_for_trade:
        _register_violation(
            "Trade notional exceeds NAV percentage limit: "
            f"trade={trade_notional:.2f} limit={nav_cap_for_trade:.2f}",
            details={"trade_notional": trade_notional, "nav_cap": nav_cap_for_trade},
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
    _STUB_ACCOUNT_USAGE[account_id] = usage


def set_stub_market_telemetry(instrument_id: str, telemetry: Dict[str, float]) -> None:
    _STUB_MARKET_TELEMETRY[instrument_id] = telemetry


def set_stub_prometheus_metric(name: str, value: float, instrument_id: Optional[str] = None) -> None:
    if instrument_id:
        key = f"{name}:{instrument_id}"
        _STUB_PROM_METRICS[key] = value
    else:
        _STUB_PROM_METRICS[name] = value
