
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


from pydantic import BaseModel, Field, model_serializer, model_validator

from fastapi import HTTPException, status



GTD_EXPIRE_TIME_REQUIRED = "expire_time must be provided when time_in_force is GTD."


class FeeDetail(BaseModel):
    """Detailed fee information anchored to a tier lookup."""

    bps: float = Field(..., ge=0.0, description="Fee rate expressed in basis points")
    usd: float = Field(..., ge=0.0, description="Fee amount converted to USD")
    tier_id: str = Field(..., description="Identifier of the matched fee tier")
    basis_ts: datetime = Field(..., description="Timestamp associated with the fee tier basis")


class FeeBreakdown(BaseModel):
    """Represents fees attributed to a trading action."""

    currency: str = Field(..., description="Fee currency")
    maker: float = Field(..., ge=0.0, description="Maker fee amount")
    taker: float = Field(..., ge=0.0, description="Taker fee amount")
    maker_detail: Optional[FeeDetail] = Field(
        None,
        description=(
            "Extended maker fee information including tier, basis timestamp, and USD conversion."
        ),
    )
    taker_detail: Optional[FeeDetail] = Field(
        None,
        description=(
            "Extended taker fee information including tier, basis timestamp, and USD conversion."
        ),
    )

    @model_serializer(mode="wrap")
    def _serialize(self, handler):  # type: ignore[override]
        payload = handler(self)
        return {key: value for key, value in payload.items() if value is not None}


class BookSnapshot(BaseModel):
    """Simplified representation of the order book at decision time."""

    mid_price: float = Field(..., gt=0.0, description="Current mid price")
    spread_bps: float = Field(..., ge=0.0, description="Bid/ask spread in basis points")
    imbalance: float = Field(
        ..., ge=-1.0, le=1.0, description="Normalized order book imbalance"
    )


class PolicyState(BaseModel):
    """Operational state used when evaluating policy decisions."""

    regime: str = Field(..., description="Current market regime label")
    volatility: float = Field(..., ge=0.0, description="Annualized volatility estimate")
    liquidity_score: float = Field(
        ..., ge=0.0, le=1.0, description="Normalized liquidity score"
    )
    conviction: float = Field(
        ..., ge=0.0, le=1.0, description="Model conviction in the current signal"
    )


class ConfidenceMetrics(BaseModel):
    """Confidence metrics aligned with the Soloing spec."""

    model_confidence: float = Field(..., ge=0.0, le=1.0)
    state_confidence: float = Field(..., ge=0.0, le=1.0)
    execution_confidence: float = Field(..., ge=0.0, le=1.0)
    overall_confidence: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Composite confidence used for gating"
    )

    @model_validator(mode="after")
    def _default_overall(self) -> "ConfidenceMetrics":  # type: ignore[override]
        if self.overall_confidence is None:
            composite = (
                self.model_confidence
                + self.state_confidence
                + self.execution_confidence
            ) / 3.0
            self.overall_confidence = round(composite, 4)
        return self


class ActionTemplate(BaseModel):
    """Represents a candidate execution template for the policy engine."""

    name: str = Field(..., description="Template identifier")
    venue_type: str = Field(..., description="maker/taker or other venue qualifier")
    edge_bps: float = Field(..., description="Fee-adjusted edge in basis points")
    fee_bps: float = Field(..., ge=0.0, description="Fee cost in basis points")
    confidence: float = Field(
        ..., ge=0.0, le=1.0, description="Confidence that this template will execute"
    )


class PolicyDecisionRequest(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    order_id: str = Field(..., description="Client order identifier")
    instrument: str = Field(..., description="Instrument identifier")
    side: str = Field(..., pattern="^(BUY|SELL)$", description="Order side")
    quantity: float = Field(..., gt=0.0, description="Order quantity")
    price: float = Field(..., gt=0.0, description="Limit price")
    fee: FeeBreakdown = Field(..., description="Applicable fees for the order")
    features: List[float] = Field(
        default_factory=list,
        description="Feature vector supplied by the caller for model scoring",
    )
    book_snapshot: Optional[BookSnapshot] = Field(
        None, description="Optional caller-provided book snapshot"
    )
    state: Optional[PolicyState] = Field(
        None, description="Optional caller-provided state overrides"
    )
    expected_edge_bps: Optional[float] = Field(
        None,
        description="Caller expected edge in basis points prior to policy adjustments",
    )
    slippage_bps: Optional[float] = Field(
        None,
        ge=0.0,
        description="Expected slippage cost in basis points applied during execution",
    )
    take_profit_bps: Optional[float] = Field(
        None, description="Target take-profit distance in basis points"
    )
    stop_loss_bps: Optional[float] = Field(
        None, description="Target stop-loss distance in basis points"
    )
    confidence: Optional[ConfidenceMetrics] = Field(
        None, description="Caller confidence metrics aligned with Soloing spec"
    )


class PolicyDecisionResponse(BaseModel):
    approved: bool = Field(..., description="Whether the order is approved")
    reason: Optional[str] = Field(None, description="Optional rejection reason")
    effective_fee: FeeBreakdown = Field(..., description="Effective fees considered")
    expected_edge_bps: float = Field(..., description="Model-computed edge before fees")
    fee_adjusted_edge_bps: float = Field(..., description="Edge after deducting fees")
    selected_action: str = Field(..., description="Chosen execution template")
    action_templates: List[ActionTemplate] = Field(
        ..., description="Full set of evaluated execution templates"
    )
    confidence: ConfidenceMetrics = Field(
        ..., description="Final confidence metrics driving the decision"
    )
    features: List[float] = Field(..., description="Feature vector used for evaluation")
    book_snapshot: BookSnapshot = Field(..., description="Snapshot used during evaluation")
    state: PolicyState = Field(..., description="State context used during evaluation")
    take_profit_bps: float = Field(..., description="Configured take-profit distance")
    stop_loss_bps: float = Field(..., description="Configured stop-loss distance")


class RiskIntentMetrics(BaseModel):
    """Risk metrics produced by the policy engine for a specific intent."""

    net_exposure: float = Field(..., description="Projected net exposure after applying the intent")
    gross_notional: float = Field(..., ge=0.0, description="Gross notional value for the intent")
    projected_loss: float = Field(..., ge=0.0, description="Projected incremental loss for the trading day")
    projected_fee: float = Field(..., ge=0.0, description="Projected incremental fees for the trading day")
    var_95: float = Field(..., ge=0.0, description="Projected 95% VaR for the intent")
    spread_bps: float = Field(..., ge=0.0, description="Expected spread in basis points")
    latency_ms: float = Field(..., ge=0.0, description="Observed decision latency in milliseconds")


class PolicyDecisionPayload(BaseModel):
    """Container for the policy decision request/response pair."""

    request: PolicyDecisionRequest = Field(..., description="Original policy decision request")
    response: Optional[PolicyDecisionResponse] = Field(
        None, description="Optional policy decision response produced by the policy engine"
    )

    def resolved_fee(self) -> FeeBreakdown:
        if self.response is not None:
            return self.response.effective_fee
        return self.request.fee


class RiskIntentPayload(BaseModel):
    """Full policy intent envelope supplied to the risk service."""

    policy_decision: PolicyDecisionPayload = Field(
        ..., description="Policy decision request/response context for the intent"
    )
    metrics: RiskIntentMetrics = Field(..., description="Risk-related metrics for the intent")
    book_snapshot: Optional[BookSnapshot] = Field(
        None, description="Book snapshot aligned with the policy evaluation"
    )
    state: Optional[PolicyState] = Field(
        None, description="Policy state context aligned with the decision"
    )
    confidence: Optional[ConfidenceMetrics] = Field(
        None, description="Confidence metrics carried from the policy engine"
    )

    @model_validator(mode="after")
    def _populate_defaults(self) -> "RiskIntentPayload":  # type: ignore[override]
        if self.book_snapshot is None:
            if self.policy_decision.response is not None:
                self.book_snapshot = self.policy_decision.response.book_snapshot
            elif self.policy_decision.request.book_snapshot is not None:
                self.book_snapshot = self.policy_decision.request.book_snapshot

        if self.state is None:
            if self.policy_decision.response is not None:
                self.state = self.policy_decision.response.state
            elif self.policy_decision.request.state is not None:
                self.state = self.policy_decision.request.state

        if self.confidence is None and self.policy_decision.response is not None:
            self.confidence = self.policy_decision.response.confidence

        return self


class PortfolioState(BaseModel):
    """Snapshot of the portfolio used to contextualise risk checks."""

    nav: float = Field(..., gt=0.0, description="Total net asset value for the account")
    loss_to_date: float = Field(..., ge=0.0, description="Realised losses accrued today")
    fee_to_date: float = Field(..., ge=0.0, description="Fees accrued today")
    instrument_exposure: Dict[str, float] = Field(
        default_factory=dict,
        description="Existing gross exposure by instrument prior to this intent",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional additional portfolio metadata for auditing",
    )


class RiskValidationRequest(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    intent: RiskIntentPayload = Field(..., description="Policy intent and metrics for validation")
    portfolio_state: PortfolioState = Field(..., description="Current portfolio state summary")

    instrument: Optional[str] = Field(
        None, description="Instrument identifier for the intent"
    )
    net_exposure: Optional[float] = Field(
        None, description="Net exposure after order"
    )
    gross_notional: Optional[float] = Field(
        None, ge=0.0, description="Gross notional value of the order"
    )
    projected_loss: Optional[float] = Field(
        None, ge=0.0, description="Projected incremental loss for the trading day"
    )
    projected_fee: Optional[float] = Field(
        None, ge=0.0, description="Projected incremental fees for the trading day"
    )
    var_95: Optional[float] = Field(
        None, ge=0.0, description="Projected 95% VaR for the intent"
    )
    spread_bps: Optional[float] = Field(
        None, ge=0.0, description="Expected spread in basis points"
    )
    latency_ms: Optional[float] = Field(
        None, ge=0.0, description="Observed order latency in milliseconds"
    )
    fee: Optional[FeeBreakdown] = Field(
        None, description="Fees associated with the order"
    )

    @model_validator(mode="after")
    def _populate_summaries(self) -> "RiskValidationRequest":  # type: ignore[override]
        decision = self.intent.policy_decision
        metrics = self.intent.metrics

        if self.instrument is None:
            self.instrument = decision.request.instrument

        scalar_defaults = {
            "net_exposure": metrics.net_exposure,
            "gross_notional": metrics.gross_notional,
            "projected_loss": metrics.projected_loss,
            "projected_fee": metrics.projected_fee,
            "var_95": metrics.var_95,
            "spread_bps": metrics.spread_bps,
            "latency_ms": metrics.latency_ms,
        }

        for field_name, default_value in scalar_defaults.items():
            if getattr(self, field_name) is None:
                setattr(self, field_name, default_value)

        if self.fee is None:
            self.fee = decision.resolved_fee()

        missing = [
            name
            for name in ["instrument", *scalar_defaults.keys(), "fee"]
            if getattr(self, name) is None
        ]
        if missing:
            raise ValueError(
                "Missing required scalar summaries derived from intent: "
                + ", ".join(sorted(missing))
            )

        return self


class RiskValidationResponse(BaseModel):
    valid: bool = Field(..., description="Whether the order passes risk checks")
    reasons: List[str] = Field(default_factory=list, description="Reasons for any failure")
    fee: FeeBreakdown = Field(..., description="Fees applied in validation")


class OrderPlacementRequest(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    order_id: str = Field(..., description="Order identifier")
    instrument: str = Field(..., description="Instrument identifier")
    side: str = Field(..., pattern="^(BUY|SELL)$", description="Order side")
    quantity: float = Field(..., gt=0.0, description="Order quantity")
    price: float = Field(..., gt=0.0, description="Limit price")
    fee: FeeBreakdown = Field(..., description="Fees applied at placement")
    post_only: bool | None = Field(
        default=None,
        description="Whether the order should avoid taking liquidity",
    )
    reduce_only: bool | None = Field(
        default=None,
        description="Only reduce an existing position",
    )
    time_in_force: str | None = Field(
        default=None,
        pattern="^(GTC|IOC|GTD)$",
        description="Time in force constraint",
    )
    expire_time: datetime | None = Field(
        default=None,

        description="UTC expiration timestamp required when time in force is GTD",

    )
    take_profit: float | None = Field(
        default=None,
        gt=0.0,
        description="Take profit trigger price",
    )

    @model_validator(mode="after")
    def _validate_expire_time(self) -> "OrderPlacementRequest":  # type: ignore[override]
        if self.expire_time is not None:
            if self.expire_time.tzinfo is None:
                raise ValueError("expire_time must include timezone information.")
            self.expire_time = self.expire_time.astimezone(timezone.utc)
        if self.time_in_force == "GTD" and self.expire_time is None:
            raise ValueError(GTD_EXPIRE_TIME_REQUIRED)
        return self
    stop_loss: float | None = Field(
        default=None,
        gt=0.0,
        description="Stop loss trigger price",
    )

    @model_validator(mode="after")
    def _validate_expire_time(self) -> "OrderPlacementRequest":  # type: ignore[override]
        if self.time_in_force == "GTD":
            if self.expire_time is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="expire_time is required when time_in_force is GTD",
                )
            if self.expire_time.tzinfo is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="expire_time must include timezone information",
                )
            self.expire_time = self.expire_time.astimezone(timezone.utc)
        return self


class OrderPlacementResponse(BaseModel):
    accepted: bool = Field(..., description="Whether the order was accepted by OMS")
    routed_venue: str = Field(..., description="Venue to which the order is routed")
    fee: FeeBreakdown = Field(..., description="Final fees for the order")


class FeeScheduleResponse(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    effective_from: datetime = Field(..., description="Timestamp when the fee schedule takes effect")
    fee: FeeBreakdown = Field(..., description="Fee structure")


class ApprovedUniverseResponse(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    instruments: List[str] = Field(..., description="List of approved instruments")
    fee_overrides: Dict[str, FeeBreakdown] = Field(
        default_factory=dict,
        description="Per-instrument fee overrides",
    )


class KrakenCredentialRequest(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    api_key: str = Field(..., description="Kraken API key material")
    api_secret: str = Field(..., description="Kraken API secret material")


class KrakenCredentialResponse(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    secret_name: str = Field(..., description="Kubernetes secret storing credentials")
    created_at: datetime = Field(..., description="First time the secret was written")
    rotated_at: datetime = Field(..., description="Most recent rotation timestamp")


class KrakenSecretStatusResponse(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    secret_name: str = Field(..., description="Kubernetes secret storing credentials")
    created_at: datetime = Field(..., description="First time the secret was written")
    rotated_at: datetime = Field(..., description="Most recent rotation timestamp")

