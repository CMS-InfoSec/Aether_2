
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional


from pydantic import BaseModel, Field, model_validator



class FeeBreakdown(BaseModel):
    """Represents fees attributed to a trading action."""

    currency: str = Field(..., description="Fee currency")
    maker: float = Field(..., ge=0.0, description="Maker fee amount")
    taker: float = Field(..., ge=0.0, description="Taker fee amount")


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


class RiskValidationRequest(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    instrument: str = Field(..., description="Instrument identifier for the intent")
    net_exposure: float = Field(..., description="Net exposure after order")
    gross_notional: float = Field(..., ge=0.0, description="Gross notional value of the order")
    projected_loss: float = Field(..., ge=0.0, description="Projected incremental loss for the trading day")
    projected_fee: float = Field(..., ge=0.0, description="Projected incremental fees for the trading day")
    var_95: float = Field(..., ge=0.0, description="Projected 95% VaR for the intent")
    spread_bps: float = Field(..., ge=0.0, description="Expected spread in basis points")
    latency_ms: float = Field(..., ge=0.0, description="Observed order latency in milliseconds")
    fee: FeeBreakdown = Field(..., description="Fees associated with the order")


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
    take_profit: float | None = Field(
        default=None,
        gt=0.0,
        description="Take profit trigger price",
    )
    stop_loss: float | None = Field(
        default=None,
        gt=0.0,
        description="Stop loss trigger price",
    )


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

