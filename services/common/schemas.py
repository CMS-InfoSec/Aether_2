
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional


from pydantic import BaseModel, Field



class FeeBreakdown(BaseModel):
    """Represents fees attributed to a trading action."""

    currency: str = Field(..., description="Fee currency")
    maker: float = Field(..., ge=0.0, description="Maker fee amount")
    taker: float = Field(..., ge=0.0, description="Taker fee amount")


class PolicyDecisionRequest(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    order_id: str = Field(..., description="Client order identifier")
    instrument: str = Field(..., description="Instrument identifier")
    side: str = Field(..., pattern="^(BUY|SELL)$", description="Order side")
    quantity: float = Field(..., gt=0.0, description="Order quantity")
    price: float = Field(..., gt=0.0, description="Limit price")
    fee: FeeBreakdown = Field(..., description="Applicable fees for the order")


class PolicyDecisionResponse(BaseModel):
    approved: bool = Field(..., description="Whether the order is approved")
    reason: Optional[str] = Field(None, description="Optional rejection reason")
    effective_fee: FeeBreakdown = Field(..., description="Effective fees considered")


class RiskValidationRequest(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    net_exposure: float = Field(..., description="Net exposure after order")
    gross_notional: float = Field(..., description="Gross notional value of the order")
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


class KrakenSecretRotationRequest(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    api_key: str = Field(..., description="Rotated Kraken API key")
    api_secret: str = Field(..., description="Rotated Kraken API secret")


class KrakenSecretRotationResponse(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    secret_name: str = Field(..., description="Kubernetes secret storing credentials")
    rotated_at: datetime = Field(..., description="Timestamp of the last rotation event")


class KrakenSecretStatusResponse(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    secret_name: str = Field(..., description="Kubernetes secret storing credentials")
    created_at: datetime = Field(..., description="First time the secret was written")
    rotated_at: datetime = Field(..., description="Most recent rotation timestamp")

