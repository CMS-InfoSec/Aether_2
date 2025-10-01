"""Shared Pydantic schemas for service APIs."""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class AccountIsolation(BaseModel):
    """Represents the account isolation attributes required on every payload."""

    account_id: str = Field(..., description="Unique identifier for the account that owns the request.")
    isolation_segment: str = Field(
        ..., description="Logical isolation segment used to scope access to account-specific data."
    )


class FeeAware(BaseModel):
    """Mixin providing fee related configuration for requests."""

    fee_tier: str = Field(
        default="standard",
        description="Named fee tier used to evaluate maker/taker and rebate schedules.",
    )
    include_fees: bool = Field(
        default=True,
        description="Whether downstream services should include fee calculations in responses.",
    )


class FeeBreakdown(BaseModel):
    maker_bps: float = Field(default=0.0, description="Maker fee in basis points.")
    taker_bps: float = Field(default=0.0, description="Taker fee in basis points.")
    rebates_bps: float = Field(default=0.0, description="Rebate schedule in basis points, if any.")


class PolicyDecisionRequest(AccountIsolation, FeeAware):
    instrument: str = Field(..., description="Instrument identifier for which the policy decision is requested.")
    quantity: float = Field(..., description="Absolute notional quantity of the order candidate.")
    price: float = Field(..., description="Price attached to the order candidate.")


class PolicyDecisionResponse(AccountIsolation):
    approved: bool = Field(..., description="Whether the order candidate complies with policy.")
    reason: str = Field(..., description="Explains the policy decision when disapproved.")
    fee_tier: str = Field(..., description="Fee tier applied when computing total_fees.")
    total_fees: float = Field(..., description="Estimated total fees for the candidate order.")


class RiskValidationRequest(AccountIsolation, FeeAware):
    net_exposure: float = Field(..., description="Projected net exposure after executing the trade.")
    symbol: str = Field(..., description="Trading symbol under validation.")


class RiskValidationResponse(AccountIsolation):
    valid: bool = Field(..., description="Whether the risk check passed.")
    reason: Optional[str] = Field(None, description="Additional context when the risk check fails.")
    fee_tier: str = Field(..., description="Fee tier consulted when performing the validation.")


class OMSPlaceRequest(AccountIsolation, FeeAware):
    order_id: str = Field(..., description="Client provided identifier for the order.")
    side: str = Field(..., description="Order side (buy/sell).")
    quantity: float = Field(..., description="Order quantity.")
    price: Optional[float] = Field(None, description="Optional limit price when order is not a market order.")


class OMSPlaceResponse(AccountIsolation):
    status: str = Field(..., description="Resulting status of the order placement attempt.")
    accepted: bool = Field(..., description="Whether the order was accepted by the OMS.")
    fee_tier: str = Field(..., description="Fee tier assigned to the order for settlement.")


class FeesEffectiveResponse(AccountIsolation):
    fee_schedule: FeeBreakdown = Field(..., description="Fee schedule effective for the account.")
    effective_at: datetime = Field(..., description="Timestamp when the fee schedule became effective.")


class UniverseApprovedResponse(AccountIsolation):
    symbols: List[str] = Field(default_factory=list, description="Universe of symbols approved for trading.")
    fee_tier: str = Field(..., description="Fee tier that governs the trading universe.")


class KrakenSecretRequest(AccountIsolation):
    scope: str = Field(..., description="Scope of the Kraken secret being requested (e.g., trading, funding).")


class KrakenSecretResponse(AccountIsolation):
    key: str = Field(..., description="API key extracted from the Kubernetes secret mount.")
    secret: str = Field(..., description="API secret extracted from the Kubernetes secret mount.")


__all__ = [
    "AccountIsolation",
    "FeeAware",
    "FeeBreakdown",
    "PolicyDecisionRequest",
    "PolicyDecisionResponse",
    "RiskValidationRequest",
    "RiskValidationResponse",
    "OMSPlaceRequest",
    "OMSPlaceResponse",
    "FeesEffectiveResponse",
    "UniverseApprovedResponse",
    "KrakenSecretRequest",
    "KrakenSecretResponse",
]
