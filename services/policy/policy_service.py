"""FastAPI microservice for policy intent decisions."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

try:  # pragma: no cover - optional dependency
    from . import models
except ImportError:  # pragma: no cover - the inference module is optional at runtime
    models = None


class PolicyDecisionRequest(BaseModel):
    """Request schema for the policy decision endpoint."""

    account_id: UUID = Field(..., description="Unique account identifier")
    symbol: str = Field(..., description="Trading symbol for the decision")
    features: Optional[Union[List[Any], Dict[str, Any]]] = Field(
        None, description="Feature vector or mapping for model inference"
    )
    book_snapshot: Dict[str, Any] = Field(
        default_factory=dict,
        description="Latest order book snapshot or market microstructure data",
    )
    account_state: Dict[str, Any] = Field(
        default_factory=dict,
        description="Account state such as positions, balances, and risk metrics",
    )


class PolicyIntent(BaseModel):
    """Response schema describing the policy intent."""

    action: str = Field(..., description="Intent action: enter, exit, or scale")
    side: str = Field(..., description="Trade side associated with the decision")
    qty: float = Field(..., description="Quantity to trade")
    preference: str = Field(
        ..., description="Venue preference such as maker or taker"
    )
    type: str = Field(..., description="Order type such as limit or market")
    limit_px: Optional[float] = Field(
        None, description="Optional limit price for limit orders"
    )
    tif: Optional[str] = Field(
        None, description="Time in force policy for order placement"
    )
    tp: Optional[float] = Field(None, description="Target take-profit level")
    sl: Optional[float] = Field(None, description="Protective stop-loss level")
    expected_edge_bps: float = Field(
        ..., description="Expected edge expressed in basis points"
    )
    expected_cost_bps: float = Field(
        ..., description="Estimated cost expressed in basis points"
    )
    confidence: float = Field(..., description="Confidence score for the decision")


app = FastAPI(title="Policy Service")


@app.post("/policy/decide", response_model=PolicyIntent, status_code=status.HTTP_200_OK)
def decide_policy_intent(request: PolicyDecisionRequest) -> PolicyIntent:
    """Generate a policy intent decision from the provided market and account context."""

    if models is None or not hasattr(models, "predict_intent"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Intent prediction model is not available.",
        )

    try:
        intent_payload = models.predict_intent(
            account_id=str(request.account_id),
            symbol=request.symbol,
            features=request.features,
            book_snapshot=request.book_snapshot,
            account_state=request.account_state,
        )
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive runtime check
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate policy intent.",
        ) from exc

    if not isinstance(intent_payload, dict):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Model response is not a valid intent payload.",
        )

    try:
        return PolicyIntent(**intent_payload)
    except Exception as exc:  # pragma: no cover - ensures schema compatibility
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Model response failed schema validation.",
        ) from exc
