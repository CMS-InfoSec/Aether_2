"""FastAPI microservice for policy intent decisions."""

from __future__ import annotations

import time
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Union

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator

try:  # pragma: no cover - optional dependency
    from . import models
except ImportError:  # pragma: no cover - the inference module is optional at runtime
    models = None

from metrics import (
    observe_policy_inference_latency,
    record_abstention_rate,
    record_drift_score,
    setup_metrics,
    traced_span,
)
from services.common.security import ADMIN_ACCOUNTS
from services.policy.trade_intensity_controller import (
    controller as trade_intensity_controller,
)


class PolicyDecisionRequest(BaseModel):
    """Request schema for the policy decision endpoint."""

    account_id: str = Field(..., description="Unique account identifier")
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

    @field_validator("account_id")
    @classmethod
    def _validate_account_id(cls, value: str) -> str:
        if value not in ADMIN_ACCOUNTS:
            raise ValueError("Account must be an authorized admin.")
        return value


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


class TradeIntensityResponse(BaseModel):
    """Response schema for the trade intensity controller."""

    account_id: str = Field(..., description="Account identifier associated with the query")
    symbol: str = Field(..., description="Trading symbol for the intensity context")
    multiplier: float = Field(..., description="Smoothed multiplier applied to position sizing")
    raw_multiplier: float = Field(
        ..., description="Raw multiplier prior to smoothing and clipping"
    )
    alpha: float = Field(..., description="EMA smoothing factor")
    floor: float = Field(..., description="Lower bound on the multiplier")
    ceiling: float = Field(..., description="Upper bound on the multiplier")
    diagnostics: Dict[str, float] = Field(
        default_factory=dict,
        description="Component level adjustments that produced the multiplier",
    )
    last_updated: float = Field(..., description="Epoch timestamp of the last update")


APP_VERSION = "2.0.0"

app = FastAPI(title="Policy Service", version=APP_VERSION)
setup_metrics(app, service_name="policy-service")


@app.post("/policy/decide", response_model=PolicyIntent, status_code=status.HTTP_200_OK)
def decide_policy_intent(request: PolicyDecisionRequest) -> PolicyIntent:
    """Generate a policy intent decision from the provided market and account context."""

    if models is None or not hasattr(models, "predict_intent"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Intent prediction model is not available.",
        )

    try:
        with traced_span(
            "policy.inference",
            account_id=request.account_id,
            symbol=request.symbol,
        ):
            inference_start = time.perf_counter()
            intent_payload = models.predict_intent(
                account_id=request.account_id,
                symbol=request.symbol,
                features=request.features,
                book_snapshot=request.book_snapshot,
                account_state=request.account_state,
            )
        observe_policy_inference_latency((time.perf_counter() - inference_start) * 1000.0)
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive runtime check
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate policy intent.",
        ) from exc

    if is_dataclass(intent_payload):
        intent_payload = asdict(intent_payload)
    elif hasattr(intent_payload, "model_dump") and callable(intent_payload.model_dump):
        intent_payload = intent_payload.model_dump()
    elif hasattr(intent_payload, "dict") and callable(intent_payload.dict):
        intent_payload = intent_payload.dict()

    if not isinstance(intent_payload, dict):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Model response is not a valid intent payload.",
        )

    try:
        response = PolicyIntent(**intent_payload)
    except Exception as exc:  # pragma: no cover - ensures schema compatibility
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Model response failed schema validation.",
        ) from exc

    drift = 0.0
    account_state = request.account_state or {}
    if isinstance(account_state, dict):
        raw = account_state.get("drift_score", 0.0)
        try:
            drift = float(raw)
        except (TypeError, ValueError):
            drift = 0.0
    record_drift_score(request.account_id, request.symbol, drift)

    action = (response.action or "").lower()
    abstain = 1.0 if action in {"hold", "abstain"} else 0.0
    record_abstention_rate(request.account_id, request.symbol, abstain)

    return response


@app.get("/policy/intensity", response_model=TradeIntensityResponse, status_code=status.HTTP_200_OK)
def get_trade_intensity(
    account_id: str = Query(..., description="Authorized account identifier"),
    symbol: str = Query(..., description="Trading symbol to query"),
    signal_confidence: float = Query(
        0.5, ge=0.0, le=1.0, description="Model or strategy confidence in the signal"
    ),
    regime: str = Query("unknown", description="Detected market regime label"),
    queue_depth: float = Query(
        0.0,
        ge=0.0,
        le=1.0,
        description="Execution backpressure score where 1 is fully saturated",
    ),
    win_rate: float = Query(
        0.5,
        ge=0.0,
        le=1.0,
        description="Recent win-rate of fills or strategy performance",
    ),
    drawdown: float = Query(
        0.0,
        ge=0.0,
        le=1.0,
        description="Normalized drawdown where 1 equals risk limits breached",
    ),
    fee_pressure: float = Query(
        0.0,
        ge=0.0,
        le=1.0,
        description="Fee pressure score capturing venue cost headwinds",
    ),
) -> TradeIntensityResponse:
    """Return the current trade intensity multiplier and diagnostics."""

    if account_id not in ADMIN_ACCOUNTS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account must be an authorized admin.",
        )

    payload = trade_intensity_controller.evaluate(
        account_id=account_id,
        symbol=symbol,
        signal_confidence=signal_confidence,
        regime=regime,
        queue_depth=queue_depth,
        win_rate=win_rate,
        drawdown=drawdown,
        fee_pressure=fee_pressure,
    )
    return TradeIntensityResponse(**payload)
