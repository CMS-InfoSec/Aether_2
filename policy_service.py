"""FastAPI microservice that produces policy decisions based on model intents."""

from __future__ import annotations

import os
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, List, Union

import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.models.model_server import Intent, predict_intent

FEES_SERVICE_URL = os.getenv("FEES_SERVICE_URL", "http://localhost:8000")
FEES_REQUEST_TIMEOUT = float(os.getenv("FEES_SERVICE_TIMEOUT", "3.0"))

KRAKEN_PRECISION: Dict[str, Dict[str, float]] = {
    "BTC-USD": {"tick": 0.1, "lot": 0.0001},
    "ETH-USD": {"tick": 0.01, "lot": 0.001},
    "SOL-USD": {"tick": 0.001, "lot": 0.01},
}


def _snap(value: float, step: float) -> float:
    """Snap a numeric value to the closest Kraken-supported increment."""

    if step <= 0:
        return value
    quant = Decimal(str(step))
    snapped = (Decimal(str(value)) / quant).to_integral_value(rounding=ROUND_HALF_UP) * quant
    return float(snapped)


class BookSnapshotPayload(BaseModel):
    """Minimal book snapshot used to evaluate the intent."""

    mid_price: float = Field(..., gt=0.0, description="Mid price used during evaluation")
    spread_bps: float = Field(
        ..., ge=0.0, description="Bid/ask spread in basis points at decision time"
    )
    imbalance: float = Field(
        ..., ge=-1.0, le=1.0, description="Normalized order book imbalance"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "mid_price": 30125.4,
                    "spread_bps": 2.4,
                    "imbalance": 0.12,
                }
            ]
        }
    )


FeaturesPayload = Union[List[float], Dict[str, float]]


class PolicyDecisionRequest(BaseModel):
    """Incoming payload describing the desired trade and market context."""

    account_id: str = Field(..., description="Trading account identifier")
    symbol: str = Field(..., description="Kraken trading pair, e.g. BTC-USD")
    side: str = Field(..., pattern="^(?i)(buy|sell)$", description="Intended trade side")
    qty: float = Field(..., gt=0.0, description="Requested order quantity")
    price: float = Field(..., gt=0.0, description="Requested limit price")
    impact_bps: float = Field(
        0.0,
        ge=0.0,
        description="Estimated market impact in basis points to include in the gate",
    )
    features: FeaturesPayload = Field(
        default_factory=list,
        description="Feature vector consumed by the intent model",
    )
    book_snapshot: BookSnapshotPayload = Field(
        ..., description="Order book snapshot aligned with the request"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "account_id": "company",
                    "symbol": "BTC-USD",
                    "side": "buy",
                    "qty": 0.5234,
                    "price": 30120.45,
                    "impact_bps": 1.2,
                    "features": [0.4, -0.1, 2.8],
                    "book_snapshot": {
                        "mid_price": 30125.4,
                        "spread_bps": 2.4,
                        "imbalance": 0.12,
                    },
                }
            ]
        }
    )

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        return value.upper()

    @property
    def features_vector(self) -> List[float]:
        """Return the model-ready feature vector."""

        payload = self.features
        if isinstance(payload, dict):
            return [float(v) for _, v in sorted(payload.items())]
        if isinstance(payload, (list, tuple)):
            return [float(v) for v in payload]
        return []


class PolicyDecisionResponse(BaseModel):
    """Decision payload returned by the policy service."""

    account_id: str = Field(..., description="Trading account identifier")
    symbol: str = Field(..., description="Trading pair evaluated")
    action: str = Field(..., description="Model-selected action (maker/taker/hold)")
    side: str = Field(..., description="Execution side (buy/sell/none)")
    order_type: str = Field(..., description="Order type to use when approved")
    qty: float = Field(..., ge=0.0, description="Quantity snapped to Kraken precision")
    price: float = Field(..., ge=0.0, description="Price snapped to Kraken precision")
    limit_px: float | None = Field(
        None, description="Limit price to submit when action proceeds"
    )
    tif: str | None = Field(None, description="Optional time-in-force policy")
    take_profit_bps: float = Field(
        ..., ge=0.0, description="Take-profit distance suggested by the model"
    )
    stop_loss_bps: float = Field(
        ..., ge=0.0, description="Stop-loss distance suggested by the model"
    )
    expected_edge_bps: float = Field(
        ..., description="Edge associated with the selected action"
    )
    spread_bps: float = Field(..., ge=0.0, description="Spread at decision time")
    effective_fee_bps: float = Field(
        ..., ge=0.0, description="Effective fee fetched from the fee service"
    )
    impact_bps: float = Field(..., ge=0.0, description="Impact cost incorporated in the gate")
    expected_cost_bps: float = Field(
        ..., ge=0.0, description="Total expected cost including spread, fees, and impact"
    )
    confidence: float = Field(
        ..., ge=0.0, le=1.0, description="Overall confidence score from the model"
    )
    approved: bool = Field(..., description="Whether the decision passed gating")
    reason: str | None = Field(None, description="Optional rejection reason")

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "account_id": "company",
                    "symbol": "BTC-USD",
                    "action": "maker",
                    "side": "buy",
                    "order_type": "limit",
                    "qty": 0.5234,
                    "price": 30120.5,
                    "limit_px": 30120.5,
                    "tif": "GTC",
                    "take_profit_bps": 24.0,
                    "stop_loss_bps": 12.0,
                    "expected_edge_bps": 18.6,
                    "spread_bps": 2.4,
                    "effective_fee_bps": 4.0,
                    "impact_bps": 1.2,
                    "expected_cost_bps": 7.6,
                    "confidence": 0.82,
                    "approved": True,
                    "reason": None,
                }
            ]
        }
    )


app = FastAPI(title="Policy Service", version="2.0.0")


def _resolve_precision(symbol: str) -> Dict[str, float]:
    return KRAKEN_PRECISION.get(symbol.upper(), {"tick": 0.01, "lot": 0.0001})


async def _fetch_effective_fee(
    account_id: str, symbol: str, liquidity: str, notional: float
) -> float:
    """Fetch effective fee basis points for the decision."""

    liquidity_normalized = liquidity.lower() if liquidity else "maker"
    if liquidity_normalized not in {"maker", "taker"}:
        liquidity_normalized = "maker"

    params = {
        "pair": symbol,
        "liquidity": liquidity_normalized,
        "notional": f"{max(notional, 0.0):.8f}",
    }
    headers = {"X-Account-ID": account_id}
    timeout = httpx.Timeout(FEES_REQUEST_TIMEOUT)

    async with httpx.AsyncClient(base_url=FEES_SERVICE_URL, timeout=timeout) as client:
        try:
            response = await client.get("/fees/effective", params=params, headers=headers)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code,
                detail="Fee service returned an error",
            ) from exc
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to contact fee service",
            ) from exc

    payload = response.json()
    fee_payload = payload.get("fee") if isinstance(payload, dict) else None
    if not isinstance(fee_payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Fee service response malformed",
        )

    key = "maker" if liquidity_normalized == "maker" else "taker"
    try:
        return float(fee_payload[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Fee service response missing expected fields",
        ) from exc


def _select_template(intent: Intent, liquidity: str):
    templates = intent.action_templates or []
    for template in templates:
        if template.name.lower() == liquidity.lower():
            return template
    if templates:
        return max(templates, key=lambda template: template.edge_bps)
    return ()


def _confidence(intent: Intent) -> float:
    metrics = intent.confidence
    if metrics.overall_confidence is not None:
        return float(metrics.overall_confidence)
    composite = (
        metrics.model_confidence
        + metrics.state_confidence
        + metrics.execution_confidence
    ) / 3.0
    return float(round(composite, 4))


@app.get("/health", tags=["health"])
async def health() -> Dict[str, str]:
    """Liveness probe for the service."""

    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> Dict[str, str]:
    """Readiness probe ensuring critical dependencies are importable."""

    if predict_intent is None:  # pragma: no cover - defensive guard
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model server unavailable",
        )
    return {"status": "ready"}


@app.post(
    "/policy/decide",
    response_model=PolicyDecisionResponse,
    status_code=status.HTTP_200_OK,
)
async def decide_policy(request: PolicyDecisionRequest) -> PolicyDecisionResponse:
    """Evaluate the latest intent and return a gated execution decision."""

    precision = _resolve_precision(request.symbol)
    snapped_price = _snap(request.price, precision["tick"])
    snapped_qty = _snap(request.qty, precision["lot"])

    if snapped_price <= 0 or snapped_qty <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Snapped price or quantity is non-positive",
        )

    features_vector = request.features_vector
    intent = predict_intent(
        account_id=request.account_id,
        symbol=request.symbol,
        features=features_vector,
        book_snapshot=request.book_snapshot.model_dump(),
    )

    liquidity = intent.selected_action or "maker"
    template = _select_template(intent, liquidity)
    template_edge = (
        float(template.edge_bps)
        if hasattr(template, "edge_bps")
        else float(intent.edge_bps)
    )

    effective_fee_bps = await _fetch_effective_fee(
        account_id=request.account_id,
        symbol=request.symbol,
        liquidity=liquidity,
        notional=snapped_price * snapped_qty,
    )

    spread_bps = float(request.book_snapshot.spread_bps)
    impact_bps = float(request.impact_bps)
    expected_edge_bps = float(template_edge)
    expected_cost_bps = spread_bps + effective_fee_bps + impact_bps

    gate_passed = expected_edge_bps > expected_cost_bps
    approved = bool(intent.approved and gate_passed)

    if approved:
        action = liquidity.lower()
        side = request.side.lower()
        order_type = "limit"
        limit_px = snapped_price
        reason = intent.reason
        qty = snapped_qty
    else:
        action = "hold"
        side = "none"
        order_type = "none"
        limit_px = None
        qty = 0.0
        if not intent.approved and intent.reason:
            reason = intent.reason
        elif not gate_passed:
            reason = "Fee-adjusted edge non-positive"
        else:
            reason = "Intent rejected"

    response = PolicyDecisionResponse(
        account_id=request.account_id,
        symbol=request.symbol,
        action=action,
        side=side,
        order_type=order_type,
        qty=round(qty, 8),
        price=round(snapped_price, 8),
        limit_px=limit_px,
        tif=None,
        take_profit_bps=float(getattr(intent, "take_profit_bps", 0.0)),
        stop_loss_bps=float(getattr(intent, "stop_loss_bps", 0.0)),
        expected_edge_bps=round(expected_edge_bps, 4),
        spread_bps=round(spread_bps, 4),
        effective_fee_bps=round(effective_fee_bps, 4),
        impact_bps=round(impact_bps, 4),
        expected_cost_bps=round(expected_cost_bps, 4),
        confidence=round(_confidence(intent), 4),
        approved=approved,
        reason=reason,
    )

    return response

