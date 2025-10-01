"""FastAPI microservice that produces policy decisions based on model intents."""

from __future__ import annotations

import os
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, List

import httpx
from fastapi import FastAPI, HTTPException, status

from services.common.schemas import (
    ActionTemplate,
    BookSnapshot,
    ConfidenceMetrics,
    FeeBreakdown,
    PolicyDecisionRequest,
    PolicyDecisionResponse,
    PolicyState,
)
from services.models.model_server import predict_intent

from metrics import record_abstention_rate, record_drift_score, setup_metrics

FEES_SERVICE_URL = os.getenv("FEES_SERVICE_URL", "http://fees-service")
FEES_REQUEST_TIMEOUT = float(os.getenv("FEES_REQUEST_TIMEOUT", "1.0"))
CONFIDENCE_THRESHOLD = float(os.getenv("POLICY_CONFIDENCE_THRESHOLD", "0.55"))

KRAKEN_PRECISION: Dict[str, Dict[str, float]] = {
    "BTC-USD": {"tick": 0.1, "lot": 0.0001},
    "ETH-USD": {"tick": 0.01, "lot": 0.001},
    "SOL-USD": {"tick": 0.001, "lot": 0.01},
}

app = FastAPI(title="Policy Service", version="2.0.0")
setup_metrics(app)


def _default_state() -> PolicyState:
    return PolicyState(regime="unknown", volatility=0.0, liquidity_score=0.0, conviction=0.0)


def _resolve_precision(symbol: str) -> Dict[str, float]:
    return KRAKEN_PRECISION.get(symbol.upper(), {"tick": 0.01, "lot": 0.0001})


def _snap(value: float, step: float) -> float:
    if step <= 0:
        return float(value)
    quant = Decimal(str(step))
    snapped = (Decimal(str(value)) / quant).to_integral_value(rounding=ROUND_HALF_UP) * quant
    return float(snapped)


async def _fetch_effective_fee(account_id: str, symbol: str, liquidity: str, notional: float) -> float:
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
        except httpx.HTTPStatusError as exc:  # pragma: no cover - surface upstream errors
            raise HTTPException(
                status_code=exc.response.status_code,
                detail="Fee service returned an error",
            ) from exc
        except httpx.HTTPError as exc:  # pragma: no cover - network failures
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


@app.get("/health", tags=["health"])
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> Dict[str, str]:
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
    precision = _resolve_precision(request.instrument)
    snapped_price = _snap(request.price, precision["tick"])
    snapped_qty = _snap(request.quantity, precision["lot"])

    if snapped_price <= 0 or snapped_qty <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Snapped price or quantity is non-positive",
        )

    if request.book_snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Book snapshot is required for policy evaluation",
        )

    book_snapshot = request.book_snapshot
    features: List[float] = [float(value) for value in request.features]

    intent = predict_intent(
        account_id=request.account_id,
        symbol=request.instrument,
        features=features,
        book_snapshot=book_snapshot,
    )

    notional = float(Decimal(str(snapped_price)) * Decimal(str(snapped_qty)))
    maker_fee_bps = await _fetch_effective_fee(request.account_id, request.instrument, "maker", notional)
    taker_fee_bps = await _fetch_effective_fee(request.account_id, request.instrument, "taker", notional)

    effective_fee = FeeBreakdown(
        currency=request.fee.currency,
        maker=round(maker_fee_bps, 4),
        taker=round(taker_fee_bps, 4),
        maker_detail=request.fee.maker_detail,
        taker_detail=request.fee.taker_detail,
    )

    caller_confidence = request.confidence
    confidence = intent.confidence
    if caller_confidence is not None:
        confidence = ConfidenceMetrics(
            model_confidence=max(confidence.model_confidence, caller_confidence.model_confidence),
            state_confidence=max(confidence.state_confidence, caller_confidence.state_confidence),
            execution_confidence=max(
                confidence.execution_confidence, caller_confidence.execution_confidence
            ),
            overall_confidence=max(
                confidence.overall_confidence or 0.0,
                caller_confidence.overall_confidence or 0.0,
            ),
        )

    expected_edge = float(intent.edge_bps or 0.0)
    maker_edge = round(expected_edge - effective_fee.maker, 4)
    taker_edge = round(expected_edge - effective_fee.taker, 4)

    template_lookup = {template.name.lower(): template for template in intent.action_templates or []}
    maker_template = template_lookup.get("maker")
    taker_template = template_lookup.get("taker")

    action_templates = [
        ActionTemplate(
            name="maker",
            venue_type="maker",
            edge_bps=maker_edge,
            fee_bps=round(effective_fee.maker, 4),
            confidence=round(
                maker_template.confidence if maker_template else confidence.execution_confidence, 4
            ),
        ),
        ActionTemplate(
            name="taker",
            venue_type="taker",
            edge_bps=taker_edge,
            fee_bps=round(effective_fee.taker, 4),
            confidence=round(
                taker_template.confidence
                if taker_template
                else confidence.execution_confidence * 0.95,
                4,
            ),
        ),
    ]

    selected_template = next(
        (
            template
            for template in action_templates
            if template.name.lower() == (intent.selected_action or "").lower()
        ),
        None,
    )
    if selected_template is None and action_templates:
        selected_template = max(action_templates, key=lambda template: template.edge_bps)

    fee_adjusted_edge = selected_template.edge_bps if selected_template else 0.0

    approved = (
        intent.approved
        and fee_adjusted_edge > 0
        and (confidence.overall_confidence or 0.0) >= CONFIDENCE_THRESHOLD
    )

    reason = intent.reason
    selected_action = intent.selected_action or "abstain"
    if not approved:
        if reason is None:
            if (confidence.overall_confidence or 0.0) < CONFIDENCE_THRESHOLD:
                reason = "Confidence below threshold"
            else:
                reason = "Fee-adjusted edge non-positive"
        selected_action = "abstain"
        fee_adjusted_edge = min(fee_adjusted_edge, 0.0)
    else:
        selected_action = selected_template.name if selected_template else selected_action

    take_profit = request.take_profit_bps or intent.take_profit_bps or 0.0
    stop_loss = request.stop_loss_bps or intent.stop_loss_bps or 0.0

    state_model = request.state or _default_state()
    drift_value = getattr(state_model, "conviction", 0.0)
    try:
        drift_value = float(drift_value)
    except (TypeError, ValueError):
        drift_value = 0.0

    record_drift_score(request.account_id, request.instrument, drift_value)
    abstain_metric = 0.0 if approved and selected_action != "abstain" else 1.0
    record_abstention_rate(request.account_id, request.instrument, abstain_metric)

    response = PolicyDecisionResponse(
        approved=approved,
        reason=reason,
        effective_fee=effective_fee,
        expected_edge_bps=round(expected_edge, 4),
        fee_adjusted_edge_bps=round(fee_adjusted_edge, 4),
        selected_action=selected_action,
        action_templates=action_templates,
        confidence=confidence,
        features=features,
        book_snapshot=book_snapshot,
        state=state_model,
        take_profit_bps=round(float(take_profit), 4),
        stop_loss_bps=round(float(stop_loss), 4),
    )

    return response
