
from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException, status

from services.common.adapters import KafkaNATSAdapter, RedisFeastAdapter, TimescaleAdapter
from services.common.schemas import (
    ActionTemplate,
    BookSnapshot,
    ConfidenceMetrics,
    PolicyDecisionRequest,
    PolicyDecisionResponse,
    PolicyState,
)
from services.common.security import require_admin_account
from shared.models.registry import get_model_registry
from services.policy.model_server import predict_intent

app = FastAPI(title="Policy Service")



@app.post("/policy/decide", response_model=PolicyDecisionResponse)
def decide_policy(

    request: PolicyDecisionRequest,
    account_id: str = Depends(require_admin_account),
) -> PolicyDecisionResponse:
    if request.account_id != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )

    registry = get_model_registry()
    ensemble = registry.get_latest_ensemble(account_id, request.instrument)

    redis = RedisFeastAdapter(account_id=account_id)
    online_features = redis.fetch_online_features(request.instrument)

    features = request.features or online_features.get("features", [])
    book_snapshot_payload = request.book_snapshot or online_features.get("book_snapshot")
    state_payload = request.state or online_features.get("state")
    expected_edge = request.expected_edge_bps or online_features.get("expected_edge_bps") or 0.0

    if book_snapshot_payload is None or state_payload is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing market context for policy evaluation.",
        )

    book_snapshot_model = (
        book_snapshot_payload
        if isinstance(book_snapshot_payload, BookSnapshot)
        else BookSnapshot(**book_snapshot_payload)
    )
    state_model = (
        state_payload if isinstance(state_payload, PolicyState) else PolicyState(**state_payload)
    )

    intent = predict_intent(
        account_id=account_id,
        symbol=request.instrument,
        features=features,
        book_snapshot=book_snapshot_model,
    )

    base_confidence_payload = online_features.get("confidence") or {}
    baseline_confidence = ConfidenceMetrics(
        model_confidence=base_confidence_payload.get("model_confidence", 0.5),
        state_confidence=base_confidence_payload.get("state_confidence", 0.5),
        execution_confidence=base_confidence_payload.get("execution_confidence", 0.5),
    )

    caller_confidence = request.confidence
    if caller_confidence is not None:
        baseline_confidence = ConfidenceMetrics(
            model_confidence=min(
                1.0,
                (baseline_confidence.model_confidence + caller_confidence.model_confidence) / 2.0,
            ),
            state_confidence=min(
                1.0,
                (baseline_confidence.state_confidence + caller_confidence.state_confidence) / 2.0,
            ),
            execution_confidence=min(
                1.0,
                (baseline_confidence.execution_confidence + caller_confidence.execution_confidence) / 2.0,
            ),
        )

    confidence = ConfidenceMetrics(
        model_confidence=max(baseline_confidence.model_confidence, intent.confidence.model_confidence),
        state_confidence=max(baseline_confidence.state_confidence, intent.confidence.state_confidence),
        execution_confidence=max(
            baseline_confidence.execution_confidence, intent.confidence.execution_confidence
        ),
        overall_confidence=max(
            baseline_confidence.overall_confidence or 0.0,
            intent.confidence.overall_confidence or 0.0,
        ),
    )

    take_profit_bps = (
        request.take_profit_bps
        or online_features.get("take_profit_bps")
        or intent.take_profit_bps
    )
    stop_loss_bps = (
        request.stop_loss_bps
        or online_features.get("stop_loss_bps")
        or intent.stop_loss_bps
    )

    expected_edge = intent.edge_bps if intent.edge_bps is not None else expected_edge
    maker_edge = expected_edge - request.fee.maker
    taker_edge = expected_edge - request.fee.taker

    action_templates = [
        ActionTemplate(
            name="maker",
            venue_type="maker",
            edge_bps=round(maker_edge, 4),
            fee_bps=request.fee.maker,
            confidence=round(confidence.execution_confidence, 4),
        ),
        ActionTemplate(
            name="taker",
            venue_type="taker",
            edge_bps=round(taker_edge, 4),
            fee_bps=request.fee.taker,
            confidence=round(confidence.execution_confidence * 0.95, 4),
        ),
    ]

    preferred_template = max(action_templates, key=lambda template: template.edge_bps)
    approved = (
        intent.approved
        and preferred_template.edge_bps > 0
        and confidence.overall_confidence >= ensemble.confidence_threshold
    )

    if approved:
        reason = intent.reason
        selected_action = preferred_template.name
        fee_adjusted_edge = preferred_template.edge_bps
    else:
        reason = intent.reason
        if reason is None:
            if confidence.overall_confidence < ensemble.confidence_threshold:
                reason = "Confidence below threshold"
            else:
                reason = "Fee-adjusted edge non-positive"
        if intent.is_null and intent.reason:
            reason = intent.reason
        selected_action = "abstain"
        fee_adjusted_edge = min(preferred_template.edge_bps, 0.0)

    kafka = KafkaNATSAdapter(account_id=account_id)
    kafka.publish(
        topic="policy.decisions",
        payload={
            "order_id": request.order_id,
            "instrument": request.instrument,
            "approved": approved,
            "reason": reason,
            "edge_bps": round(expected_edge, 4),
            "fee_adjusted_edge_bps": round(fee_adjusted_edge, 4),
            "confidence": confidence.model_dump(),
            "selected_action": selected_action,
            "action_templates": [template.model_dump() for template in action_templates],
        },
    )

    timescale = TimescaleAdapter(account_id=account_id)
    timescale.record_decision(
        order_id=request.order_id,
        payload={
            "instrument": request.instrument,
            "edge_bps": expected_edge,
            "fee_adjusted_edge_bps": fee_adjusted_edge,
            "confidence": confidence.model_dump(),
            "approved": approved,
            "features": features,
        },
    )

    return PolicyDecisionResponse(
        approved=approved,
        reason=reason,
        effective_fee=request.fee,
        expected_edge_bps=round(expected_edge, 4),
        fee_adjusted_edge_bps=round(fee_adjusted_edge, 4),
        selected_action=selected_action,
        action_templates=action_templates,
        confidence=confidence,
        features=list(features),
        book_snapshot=book_snapshot_model,
        state=state_model,
        take_profit_bps=round(take_profit_bps, 4),
        stop_loss_bps=round(stop_loss_bps, 4),
    )

