
from __future__ import annotations

import logging
import math
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, TypeVar, cast, SupportsFloat, SupportsIndex

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
from services.policy.adaptive_horizon import get_horizon
from services.policy.model_server import predict_intent
from shared.async_utils import dispatch_async
from shared.health import setup_health_checks

from metrics import (
    metric_context,
    record_abstention_rate,
    record_drift_score,
    setup_metrics,
)

LOGGER = logging.getLogger(__name__)

app = FastAPI(title="Policy Service")
setup_metrics(app, service_name="policy-service")
setup_health_checks(app, {"model_registry": get_model_registry})

RouteFn = TypeVar("RouteFn", bound=Callable[..., Any])


def _app_post(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``FastAPI.post`` to satisfy strict type checks."""

    return cast(Callable[[RouteFn], RouteFn], app.post(*args, **kwargs))


def _normalise_feature_vector(raw: object) -> list[float]:
    """Coerce feature payloads to a numeric vector for responses and telemetry."""

    if isinstance(raw, Mapping):
        iterable: Iterable[object] = raw.values()
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        iterable = raw
    else:
        return []

    vector: list[float] = []
    for value in iterable:
        numeric = _to_numeric(value)
        if numeric is None:
            continue
        vector.append(numeric)
    return vector


@_app_post("/policy/decide", response_model=PolicyDecisionResponse)
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

    online_payload: Mapping[str, Any] | dict[str, Any]
    if isinstance(online_features, Mapping):
        online_payload = online_features
    else:
        online_payload = {}
    provided_fields: set[str] = getattr(request, "model_fields_set", set())

    if "features" in provided_fields:
        features = request.features
    else:
        features = online_payload.get("features", [])

    book_snapshot_payload = (
        request.book_snapshot
        if request.book_snapshot is not None
        else online_payload.get("book_snapshot")
    )
    state_payload = (
        request.state if request.state is not None else online_payload.get("state")
    )

    def _coerce_edge(value: object, default: float) -> float:
        numeric = _to_numeric(value)
        if numeric is None:
            return default
        return numeric

    expected_edge = _coerce_edge(
        request.expected_edge_bps,
        _coerce_edge(online_payload.get("expected_edge_bps"), 0.0),
    )

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

    state_dump = (
        state_model.model_dump()
        if hasattr(state_model, "model_dump")
        else {
            "regime": getattr(state_model, "regime", "unknown"),
            "volatility": getattr(state_model, "volatility", 0.0),
            "liquidity_score": getattr(state_model, "liquidity_score", 0.0),
            "conviction": getattr(state_model, "conviction", 0.0),
        }
    )
    horizon_context = {
        "symbol": request.instrument,
        "regime": state_dump.get("regime"),
        "state": state_dump,
        "features": list(features or []),
    }
    horizon_seconds = get_horizon(horizon_context)

    intent = predict_intent(
        account_id=account_id,
        symbol=request.instrument,
        features=features,
        book_snapshot=book_snapshot_model,
        horizon=horizon_seconds,
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

    take_profit_bps = _coerce_edge(
        request.take_profit_bps,
        _coerce_edge(
            online_payload.get("take_profit_bps"), intent.take_profit_bps
        ),
    )
    stop_loss_bps = _coerce_edge(
        request.stop_loss_bps,
        _coerce_edge(
            online_payload.get("stop_loss_bps"), intent.stop_loss_bps
        ),
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
    dispatch_async(
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
        ),
        context="publish policy.decisions",
        logger=LOGGER,
    )

    timescale = TimescaleAdapter(account_id=account_id)
    feature_vector = _normalise_feature_vector(features)

    timescale.record_decision(
        order_id=request.order_id,
        payload={
            "instrument": request.instrument,
            "edge_bps": expected_edge,
            "fee_adjusted_edge_bps": fee_adjusted_edge,
            "confidence": confidence.model_dump(),
            "approved": approved,
            "features": feature_vector,
        },
    )

    response = PolicyDecisionResponse(
        approved=approved,
        reason=reason,
        effective_fee=request.fee,
        expected_edge_bps=round(expected_edge, 4),
        fee_adjusted_edge_bps=round(fee_adjusted_edge, 4),
        selected_action=selected_action,
        action_templates=action_templates,
        confidence=confidence,
        features=feature_vector,
        book_snapshot=book_snapshot_model,
        state=state_model,
        take_profit_bps=round(take_profit_bps, 4),
        stop_loss_bps=round(stop_loss_bps, 4),
    )

    drift_value = 0.0
    drift_source = online_features.get("drift_score") if isinstance(online_features, dict) else None
    if drift_source is None and isinstance(intent.metadata, Mapping):
        drift_metadata = intent.metadata.get("drift")
        if isinstance(drift_metadata, Mapping):
            drift_source = drift_metadata.get("max_severity")
    if drift_source is None and isinstance(request.state, PolicyState):
        drift_source = getattr(request.state, "conviction", None)
    if drift_source is not None:
        try:
            drift_value = float(drift_source)
        except (TypeError, ValueError):
            drift_value = 0.0
    metrics_ctx = metric_context(account_id=account_id, symbol=request.instrument)
    record_drift_score(
        account_id,
        request.instrument,
        drift_value,
        context=metrics_ctx,
    )

    abstain = 0.0 if response.approved and response.selected_action != "abstain" else 1.0
    record_abstention_rate(
        account_id,
        request.instrument,
        abstain,
        context=metrics_ctx,
    )

    return response

CoercibleScalar = SupportsFloat | SupportsIndex | str | bytes | bytearray


def _to_numeric(value: object) -> float | None:
    """Best-effort conversion of arbitrary objects to ``float`` values."""

    try:
        numeric = float(cast(CoercibleScalar, value))
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric

