"""FastAPI microservice for policy intent decisions."""

from __future__ import annotations

import time
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Union

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator

try:  # pragma: no cover - optional dependency
    from . import models
except ImportError:  # pragma: no cover - the inference module is optional at runtime
    models = None


SAFETY_MARGIN_BPS = 1.0


class FeeServiceClient:
    """Lightweight client used to obtain fee estimates for intents."""

    def fee_bps_estimate(
        self,
        *,
        account_id: str,
        symbol: str,
        liquidity: str,
        size: float | None = None,
    ) -> float:
        del account_id, symbol, liquidity, size
        return 0.0


class SlippageEstimator:
    """Simple estimator that provides expected slippage in basis points."""

    def estimate_slippage_bps(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str | None,
        size: float | None = None,
    ) -> float:
        del account_id, symbol, side, size
        return 0.0


class PositionSizerAdapter:
    """Adapter that suggests executable size for an intent."""

    def suggest_size(
        self,
        *,
        account_id: str,
        symbol: str,
        intent: Dict[str, Any],
        account_state: Dict[str, Any],
    ) -> float:
        del account_id, symbol, account_state
        qty = intent.get("qty")
        try:
            return float(qty)
        except (TypeError, ValueError):
            return 0.0


class DiversificationAllocator:
    """Allocator that adjusts intents to satisfy diversification constraints."""

    def adjust_intent_for_diversification(
        self,
        *,
        account_id: str,
        symbol: str,
        size: float,
        intent: Dict[str, Any],
        account_state: Dict[str, Any],
    ) -> Dict[str, Any]:
        del account_id, symbol, intent, account_state
        return {"size": size, "symbol": None, "reason": None}


fee_service = FeeServiceClient()
slippage_estimator = SlippageEstimator()
position_sizer = PositionSizerAdapter()
diversification_allocator = DiversificationAllocator()


def _to_float(value: Any, *, default: float = 0.0) -> float:
    """Best-effort conversion of ``value`` to ``float`` with fallback."""

    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


from auth.service import InMemorySessionStore, SessionStoreProtocol

from metrics import (
    observe_policy_inference_latency,
    record_abstention_rate,
    record_drift_score,
    setup_metrics,
    traced_span,
)
from services.common import security
from services.common.security import ADMIN_ACCOUNTS, require_admin_account
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
    symbol_override: Optional[str] = Field(
        None, description="Optional symbol override applied by diversification"
    )
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
    diagnostics: Dict[str, Any] = Field(
        default_factory=dict,
        description="Auxiliary diagnostics for explainability and auditing",
    )


class PolicyDiagnostics(BaseModel):
    """Diagnostics payload emitted alongside policy intents."""

    edge_bps: float = Field(..., description="Raw model edge prior to costs")
    fee_bps: float = Field(..., description="Estimated fee cost in basis points")
    slippage_bps: float = Field(
        ..., description="Estimated slippage applied to the proposed trade"
    )
    net_edge_bps: float = Field(..., description="Net edge after fees and slippage")
    diversification_reason: Optional[str] = Field(
        None, description="Explanation of diversification adjustments"
    )
    safety_margin_bps: float = Field(
        SAFETY_MARGIN_BPS,
        description="Safety buffer applied when validating net edge",
    )
    fee_breakdown_bps: Dict[str, float] = Field(
        default_factory=dict,
        description="Per-liquidity fee estimates used in evaluation",
    )

    model_config = {"populate_by_name": True}


class PolicyDecision(BaseModel):
    """Event emitted for downstream explainability tooling."""

    account_id: str = Field(..., description="Account evaluated by the policy engine")
    symbol: str = Field(..., description="Instrument evaluated")
    intent: PolicyIntent = Field(..., description="Resolved policy intent payload")
    diagnostics: PolicyDiagnostics = Field(
        ..., description="Diagnostics accompanying the decision"
    )


def emit_policy_decision(decision: PolicyDecision) -> None:
    """Emit the decision for downstream consumers.

    The default implementation is a no-op so that deployments without a
    messaging layer can still utilise the service.  Tests are expected to
    monkeypatch this hook when verifying emission behaviour.
    """

    del decision


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


def _configure_session_store(application: FastAPI) -> SessionStoreProtocol:
    existing = getattr(application.state, "session_store", None)
    if isinstance(existing, SessionStoreProtocol):
        store = existing
    else:
        store = InMemorySessionStore()
        application.state.session_store = store
    security.set_default_session_store(store)
    return store


SESSION_STORE = _configure_session_store(app)


@app.post("/policy/decide", response_model=PolicyIntent, status_code=status.HTTP_200_OK)
def decide_policy_intent(
    request: PolicyDecisionRequest,
    caller_account: str = Depends(require_admin_account),
) -> PolicyIntent:
    """Generate a policy intent decision from the provided market and account context."""

    if caller_account != request.account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between authenticated session and payload.",
        )

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

    account_state: Dict[str, Any] = {}
    if isinstance(request.account_state, dict):
        account_state = request.account_state

    preference = str(intent_payload.get("preference") or "maker").lower()
    base_qty = _to_float(intent_payload.get("qty"))
    expected_edge_bps = _to_float(
        intent_payload.get("expected_edge_bps", intent_payload.get("edge_bps")),
        default=0.0,
    )
    intent_payload.setdefault("expected_edge_bps", expected_edge_bps)

    fee_breakdown: Dict[str, float] = {}
    for tier in ("maker", "taker"):
        try:
            estimate = fee_service.fee_bps_estimate(
                account_id=request.account_id,
                symbol=request.symbol,
                liquidity=tier,
                size=base_qty if base_qty > 0 else None,
            )
        except Exception:
            estimate = 0.0
        fee_breakdown[tier] = _to_float(estimate)

    fee_bps = fee_breakdown.get(preference)
    if fee_bps is None:
        fee_bps = max(fee_breakdown.values(), default=0.0)

    try:
        slippage_value = slippage_estimator.estimate_slippage_bps(
            account_id=request.account_id,
            symbol=request.symbol,
            side=intent_payload.get("side"),
            size=base_qty if base_qty > 0 else None,
        )
    except Exception:
        slippage_value = 0.0
    slippage_bps = _to_float(slippage_value)

    total_cost_with_margin = fee_bps + slippage_bps + SAFETY_MARGIN_BPS
    if expected_edge_bps <= total_cost_with_margin:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="insufficient net edge",
        )

    try:
        sized_qty = position_sizer.suggest_size(
            account_id=request.account_id,
            symbol=request.symbol,
            intent=intent_payload,
            account_state=account_state,
        )
    except Exception as exc:  # pragma: no cover - protective guard
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to determine position size.",
        ) from exc

    adjusted_qty = _to_float(sized_qty)
    if adjusted_qty <= 0.0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="position size is zero",
        )

    diversification_reason: Optional[str] = None
    try:
        diversification_adjustment = diversification_allocator.adjust_intent_for_diversification(
            account_id=request.account_id,
            symbol=request.symbol,
            size=adjusted_qty,
            intent=intent_payload,
            account_state=account_state,
        )
    except Exception as exc:  # pragma: no cover - diversification must not break requests
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to apply diversification constraints.",
        ) from exc

    if isinstance(diversification_adjustment, dict):
        adjusted_qty = _to_float(
            diversification_adjustment.get("size"), default=adjusted_qty
        )
        symbol_override = diversification_adjustment.get("symbol") or diversification_adjustment.get(
            "symbol_override"
        )
        if symbol_override:
            intent_payload["symbol_override"] = str(symbol_override)
        diversification_reason = diversification_adjustment.get("reason")

    if adjusted_qty <= 0.0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="position size is zero",
        )

    intent_payload["qty"] = adjusted_qty

    expected_cost_bps = fee_bps + slippage_bps
    intent_payload["expected_cost_bps"] = expected_cost_bps
    net_edge_bps = expected_edge_bps - expected_cost_bps

    diagnostics_model = PolicyDiagnostics(
        edge_bps=expected_edge_bps,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        net_edge_bps=net_edge_bps,
        diversification_reason=diversification_reason,
        fee_breakdown_bps=fee_breakdown,
    )
    intent_payload["diagnostics"] = diagnostics_model.model_dump()

    try:
        response = PolicyIntent(**intent_payload)
    except Exception as exc:  # pragma: no cover - ensures schema compatibility
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Model response failed schema validation.",
        ) from exc

    decision = PolicyDecision(
        account_id=request.account_id,
        symbol=request.symbol,
        intent=response,
        diagnostics=diagnostics_model,
    )
    try:
        emit_policy_decision(decision)
    except Exception:  # pragma: no cover - telemetry should not break requests
        pass

    drift = 0.0
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
