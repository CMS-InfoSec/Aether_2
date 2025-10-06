"""FastAPI microservice for policy intent decisions."""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from fastapi import Depends, FastAPI, HTTPException, Query, status
from auth.service import build_session_store_from_url, SessionStoreProtocol
from shared.session_config import load_session_ttl_minutes
from pydantic import BaseModel, Field, field_validator

try:  # pragma: no cover - optional dependency
    from . import models
except ImportError:  # pragma: no cover - the inference module is optional at runtime
    models = None


SAFETY_MARGIN_BPS = 1.0


class FeeServiceClient:
    """Lightweight client used to obtain fee estimates for intents.

    The client consults the OMS/Timescale fee tiers exposed through
    :class:`services.common.adapters.TimescaleAdapter`.  Results are cached per
    ``(account_id, symbol)`` for a short period to avoid repeatedly fetching the
    same tier data.  When the upstream source is unavailable the method falls
    back to ``0.0`` bps, which is documented so callers understand that cost
    estimates may be missing instead of silently failing.
    """

    _DEFAULT_CACHE_TTL_SECONDS = 300.0

    def __init__(
        self,
        *,
        adapter_factory: type["TimescaleAdapter"] | None = None,
        cache_ttl_seconds: float = _DEFAULT_CACHE_TTL_SECONDS,
    ) -> None:
        from services.common.adapters import TimescaleAdapter  # local import to avoid cycles

        self._adapter_factory: type[TimescaleAdapter] = adapter_factory or TimescaleAdapter
        self._cache_ttl = max(1.0, float(cache_ttl_seconds))
        self._cache: Dict[tuple[str, str], tuple[float, List[Dict[str, float]]]] = {}
        self._lock = threading.Lock()

    def _load_fee_tiers(self, account_id: str, symbol: str) -> List[Dict[str, float]]:
        cache_key = (account_id.lower(), symbol.upper())
        now = time.monotonic()
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached and cached[0] > now:
                return [dict(entry) for entry in cached[1]]

        try:
            adapter = self._adapter_factory(account_id=account_id)
            tiers = adapter.fee_tiers(symbol)
        except Exception:
            tiers = []

        if tiers:
            with self._lock:
                self._cache[cache_key] = (now + self._cache_ttl, [dict(entry) for entry in tiers])
        return [dict(entry) for entry in tiers]

    @staticmethod
    def _select_tier(
        tiers: Sequence[Mapping[str, float]],
        *,
        liquidity: str,
        size: float | None,
    ) -> float:
        liquidity_key = "maker" if str(liquidity).lower() == "maker" else "taker"
        if not tiers:
            return 0.0

        ordered = sorted(
            (
                {
                    "threshold": float(entry.get("notional_threshold", 0.0) or 0.0),
                    "maker": float(entry.get("maker", entry.get("maker_bps", 0.0)) or 0.0),
                    "taker": float(entry.get("taker", entry.get("taker_bps", 0.0)) or 0.0),
                }
                for entry in tiers
            ),
            key=lambda item: item["threshold"],
        )

        target_size = abs(float(size)) if size is not None else 0.0
        selected = ordered[0]
        for tier in ordered[1:]:
            if target_size < tier["threshold"]:
                break
            selected = tier
        return float(max(selected.get(liquidity_key, 0.0), 0.0))

    def fee_bps_estimate(
        self,
        *,
        account_id: str,
        symbol: str,
        liquidity: str,
        size: float | None = None,
    ) -> float:
        tiers = self._load_fee_tiers(account_id, symbol)
        if not tiers:
            return 0.0
        return self._select_tier(tiers, liquidity=liquidity, size=size)


class SlippageEstimator:
    """Estimator that derives historical slippage curves from Timescale/OMS data.

    The estimator queries :mod:`services.oms.impact_store` for historical fill
    impact points and interpolates the expected basis-point cost for the
    requested size.  Results are cached per ``(account_id, symbol)`` so repeated
    requests share the same data.  If the data source is unavailable the method
    returns ``0.0`` bps, which mirrors the documented fallback for the fee
    estimator.
    """

    _DEFAULT_CACHE_TTL_SECONDS = 120.0
    _DEFAULT_ASYNC_TIMEOUT = 1.0

    def __init__(
        self,
        *,
        impact_store: Any | None = None,
        cache_ttl_seconds: float = _DEFAULT_CACHE_TTL_SECONDS,
        async_timeout: float = _DEFAULT_ASYNC_TIMEOUT,
    ) -> None:
        from services.oms.impact_store import impact_store as default_store  # lazy import

        self._impact_store = impact_store or default_store
        self._cache_ttl = max(1.0, float(cache_ttl_seconds))
        self._async_timeout = max(0.1, float(async_timeout))
        self._cache: Dict[tuple[str, str], tuple[float, List[Dict[str, float]]]] = {}
        self._lock = threading.Lock()

    def _fetch_curve(self, account_id: str, symbol: str) -> List[Dict[str, float]]:
        cache_key = (account_id.lower(), symbol.upper())
        now = time.monotonic()
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached and cached[0] > now:
                return [dict(entry) for entry in cached[1]]

        try:
            coroutine = self._impact_store.impact_curve(account_id=account_id, symbol=symbol)
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
                points = future.result(timeout=self._async_timeout)
            else:
                points = asyncio.run(coroutine)
        except Exception:
            points = []

        if points:
            with self._lock:
                self._cache[cache_key] = (now + self._cache_ttl, [dict(entry) for entry in points])
        return [dict(entry) for entry in points]

    @staticmethod
    def _interpolate(
        ordered: Sequence[Mapping[str, float]],
        target_size: float,
    ) -> float:
        if not ordered:
            return 0.0

        sorted_points = sorted(
            (
                {
                    "size": max(float(point.get("size", 0.0) or 0.0), 0.0),
                    "impact": float(point.get("impact_bps", 0.0) or 0.0),
                }
                for point in ordered
            ),
            key=lambda entry: entry["size"],
        )

        if not sorted_points:
            return 0.0

        if target_size <= 0.0:
            return abs(sorted_points[0]["impact"])

        previous = sorted_points[0]
        for current in sorted_points[1:]:
            if target_size <= current["size"]:
                span = current["size"] - previous["size"]
                if span <= 0:
                    return abs(current["impact"])
                weight = (target_size - previous["size"]) / span
                interpolated = previous["impact"] + (current["impact"] - previous["impact"]) * weight
                return abs(interpolated)
            previous = current

        return abs(sorted_points[-1]["impact"])

    def estimate_slippage_bps(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str | None,
        size: float | None = None,
    ) -> float:
        del side  # direction currently unused because impact data is signed

        points = self._fetch_curve(account_id, symbol)
        if not points:
            return 0.0

        target_size = abs(float(size)) if size is not None else 0.0
        return self._interpolate(points, target_size)


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


from auth.service import SessionStoreProtocol

from metrics import (
    metric_context,
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
        raw_url = os.getenv("SESSION_REDIS_URL")
        if raw_url is None:
            raise RuntimeError(
                "SESSION_REDIS_URL is not configured. Provide a redis:// DSN to enable policy service authentication.",
            )

        redis_url = raw_url.strip()
        if not redis_url:
            raise RuntimeError(
                "SESSION_REDIS_URL is set but empty; configure a redis:// or rediss:// DSN.",
            )

        normalized = redis_url.lower()
        if normalized.startswith("memory://") and "pytest" not in sys.modules:
            raise RuntimeError(
                "SESSION_REDIS_URL must use a redis:// or rediss:// DSN outside pytest so policy sessions persist across restarts.",
            )

        ttl_minutes = load_session_ttl_minutes()
        if normalized.startswith("memory://"):
            from auth.service import InMemorySessionStore  # local import to avoid optional dependency

            store = InMemorySessionStore(ttl_minutes=ttl_minutes)
        else:
            store = build_session_store_from_url(redis_url, ttl_minutes=ttl_minutes)
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
    metrics_ctx = metric_context(account_id=request.account_id, symbol=request.symbol)
    record_drift_score(
        request.account_id,
        request.symbol,
        drift,
        context=metrics_ctx,
    )

    action = (response.action or "").lower()
    abstain = 1.0 if action in {"hold", "abstain"} else 0.0
    record_abstention_rate(
        request.account_id,
        request.symbol,
        abstain,
        context=metrics_ctx,
    )

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
