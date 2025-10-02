"""FastAPI microservice that produces policy decisions based on model intents."""

from __future__ import annotations


import logging
import math
import os
from collections import defaultdict
from dataclasses import dataclass

from decimal import ROUND_HALF_UP, Decimal
from threading import Lock
from typing import TYPE_CHECKING, Dict, List, MutableMapping, Sequence

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
from services.policy.adaptive_horizon import get_horizon


from exchange_adapter import get_exchange_adapter, get_exchange_adapters_status
from metrics import record_abstention_rate, record_drift_score, setup_metrics
from services.common.security import ADMIN_ACCOUNTS
from shared.graceful_shutdown import flush_logging_handlers, setup_graceful_shutdown


FEES_SERVICE_URL = os.getenv("FEES_SERVICE_URL", "http://fees-service")
FEES_REQUEST_TIMEOUT = float(os.getenv("FEES_REQUEST_TIMEOUT", "1.0"))
CONFIDENCE_THRESHOLD = float(os.getenv("POLICY_CONFIDENCE_THRESHOLD", "0.55"))
ENABLE_SHADOW_EXECUTION = os.getenv("ENABLE_SHADOW_EXECUTION", "true").lower() in {
    "1",
    "true",
    "yes",
}
SHADOW_CLIENT_SUFFIX = os.getenv("SHADOW_CLIENT_SUFFIX", "-shadow")
DEFAULT_EXCHANGE = os.getenv("PRIMARY_EXCHANGE", "kraken")

MODEL_VARIANTS: List[str] = ["trend_model", "meanrev_model", "vol_breakout"]
DEFAULT_MODEL_SHARPES: Dict[str, float] = {
    "trend_model": 1.0,
    "meanrev_model": 0.9,
    "vol_breakout": 1.1,
}

_ATR_CACHE: Dict[str, float] = {}

KRAKEN_PRECISION: Dict[str, Dict[str, float]] = {
    "BTC-USD": {"tick": 0.1, "lot": 0.0001},
    "ETH-USD": {"tick": 0.01, "lot": 0.001},
    "SOL-USD": {"tick": 0.001, "lot": 0.01},
}


logger = logging.getLogger(__name__)


SHUTDOWN_TIMEOUT = float(os.getenv("POLICY_SHUTDOWN_TIMEOUT", os.getenv("SERVICE_SHUTDOWN_TIMEOUT", "60.0")))

app = FastAPI(title="Policy Service", version="2.0.0")
setup_metrics(app, service_name="policy-service")
EXCHANGE_ADAPTER = get_exchange_adapter(DEFAULT_EXCHANGE)

shutdown_manager = setup_graceful_shutdown(
    app,
    service_name="policy-service",
    allowed_paths={"/", "/docs", "/openapi.json"},
    shutdown_timeout=SHUTDOWN_TIMEOUT,
    logger_instance=logger,
)


if TYPE_CHECKING:  # pragma: no cover - type checking only
    from services.models.model_server import Intent


def _predict_intent(**kwargs) -> "Intent":
    from services.models.model_server import predict_intent as _predict

    return _predict(**kwargs)


def predict_intent(**kwargs) -> "Intent":
    """Compatibility wrapper for callers patching predict_intent."""

    return _predict_intent(**kwargs)


@app.get("/exchange/adapters", tags=["exchange"])
async def list_exchange_adapters() -> List[Dict[str, object]]:
    """Expose discovery metadata for configured exchange adapters."""

    return await get_exchange_adapters_status()


@dataclass
class RegimeSnapshot:
    """Container holding the latest regime classification for a symbol."""

    symbol: str
    regime: str
    volatility: float
    trend_strength: float
    feature_scale: float
    size_scale: float
    sample_count: int
    updated_at: datetime

    def as_payload(self) -> Dict[str, float | int | str]:
        return {
            "symbol": self.symbol,
            "regime": self.regime,
            "volatility": round(self.volatility, 6),
            "trend_strength": round(self.trend_strength, 6),
            "feature_scale": round(self.feature_scale, 4),
            "size_scale": round(self.size_scale, 4),
            "sample_count": self.sample_count,
            "updated_at": self.updated_at.isoformat(),
        }


class RegimeClassifier:
    """Rolling-volatility regime classifier with lightweight trend detection."""

    def __init__(
        self,
        window: int = 50,
        min_samples: int = 5,
        high_vol_threshold: float = 0.012,
        trend_signal_threshold: float = 1.35,
        feature_scale_map: Dict[str, float] | None = None,
        size_scale_map: Dict[str, float] | None = None,
    ) -> None:
        self.window = max(window, 5)
        self.min_samples = max(min_samples, 2)
        self.high_vol_threshold = max(high_vol_threshold, 0.0)
        self.trend_signal_threshold = max(trend_signal_threshold, 0.0)
        self._prices: MutableMapping[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self.window)
        )
        self._snapshots: Dict[str, RegimeSnapshot] = {}
        self._lock = Lock()
        self._feature_scale_map = feature_scale_map or {
            "trend": 1.1,
            "range": 1.0,
            "high_vol": 0.85,
        }
        self._size_scale_map = size_scale_map or {
            "trend": 1.15,
            "range": 0.85,
            "high_vol": 0.6,
        }

    def observe(self, symbol: str, price: float) -> RegimeSnapshot:
        norm_symbol = symbol.upper()
        with self._lock:
            price_series = self._prices[norm_symbol]
            if price > 0:
                price_series.append(float(price))
            volatility = self._compute_volatility(price_series)
            trend_strength = self._compute_trend_strength(price_series)
            regime = self._classify(volatility, trend_strength, len(price_series))
            feature_scale = self._feature_scale_map.get(regime, 1.0)
            size_scale = self._size_scale_map.get(regime, 1.0)
            snapshot = RegimeSnapshot(
                symbol=norm_symbol,
                regime=regime,
                volatility=volatility,
                trend_strength=trend_strength,
                feature_scale=feature_scale,
                size_scale=size_scale,
                sample_count=len(price_series),
                updated_at=datetime.now(timezone.utc),
            )
            self._snapshots[norm_symbol] = snapshot
            return snapshot

    def get_snapshot(self, symbol: str) -> RegimeSnapshot | None:
        norm_symbol = symbol.upper()
        with self._lock:
            snapshot = self._snapshots.get(norm_symbol)
            return replace(snapshot) if snapshot is not None else None

    def _compute_volatility(self, prices: Sequence[float]) -> float:
        if len(prices) < 2:
            return 0.0
        series = list(prices)
        log_returns: List[float] = []
        previous = series[0]
        for price in series[1:]:
            if previous <= 0 or price <= 0:
                continue
            log_returns.append(math.log(price / previous))
            previous = price
        if not log_returns:
            return 0.0
        mean_return = sum(log_returns) / len(log_returns)
        variance = sum((ret - mean_return) ** 2 for ret in log_returns) / len(log_returns)
        return math.sqrt(max(variance, 0.0))

    def _compute_trend_strength(self, prices: Sequence[float]) -> float:
        series = list(prices)
        count = len(series)
        if count < 2:
            return 0.0
        x_values = range(count)
        mean_x = (count - 1) / 2.0
        mean_y = sum(series) / float(count)
        numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, series))
        denominator = sum((x - mean_x) ** 2 for x in x_values)
        if denominator <= 0:
            return 0.0
        slope = numerator / denominator
        latest_price = series[-1] if series[-1] != 0 else 1.0
        return slope / latest_price

    def _classify(self, volatility: float, trend_strength: float, sample_count: int) -> str:
        if sample_count < self.min_samples:
            return "range"
        if volatility >= self.high_vol_threshold:
            return "high_vol"
        signal = abs(trend_strength) / max(volatility, 1e-6)
        if signal >= self.trend_signal_threshold:
            return "trend"
        return "range"


regime_classifier = RegimeClassifier()


def _flush_policy_event_buffers() -> None:
    """Flush buffered policy events/loggers prior to shutdown."""

    flush_logging_handlers("", __name__)
    snapshot_count = len(regime_classifier._snapshots)  # noqa: SLF001 - intentional diagnostic access
    if snapshot_count:
        logger.info(
            "Draining cached regime snapshots before shutdown",
            extra={"snapshot_count": snapshot_count},
        )


shutdown_manager.register_flush_callback(_flush_policy_event_buffers)


def _default_state() -> PolicyState:
    return PolicyState(regime="unknown", volatility=0.0, liquidity_score=0.0, conviction=0.0)


def _reset_regime_state() -> None:
    """Reset cached regime state. Intended for test isolation."""

    with regime_classifier._lock:  # type: ignore[attr-defined]
        regime_classifier._prices.clear()  # type: ignore[attr-defined]
        regime_classifier._snapshots.clear()  # type: ignore[attr-defined]



def _resolve_precision(symbol: str) -> Dict[str, float]:
    return KRAKEN_PRECISION.get(symbol.upper(), {"tick": 0.01, "lot": 0.0001})


def _snap(value: float, step: float) -> float:

    if step <= 0:
        return float(value)
    quant = Decimal(str(step))
    snapped = (Decimal(str(value)) / quant).to_integral_value(rounding=ROUND_HALF_UP) * quant
    return float(snapped)




def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _model_sharpe_weights() -> Dict[str, float]:
    weights: Dict[str, float] = {}
    for variant in MODEL_VARIANTS:
        env_key = f"POLICY_{variant.upper()}_SHARPE"
        default = DEFAULT_MODEL_SHARPES.get(variant, 1.0)
        raw_value = os.getenv(env_key)
        try:
            weight = float(raw_value) if raw_value is not None else default
        except (TypeError, ValueError):
            weight = default
        weights[variant] = max(weight, 0.0)
    return weights


def _normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    filtered = {key: max(value, 0.0) for key, value in weights.items()}
    total = sum(filtered.values())
    if total <= 0:
        if not MODEL_VARIANTS:
            return {}
        uniform = 1.0 / float(len(MODEL_VARIANTS))
        return {variant: uniform for variant in MODEL_VARIANTS}
    return {key: (value / total if total > 0 else 0.0) for key, value in filtered.items()}


def _blend_confidence(intents: Dict[str, "Intent"], weights: Dict[str, float]) -> ConfidenceMetrics:
    model_conf = 0.0
    state_conf = 0.0
    exec_conf = 0.0
    overall = 0.0

    for key, intent in intents.items():
        weight = weights.get(key, 0.0)
        if weight <= 0:
            continue
        confidence = intent.confidence
        model_conf += confidence.model_confidence * weight
        state_conf += confidence.state_confidence * weight
        exec_conf += confidence.execution_confidence * weight
        overall += (confidence.overall_confidence or 0.0) * weight

    blended = ConfidenceMetrics(
        model_confidence=round(_clamp(model_conf), 4),
        state_confidence=round(_clamp(state_conf), 4),
        execution_confidence=round(_clamp(exec_conf), 4),
        overall_confidence=round(_clamp(overall), 4),
    )
    return blended


def _blend_template_confidences(intents: Dict[str, "Intent"], weights: Dict[str, float]) -> Dict[str, float]:
    confidence_totals: Dict[str, float] = defaultdict(float)
    weight_totals: Dict[str, float] = defaultdict(float)

    for key, intent in intents.items():
        weight = weights.get(key, 0.0)
        if weight <= 0:
            continue
        for template in intent.action_templates or []:
            template_key = template.name.lower()
            confidence_totals[template_key] += template.confidence * weight
            weight_totals[template_key] += weight

    blended: Dict[str, float] = {}
    for template_key, total_conf in confidence_totals.items():
        weight = weight_totals.get(template_key, 0.0)
        if weight <= 0:
            blended[template_key] = 0.0
        else:
            blended[template_key] = round(_clamp(total_conf / weight), 4)
    return blended


def _vote_entropy(votes: Dict[str, float]) -> float:
    total = sum(value for value in votes.values() if value > 0)
    if total <= 0:
        return 0.0
    entropy = 0.0
    for value in votes.values():
        if value <= 0:
            continue
        probability = value / total
        entropy -= probability * math.log(probability)
    return entropy


def _resolve_atr(symbol: str, snapshot: BookSnapshot, state: PolicyState | None) -> float:
    symbol_key = symbol.upper()

    atr_candidates: List[float] = []
    if state is not None:
        try:
            atr_candidates.append(float(getattr(state, "volatility", 0.0)) * 100.0)
        except (TypeError, ValueError):
            pass

    try:
        atr_candidates.append(float(snapshot.spread_bps) * 1.5)
    except (TypeError, ValueError):
        pass

    atr = next((value for value in atr_candidates if value and math.isfinite(value) and value > 0), None)
    if atr is None:
        atr = _ATR_CACHE.get(symbol_key, 10.0)
    else:
        previous = _ATR_CACHE.get(symbol_key)
        if previous is not None:
            atr = 0.5 * previous + 0.5 * atr
        _ATR_CACHE[symbol_key] = atr

    atr = max(_ATR_CACHE.get(symbol_key, atr), 1.0)
    _ATR_CACHE[symbol_key] = atr
    return atr


def _resolve_risk_band(
    request_value: float | None,
    intents: Dict[str, "Intent"],
    weights: Dict[str, float],
    attribute: str,
) -> float | None:
    """Resolve a blended risk band while honouring overrides."""

    def _coerce(value: object | None) -> float | None:
        try:
            numeric = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric) or numeric <= 0:
            return None
        return numeric

    candidate = _coerce(request_value)
    if candidate is not None:
        return candidate

    weighted_total = 0.0
    total_weight = 0.0
    fallback: float | None = None

    for variant, intent in intents.items():
        value = _coerce(getattr(intent, attribute, None))
        if value is None:
            continue
        if fallback is None:
            fallback = value
        weight = weights.get(variant, 0.0)
        if weight <= 0:
            continue
        weighted_total += value * weight
        total_weight += weight

    if total_weight > 0:
        return weighted_total / total_weight

    return fallback



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
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Fee service response malformed",
        )

    try:
        return float(payload["bps"])
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
    try:
        from services.models.model_server import predict_intent as _predict  # noqa: F401
    except ImportError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model server unavailable",
        ) from exc
    return {"status": "ready"}


@app.get("/policy/regime", tags=["policy"])
async def get_regime(symbol: str) -> Dict[str, float | int | str]:
    snapshot = regime_classifier.get_snapshot(symbol)
    if snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No regime information available for symbol",
        )
    return snapshot.as_payload()


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

    state_model = request.state or _default_state()
    state_payload = (
        state_model.model_dump()
        if hasattr(state_model, "model_dump")
        else {
            "regime": getattr(state_model, "regime", "unknown"),
            "volatility": getattr(state_model, "volatility", 0.0),
            "liquidity_score": getattr(state_model, "liquidity_score", 0.0),
            "conviction": getattr(state_model, "conviction", 0.0),
        }
    )
    horizon_features = {
        "symbol": request.instrument,
        "regime": state_payload.get("regime"),
        "state": state_payload,
        "features": list(request.features or []),
    }
    horizon_seconds = get_horizon(horizon_features)
    features: List[float] = [float(value) for value in request.features]


    raw_weights = _model_sharpe_weights()
    weights = _normalize_weights(raw_weights)

    intents: Dict[str, "Intent"] = {}
    for variant in MODEL_VARIANTS:
        intents[variant] = predict_intent(
            account_id=request.account_id,
            symbol=request.instrument,
            features=features,
            book_snapshot=book_snapshot,
            model_variant=variant,
            horizon=horizon_seconds,
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

    confidence = _blend_confidence(intents, weights)
    caller_confidence = request.confidence
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

    expected_edge = 0.0
    for variant, intent in intents.items():
        expected_edge += weights.get(variant, 0.0) * float(intent.edge_bps or 0.0)

    slippage_bps = float(request.slippage_bps or 0.0)
    maker_edge = round(expected_edge - (effective_fee.maker + slippage_bps), 4)
    taker_edge = round(expected_edge - (effective_fee.taker + slippage_bps), 4)

    template_confidences = _blend_template_confidences(intents, weights)
    maker_confidence = _clamp(template_confidences.get("maker", confidence.execution_confidence))
    taker_confidence = _clamp(
        template_confidences.get("taker", confidence.execution_confidence * 0.95)
    )

    action_templates = [
        ActionTemplate(
            name="maker",
            venue_type="maker",
            edge_bps=maker_edge,
            fee_bps=round(effective_fee.maker, 4),
            confidence=round(maker_confidence, 4),
        ),
        ActionTemplate(
            name="taker",
            venue_type="taker",
            edge_bps=taker_edge,
            fee_bps=round(effective_fee.taker, 4),
            confidence=round(taker_confidence, 4),
        ),
    ]

    vote_totals: Dict[str, float] = defaultdict(float)
    for variant, intent in intents.items():
        action = (intent.selected_action or "abstain").lower()
        if action not in {"maker", "taker"}:
            action = "abstain"
        vote_totals[action] += weights.get(variant, 0.0)
    for label in ("maker", "taker", "abstain"):
        vote_totals.setdefault(label, 0.0)

    action_priority = {"maker": 2, "taker": 1, "abstain": 0}
    winning_action = max(
        vote_totals.items(),
        key=lambda item: (item[1], action_priority.get(item[0], -1)),
    )[0]

    entropy = _vote_entropy(vote_totals)

    selected_template = next(
        (
            template
            for template in action_templates
            if template.name.lower() == winning_action
        ),
        None,
    )
    if selected_template is None and action_templates:
        selected_template = max(action_templates, key=lambda template: template.edge_bps)

    fee_adjusted_edge = selected_template.edge_bps if selected_template else 0.0
    approval_score = sum(
        weights.get(variant, 0.0) for variant, intent in intents.items() if intent.approved
    )

    atr = _resolve_atr(request.instrument, book_snapshot, state_model)
    take_profit_override = _resolve_risk_band(
        request.take_profit_bps, intents, weights, "take_profit_bps"
    )
    stop_loss_override = _resolve_risk_band(
        request.stop_loss_bps, intents, weights, "stop_loss_bps"
    )
    take_profit = take_profit_override if take_profit_override is not None else 3.0 * atr
    stop_loss = stop_loss_override if stop_loss_override is not None else 2.0 * atr

    approved = (
        winning_action in {"maker", "taker"}
        and approval_score >= 0.5
        and fee_adjusted_edge > 0
        and (confidence.overall_confidence or 0.0) >= CONFIDENCE_THRESHOLD
        and entropy <= 0.3
    )

    reason: str | None = None
    selected_action = winning_action if winning_action in {"maker", "taker"} else "abstain"
    if not approved:

        if fee_adjusted_edge <= 0:
            reason = "Fee-adjusted edge non-positive"
        elif entropy > 0.3:
            reason = "High ensemble entropy"
        elif winning_action not in {"maker", "taker"}:
            reason = "Ensemble voted to abstain"
        elif approval_score < 0.5:
            reason = "Insufficient ensemble approval"
        elif (confidence.overall_confidence or 0.0) < CONFIDENCE_THRESHOLD:
            reason = "Confidence below threshold"
        else:
            reason = "Fee-adjusted edge non-positive"

        selected_action = "abstain"
        fee_adjusted_edge = min(fee_adjusted_edge, 0.0)
    else:
        selected_action = selected_template.name if selected_template else selected_action


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

    await _dispatch_shadow_orders(request, response)

    return response


async def _dispatch_shadow_orders(
    request: PolicyDecisionRequest, response: PolicyDecisionResponse
) -> None:
    """Submit the primary execution as well as the paper shadow copy."""

    if not response.approved or response.selected_action.lower() == "abstain":
        return

    await _submit_execution(request, response, shadow=False)

    if not ENABLE_SHADOW_EXECUTION:
        return

    try:
        await _submit_execution(request, response, shadow=True)
    except Exception as exc:  # pragma: no cover - best-effort shadow dispatch
        logger.warning(
            "Shadow execution submission failed for order %s: %s",
            request.order_id,
            exc,
        )


async def _submit_execution(
    request: PolicyDecisionRequest,
    response: PolicyDecisionResponse,
    *,
    shadow: bool,
) -> None:
    """Submit the execution payload to the configured OMS endpoint."""

    precision = _resolve_precision(request.instrument)
    snapped_price = _snap(request.price, precision["tick"])
    snapped_qty = _snap(request.quantity, precision["lot"])

    order_type = "limit" if response.selected_action.lower() == "maker" else "market"
    client_id = request.order_id
    if shadow and SHADOW_CLIENT_SUFFIX:
        client_id = f"{client_id}{SHADOW_CLIENT_SUFFIX}"

    payload: Dict[str, object] = {
        "account_id": request.account_id,
        "client_id": client_id,
        "symbol": request.instrument,
        "side": request.side.lower(),
        "order_type": order_type,
        "qty": snapped_qty,
        "post_only": response.selected_action.lower() == "maker",
        "reduce_only": False,
        "flags": [],
        "shadow": shadow,
    }
    if order_type == "limit":
        payload["limit_px"] = snapped_price

    if not EXCHANGE_ADAPTER.supports("place_order"):
        raise RuntimeError(
            f"Exchange adapter '{EXCHANGE_ADAPTER.name}' does not support order placement"
        )

    try:
        await EXCHANGE_ADAPTER.place_order(request.account_id, payload, shadow=shadow)
    except httpx.HTTPError as exc:
        if shadow:
            raise
        logger.error(
            "Primary OMS submission failed for order %s: %s",
            request.order_id,
            exc,
        )
        raise
