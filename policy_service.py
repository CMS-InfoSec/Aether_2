"""FastAPI microservice that produces policy decisions based on model intents."""

from __future__ import annotations


import math

import os
from collections import defaultdict, deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from threading import Lock
from typing import Dict, List, MutableMapping, Sequence

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
from services.common.security import ADMIN_ACCOUNTS


FEES_SERVICE_URL = os.getenv("FEES_SERVICE_URL", "http://fees-service")
FEES_REQUEST_TIMEOUT = float(os.getenv("FEES_REQUEST_TIMEOUT", "1.0"))
CONFIDENCE_THRESHOLD = float(os.getenv("POLICY_CONFIDENCE_THRESHOLD", "0.55"))
OMS_SERVICE_URL = os.getenv("OMS_SERVICE_URL", "http://oms-service")
PAPER_OMS_SERVICE_URL = os.getenv("PAPER_OMS_SERVICE_URL", "http://paper-oms-service")
OMS_REQUEST_TIMEOUT = float(os.getenv("OMS_REQUEST_TIMEOUT", "1.0"))
ENABLE_SHADOW_EXECUTION = os.getenv("ENABLE_SHADOW_EXECUTION", "true").lower() in {
    "1",
    "true",
    "yes",
}
SHADOW_CLIENT_SUFFIX = os.getenv("SHADOW_CLIENT_SUFFIX", "-shadow")

KRAKEN_PRECISION: Dict[str, Dict[str, float]] = {
    "BTC-USD": {"tick": 0.1, "lot": 0.0001},
    "ETH-USD": {"tick": 0.01, "lot": 0.001},
    "SOL-USD": {"tick": 0.001, "lot": 0.01},
}


logger = logging.getLogger(__name__)


app = FastAPI(title="Policy Service", version="2.0.0")
setup_metrics(app)


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



def _scale_features(values: Sequence[float], multiplier: float) -> List[float]:
    if not values:
        return []
    return [float(value) * multiplier for value in values]



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
    if predict_intent is None:  # pragma: no cover - defensive guard
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model server unavailable",
        )
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
    regime_snapshot = regime_classifier.observe(
        request.instrument,
        book_snapshot.mid_price,
    )
    features: List[float] = _scale_features(request.features, regime_snapshot.feature_scale)

    if request.state is not None:
        state_model = request.state.model_copy(deep=True)
    else:
        state_model = _default_state()
    state_model.regime = regime_snapshot.regime
    state_model.volatility = round(regime_snapshot.volatility, 6)
    base_conviction = float(getattr(state_model, "conviction", 0.0))
    regime_conviction = min(max(regime_snapshot.size_scale, 0.0), 1.0)
    state_model.conviction = round(
        min(1.0, max(0.0, (base_conviction + regime_conviction) / 2.0)),
        4,
    )


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
        overall_conf = confidence.overall_confidence or 0.0
        if overall_conf < CONFIDENCE_THRESHOLD:
            reason = "Confidence below threshold"
        elif fee_adjusted_edge <= 0:
            reason = "Fee-adjusted edge non-positive"
        elif reason is None:
            reason = "Intent rejected by policy"
        selected_action = "abstain"
        fee_adjusted_edge = min(fee_adjusted_edge, 0.0)
    else:
        selected_action = selected_template.name if selected_template else selected_action

    take_profit = request.take_profit_bps or intent.take_profit_bps or 0.0
    stop_loss = request.stop_loss_bps or intent.stop_loss_bps or 0.0

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

    base_url = PAPER_OMS_SERVICE_URL if shadow else OMS_SERVICE_URL
    if not base_url:
        return

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

    headers = {"X-Account-ID": request.account_id}
    timeout = httpx.Timeout(OMS_REQUEST_TIMEOUT)
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        try:
            response = await client.post("/oms/place", json=payload, headers=headers)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            if shadow:
                raise
            logger.error(
                "Primary OMS submission failed for order %s: %s",
                request.order_id,
                exc,
            )
            raise
