"""Rules-based fallback policy used when model inference is unavailable."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Iterable, List, Sequence

from services.common.schemas import (
    ActionTemplate,
    BookSnapshot,
    ConfidenceMetrics,
    FeeBreakdown,
    PolicyDecisionRequest,
    PolicyDecisionResponse,
    PolicyState,
)
from shared.spot import filter_spot_symbols, is_spot_symbol, normalize_spot_symbol


fallback_log = logging.getLogger("fallback_log")


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def _default_state() -> PolicyState:
    return PolicyState(regime="unknown", volatility=0.0, liquidity_score=0.0, conviction=0.0)


def _momentum_score(features: Sequence[float], state: PolicyState) -> float:
    if state.conviction:
        try:
            conviction = float(state.conviction)
        except (TypeError, ValueError):
            conviction = 0.0
    else:
        conviction = 0.0

    bounded_features: List[float] = []
    for value in features:
        try:
            bounded_features.append(float(value))
        except (TypeError, ValueError):
            continue

    if not bounded_features:
        return _clamp(conviction)

    positives = [value for value in bounded_features if value > 0]
    if not positives:
        return _clamp(conviction)

    average_positive = sum(positives) / len(positives)
    normalized = average_positive / (1.0 + abs(average_positive))
    combined = max(normalized, conviction)
    return _clamp(combined)


@dataclass
class FallbackDecision:
    """Container holding the adjusted request and deterministic response."""

    request: PolicyDecisionRequest
    response: PolicyDecisionResponse
    reason: str


@dataclass
class FallbackPolicy:
    """Simple deterministic policy engaged when ML inference is unavailable."""

    top_symbols: Sequence[str]
    size_fraction: float = 0.35
    momentum_threshold: float = 0.6
    max_risk_band_bps: float = 25.0
    _top_symbol_set: set[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        canonical_symbols = filter_spot_symbols(self.top_symbols, logger=fallback_log)
        if not canonical_symbols:
            raise ValueError(
                "FallbackPolicy requires at least one top liquidity spot symbol"
            )

        self.top_symbols = tuple(canonical_symbols)
        self._top_symbol_set = set(self.top_symbols)
        self.size_fraction = _clamp(float(self.size_fraction), lower=0.05, upper=1.0)
        self.momentum_threshold = _clamp(float(self.momentum_threshold), lower=0.0, upper=1.0)
        self.max_risk_band_bps = max(float(self.max_risk_band_bps), 1.0)

    def evaluate(
        self,
        *,
        request: PolicyDecisionRequest,
        book_snapshot: BookSnapshot,
        reason: str,
    ) -> FallbackDecision:
        symbol = normalize_spot_symbol(request.instrument)
        state = request.state or _default_state()

        if not symbol or not is_spot_symbol(symbol):
            response = self._abstain_response(
                request=request,
                state=state,
                book_snapshot=book_snapshot,
                reason="Instrument is not a supported spot market pair",
            )
            return FallbackDecision(request=request, response=response, reason=reason)

        if symbol not in self._top_symbol_set:
            response = self._abstain_response(
                request=request,
                state=state,
                book_snapshot=book_snapshot,
                reason="Instrument outside top liquidity universe",
            )
            return FallbackDecision(request=request, response=response, reason=reason)

        scaled_quantity = max(request.quantity * self.size_fraction, 0.0)
        adjusted_request = request.copy(update={"quantity": scaled_quantity})

        momentum = _momentum_score(request.features, state)
        if momentum < self.momentum_threshold:
            response = self._abstain_response(
                request=adjusted_request,
                state=state,
                book_snapshot=book_snapshot,
                reason="Momentum below fallback threshold",
            )
            return FallbackDecision(request=adjusted_request, response=response, reason=reason)

        slippage = float(request.slippage_bps or 0.0)
        maker_fee = float(request.fee.maker)
        taker_fee = float(request.fee.taker)

        expected_edge = max((momentum - self.momentum_threshold) * 50.0, 0.0)
        maker_edge = expected_edge - maker_fee - slippage
        taker_edge = expected_edge - taker_fee - slippage - 1.0

        risk_cap = self.max_risk_band_bps
        take_profit_bps = min(float(request.take_profit_bps or risk_cap), risk_cap)
        stop_loss_bps = min(float(request.stop_loss_bps or (risk_cap * 0.5)), risk_cap)

        execution_confidence = 0.55 if maker_edge > 0 else 0.35
        confidence = ConfidenceMetrics(
            model_confidence=0.3,
            state_confidence=_clamp(momentum),
            execution_confidence=_clamp(execution_confidence),
        )

        action_templates = [
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=round(maker_edge, 4),
                fee_bps=round(maker_fee, 4),
                confidence=round(confidence.execution_confidence, 4),
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=round(taker_edge, 4),
                fee_bps=round(taker_fee, 4),
                confidence=round(confidence.execution_confidence * 0.9, 4),
            ),
        ]

        selected_action = "maker" if maker_edge > 0 else "abstain"
        approved = maker_edge > 0 and maker_edge > taker_edge and maker_edge > 0

        reason_detail = None if approved else "Fee-adjusted edge non-positive"

        response = PolicyDecisionResponse(
            approved=approved,
            reason=reason_detail,
            effective_fee=FeeBreakdown(
                currency=request.fee.currency,
                maker=round(maker_fee, 4),
                taker=round(taker_fee, 4),
                maker_detail=request.fee.maker_detail,
                taker_detail=request.fee.taker_detail,
            ),
            expected_edge_bps=round(expected_edge, 4),
            fee_adjusted_edge_bps=round(maker_edge if approved else min(maker_edge, 0.0), 4),
            selected_action=selected_action,
            action_templates=action_templates,
            confidence=confidence,
            features=list(request.features),
            book_snapshot=book_snapshot,
            state=state,
            take_profit_bps=round(take_profit_bps, 4),
            stop_loss_bps=round(stop_loss_bps, 4),
        )

        return FallbackDecision(request=adjusted_request, response=response, reason=reason)

    def _abstain_response(
        self,
        *,
        request: PolicyDecisionRequest,
        state: PolicyState,
        book_snapshot: BookSnapshot,
        reason: str,
    ) -> PolicyDecisionResponse:
        confidence = ConfidenceMetrics(
            model_confidence=0.2,
            state_confidence=_clamp(float(state.conviction or 0.0)),
            execution_confidence=0.2,
        )

        action_templates = [
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=-request.fee.maker,
                fee_bps=round(request.fee.maker, 4),
                confidence=round(confidence.execution_confidence, 4),
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=-request.fee.taker,
                fee_bps=round(request.fee.taker, 4),
                confidence=round(confidence.execution_confidence, 4),
            ),
        ]

        return PolicyDecisionResponse(
            approved=False,
            reason=reason,
            effective_fee=FeeBreakdown(
                currency=request.fee.currency,
                maker=round(request.fee.maker, 4),
                taker=round(request.fee.taker, 4),
                maker_detail=request.fee.maker_detail,
                taker_detail=request.fee.taker_detail,
            ),
            expected_edge_bps=0.0,
            fee_adjusted_edge_bps=-max(request.fee.maker, request.fee.taker),
            selected_action="abstain",
            action_templates=action_templates,
            confidence=confidence,
            features=list(request.features),
            book_snapshot=book_snapshot,
            state=state,
            take_profit_bps=min(float(request.take_profit_bps or self.max_risk_band_bps), self.max_risk_band_bps),
            stop_loss_bps=min(float(request.stop_loss_bps or (self.max_risk_band_bps * 0.5)), self.max_risk_band_bps),
        )

    def log_activation(self, *, reason: str, duration: float, tags: Iterable[str] | None = None) -> None:
        extra_tags = list(tags or [])
        fallback_log.info(
            "fallback_policy_activated",
            extra={
                "reason": reason,
                "duration_seconds": round(duration, 4),
                "tags": extra_tags,
            },
        )

