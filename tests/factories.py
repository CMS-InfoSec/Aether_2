"""Factories for constructing representative domain payloads used in tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from services.common.schemas import (
    ActionTemplate,
    BookSnapshot,
    ConfidenceMetrics,
    FeeBreakdown,
    FeeDetail,
    PolicyDecisionRequest,
    PolicyState,
)
from services.common.schemas import PolicyDecisionPayload as SchemaPolicyDecisionPayload
from services.common.schemas import PolicyDecisionResponse
from services.common.schemas import RiskIntentPayload
from services.common.schemas import RiskIntentMetrics as SchemaRiskIntentMetrics


def fee_detail(**overrides: Any) -> FeeDetail:
    defaults = {
        "bps": 10.0,
        "usd": 1.0,
        "tier_id": "tier_1",
        "basis_ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }
    defaults.update(overrides)
    return FeeDetail(**defaults)


def fee_breakdown(**overrides: Any) -> FeeBreakdown:
    defaults = {
        "currency": "USD",
        "maker": 5.0,
        "taker": 7.5,
        "maker_detail": fee_detail(tier_id="maker"),
        "taker_detail": fee_detail(tier_id="taker"),
    }
    defaults.update(overrides)
    return FeeBreakdown(**defaults)


def confidence(**overrides: Any) -> ConfidenceMetrics:
    defaults = {
        "model_confidence": 0.9,
        "state_confidence": 0.85,
        "execution_confidence": 0.8,
        "overall_confidence": 0.85,
    }
    defaults.update(overrides)
    return ConfidenceMetrics(**defaults)


def book_snapshot(**overrides: Any) -> BookSnapshot:
    defaults = {"mid_price": 25000.0, "spread_bps": 5.0, "imbalance": 0.1}
    defaults.update(overrides)
    return BookSnapshot(**defaults)


def policy_state(**overrides: Any) -> PolicyState:
    defaults = {
        "regime": "bull",
        "volatility": 0.2,
        "liquidity_score": 0.7,
        "conviction": 0.8,
    }
    defaults.update(overrides)
    return PolicyState(**defaults)


def action_templates() -> List[ActionTemplate]:
    return [
        ActionTemplate(name="maker", venue_type="maker", edge_bps=25.0, fee_bps=5.0, confidence=0.8),
        ActionTemplate(name="taker", venue_type="taker", edge_bps=20.0, fee_bps=7.5, confidence=0.75),
    ]


def policy_decision_request(**overrides: Any) -> PolicyDecisionRequest:
    defaults = {
        "account_id": "company",
        "order_id": "ORD-1",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 0.5,
        "price": 25000.0,
        "fee": fee_breakdown(),
        "features": [0.1, 0.2, 0.3],
        "book_snapshot": book_snapshot(),
        "state": policy_state(),
        "confidence": confidence(),
        "take_profit_bps": 50.0,
        "stop_loss_bps": 25.0,
    }
    defaults.update(overrides)
    return PolicyDecisionRequest(**defaults)


def policy_decision_response(
    *,
    approved: bool = True,
    expected_edge_bps: float = 30.0,
    fee_adjusted_edge_bps: float = 24.0,
    selected_action: str = "maker",
    templates: Optional[Iterable[ActionTemplate]] = None,
    **overrides: Any,
) -> PolicyDecisionResponse:
    defaults: Dict[str, Any] = {
        "approved": approved,
        "reason": None,
        "effective_fee": fee_breakdown(),
        "expected_edge_bps": expected_edge_bps,
        "fee_adjusted_edge_bps": fee_adjusted_edge_bps,
        "selected_action": selected_action,
        "action_templates": list(templates or action_templates()),
        "confidence": confidence(),
        "features": [0.1, 0.2, 0.3],
        "book_snapshot": book_snapshot(),
        "state": policy_state(),
        "take_profit_bps": 50.0,
        "stop_loss_bps": 25.0,
    }
    defaults.update(overrides)
    return PolicyDecisionResponse(**defaults)


def policy_decision_payload(**overrides: Any) -> SchemaPolicyDecisionPayload:
    defaults = {
        "request": policy_decision_request(),
        "response": policy_decision_response(),
    }
    defaults.update(overrides)
    return SchemaPolicyDecisionPayload(**defaults)


def risk_metrics(**overrides: Any) -> SchemaRiskIntentMetrics:
    defaults = {
        "net_exposure": 100_000.0,
        "gross_notional": 125_000.0,
        "projected_loss": 500.0,
        "projected_fee": 75.0,
        "var_95": 2500.0,
        "spread_bps": 8.0,
        "latency_ms": 120.0,
    }
    defaults.update(overrides)
    return SchemaRiskIntentMetrics(**defaults)


def risk_intent_payload(**overrides: Any) -> RiskIntentPayload:
    defaults = {
        "policy_decision": policy_decision_payload(),
        "metrics": risk_metrics(),
        "book_snapshot": book_snapshot(),
        "state": policy_state(),
        "confidence": confidence(),
    }
    defaults.update(overrides)
    return RiskIntentPayload(**defaults)

