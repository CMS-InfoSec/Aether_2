from __future__ import annotations

import contextlib
import os
import sys
import types
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Iterator

import pytest
from fastapi.testclient import TestClient

from auth.service import InMemorySessionStore

os.environ.setdefault("SESSION_REDIS_URL", "memory://policy-tests")

import services.common.security as security
from services.common.schemas import ActionTemplate, ConfidenceMetrics, PolicyDecisionResponse
from tests import factories


if "metrics" not in sys.modules:
    class _TransportMember:
        def __init__(self, value: str) -> None:
            self.value = value

        def __str__(self) -> str:
            return self.value

    class _TransportType:
        UNKNOWN = _TransportMember("unknown")
        INTERNAL = _TransportMember("internal")
        REST = _TransportMember("rest")
        WEBSOCKET = _TransportMember("websocket")
        FIX = _TransportMember("fix")
        BATCH = _TransportMember("batch")

    metrics_stub = types.SimpleNamespace(
        record_abstention_rate=lambda *args, **kwargs: None,
        record_drift_score=lambda *args, **kwargs: None,
        setup_metrics=lambda *args, **kwargs: None,
        get_request_id=lambda *args, **kwargs: "test-request",
        TransportType=_TransportType,
    )
    metrics_stub.bind_metric_context = lambda *args, **kwargs: contextlib.nullcontext()
    metrics_stub.metric_context = lambda *args, **kwargs: contextlib.nullcontext()
    sys.modules["metrics"] = metrics_stub

import policy_service


@dataclass
class StaticIntent:
    edge_bps: float
    confidence: ConfidenceMetrics
    take_profit_bps: float
    stop_loss_bps: float
    selected_action: str
    action_templates: list[ActionTemplate]
    approved: bool


@pytest.fixture(name="session_store")
def _session_store() -> Iterator[InMemorySessionStore]:
    previous_store = getattr(policy_service.app.state, "session_store", None)
    previous_global = getattr(policy_service, "SESSION_STORE", None)
    previous_default = getattr(security, "_DEFAULT_SESSION_STORE", None)

    store = InMemorySessionStore()
    policy_service.app.state.session_store = store
    policy_service.SESSION_STORE = store
    security.set_default_session_store(store)

    try:
        yield store
    finally:
        if previous_store is None:
            policy_service.app.state.session_store = None
        else:
            policy_service.app.state.session_store = previous_store
        policy_service.SESSION_STORE = previous_global
        security.set_default_session_store(previous_default)


@pytest.fixture(name="policy_client")
def _policy_client(
    monkeypatch: pytest.MonkeyPatch,
    session_store: InMemorySessionStore,
) -> Iterator[TestClient]:
    async def _health_ok() -> bool:
        return True

    monkeypatch.setattr(policy_service, "_model_health_ok", _health_ok)
    monkeypatch.setattr(policy_service, "_enforce_stablecoin_guard", lambda: None)

    async def _noop_dispatch(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(policy_service, "_dispatch_shadow_orders", _noop_dispatch)
    policy_service._ATR_CACHE.clear()
    policy_service._reset_regime_state()
    del session_store
    monkeypatch.setattr(
        "shared.graceful_shutdown.install_sigterm_handler",
        lambda manager: None,
    )
    async def _noop_place_order(*args: object, **kwargs: object) -> dict[str, str]:
        return {"order_id": "test"}

    monkeypatch.setattr(
        policy_service.EXCHANGE_ADAPTER,
        "place_order",
        _noop_place_order,
    )
    with TestClient(policy_service.app) as client:
        yield client


@pytest.fixture
def auth_headers(session_store: InMemorySessionStore) -> Callable[[str], dict[str, str]]:
    def _build(account: str) -> dict[str, str]:
        session = session_store.create(account)
        return {
            "Authorization": f"Bearer {session.token}",
            "X-Account-ID": account,
        }

    return _build


def _intent(edge_bps: float) -> StaticIntent:
    return StaticIntent(
        edge_bps=edge_bps,
        confidence=factories.confidence(overall_confidence=0.9),
        take_profit_bps=45.0,
        stop_loss_bps=22.5,
        selected_action="maker",
        action_templates=[
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=edge_bps,
                fee_bps=0.0,
                confidence=0.85,
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=edge_bps - 1.0,
                fee_bps=0.0,
                confidence=0.8,
            ),
        ],
        approved=True,
    )


def test_fee_gate_rejects_when_edge_below_total_cost(
    monkeypatch: pytest.MonkeyPatch,
    policy_client: TestClient,
    auth_headers: Callable[[str], dict[str, str]],
) -> None:
    monkeypatch.setattr(policy_service, "MODEL_VARIANTS", ["alpha"], raising=False)
    monkeypatch.setattr(policy_service, "_model_sharpe_weights", lambda: {"alpha": 1.0})

    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: _intent(10.0))

    async def _high_fee(
        account_id: str, instrument: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        del account_id, instrument, notional
        return {"maker": Decimal("7.0"), "taker": Decimal("7.0")}[liquidity]

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _high_fee)

    request = factories.policy_decision_request(
        slippage_bps=5.0,
        fee=factories.fee_breakdown(maker=7.0, taker=7.0),
    )

    response = policy_client.post(
        "/policy/decide",
        json=request.model_dump(mode="json"),
        headers=auth_headers(request.account_id),
    )
    assert response.status_code == 200

    decision = PolicyDecisionResponse.model_validate(response.json())
    assert decision.approved is False
    assert decision.selected_action == "abstain"
    assert decision.fee_adjusted_edge_bps == pytest.approx(-2.0)
    assert decision.reason == "Fee-adjusted edge non-positive"


def test_fee_gate_approves_when_edge_exceeds_total_cost(
    monkeypatch: pytest.MonkeyPatch,
    policy_client: TestClient,
    auth_headers: Callable[[str], dict[str, str]],
) -> None:
    monkeypatch.setattr(policy_service, "MODEL_VARIANTS", ["alpha"], raising=False)
    monkeypatch.setattr(policy_service, "_model_sharpe_weights", lambda: {"alpha": 1.0})

    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: _intent(18.0))

    async def _moderate_fee(
        account_id: str, instrument: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        del account_id, instrument, notional
        return {"maker": Decimal("4.0"), "taker": Decimal("5.5")}[liquidity]

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _moderate_fee)

    request = factories.policy_decision_request(
        slippage_bps=2.0,
        fee=factories.fee_breakdown(maker=4.0, taker=5.5),
    )

    response = policy_client.post(
        "/policy/decide",
        json=request.model_dump(mode="json"),
        headers=auth_headers(request.account_id),
    )
    assert response.status_code == 200

    decision = PolicyDecisionResponse.model_validate(response.json())
    assert decision.approved is True
    assert decision.selected_action == "maker"
    assert decision.fee_adjusted_edge_bps == pytest.approx(12.0)

    maker_template, taker_template = decision.action_templates
    assert maker_template.edge_bps == pytest.approx(12.0)
    assert maker_template.fee_bps == pytest.approx(4.0)
    assert taker_template.edge_bps == pytest.approx(10.5)
    assert taker_template.fee_bps == pytest.approx(5.5)
