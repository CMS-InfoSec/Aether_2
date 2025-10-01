"""Integration tests for the standalone policy service module."""

from __future__ import annotations

import sys
import types
from decimal import Decimal

from typing import List


import pytest
from fastapi.testclient import TestClient

if "metrics" not in sys.modules:
    metrics_stub = types.ModuleType("metrics")
    metrics_stub.setup_metrics = lambda app: None
    metrics_stub.record_abstention_rate = lambda *args, **kwargs: None
    metrics_stub.record_drift_score = lambda *args, **kwargs: None
    sys.modules["metrics"] = metrics_stub

import policy_service

from services.common.schemas import (
    ActionTemplate,
    BookSnapshot,
    ConfidenceMetrics,
    FeeBreakdown,
    PolicyDecisionRequest,
    PolicyDecisionResponse,
    PolicyState,
)

from services.models.model_server import Intent


@pytest.fixture(name="client")
def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def _noop_place_order(account_id: str, payload: dict, shadow: bool = False):
        return {"order_id": payload.get("client_id", "order")}

    monkeypatch.setattr(policy_service.EXCHANGE_ADAPTER, "place_order", _noop_place_order)
    monkeypatch.setattr(policy_service.EXCHANGE_ADAPTER, "supports", lambda op: True)
    return TestClient(policy_service.app)



def _intent(*, edge_bps: float, approved: bool, selected: str, reason: str | None = None) -> Intent:

    return Intent(
        edge_bps=edge_bps,
        confidence=ConfidenceMetrics(
            model_confidence=0.8,
            state_confidence=0.78,
            execution_confidence=0.76,
            overall_confidence=0.8,
        ),
        take_profit_bps=25.0,
        stop_loss_bps=12.0,
        selected_action=selected,
        action_templates=[
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=edge_bps,
                fee_bps=0.0,
                confidence=0.91,
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=edge_bps - 4.0,
                fee_bps=0.0,
                confidence=0.82,
            ),
        ],
        approved=approved,
        reason=reason,
    )



def _validate_response(payload: dict) -> PolicyDecisionResponse:
    return PolicyDecisionResponse.model_validate(payload)

def test_policy_decide_approves_when_edge_beats_costs(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    recorded: List[dict[str, object]] = []
    dispatched: List[dict[str, object]] = []

    async def _fake_fee(account_id: str, symbol: str, liquidity: str, notional: float) -> float:
        recorded.append(
            {
                "account_id": account_id,
                "symbol": symbol,
                "liquidity": liquidity,
                "notional": notional,
            }
        )
        return {"maker": 4.5, "taker": 7.5}[liquidity]

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(
        policy_service,
        "predict_intent",
        lambda **_: _intent(edge_bps=22.0, approved=True, selected="maker"),
    )

    async def _fake_dispatch(
        request_obj: PolicyDecisionRequest, response_obj: PolicyDecisionResponse
    ) -> None:
        dispatched.append({"order_id": request_obj.order_id, "approved": response_obj.approved})

    monkeypatch.setattr(policy_service, "_dispatch_shadow_orders", _fake_dispatch)

    payload = {

        "account_id": "company",
        "order_id": "abc-123",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 0.1234567,

        "price": 30120.4567,
        "fee": {"currency": "USD", "maker": 4.0, "taker": 6.0},
        "features": [0.4, -0.1, 2.8],
        "book_snapshot": {"mid_price": 30125.4, "spread_bps": 2.4, "imbalance": 0.05},
    }

    response = client.post("/policy/decide", json=payload)
    assert response.status_code == 200


    body = _validate_response(response.json())

    assert body.approved is True
    assert body.selected_action == "maker"
    assert body.effective_fee.maker == pytest.approx(4.5)
    assert body.effective_fee.taker == pytest.approx(7.5)
    assert body.expected_edge_bps == pytest.approx(22.0)
    assert body.fee_adjusted_edge_bps == pytest.approx(17.5)

    maker_template = next(template for template in body.action_templates if template.name == "maker")
    taker_template = next(template for template in body.action_templates if template.name == "taker")
    assert maker_template.edge_bps == pytest.approx(17.5)
    assert maker_template.fee_bps == pytest.approx(4.5)
    assert taker_template.edge_bps == pytest.approx(14.5)
    assert taker_template.fee_bps == pytest.approx(7.5)

    assert body.features == pytest.approx(payload["features"])  # type: ignore[arg-type]
    assert body.book_snapshot.mid_price == pytest.approx(payload["book_snapshot"]["mid_price"])
    assert body.state.regime == "unknown"
    assert body.take_profit_bps == pytest.approx(25.0)
    assert body.stop_loss_bps == pytest.approx(12.0)

    snapped_price = Decimal("30120.5")
    snapped_quantity = Decimal("0.1235")
    expected_notional = float(snapped_price * snapped_quantity)
    assert recorded == [
        {
            "account_id": "company",
            "symbol": "BTC-USD",
            "liquidity": "maker",
            "notional": pytest.approx(expected_notional),
        },
        {
            "account_id": "company",
            "symbol": "BTC-USD",
            "liquidity": "taker",
            "notional": pytest.approx(expected_notional),
        },
    ]
    assert dispatched == [{"order_id": "abc-123", "approved": True}]


def test_policy_decide_rejects_when_slippage_erodes_edge(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    async def _fake_fee(account_id: str, symbol: str, liquidity: str, notional: float) -> float:
        return {"maker": 4.0, "taker": 6.0}[liquidity]

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(
        policy_service,
        "predict_intent",
        lambda **_: _intent(edge_bps=10.0, approved=True, selected="maker"),
    )

    payload = {
        "account_id": "company",
        "order_id": "edge-001",
        "instrument": "ETH-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 2100.0,
        "fee": {"currency": "USD", "maker": 4.0, "taker": 6.0},
        "features": [0.1, -0.2, 0.3],
        "slippage_bps": 8.0,
        "book_snapshot": {"mid_price": 2100.0, "spread_bps": 3.0, "imbalance": 0.0},
    }

    response = client.post("/policy/decide", json=payload)
    assert response.status_code == 200

    body = response.json()
    assert body["approved"] is False
    assert body["reason"] == "Fee-adjusted edge non-positive"
    assert body["fee_adjusted_edge_bps"] <= 0


def test_policy_decide_honours_request_risk_overrides(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    async def _fake_fee(account_id: str, symbol: str, liquidity: str, notional: float) -> float:
        return 4.5

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(
        policy_service,
        "predict_intent",
        lambda **_: _intent(edge_bps=24.0, approved=True, selected="maker"),
    )

    payload = {
        "account_id": "company",
        "order_id": "abc-456",
        "instrument": "BTC-USD",
        "side": "SELL",
        "quantity": 0.5,
        "price": 20000.0,
        "fee": {"currency": "USD", "maker": 4.0, "taker": 6.0},
        "features": [0.2, 0.0, 1.5],
        "book_snapshot": {"mid_price": 20010.0, "spread_bps": 3.0, "imbalance": -0.02},
        "take_profit_bps": 18.0,
        "stop_loss_bps": 9.0,
    }

    response = client.post("/policy/decide", json=payload)
    assert response.status_code == 200

    body = _validate_response(response.json())
    assert body.take_profit_bps == pytest.approx(18.0)
    assert body.stop_loss_bps == pytest.approx(9.0)


def test_policy_decide_rejects_unknown_account(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    async def _fake_fee(*_: object, **__: object) -> float:
        return 4.5

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(
        policy_service,
        "predict_intent",
        lambda **_: _intent(
            edge_bps=22.0,
            maker_edge=18.0,
            taker_edge=12.0,
            approved=True,
            selected="maker",
        ),
    )

    payload = {
        "account_id": "shadow-account",
        "symbol": "BTC-USD",
        "side": "buy",
        "qty": 0.1234567,
        "price": 30120.4567,
        "impact_bps": 1.0,
        "features": [0.1, 0.2],
        "book_snapshot": {"mid_price": 30125.4, "spread_bps": 2.4, "imbalance": 0.05},
    }

    response = client.post("/policy/decide", json=payload)
    assert response.status_code == 422


def test_policy_decide_rejects_when_costs_exceed_edge(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    async def _fake_fee(account_id: str, symbol: str, liquidity: str, notional: float) -> float:
        return {"maker": 18.0, "taker": 21.0}[liquidity]

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(
        policy_service,
        "predict_intent",
        lambda **_: _intent(edge_bps=10.0, approved=True, selected="maker"),
    )

    payload = {
        "account_id": "company",
        "order_id": "def-456",
        "instrument": "BTC-USD",
        "side": "SELL",
        "quantity": 0.25,
        "price": 25000.0,
        "fee": {"currency": "USD", "maker": 4.0, "taker": 6.0},
        "features": [0.1, 0.2],
        "book_snapshot": {"mid_price": 25010.0, "spread_bps": 4.0, "imbalance": -0.2},
    }

    response = client.post("/policy/decide", json=payload)
    assert response.status_code == 200

    body = _validate_response(response.json())

    assert body.approved is False
    assert body.selected_action == "abstain"
    assert body.reason == "Fee-adjusted edge non-positive"
    assert body.fee_adjusted_edge_bps == pytest.approx(-8.0)

    maker_template = next(template for template in body.action_templates if template.name == "maker")
    assert maker_template.edge_bps <= 0.0


@pytest.mark.anyio("asyncio")
async def test_dispatch_shadow_orders_invokes_shadow_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    submitted: List[dict[str, object]] = []

    async def _fake_submit(
        request_obj: PolicyDecisionRequest,
        response_obj: PolicyDecisionResponse,
        *,
        shadow: bool,
    ) -> None:
        submitted.append({
            "shadow": shadow,
            "client_id": request_obj.order_id,
        })

    monkeypatch.setattr(policy_service, "_submit_execution", _fake_submit)
    monkeypatch.setattr(policy_service, "ENABLE_SHADOW_EXECUTION", True, raising=False)

    state = PolicyState(regime="bull", volatility=0.1, liquidity_score=0.2, conviction=0.3)
    book = BookSnapshot(mid_price=20000.0, spread_bps=2.0, imbalance=0.1)
    request_obj = PolicyDecisionRequest(
        account_id="company",
        order_id="order-1",
        instrument="BTC-USD",
        side="BUY",
        quantity=0.5,
        price=20050.0,
        fee=FeeBreakdown(currency="USD", maker=4.0, taker=6.0),
        features=[0.1, 0.2],
        book_snapshot=book,
        state=state,
    )
    response_obj = PolicyDecisionResponse(
        approved=True,
        reason=None,
        effective_fee=request_obj.fee,
        expected_edge_bps=12.0,
        fee_adjusted_edge_bps=8.0,
        selected_action="maker",
        action_templates=[
            ActionTemplate(name="maker", venue_type="maker", edge_bps=8.0, fee_bps=4.0, confidence=0.9),
            ActionTemplate(name="taker", venue_type="taker", edge_bps=6.0, fee_bps=6.0, confidence=0.85),
        ],
        confidence=ConfidenceMetrics(
            model_confidence=0.9,
            state_confidence=0.9,
            execution_confidence=0.9,
            overall_confidence=0.9,
        ),
        features=request_obj.features,
        book_snapshot=book,
        state=state,
        take_profit_bps=20.0,
        stop_loss_bps=10.0,
    )

    await policy_service._dispatch_shadow_orders(request_obj, response_obj)

    assert submitted == [
        {"shadow": False, "client_id": "order-1"},
        {"shadow": True, "client_id": "order-1"},
    ]


@pytest.mark.anyio("asyncio")
async def test_dispatch_shadow_orders_skips_when_not_approved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: List[bool] = []

    async def _fake_submit(*args: object, **kwargs: object) -> None:
        called.append(True)

    monkeypatch.setattr(policy_service, "_submit_execution", _fake_submit)
    monkeypatch.setattr(policy_service, "ENABLE_SHADOW_EXECUTION", True, raising=False)

    state = PolicyState(regime="bear", volatility=0.5, liquidity_score=0.1, conviction=0.2)
    book = BookSnapshot(mid_price=15000.0, spread_bps=3.0, imbalance=-0.05)
    request_obj = PolicyDecisionRequest(
        account_id="company",
        order_id="order-2",
        instrument="ETH-USD",
        side="SELL",
        quantity=1.25,
        price=14950.0,
        fee=FeeBreakdown(currency="USD", maker=3.0, taker=5.0),
        features=[-0.1, 0.3],
        book_snapshot=book,
        state=state,
    )
    response_obj = PolicyDecisionResponse(
        approved=False,
        reason="Risk rejected",
        effective_fee=request_obj.fee,
        expected_edge_bps=5.0,
        fee_adjusted_edge_bps=-1.0,
        selected_action="abstain",
        action_templates=[
            ActionTemplate(name="maker", venue_type="maker", edge_bps=-1.0, fee_bps=3.0, confidence=0.7),
            ActionTemplate(name="taker", venue_type="taker", edge_bps=-2.0, fee_bps=5.0, confidence=0.6),
        ],
        confidence=ConfidenceMetrics(
            model_confidence=0.7,
            state_confidence=0.6,
            execution_confidence=0.5,
            overall_confidence=0.6,
        ),
        features=request_obj.features,
        book_snapshot=book,
        state=state,
        take_profit_bps=0.0,
        stop_loss_bps=0.0,
    )

    await policy_service._dispatch_shadow_orders(request_obj, response_obj)

    assert called == []


@pytest.mark.anyio("asyncio")
async def test_fetch_effective_fee_parses_flat_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    response_payload = {
        "bps": "5.25",
        "usd": 1.23,
        "tier_id": "tier_1",
        "basis_ts": "2023-01-01T00:00:00+00:00",
    }
    recorded: dict[str, object] = {}

    monkeypatch.setattr(policy_service, "FEES_SERVICE_URL", "https://fees.test", raising=False)
    monkeypatch.setattr(policy_service, "FEES_REQUEST_TIMEOUT", 1.5, raising=False)

    class _DummyResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            recorded["status_checked"] = True

        def json(self) -> dict[str, object]:
            return self._payload

    class _DummyClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            recorded["client_kwargs"] = kwargs

        async def __aenter__(self) -> "_DummyClient":
            recorded["entered"] = True
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: object,
        ) -> None:
            recorded["exited"] = True

        async def get(
            self,
            path: str,
            *,
            params: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
        ) -> _DummyResponse:
            recorded["request"] = {
                "path": path,
                "params": params,
                "headers": headers,
            }
            return _DummyResponse(response_payload)

    monkeypatch.setattr(policy_service.httpx, "AsyncClient", _DummyClient)

    fee = await policy_service._fetch_effective_fee(
        account_id="acct-123",
        symbol="ETH-USD",
        liquidity="MaKeR",
        notional=123.456789,
    )

    assert fee == pytest.approx(5.25)
    request = recorded["request"]
    assert request["path"] == "/fees/effective"
    assert request["headers"] == {"X-Account-ID": "acct-123"}
    assert request["params"] == {
        "pair": "ETH-USD",
        "liquidity": "maker",
        "notional": "123.45678900",
    }
    assert recorded["status_checked"] is True


def test_health_endpoints(client: TestClient) -> None:
    health = client.get("/health")
    ready = client.get("/ready")

    assert health.status_code == 200
    assert ready.status_code == 200
    assert health.json() == {"status": "ok"}
    assert ready.json() == {"status": "ready"}
