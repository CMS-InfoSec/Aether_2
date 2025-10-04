from __future__ import annotations

import os
from decimal import Decimal
from typing import Callable, Iterator

import pytest
from fastapi.testclient import TestClient

from auth.service import InMemorySessionStore

os.environ.setdefault("SESSION_REDIS_URL", "memory://policy-tests")

import services.common.security as security
import policy_service
from services.common.schemas import ActionTemplate, ConfidenceMetrics
from services.models.model_server import Intent


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


@pytest.fixture(name="client")
def _client(
    session_store: InMemorySessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[TestClient]:
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


def _intent() -> Intent:
    return Intent(
        edge_bps=22.0,
        confidence=ConfidenceMetrics(
            model_confidence=0.8,
            state_confidence=0.78,
            execution_confidence=0.76,
            overall_confidence=0.8,
        ),
        take_profit_bps=25.0,
        stop_loss_bps=12.0,
        selected_action="maker",
        action_templates=[
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=18.0,
                fee_bps=0.0,
                confidence=0.9,
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=12.0,
                fee_bps=0.0,
                confidence=0.85,
            ),
        ],
        approved=True,
        reason=None,
    )


def test_policy_service_decision(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    auth_headers: Callable[[str], dict[str, str]],
) -> None:
    async def _fake_fee(
        account_id: str, symbol: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        assert account_id == "company"
        assert symbol == "BTC-USD"
        assert liquidity in {"maker", "taker"}
        dec_notional = notional if isinstance(notional, Decimal) else Decimal(str(notional))
        expected_notional = Decimal("30120.5") * Decimal("0.1235")
        assert dec_notional == expected_notional
        return Decimal("4.5")

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: _intent())

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

    response = client.post("/policy/decide", json=payload, headers=auth_headers("company"))

    assert response.status_code == 200
    body = response.json()
    assert body["approved"] is True
    assert body["selected_action"] == "maker"
    assert body["expected_edge_bps"] == pytest.approx(22.0)
    assert body["fee_adjusted_edge_bps"] == pytest.approx(17.5)
    assert body["take_profit_bps"] == pytest.approx(25.0)
    assert body["stop_loss_bps"] == pytest.approx(12.0)
