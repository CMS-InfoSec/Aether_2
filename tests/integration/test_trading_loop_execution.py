from __future__ import annotations

import asyncio
import importlib
import sys
from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from services.common.schemas import PolicyDecisionResponse
from tests import factories
from tests.fixtures.mock_kraken import MockKrakenServer
from tests.helpers.authentication import override_admin_auth


@pytest.mark.integration
@pytest.mark.slow
def test_trading_loop_places_kraken_order(
    monkeypatch: pytest.MonkeyPatch, kraken_mock_server: MockKrakenServer
) -> None:
    """End-to-end policy decision to Kraken order submission flow."""

    sys.modules.pop("policy_service", None)
    policy_service = importlib.import_module("policy_service")

    policy_client = TestClient(policy_service.app)

    confidence = factories.confidence(overall_confidence=0.9)
    intent_stub = policy_service.Intent(
        edge_bps=32.0,
        confidence=confidence,
        take_profit_bps=48.0,
        stop_loss_bps=20.0,
        selected_action="maker",
        action_templates=list(factories.action_templates()),
        approved=True,
        reason=None,
    )
    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: intent_stub)
    policy_service.ENABLE_SHADOW_EXECUTION = False

    async def _fake_fetch_effective_fee(
        account_id: str, symbol: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        _ = (account_id, symbol, liquidity, notional)
        return Decimal("5.0")

    async def _noop_submit_execution(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fetch_effective_fee)
    monkeypatch.setattr(policy_service, "_submit_execution", _noop_submit_execution)

    account_id = "company"
    instrument = "BTC-USD"
    requested_qty = 0.25
    requested_price = 30_050.0
    order_id = "ORD-E2E-42"

    policy_request = factories.policy_decision_request(
        account_id=account_id,
        order_id=order_id,
        instrument=instrument,
        side="BUY",
        quantity=requested_qty,
        price=requested_price,
        features=[0.12, 0.24, 0.36],
    )

    with override_admin_auth(
        policy_client.app, policy_service.require_admin_account, account_id
    ) as policy_headers:
        request_headers = {**policy_headers, "X-Account-ID": account_id}
        policy_response_raw = policy_client.post(
            "/policy/decide",
            json=policy_request.model_dump(mode="json"),
            headers=request_headers,
        )
    policy_response_raw.raise_for_status()
    decision = PolicyDecisionResponse.model_validate(policy_response_raw.json())

    assert decision.approved is True
    assert decision.selected_action == "maker"

    # Simulate an approved risk decision with a slight size adjustment.
    risk_decision = SimpleNamespace(pass_=True, reasons=[], adjusted_qty=requested_qty * 0.9)
    approved_qty = float(risk_decision.adjusted_qty or requested_qty)
    assert approved_qty < requested_qty

    order_response = asyncio.run(
        kraken_mock_server.add_order(
            pair=instrument.replace("-", "/"),
            side="buy",
            volume=approved_qty,
            price=None,
            ordertype="market",
            account=account_id,
            userref=order_id,
        )
    )

    fills = order_response.get("fills", [])
    assert fills, "Kraken mock should report executed fills"

    filled_qty = sum(float(fill["volume"]) for fill in fills)
    assert pytest.approx(filled_qty, rel=1e-6) == pytest.approx(approved_qty, rel=1e-6)

    trades = asyncio.run(
        kraken_mock_server.get_trades(
            account=account_id, pair=instrument.replace("-", "/")
        )
    )
    assert trades, "Trade history should include executed orders"

    avg_price = sum(float(fill["price"]) * float(fill["volume"]) for fill in fills) / filled_qty
    balances = asyncio.run(kraken_mock_server.get_balance(account=account_id))
    base, quote = instrument.split("-")
    assert balances[base] == pytest.approx(filled_qty, rel=1e-6)
    expected_quote_delta = -avg_price * filled_qty
    assert balances[quote] == pytest.approx(expected_quote_delta, rel=1e-6)

    slippage_bps = (avg_price - requested_price) / requested_price * 10_000
    assert abs(slippage_bps) < 50, "Slippage should remain within realistic thresholds"
