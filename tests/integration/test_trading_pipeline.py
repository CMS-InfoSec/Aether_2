from __future__ import annotations

import contextlib
import importlib
import sys
import types
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import pytest

pytest.importorskip("services.common.security")
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from services.common.schemas import ActionTemplate, ConfidenceMetrics, PolicyDecisionResponse
from tests import factories
from tests.helpers.authentication import override_admin_auth
from tests.helpers.risk import risk_service_instance


@dataclass
class DummyIntent:
    account_id: str
    instrument: str
    side: str
    quantity: float
    price: float
    features: list[float]


@dataclass
class FillEvent:
    order_id: str
    quantity: float
    fee: float
    status: str


class Sequencer:
    """Coordinates policy and risk services for integration testing."""

    def __init__(self, *, policy_client: TestClient, risk_client: TestClient, risk_module) -> None:
        self._policy_client = policy_client
        self._risk_client = risk_client
        self._risk_module = risk_module

    def submit_intent(self, intent: DummyIntent, order_id: str = "ORD-PIPELINE-1") -> dict[str, object]:
        policy_request = factories.policy_decision_request(
            account_id=intent.account_id,
            order_id=order_id,
            instrument=intent.instrument,
            side=intent.side.upper(),
            quantity=intent.quantity,
            price=intent.price,
            features=intent.features,
        )

        policy_response = self._policy_client.post(
            "/policy/decide",
            json=policy_request.model_dump(mode="json"),
            headers={"X-Account-ID": intent.account_id},
        )
        policy_response.raise_for_status()
        decision = PolicyDecisionResponse.model_validate(policy_response.json())

        trade_intent = self._risk_module.TradeIntent(
            policy_id=order_id,
            instrument_id=intent.instrument,
            side=intent.side.lower(),
            quantity=intent.quantity,
            price=intent.price,
        )
        portfolio_state = self._risk_module.AccountPortfolioState(
            net_asset_value=2_000_000.0,
            notional_exposure=125_000.0,
            realized_daily_loss=0.0,
            fees_paid=0.0,
            instrument_exposure={intent.instrument: 125_000.0},
        )
        risk_request = self._risk_module.RiskValidationRequest(
            account_id=intent.account_id,
            intent=trade_intent,
            portfolio_state=portfolio_state,
        )

        with override_admin_auth(
            self._risk_client.app,
            self._risk_module.require_admin_account,
            intent.account_id,
        ) as headers:
            risk_headers = {**headers, "X-Account-ID": intent.account_id}
            risk_response = self._risk_client.post(
                "/risk/validate",
                json=risk_request.model_dump(by_alias=True, mode="json"),
                headers=risk_headers,
            )
        risk_response.raise_for_status()
        risk_decision = self._risk_module.RiskValidationResponse.model_validate(risk_response.json())

        return {
            "policy_request": policy_request,
            "policy_response": decision,
            "risk_request": risk_request,
            "risk_response": risk_decision,
        }


@pytest.mark.integration
@pytest.mark.slow
def test_trading_pipeline_emits_fill_event(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    metrics_stub = types.SimpleNamespace(
        setup_metrics=lambda *args, **kwargs: None,
        record_abstention_rate=lambda *args, **kwargs: None,
        record_drift_score=lambda *args, **kwargs: None,
        increment_trade_rejection=lambda *args, **kwargs: None,
        record_fees_nav_pct=lambda *args, **kwargs: None,
        increment_oms_child_orders_total=lambda *args, **kwargs: None,
        increment_oms_error_count=lambda *args, **kwargs: None,
        record_oms_latency=lambda *args, **kwargs: None,
        record_ws_latency=lambda *args, **kwargs: None,
        record_oms_submit_ack=lambda *args, **kwargs: None,
        record_scaling_state=lambda *args, **kwargs: None,
        observe_scaling_evaluation=lambda *args, **kwargs: None,
        observe_risk_validation_latency=lambda *args, **kwargs: None,
        traced_span=lambda *args, **kwargs: contextlib.nullcontext(),
        get_request_id=lambda: None,
    )
    sys.modules["metrics"] = metrics_stub

    monkeypatch.setenv("ENABLE_SHADOW_EXECUTION", "false")

    fees_app = FastAPI()

    @fees_app.get("/fees/effective")
    async def effective_fee(pair: str, liquidity: str, notional: str, request: Request) -> dict[str, object]:
        if request.headers.get("X-Account-ID") is None:
            raise HTTPException(status_code=401, detail="missing account header")
        notional_value = float(notional)
        bps = 4.2 if liquidity.lower() == "maker" else 7.9
        fee_usd = notional_value * bps / 10000.0
        basis_ts = datetime.now(timezone.utc).isoformat()
        return {"bps": bps, "usd": fee_usd, "tier_id": "tier-1", "basis_ts": basis_ts}

    oms_app = FastAPI()
    fill_events: list[FillEvent] = []

    @oms_app.post("/oms/place")
    async def place_order(payload: dict, request: Request) -> dict[str, object]:
        if request.headers.get("X-Account-ID") is None:
            raise HTTPException(status_code=401, detail="missing account header")
        quantity = float(payload["qty"])
        fee_amount = round(quantity * 0.25, 6)
        event = FillEvent(
            order_id=str(payload["client_id"]),
            quantity=quantity,
            fee=fee_amount,
            status="FILLED",
        )
        fill_events.append(event)
        return {"status": "accepted", "txid": f"tx-{payload['client_id']}"}

    fees_client = TestClient(fees_app)
    oms_client = TestClient(oms_app)

    sys.modules.pop("policy_service", None)
    policy_module = importlib.import_module("policy_service")

    with risk_service_instance(tmp_path, monkeypatch) as risk_module:
        confidence = ConfidenceMetrics(
            model_confidence=0.9,
            state_confidence=0.88,
            execution_confidence=0.86,
            overall_confidence=0.9,
        )
        intent_stub = policy_module.Intent(
            edge_bps=38.0,
            confidence=confidence,
            take_profit_bps=45.0,
            stop_loss_bps=20.0,
            selected_action="maker",
            action_templates=[
                ActionTemplate(
                    name="maker", venue_type="maker", edge_bps=38.0, fee_bps=0.0, confidence=0.86
                ),
                ActionTemplate(
                    name="taker", venue_type="taker", edge_bps=18.0, fee_bps=0.0, confidence=0.8
                ),
            ],
            approved=True,
            reason=None,
        )
        monkeypatch.setattr(policy_module, "predict_intent", lambda **_: intent_stub)
        policy_module.ENABLE_SHADOW_EXECUTION = False

        async def fake_fetch_effective_fee(
            account_id: str, symbol: str, liquidity: str, notional: float | Decimal
        ) -> Decimal:
            notional_decimal = notional if isinstance(notional, Decimal) else Decimal(str(notional))
            notional_str = f"{notional_decimal.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)}"
            response = fees_client.get(
                "/fees/effective",
                params={"pair": symbol, "liquidity": liquidity, "notional": notional_str},
                headers={"X-Account-ID": account_id},
            )
            response.raise_for_status()
            return Decimal(str(response.json()["bps"]))

        async def fake_submit_execution(
            request, response, *, shadow: bool, actor: str | None = None
        ) -> None:
            if shadow:
                return
            precision = await policy_module._resolve_precision(request.instrument)
            snapped_price = policy_module._snap(request.price, precision["tick"])
            snapped_qty = policy_module._snap(request.quantity, precision["lot"])
            order_type = "limit" if response.selected_action.lower() == "maker" else "market"
            client_id = request.order_id
            payload = {
                "client_id": client_id,
                "symbol": request.instrument,
                "side": request.side.lower(),
                "qty": snapped_qty,
                "order_type": order_type,
            }
            if order_type == "limit":
                payload["limit_px"] = snapped_price
            response_obj = oms_client.post(
                "/oms/place",
                json=payload,
                headers={"X-Account-ID": request.account_id},
            )
            response_obj.raise_for_status()

        monkeypatch.setattr(policy_module, "_fetch_effective_fee", fake_fetch_effective_fee)
        monkeypatch.setattr(policy_module, "_submit_execution", fake_submit_execution)

        history = [
            {"high": 30200.0 + idx, "low": 29800.0 + idx, "close": 30000.0 + idx}
            for idx in range(30)
        ]
        risk_module.set_stub_price_history("BTC-USD", history)
        risk_module.set_stub_account_returns("company", [50.0, -20.0, 10.0, -5.0] * 70)
        timestamp = datetime.now(timezone.utc).isoformat()
        risk_module.set_stub_fills(
            [{"account_id": "company", "timestamp": timestamp, "pnl": 0.0, "fee": 0.0}]
        )
        risk_module.set_stub_market_telemetry(
            "BTC-USD", {"spread_bps": 4.0, "latency_seconds": 0.6, "exchange_outage": 0}
        )

        with TestClient(policy_module.app) as policy_client, TestClient(risk_module.app) as risk_client:
            sequencer = Sequencer(
                policy_client=policy_client,
                risk_client=risk_client,
                risk_module=risk_module,
            )
            intent = DummyIntent(
                account_id="company",
                instrument="BTC-USD",
                side="buy",
                quantity=0.75,
                price=30120.0,
                features=[0.4, -0.1, 2.3],
            )
            result = sequencer.submit_intent(intent)

        policy_response = result["policy_response"]
        risk_response = result["risk_response"]

        assert policy_response.approved is True
        assert policy_response.selected_action == "maker"
        assert policy_response.effective_fee.maker == pytest.approx(4.2)
        assert policy_response.effective_fee.taker == pytest.approx(7.9)

        assert risk_response.pass_ is True
        assert not risk_response.reasons

    assert len(fill_events) == 1
    fill = fill_events[0]
    assert fill.order_id == result["policy_request"].order_id
    assert fill.quantity == pytest.approx(intent.quantity)
    assert fill.fee == pytest.approx(round(intent.quantity * 0.25, 6))
    assert fill.status == "FILLED"
