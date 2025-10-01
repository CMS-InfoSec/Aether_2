"""End-to-end integration test that exercises the trading pipeline."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient
from prometheus_client import generate_latest

import policy_service
import risk_service
import safe_mode
import metrics as metrics_module
from metrics import (
    increment_trades_submitted,
    observe_policy_inference_latency,
    observe_risk_validation_latency,
    set_oms_latency,
    set_pipeline_latency,
)
from sequencer import PipelineHistory, SequencerPipeline, Stage, StageResult
from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.common.schemas import PolicyDecisionResponse
from services.fees.fee_service import app as fees_app
from tests import factories
from tests.fixtures.mock_kraken import MockKrakenServer


@pytest.mark.integration
@pytest.mark.slow
def test_full_pipeline_records_audit_metrics_and_safe_mode(
    monkeypatch: pytest.MonkeyPatch, kraken_mock_server: MockKrakenServer
) -> None:
    """Run a realistic pipeline flow against mocked dependencies."""

    # ------------------------------------------------------------------
    # Global state hygiene
    # ------------------------------------------------------------------
    TimescaleAdapter.reset()
    KafkaNATSAdapter.reset()
    safe_mode.controller.reset()
    safe_mode.clear_safe_mode_log()

    # ------------------------------------------------------------------
    # Policy service configuration and stubs
    # ------------------------------------------------------------------
    policy_client = TestClient(policy_service.app)

    confidence = factories.confidence(
        model_confidence=0.92,
        state_confidence=0.9,
        execution_confidence=0.88,
        overall_confidence=0.91,
    )
    intent_stub = policy_service.Intent(
        edge_bps=42.0,
        confidence=confidence,
        take_profit_bps=55.0,
        stop_loss_bps=25.0,
        selected_action="maker",
        action_templates=list(factories.action_templates()),
        approved=True,
        reason=None,
    )
    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: intent_stub)
    policy_service.ENABLE_SHADOW_EXECUTION = False

    fees_client = TestClient(fees_app)

    async def fake_fetch_effective_fee(
        account_id: str, symbol: str, liquidity: str, notional: float
    ) -> float:
        response = fees_client.get(
            "/fees/effective",
            params={
                "pair": symbol,
                "liquidity": liquidity,
                "notional": f"{max(notional, 0.0):.8f}",
            },
            headers={"X-Account-ID": account_id},
        )
        response.raise_for_status()
        payload = response.json()
        return float(payload["bps"])

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", fake_fetch_effective_fee)

    # ------------------------------------------------------------------
    # Risk service configuration and stubs
    # ------------------------------------------------------------------
    risk_client = TestClient(risk_service.app)

    class DummyLimits:
        def __init__(self) -> None:
            self.account_id = "company"
            self.max_daily_loss = 250_000.0
            self.fee_budget = 50_000.0
            self.max_nav_pct_per_trade = 0.35
            self.notional_cap = 5_000_000.0
            self.cooldown_minutes = 0

    async def fake_evaluate(context: risk_service.RiskEvaluationContext) -> risk_service.RiskValidationResponse:
        return risk_service.RiskValidationResponse(pass_=True, reasons=[])

    monkeypatch.setattr(risk_service, "_load_account_limits", lambda _: DummyLimits())
    monkeypatch.setattr(risk_service, "_evaluate", fake_evaluate)
    monkeypatch.setattr(risk_service, "_refresh_usage_from_fills", lambda *_, **__: None)

    # ------------------------------------------------------------------
    # Helper stage implementations invoking the HTTP services
    # ------------------------------------------------------------------
    async def policy_stage(payload: Dict[str, Any], ctx) -> StageResult:
        intent = payload["intent"]
        request_model = factories.policy_decision_request(
            account_id=intent["account_id"],
            order_id=intent["order_id"],
            instrument=intent["instrument"],
            side=intent["side"].upper(),
            quantity=float(intent["quantity"]),
            price=float(intent["price"]),
            features=list(intent["features"]),
        )
        response = await asyncio.to_thread(
            policy_client.post,
            "/policy/decide",
            json=request_model.model_dump(mode="json"),
            headers={"X-Account-ID": intent["account_id"]},
        )
        response.raise_for_status()
        decision = PolicyDecisionResponse.model_validate(response.json())
        artifact = decision.model_dump(mode="json")
        new_payload = dict(payload)
        new_payload["policy_decision"] = artifact
        observe_policy_inference_latency(12.5)
        return StageResult(payload=new_payload, artifact=artifact)

    async def risk_stage(payload: Dict[str, Any], ctx) -> StageResult:
        intent = payload["intent"]
        trade_intent = risk_service.TradeIntent(
            policy_id=intent["order_id"],
            instrument_id=intent["instrument"],
            side=intent["side"].lower(),
            quantity=float(intent["quantity"]),
            price=float(intent["price"]),
        )
        portfolio_state = risk_service.AccountPortfolioState(
            net_asset_value=2_000_000.0,
            notional_exposure=500_000.0,
            realized_daily_loss=0.0,
            fees_paid=0.0,
        )
        request_model = risk_service.RiskValidationRequest(
            account_id=intent["account_id"],
            intent=trade_intent,
            portfolio_state=portfolio_state,
        )
        response = await asyncio.to_thread(
            risk_client.post,
            "/risk/validate",
            json=request_model.model_dump(by_alias=True, mode="json"),
            headers={"X-Account-ID": intent["account_id"]},
        )
        response.raise_for_status()
        decision = response.json()
        artifact: Dict[str, Any] = dict(decision)
        new_payload = dict(payload)
        new_payload["risk_validation"] = artifact
        observe_risk_validation_latency(9.3)
        return StageResult(payload=new_payload, artifact=artifact)

    async def oms_stage(payload: Dict[str, Any], ctx) -> StageResult:
        intent = payload["intent"]
        account_id = intent["account_id"]
        quantity = float(intent["quantity"])
        price = float(intent["price"])
        pair = intent["instrument"].replace("-", "/")
        side = intent["side"].lower()

        response = await kraken_mock_server.add_order(
            pair=pair,
            side=side,
            volume=quantity,
            price=price,
            ordertype="limit",
            account=account_id,
            userref=intent["order_id"],
        )

        order_payload = response["order"]
        fills = response.get("fills", [])
        fill = fills[0] if fills else None
        filled_qty = float(fill["volume"]) if fill else 0.0
        avg_price = float(fill["price"]) if fill else price

        if side == "buy":
            pnl = (price - avg_price) * filled_qty
        else:
            pnl = (avg_price - price) * filled_qty
        fee_bps = 6.5
        fee_paid = abs(avg_price * filled_qty) * fee_bps / 10_000.0

        timescale = TimescaleAdapter(account_id=account_id)
        timescale.record_fill(
            {
                "order_id": order_payload["order_id"],
                "instrument": intent["instrument"],
                "filled_qty": filled_qty,
                "avg_price": avg_price,
                "pnl": pnl,
            }
        )
        timescale.record_daily_usage(loss=max(-pnl, 0.0), fee=fee_paid)

        increment_trades_submitted()
        set_oms_latency(14.2)

        if pnl < 0.0:
            safe_mode.controller.enter(reason="pnl_limit", actor="sequencer")

        oms_result = {
            "client_order_id": order_payload["order_id"],
            "filled_qty": filled_qty,
            "avg_price": avg_price,
            "pnl": pnl,
            "fee_paid": fee_paid,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        new_payload = dict(payload)
        new_payload["oms_result"] = oms_result
        return StageResult(payload=new_payload, artifact=oms_result)

    history = PipelineHistory(capacity=10)
    pipeline = SequencerPipeline(
        stages=[
            Stage(name="policy", handler=policy_stage),
            Stage(name="risk", handler=risk_stage),
            Stage(name="oms", handler=oms_stage),
        ],
        history=history,
    )

    intent_payload = {
        "account_id": "company",
        "order_id": "INT-100",
        "instrument": "BTC-USD",
        "side": "buy",
        "quantity": 0.2,
        "price": 30000.0,
        "features": [0.12, 0.18, 0.22],
    }

    async def _run() -> Any:
        result = await pipeline.submit(intent_payload)
        set_pipeline_latency(result.latency_ms)
        return result

    result = asyncio.run(_run())

    assert result.status == "success"
    assert result.fill_event["filled_qty"] > 0.0
    assert result.fill_event["avg_price"] > 0.0

    history_snapshot = asyncio.run(history.snapshot())
    assert history_snapshot, "Pipeline history should record the run"

    account_events = TimescaleAdapter(account_id="company").events()
    assert account_events["fills"], "Fill should be recorded in Timescale"

    usage = TimescaleAdapter(account_id="company").get_daily_usage()
    assert usage["loss"] >= 0.0
    assert usage["fee"] > 0.0

    kafka_events = KafkaNATSAdapter(account_id="company").history()
    assert kafka_events, "Audit trail should contain published events"

    metrics_payload = generate_latest(metrics_module._REGISTRY)
    assert b"trades_submitted_total" in metrics_payload

    status = safe_mode.controller.status()
    assert status.active is True
    assert status.reason == "pnl_limit"

    log_entries = safe_mode.get_safe_mode_log()
    assert any(entry["state"] == "entered" for entry in log_entries)
