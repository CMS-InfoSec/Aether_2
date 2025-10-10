"""End-to-end integration test that exercises the trading pipeline."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, List

import pytest

pytest.importorskip("services.common.security")

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient
from prometheus_client import generate_latest

from tests.helpers.risk import MANAGED_RISK_DSN, patch_sqlalchemy_for_risk


_RISK_SQLITE_PATH = Path(__file__).with_name("risk_end_to_end.db")


patch_sqlalchemy_for_risk(_RISK_SQLITE_PATH)

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
from common.schemas.contracts import IntentEvent
from common.utils import audit_logger
from sequencer import PipelineHistory, SequencerPipeline, Stage, StageResult
from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.common.schemas import PolicyDecisionResponse
sequencer_module = pytest.importorskip("services.core.sequencer")
TradingSequencer = getattr(sequencer_module, "TradingSequencer", None)
if TradingSequencer is None:
    pytest.skip("TradingSequencer unavailable in this environment", allow_module_level=True)

from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger
from shared.correlation import CorrelationContext
from services.fees.fee_service import app as fees_app
from tests import factories
from tests.helpers.authentication import override_admin_auth
from tests.fixtures.mock_kraken import MockKrakenServer


class HttpRiskServiceClient:
    """Adapter that proxies risk validation requests through the FastAPI app."""

    def __init__(self, *, client: TestClient, risk_module) -> None:
        self._client = client
        self._risk = risk_module
        self.validations: List[Dict[str, Any]] = []
        self.fill_events: List[Dict[str, Any]] = []

    async def validate_intent(self, intent: IntentEvent, *, correlation_id: str) -> Dict[str, Any]:
        payload = intent.intent
        trade_intent = self._risk.TradeIntent(
            policy_id=str(
                payload.get("order_id")
                or payload.get("client_id")
                or payload.get("intent_id")
                or intent.intent.get("order_id")
            ),
            instrument_id=str(payload.get("instrument") or payload.get("symbol") or intent.symbol),
            side=str(payload.get("side", "buy")).lower(),
            quantity=float(payload.get("quantity") or payload.get("qty") or 0.0),
            price=float(payload.get("price") or 0.0),
        )
        portfolio_state = self._risk.AccountPortfolioState(
            net_asset_value=1_000_000.0,
            notional_exposure=150_000.0,
            realized_daily_loss=0.0,
            fees_paid=0.0,
        )
        request_model = self._risk.RiskValidationRequest(
            account_id=intent.account_id,
            intent=trade_intent,
            portfolio_state=portfolio_state,
        )
        with override_admin_auth(
            self._client.app,
            self._risk.require_admin_account,
            intent.account_id,
        ) as headers:
            request_headers = {
                **headers,
                "X-Account-ID": intent.account_id,
                "X-Correlation-ID": correlation_id,
            }
            response = await asyncio.to_thread(
                self._client.post,
                "/risk/validate",
                json=request_model.model_dump(by_alias=True, mode="json"),
                headers=request_headers,
            )
        response.raise_for_status()
        data: Dict[str, Any] = response.json()
        self.validations.append({"request": request_model, "response": data, "correlation_id": correlation_id})
        return data

    async def handle_fill(self, fill: Any, *, correlation_id: str) -> None:  # noqa: ANN401 - protocol compatibility
        self.fill_events.append({"fill": fill, "correlation_id": correlation_id})


class MockOMSClient:
    """Minimal OMS facade that proxies order flow into :class:`MockKrakenServer`."""

    def __init__(self, *, server: MockKrakenServer) -> None:
        self._server = server
        self._last_order: Dict[str, Any] | None = None
        self.orders: List[Dict[str, Any]] = []
        self.fills: List[Dict[str, Any]] = []

    async def place_order(
        self,
        intent: IntentEvent,
        decision: Dict[str, Any],
        *,
        correlation_id: str,
    ) -> Dict[str, Any]:
        payload = intent.intent
        symbol = str(payload.get("instrument") or payload.get("symbol") or intent.symbol)
        pair = symbol.replace("-", "/")
        side = str(payload.get("side", "buy")).lower()
        quantity = float(payload.get("quantity") or payload.get("qty") or 0.0)
        price = float(payload.get("price") or 0.0)
        response = await self._server.add_order(
            pair=pair,
            side=side,
            volume=quantity,
            price=price,
            ordertype="limit",
            account=intent.account_id,
            userref=str(payload.get("order_id") or payload.get("client_id")),
        )
        order = response["order"]
        self._last_order = response
        self.orders.append({"order": order, "decision": decision, "correlation_id": correlation_id})
        return {
            "order_id": order["order_id"],
            "status": order.get("status", "submitted"),
            "price": order.get("price"),
            "qty": order.get("volume"),
            "fills": response.get("fills", []),
        }

    async def stream_fills(
        self,
        order: Dict[str, Any],
        *,
        correlation_id: str,
    ) -> AsyncIterator[Dict[str, Any]]:  # type: ignore[override]
        fills = self._last_order.get("fills", []) if self._last_order else []
        for fill in fills:
            fee = abs(float(fill.get("price", 0.0)) * float(fill.get("volume", 0.0))) * 0.001
            payload = {
                "order_id": fill.get("order_id"),
                "qty": float(fill.get("volume", 0.0)),
                "price": float(fill.get("price", 0.0)),
                "fee": fee,
                "liquidity": "maker",
                "ts": fill.get("executed_at"),
            }
            self.fills.append({"payload": payload, "correlation_id": correlation_id})
            yield payload


class TrackingPnLTracker:
    """Records fills into Timescale and emits audit logs with correlation IDs."""

    def __init__(self, *, recorder: SensitiveActionRecorder, account_id: str) -> None:
        self._recorder = recorder
        self._account_id = account_id
        self._timescale = TimescaleAdapter(account_id=account_id)
        self._context: Dict[str, Dict[str, Any]] = {}
        self.records: List[Dict[str, Any]] = []

    def register_intent(self, event: IntentEvent) -> None:
        intent = event.intent
        self._context[event.symbol] = {
            "side": str(intent.get("side", "buy")).lower(),
            "entry_price": float(intent.get("price") or 0.0),
            "order_id": str(intent.get("order_id") or intent.get("client_id") or "unknown"),
        }

    async def handle_fill(self, fill: Any, *, correlation_id: str) -> None:  # noqa: ANN401 - protocol compatibility
        context = self._context.get(
            fill.symbol,
            {"side": "buy", "entry_price": float(fill.price), "order_id": "unknown"},
        )
        entry_price = float(context["entry_price"])
        side = str(context["side"]).lower()
        mark_price = entry_price + 125.0 if side == "buy" else entry_price - 125.0
        qty = float(fill.qty)
        price = float(fill.price)
        fee = float(fill.fee)
        pnl = (mark_price - price) * qty if side == "buy" else (price - mark_price) * qty
        self._timescale.record_fill(
            {
                "order_id": context["order_id"],
                "instrument": fill.symbol,
                "filled_qty": qty,
                "avg_price": price,
                "pnl": pnl,
            }
        )
        self._timescale.record_daily_usage(loss=max(-pnl, 0.0), fee=fee)
        with CorrelationContext(correlation_id):
            entry = self._recorder.record(
                action="pnl.recorded",
                actor_id="pnl",
                before={"symbol": fill.symbol},
                after={"pnl": pnl, "fee_paid": fee},
            )
        self.records.append({"pnl": pnl, "fee": fee, "correlation_id": correlation_id, "audit_id": entry.id})

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
        account_id: str, symbol: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        notional_decimal = notional if isinstance(notional, Decimal) else Decimal(str(notional))
        notional_str = f"{notional_decimal.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)}"
        response = fees_client.get(
            "/fees/effective",
            params={
                "pair": symbol,
                "liquidity": liquidity,
                "notional": notional_str,
            },
            headers={"X-Account-ID": account_id},
        )
        response.raise_for_status()
        payload = response.json()
        return Decimal(str(payload["bps"]))

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
        with override_admin_auth(
            risk_client.app,
            risk_service.require_admin_account,
            intent["account_id"],
        ) as headers:
            response = await asyncio.to_thread(
                risk_client.post,
                "/risk/validate",
                json=request_model.model_dump(by_alias=True, mode="json"),
                headers={**headers, "X-Account-ID": intent["account_id"]},
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
        # The mock Kraken server only fills buy limits that cross the reference ask
        # (base 30_000 plus half the 20 bps spread = 30_010).  Use a higher price so
        # the order executes and produces a fill for the assertions below.
        "price": 30050.0,
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trading_sequencer_loop_preserves_correlation_and_audit(
    monkeypatch: pytest.MonkeyPatch, kraken_mock_server: MockKrakenServer
) -> None:
    """End-to-end TradingSequencer flow should persist correlation IDs and audit records."""

    TimescaleAdapter.reset()
    KafkaNATSAdapter.reset()

    account_id = "company"
    symbol = "BTC-USD"
    order_id = "INT-E2E-001"
    features = [0.25, 0.33, 0.41]

    monkeypatch.setenv("ENABLE_SHADOW_EXECUTION", "false")
    monkeypatch.setenv("RISK_DATABASE_URL", MANAGED_RISK_DSN)
    monkeypatch.setenv("ESG_DATABASE_URL", MANAGED_RISK_DSN)
    monkeypatch.setenv("AETHER_COMPANY_TIMESCALE_DSN", "sqlite:///:memory:")
    monkeypatch.setenv("AETHER_COMPANY_TIMESCALE_SCHEMA", "acct_company")

    policy_client = TestClient(policy_service.app)
    risk_http_client = TestClient(risk_service.app)
    fees_client = TestClient(fees_app)

    confidence = factories.confidence(overall_confidence=0.93)
    intent_stub = policy_service.Intent(
        edge_bps=48.0,
        confidence=confidence,
        take_profit_bps=65.0,
        stop_loss_bps=28.0,
        selected_action="maker",
        action_templates=list(factories.action_templates()),
        approved=True,
        reason=None,
    )
    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: intent_stub)
    policy_service.ENABLE_SHADOW_EXECUTION = False

    async def fake_fetch_effective_fee(
        account_id: str, symbol: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        notional_decimal = notional if isinstance(notional, Decimal) else Decimal(str(notional))
        notional_str = f"{notional_decimal.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)}"
        response = fees_client.get(
            "/fees/effective",
            params={
                "pair": symbol,
                "liquidity": liquidity,
                "notional": notional_str,
            },
            headers={"X-Account-ID": account_id},
        )
        response.raise_for_status()
        return Decimal(str(response.json()["bps"]))

    async def noop_submit_execution(*_: Any, **__: Any) -> None:
        return None

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", fake_fetch_effective_fee)
    monkeypatch.setattr(policy_service, "_submit_execution", noop_submit_execution)

    class _Limits:
        def __init__(self) -> None:
            self.account_id = account_id
            self.max_daily_loss = 250_000.0
            self.fee_budget = 50_000.0
            self.max_nav_pct_per_trade = 0.35
            self.notional_cap = 5_000_000.0
            self.cooldown_minutes = 0

    async def fake_evaluate(context: risk_service.RiskEvaluationContext) -> risk_service.RiskValidationResponse:
        return risk_service.RiskValidationResponse(pass_=True, reasons=[])

    monkeypatch.setattr(risk_service, "_load_account_limits", lambda _: _Limits())
    monkeypatch.setattr(risk_service, "_evaluate", fake_evaluate)
    monkeypatch.setattr(risk_service, "_refresh_usage_from_fills", lambda *_, **__: None)

    audit_store = AuditLogStore()
    audit_logger = TimescaleAuditLogger(audit_store)
    recorder = SensitiveActionRecorder(audit_logger)

    pnl_tracker = TrackingPnLTracker(recorder=recorder, account_id=account_id)
    risk_client = HttpRiskServiceClient(client=risk_http_client, risk_module=risk_service)
    oms_client = MockOMSClient(server=kraken_mock_server)

    sequencer = TradingSequencer(
        risk_service=risk_client,
        oms=oms_client,
        pnl_tracker=pnl_tracker,
    )

    decision_request = factories.policy_decision_request(
        account_id=account_id,
        order_id=order_id,
        instrument=symbol,
        side="BUY",
        quantity=0.6,
        price=30_250.0,
        features=features,
    )
    policy_response = await asyncio.to_thread(
        policy_client.post,
        "/policy/decide",
        json=decision_request.model_dump(mode="json"),
        headers={"X-Account-ID": account_id},
    )
    policy_response.raise_for_status()
    decision = PolicyDecisionResponse.model_validate(policy_response.json())
    assert decision.approved is True

    raw_intent = {
        "account_id": account_id,
        "order_id": order_id,
        "instrument": symbol,
        "symbol": symbol,
        "side": "buy",
        "quantity": decision_request.quantity,
        "price": decision_request.price,
        "features": features,
        "selected_action": decision.selected_action,
    }
    event = IntentEvent(
        account_id=account_id,
        symbol=symbol,
        intent=raw_intent,
        ts=datetime.now(timezone.utc),
    )
    pnl_tracker.register_intent(event)

    result = await sequencer.process_intent(event)

    assert result.status == "filled"
    assert result.account_id == account_id

    validations = risk_client.validations
    assert validations and validations[0]["response"].get("pass") is True
    assert validations[0]["correlation_id"] == result.correlation_id
    assert risk_client.fill_events and risk_client.fill_events[0]["correlation_id"] == result.correlation_id

    assert oms_client.orders and oms_client.orders[0]["correlation_id"] == result.correlation_id
    assert oms_client.fills and oms_client.fills[0]["correlation_id"] == result.correlation_id

    fills = list(result.fills)
    assert fills and fills[0]["details"]["price"] > 0.0

    account_adapter = TimescaleAdapter(account_id=account_id)
    fill_events = account_adapter.events()["fills"]
    assert fill_events
    recorded_pnl = fill_events[0]["pnl"]
    assert pnl_tracker.records and pytest.approx(pnl_tracker.records[0]["pnl"]) == recorded_pnl

    daily_usage = account_adapter.get_daily_usage()
    assert daily_usage["fee"] > 0.0

    history = KafkaNATSAdapter(account_id=account_id).history()
    assert history
    for entry in history:
        assert entry["payload"].get("correlation_id") == result.correlation_id

    audit_entries = list(audit_store.all())
    assert audit_entries
    assert audit_entries[0].action == "pnl.recorded"
    assert audit_entries[0].correlation_id == result.correlation_id


@pytest.mark.integration
def test_policy_risk_oms_flow_emits_hashed_audit_and_metrics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    kraken_mock_server: MockKrakenServer,
) -> None:
    """Verify the full policy → risk → OMS pipeline with audit and metrics side effects."""

    TimescaleAdapter.reset()
    KafkaNATSAdapter.reset()

    metrics_module.init_metrics("sequencer")

    account_id = "company"
    normalized_account = account_id.lower()
    symbol = "BTC-USD"
    normalized_symbol = symbol.lower()

    class _AuditStub:
        def __init__(self) -> None:
            self.statements: List[Dict[str, Any]] = []

        class _Connection:
            def __init__(self, outer: "_AuditStub") -> None:
                self._outer = outer
                self._cursor = _AuditStub._Cursor(self._outer)

            def __enter__(self) -> "_AuditStub._Connection":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
                return False

            def cursor(self) -> "_AuditStub._Cursor":
                return self._cursor

        class _Cursor:
            def __init__(self, outer: "_AuditStub") -> None:
                self._outer = outer

            def __enter__(self) -> "_AuditStub._Cursor":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
                return False

            def execute(self, query: str, params: Iterable[Any]) -> None:
                self._outer.statements.append({"query": query, "params": list(params)})

        def connect(self, dsn: str) -> "_AuditStub._Connection":  # noqa: D401 - interface stub
            return _AuditStub._Connection(self)

    audit_stub = _AuditStub()

    audit_log_path = tmp_path / "audit_chain.log"
    audit_state_path = tmp_path / "audit_chain_state.json"
    monkeypatch.setenv("AUDIT_DATABASE_URL", "postgresql://audit:audit@localhost/audit")
    monkeypatch.setenv("AUDIT_CHAIN_LOG", str(audit_log_path))
    monkeypatch.setenv("AUDIT_CHAIN_STATE", str(audit_state_path))
    monkeypatch.setattr(audit_logger, "psycopg", audit_stub)
    monkeypatch.setattr(audit_logger, "_PSYCOPG_IMPORT_ERROR", None, raising=False)

    policy_client = TestClient(policy_service.app)
    risk_client = TestClient(risk_service.app)
    fees_client = TestClient(fees_app)

    confidence = factories.confidence(overall_confidence=0.94)
    intent_stub = policy_service.Intent(
        edge_bps=41.0,
        confidence=confidence,
        take_profit_bps=55.0,
        stop_loss_bps=22.0,
        selected_action="maker",
        action_templates=list(factories.action_templates()),
        approved=True,
        reason=None,
    )
    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: intent_stub)
    policy_service.ENABLE_SHADOW_EXECUTION = False

    async def fake_fetch_effective_fee(
        account_id: str, symbol: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        notional_decimal = notional if isinstance(notional, Decimal) else Decimal(str(notional))
        notional_str = f"{notional_decimal.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)}"
        response = fees_client.get(
            "/fees/effective",
            params={
                "pair": symbol,
                "liquidity": liquidity,
                "notional": notional_str,
            },
            headers={"X-Account-ID": account_id},
        )
        response.raise_for_status()
        return Decimal(str(response.json()["bps"]))

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", fake_fetch_effective_fee)

    class _Limits:
        def __init__(self) -> None:
            self.account_id = account_id
            self.max_daily_loss = 250_000.0
            self.fee_budget = 50_000.0
            self.max_nav_pct_per_trade = 0.35
            self.notional_cap = 5_000_000.0
            self.cooldown_minutes = 0

    async def fake_evaluate(context: risk_service.RiskEvaluationContext) -> risk_service.RiskValidationResponse:
        return risk_service.RiskValidationResponse(pass_=True, reasons=[])

    monkeypatch.setattr(risk_service, "_load_account_limits", lambda _: _Limits())
    monkeypatch.setattr(risk_service, "_evaluate", fake_evaluate)
    monkeypatch.setattr(risk_service, "_refresh_usage_from_fills", lambda *_, **__: None)

    async def policy_stage(payload: Dict[str, Any], ctx) -> StageResult:
        intent = payload["intent"]
        request_model = factories.policy_decision_request(
            account_id=intent["account_id"],
            order_id=intent["order_id"],
            instrument=intent["instrument"],
            side=intent["side"].upper(),
            quantity=float(intent["quantity"]),
            price=float(intent["price"]),
            features=list(intent.get("features", [])),
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
        observe_policy_inference_latency(6.4)
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
            net_asset_value=1_500_000.0,
            notional_exposure=250_000.0,
            realized_daily_loss=0.0,
            fees_paid=0.0,
        )
        request_model = risk_service.RiskValidationRequest(
            account_id=intent["account_id"],
            intent=trade_intent,
            portfolio_state=portfolio_state,
        )
        with override_admin_auth(
            risk_client.app,
            risk_service.require_admin_account,
            intent["account_id"],
        ) as headers:
            response = await asyncio.to_thread(
                risk_client.post,
                "/risk/validate",
                json=request_model.model_dump(by_alias=True, mode="json"),
                headers={**headers, "X-Account-ID": intent["account_id"]},
            )
        response.raise_for_status()
        decision: Dict[str, Any] = response.json()
        artifact = dict(decision)
        new_payload = dict(payload)
        new_payload["risk_validation"] = artifact
        observe_risk_validation_latency(4.8)
        return StageResult(payload=new_payload, artifact=artifact)

    async def oms_stage(payload: Dict[str, Any], ctx) -> StageResult:
        intent = payload["intent"]
        response = await kraken_mock_server.add_order(
            pair=intent["instrument"].replace("-", "/"),
            side=intent["side"].lower(),
            volume=float(intent["quantity"]),
            price=float(intent["price"]),
            ordertype="limit",
            account=intent["account_id"],
            userref=intent["order_id"],
        )
        order = response["order"]
        fills = response.get("fills", [])
        filled_qty = sum(float(fill.get("volume", 0.0)) for fill in fills) or float(intent["quantity"])
        notional = sum(float(fill.get("price", 0.0)) * float(fill.get("volume", 0.0)) for fill in fills)
        avg_price = notional / filled_qty if filled_qty and notional else float(intent["price"])
        artifact = {
            "accepted": True,
            "client_order_id": order.get("order_id"),
            "filled_qty": filled_qty,
            "avg_price": avg_price,
            "fills": fills,
        }
        new_payload = dict(payload)
        new_payload["oms_result"] = artifact
        set_oms_latency(11.2, account=normalized_account, symbol=normalized_symbol, transport="mock-kraken")
        return StageResult(payload=new_payload, artifact=artifact)

    history = PipelineHistory(capacity=8)
    pipeline = SequencerPipeline(
        stages=[
            Stage(name="policy", handler=policy_stage),
            Stage(name="risk", handler=risk_stage),
            Stage(name="oms", handler=oms_stage),
        ],
        history=history,
    )

    intent_payload = {
        "account_id": account_id,
        "order_id": "INT-E2E-PIPELINE",
        "instrument": symbol,
        "side": "buy",
        "quantity": 0.3,
        "price": 30250.0,
        "features": [0.15, 0.22, 0.31],
    }

    intent_metric_labels = {"service": "sequencer"}
    prior_intent_count = metrics_module._REGISTRY.get_sample_value(
        "trades_submitted_total", intent_metric_labels
    ) or 0.0

    oms_metric_labels = {
        "service": "sequencer",
        "account": normalized_account,
        "symbol": normalized_symbol,
        "transport": "mock-kraken",
    }
    prior_oms_latency = metrics_module._REGISTRY.get_sample_value(
        "oms_latency_ms", oms_metric_labels
    ) or 0.0

    result = asyncio.run(pipeline.submit(intent_payload))

    assert result.status == "success"
    assert result.fill_event["filled_qty"] > 0
    assert result.fill_event["avg_price"] > 0

    pipeline_history = asyncio.run(history.snapshot())
    assert len(pipeline_history) == 1

    kafka_events = KafkaNATSAdapter(account_id=normalized_account).history()
    topics = [event["topic"] for event in kafka_events]
    assert topics == [
        "sequencer.pipeline.start",
        "sequencer.policy.start",
        "sequencer.policy.complete",
        "sequencer.risk.start",
        "sequencer.risk.complete",
        "sequencer.oms.start",
        "sequencer.oms.complete",
        "sequencer.fill.publish",
        "sequencer.pipeline.complete",
    ]

    correlation_id = result.fill_event.get("correlation_id")
    assert correlation_id
    assert all(event["correlation_id"] == correlation_id for event in kafka_events)

    audit_logger.log_audit(
        actor="sequencer",
        action="sequencer.fill",
        entity=normalized_account,
        before={"stage": "oms"},
        after={"fill": result.fill_event},
        ip_hash=audit_logger.hash_ip("127.0.0.1"),
    )

    assert audit_log_path.exists()
    with audit_log_path.open("r", encoding="utf-8") as fh:
        audit_entries = [json.loads(line) for line in fh if line.strip()]

    assert audit_entries
    latest_entry = audit_entries[-1]
    canonical = audit_logger._canonical_payload(latest_entry)  # pylint: disable=protected-access
    serialized = audit_logger._canonical_serialized(canonical)  # pylint: disable=protected-access
    expected_hash = hashlib.sha256((latest_entry["prev_hash"] + serialized).encode("utf-8")).hexdigest()
    assert latest_entry["hash"] == expected_hash

    updated_intent_count = metrics_module._REGISTRY.get_sample_value(
        "trades_submitted_total", intent_metric_labels
    )
    assert updated_intent_count is not None
    assert updated_intent_count == pytest.approx(prior_intent_count + 1.0)

    updated_oms_latency = metrics_module._REGISTRY.get_sample_value(
        "oms_latency_ms", oms_metric_labels
    )
    assert updated_oms_latency is not None
    if prior_oms_latency:
        assert updated_oms_latency >= prior_oms_latency
    assert updated_oms_latency == pytest.approx(11.2, rel=1e-3)


@pytest.mark.integration
def test_pipeline_handles_flash_crash_and_feed_outage(
    monkeypatch: pytest.MonkeyPatch,
    kraken_mock_server: MockKrakenServer,
) -> None:
    """Exercise a full policy → risk → OMS loop under shock conditions."""

    TimescaleAdapter.reset()
    KafkaNATSAdapter.reset()
    safe_mode.controller.reset()
    safe_mode.clear_safe_mode_log()

    metrics_module.init_metrics("sequencer")

    account_id = "company"
    symbol = "BTC-USD"
    features = [0.11, 0.27, 0.38]

    policy_client = TestClient(policy_service.app)
    risk_http_client = TestClient(risk_service.app)
    fees_client = TestClient(fees_app)

    confidence = factories.confidence(overall_confidence=0.9)
    intent_stub = policy_service.Intent(
        edge_bps=44.0,
        confidence=confidence,
        take_profit_bps=55.0,
        stop_loss_bps=20.0,
        selected_action="maker",
        action_templates=list(factories.action_templates()),
        approved=True,
        reason=None,
    )

    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: intent_stub)
    policy_service.ENABLE_SHADOW_EXECUTION = False

    async def fake_fetch_effective_fee(
        account: str, pair: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        notional_decimal = notional if isinstance(notional, Decimal) else Decimal(str(notional))
        notional_str = f"{notional_decimal.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)}"
        response = fees_client.get(
            "/fees/effective",
            params={
                "pair": pair,
                "liquidity": liquidity,
                "notional": notional_str,
            },
            headers={"X-Account-ID": account},
        )
        response.raise_for_status()
        return Decimal(str(response.json()["bps"]))

    async def noop_submit_execution(*_: Any, **__: Any) -> None:
        return None

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", fake_fetch_effective_fee)
    monkeypatch.setattr(policy_service, "_submit_execution", noop_submit_execution)

    class _Limits:
        def __init__(self) -> None:
            self.account_id = account_id
            self.max_daily_loss = 250_000.0
            self.fee_budget = 50_000.0
            self.max_nav_pct_per_trade = 0.35
            self.notional_cap = 5_000_000.0
            self.cooldown_minutes = 0

    async def fake_evaluate(context: risk_service.RiskEvaluationContext) -> risk_service.RiskValidationResponse:
        return risk_service.RiskValidationResponse(pass_=True, reasons=[])

    monkeypatch.setattr(risk_service, "_load_account_limits", lambda _: _Limits())
    monkeypatch.setattr(risk_service, "_evaluate", fake_evaluate)
    monkeypatch.setattr(risk_service, "_refresh_usage_from_fills", lambda *_, **__: None)

    audit_store = AuditLogStore()
    audit_logger = TimescaleAuditLogger(audit_store)
    recorder = SensitiveActionRecorder(audit_logger)

    pnl_tracker = TrackingPnLTracker(recorder=recorder, account_id=account_id)
    risk_client = HttpRiskServiceClient(client=risk_http_client, risk_module=risk_service)
    oms_client = MockOMSClient(server=kraken_mock_server)

    sequencer = TradingSequencer(
        risk_service=risk_client,
        oms=oms_client,
        pnl_tracker=pnl_tracker,
    )

    def _submit_intent(
        *,
        order_id: str,
        side: str,
        price: float,
        adjustment: float | None = None,
    ) -> tuple[IntentEvent, PolicyDecisionResponse]:
        decision_request = factories.policy_decision_request(
            account_id=account_id,
            order_id=order_id,
            instrument=symbol,
            side=side.upper(),
            quantity=0.5,
            price=price,
            features=list(features),
        )
        response = policy_client.post(
            "/policy/decide",
            json=decision_request.model_dump(mode="json"),
            headers={"X-Account-ID": account_id},
        )
        response.raise_for_status()
        decision = PolicyDecisionResponse.model_validate(response.json())
        assert decision.approved is True

        raw_intent = {
            "account_id": account_id,
            "order_id": order_id,
            "instrument": symbol,
            "symbol": symbol,
            "side": side.lower(),
            "quantity": decision_request.quantity,
            "price": price,
            "features": list(features),
            "selected_action": decision.selected_action,
        }

        event = IntentEvent(
            account_id=account_id,
            symbol=symbol,
            intent=raw_intent,
            ts=datetime.now(timezone.utc),
        )
        pnl_tracker.register_intent(event)
        if adjustment is not None:
            pnl_tracker._context[event.symbol]["entry_price"] = adjustment  # noqa: SLF001
        return event, decision

    def _process(event: IntentEvent):
        async def _run():
            return await sequencer.process_intent(event)

        return asyncio.run(_run())

    # ------------------------------------------------------------------
    # Happy path execution
    # ------------------------------------------------------------------
    initial_event, _ = _submit_intent(order_id="CRASH-INIT", side="buy", price=30_050.0)
    initial_result = _process(initial_event)

    assert initial_result.status == "filled"
    assert initial_result.correlation_id

    first_fills = list(initial_result.fills)
    assert first_fills and first_fills[0]["details"]["price"] > 0.0

    kafka_history = KafkaNATSAdapter(account_id=account_id).history(initial_result.correlation_id)
    assert kafka_history
    assert {entry["topic"] for entry in kafka_history} >= {
        "sequencer.intent",
        "sequencer.decision",
        "sequencer.order",
        "sequencer.fill",
        "sequencer.update",
    }
    assert all(entry["correlation_id"] == initial_result.correlation_id for entry in kafka_history)

    assert risk_client.validations
    assert risk_client.validations[-1]["correlation_id"] == initial_result.correlation_id
    assert oms_client.orders and oms_client.orders[-1]["correlation_id"] == initial_result.correlation_id
    assert pnl_tracker.records and pnl_tracker.records[-1]["correlation_id"] == initial_result.correlation_id

    account_adapter = TimescaleAdapter(account_id=account_id)
    fills_payload = account_adapter.events()["fills"]
    assert fills_payload and fills_payload[-1]["pnl"] >= 0.0

    # ------------------------------------------------------------------
    # Flash crash simulation – drastic price shock produces losses
    # ------------------------------------------------------------------
    kraken_mock_server.config.base_price = 25_000.0
    crash_event, _ = _submit_intent(
        order_id="CRASH-SELL",
        side="sell",
        price=24_900.0,
        adjustment=30_050.0,
    )
    crash_result = _process(crash_event)

    assert crash_result.status == "filled"
    assert crash_result.correlation_id != initial_result.correlation_id

    crash_fills = list(crash_result.fills)
    assert crash_fills

    loss_record = pnl_tracker.records[-1]
    assert loss_record["correlation_id"] == crash_result.correlation_id
    assert loss_record["pnl"] < 0.0

    usage = account_adapter.get_daily_usage()
    assert usage["loss"] > 0.0

    crash_history = KafkaNATSAdapter(account_id=account_id).history(crash_result.correlation_id)
    assert crash_history and {entry["topic"] for entry in crash_history} >= {
        "sequencer.intent",
        "sequencer.decision",
        "sequencer.order",
        "sequencer.fill",
        "sequencer.update",
    }

    # ------------------------------------------------------------------
    # Feed outage simulation – OMS raises and sequencer surfaces error
    # ------------------------------------------------------------------
    kraken_mock_server.schedule_error(
        "add_order",
        ConnectionError("mock kraken market data unavailable"),
    )
    outage_event, _ = _submit_intent(order_id="CRASH-OUTAGE", side="buy", price=30_020.0)

    with pytest.raises(ConnectionError):
        _process(outage_event)

    history = KafkaNATSAdapter(account_id=account_id).history()
    error_events = [entry for entry in history if entry["topic"] == "sequencer.error"]
    assert error_events
    outage_record = error_events[-1]
    outage_correlation = outage_record["correlation_id"]
    assert outage_correlation

    correlated = KafkaNATSAdapter(account_id=account_id).history(outage_correlation)
    topics = [entry["topic"] for entry in correlated]
    assert topics[0] == "sequencer.intent"
    assert topics[-1] == "sequencer.error"
    assert outage_record["payload"]["error"].startswith("mock kraken")

    audit_entries = list(audit_store.all())
    assert audit_entries
    assert all(entry.correlation_id for entry in audit_entries)


@pytest.mark.integration
def test_trading_sequencer_lifecycle_preserves_correlation_and_audit_chain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    kraken_mock_server: MockKrakenServer,
) -> None:
    """Exercise the sequencer against mock services and verify lifecycle integrity."""

    TimescaleAdapter.reset()
    KafkaNATSAdapter.reset()
    safe_mode.controller.reset()

    class _PsycopgStub:
        def __init__(self) -> None:
            self.statements: List[Dict[str, Any]] = []

        class _Connection:
            def __init__(self, outer: "_PsycopgStub") -> None:
                self._outer = outer

            def __enter__(self) -> "_PsycopgStub._Connection":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
                return False

            def cursor(self) -> "_PsycopgStub._Cursor":
                return _PsycopgStub._Cursor(self._outer)

        class _Cursor:
            def __init__(self, outer: "_PsycopgStub") -> None:
                self._outer = outer

            def __enter__(self) -> "_PsycopgStub._Cursor":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
                return False

            def execute(self, query: str, params: Iterable[Any]) -> None:
                self._outer.statements.append({"query": query, "params": list(params)})

        def connect(self, dsn: str) -> "_PsycopgStub._Connection":  # noqa: D401 - stub signature
            return _PsycopgStub._Connection(self)

    psycopg_stub = _PsycopgStub()

    audit_log_path = tmp_path / "audit_chain.log"
    audit_state_path = tmp_path / "audit_chain_state.json"
    monkeypatch.setenv("AUDIT_DATABASE_URL", "postgresql://audit:audit@localhost/audit")
    monkeypatch.setenv("AUDIT_CHAIN_LOG", str(audit_log_path))
    monkeypatch.setenv("AUDIT_CHAIN_STATE", str(audit_state_path))
    monkeypatch.setattr(audit_logger, "psycopg", psycopg_stub)
    monkeypatch.setattr(audit_logger, "_PSYCOPG_IMPORT_ERROR", None, raising=False)

    policy_client = TestClient(policy_service.app)
    risk_http_client = TestClient(risk_service.app)
    fees_client = TestClient(fees_app)

    confidence = factories.confidence(overall_confidence=0.96)
    intent_stub = policy_service.Intent(
        edge_bps=37.0,
        confidence=confidence,
        take_profit_bps=62.0,
        stop_loss_bps=28.0,
        selected_action="maker",
        action_templates=list(factories.action_templates()),
        approved=True,
        reason=None,
    )
    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: intent_stub)
    policy_service.ENABLE_SHADOW_EXECUTION = False

    async def fake_fetch_effective_fee(
        account_id: str, symbol: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        notional_decimal = notional if isinstance(notional, Decimal) else Decimal(str(notional))
        notional_str = f"{notional_decimal.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)}"
        response = fees_client.get(
            "/fees/effective",
            params={
                "pair": symbol,
                "liquidity": liquidity,
                "notional": notional_str,
            },
            headers={"X-Account-ID": account_id},
        )
        response.raise_for_status()
        return Decimal(str(response.json()["bps"]))

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", fake_fetch_effective_fee)

    class _Limits:
        def __init__(self) -> None:
            self.account_id = "company"
            self.max_daily_loss = 250_000.0
            self.fee_budget = 50_000.0
            self.max_nav_pct_per_trade = 0.35
            self.notional_cap = 5_000_000.0
            self.cooldown_minutes = 0

    async def fake_evaluate(context: risk_service.RiskEvaluationContext) -> risk_service.RiskValidationResponse:
        return risk_service.RiskValidationResponse(pass_=True, reasons=[])

    monkeypatch.setattr(risk_service, "_load_account_limits", lambda _: _Limits())
    monkeypatch.setattr(risk_service, "_evaluate", fake_evaluate)
    monkeypatch.setattr(risk_service, "_refresh_usage_from_fills", lambda *_, **__: None)

    account_id = "company"
    order_id = "INT-END2END-001"
    symbol = "BTC-USD"
    features = [0.21, 0.34, 0.55]

    decision_request = factories.policy_decision_request(
        account_id=account_id,
        order_id=order_id,
        instrument=symbol,
        side="BUY",
        quantity=0.4,
        price=30_150.0,
        features=features,
    )
    decision_response = policy_client.post(
        "/policy/decide",
        json=decision_request.model_dump(mode="json"),
        headers={"X-Account-ID": account_id},
    )
    decision_response.raise_for_status()
    decision = PolicyDecisionResponse.model_validate(decision_response.json())
    assert decision.approved is True

    audit_store = AuditLogStore()
    audit_writer = TimescaleAuditLogger(audit_store)
    recorder = SensitiveActionRecorder(audit_writer)
    pnl_tracker = TrackingPnLTracker(recorder=recorder, account_id=account_id)

    risk_client = HttpRiskServiceClient(client=risk_http_client, risk_module=risk_service)
    oms_client = MockOMSClient(server=kraken_mock_server)

    sequencer = TradingSequencer(
        risk_service=risk_client,
        oms=oms_client,
        pnl_tracker=pnl_tracker,
    )

    raw_intent = {
        "account_id": account_id,
        "order_id": order_id,
        "instrument": symbol,
        "symbol": symbol,
        "side": "buy",
        "quantity": decision_request.quantity,
        "price": decision_request.price,
        "features": features,
    }
    event = IntentEvent(
        account_id=account_id,
        symbol=symbol,
        intent=raw_intent,
        ts=datetime.now(timezone.utc),
    )
    pnl_tracker.register_intent(event)

    result = asyncio.run(sequencer.process_intent(event))

    assert result.status == "filled"
    assert result.correlation_id
    assert result.decision and result.decision.get("pass") is True
    assert oms_client.orders and oms_client.orders[0]["decision"].get("approved", True)
    assert oms_client.fills
    assert risk_client.validations
    assert risk_client.validations[0]["response"].get("pass") is True
    assert risk_client.fill_events

    kafka_events = KafkaNATSAdapter(account_id=account_id).history()
    assert kafka_events
    correlation_ids = {entry["correlation_id"] for entry in kafka_events}
    assert correlation_ids == {result.correlation_id}
    expected_topics = {
        "sequencer.intent",
        "sequencer.decision",
        "sequencer.order",
        "sequencer.fill",
        "sequencer.update",
    }
    assert expected_topics.issubset({entry["topic"] for entry in kafka_events})

    fills = list(result.fills)
    assert fills and fills[0]["details"]["price"] > 0.0

    adapter = TimescaleAdapter(account_id=account_id)
    fill_events = adapter.events()["fills"]
    assert fill_events
    recorded_pnl = fill_events[0]["pnl"]
    assert pnl_tracker.records
    assert pnl_tracker.records[0]["correlation_id"] == result.correlation_id
    assert pytest.approx(pnl_tracker.records[0]["pnl"]) == recorded_pnl
    usage = adapter.get_daily_usage()
    assert usage["fee"] > 0.0 or usage["loss"] >= 0.0

    audit_logger.log_audit(
        actor="sequencer",
        action="sequencer.fill",
        entity=account_id,
        before={"order_id": order_id},
        after={"fill": result.fill_event},
        ip_hash=audit_logger.hash_ip("127.0.0.1"),
    )

    assert audit_log_path.exists()
    with audit_log_path.open("r", encoding="utf-8") as fh:
        audit_entries = [json.loads(line) for line in fh if line.strip()]

    assert audit_entries
    previous_hash = audit_logger._GENESIS_HASH  # pylint: disable=protected-access
    for entry in audit_entries:
        expected_prev = hashlib.sha256(previous_hash.encode("utf-8")).hexdigest()
        assert entry["prev_hash"] == expected_prev
        canonical = audit_logger._canonical_payload(entry)  # pylint: disable=protected-access
        serialized = audit_logger._canonical_serialized(canonical)  # pylint: disable=protected-access
        expected_hash = hashlib.sha256((expected_prev + serialized).encode("utf-8")).hexdigest()
        assert entry["hash"] == expected_hash
        previous_hash = entry["hash"]

    assert psycopg_stub.statements

