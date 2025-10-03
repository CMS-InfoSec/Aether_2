from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pytest

pytest.importorskip("services.common.security")
pytest.importorskip("fastapi")

from fastapi.testclient import TestClient
from prometheus_client import generate_latest

from common.utils import audit_logger
from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.oms.warm_start import WarmStartCoordinator
from services.common.schemas import PolicyDecisionResponse
from tests import factories
from tests.helpers.authentication import override_admin_auth
from tests.fixtures.backends import MemoryRedis
from tests.fixtures.mock_kraken import MockKrakenServer


@dataclass
class _AuditStub:
    """Lightweight stub replacing psycopg for audit logging."""

    statements: List[Dict[str, Any]]

    class _Connection:
        def __init__(self, sink: List[Dict[str, Any]]) -> None:
            self._sink = sink
            self._cursor = _AuditStub._Cursor(self._sink)

        def __enter__(self) -> "_AuditStub._Connection":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        def cursor(self) -> "_AuditStub._Cursor":
            return self._cursor

    class _Cursor:
        def __init__(self, sink: List[Dict[str, Any]]) -> None:
            self._sink = sink

        def __enter__(self) -> "_AuditStub._Cursor":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

        def execute(self, query: str, params: Iterable[Any]) -> None:
            self._sink.append({"query": query, "params": list(params)})

    def connect(self, dsn: str) -> "_AuditStub._Connection":
        return _AuditStub._Connection(self.statements)


class _StubAccount:
    """Minimal OMS account facade used for warm start verification."""

    def __init__(self, account_id: str, fills: int) -> None:
        self.account_id = account_id
        self._fills = fills
        self.resync_calls: List[str] = []

    async def resync_from_exchange(self) -> int:
        self.resync_calls.append("orders")
        return 1

    async def resync_positions(self) -> int:
        self.resync_calls.append("positions")
        return 1

    async def resync_balances(self) -> int:
        self.resync_calls.append("balances")
        return 1

    async def resync_trades(self) -> int:
        self.resync_calls.append("trades")
        return self._fills


class _StubManager:
    def __init__(self, accounts: Dict[str, _StubAccount]) -> None:
        self._accounts = accounts

    async def get_account(self, account_id: str) -> _StubAccount:
        return self._accounts[account_id]


@pytest.mark.integration
@pytest.mark.slow
def test_full_loop_across_accounts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    kraken_mock_server: MockKrakenServer,
) -> None:
    """Run a multi-account trading pipeline covering policy to PnL."""

    # ------------------------------------------------------------------
    # Reset shared state and reload modules to ensure pristine metrics.
    # ------------------------------------------------------------------
    TimescaleAdapter.reset()
    KafkaNATSAdapter.reset()

    sys.modules.pop("metrics", None)
    metrics_module = importlib.import_module("metrics")
    sys.modules.pop("safe_mode", None)
    safe_mode = importlib.import_module("safe_mode")
    sys.modules.pop("policy_service", None)
    policy_service = importlib.import_module("policy_service")
    sys.modules.pop("risk_service", None)
    risk_service = importlib.import_module("risk_service")

    from services.fees.fee_service import app as fees_app

    metrics_module.init_metrics("sequencer")

    backend = MemoryRedis()
    safe_mode.controller._state_store = safe_mode.SafeModeStateStore(redis_client=backend)  # type: ignore[attr-defined]
    safe_mode.controller.reset()
    safe_mode.clear_safe_mode_log()

    # ------------------------------------------------------------------
    # Configure audit logging to use deterministic local artefacts.
    # ------------------------------------------------------------------
    audit_statements: List[Dict[str, Any]] = []
    monkeypatch.setenv("AUDIT_DATABASE_URL", "postgresql://audit:audit@localhost/audit")
    monkeypatch.setenv("AUDIT_CHAIN_LOG", str(tmp_path / "audit_chain.log"))
    monkeypatch.setenv("AUDIT_CHAIN_STATE", str(tmp_path / "audit_chain_state.json"))

    audit_stub = _AuditStub(audit_statements)
    monkeypatch.setattr(audit_logger, "psycopg", audit_stub)
    monkeypatch.setattr(audit_logger, "_PSYCOPG_IMPORT_ERROR", None, raising=False)

    # ------------------------------------------------------------------
    # Configure service clients and deterministic policy/risk behaviour.
    # ------------------------------------------------------------------
    policy_client = TestClient(policy_service.app)
    risk_client = TestClient(risk_service.app)
    fees_client = TestClient(fees_app)

    monkeypatch.setenv("ENABLE_SHADOW_EXECUTION", "false")
    monkeypatch.setenv("RISK_DATABASE_URL", "sqlite:///:memory:")

    confidence = factories.confidence(overall_confidence=0.92)
    maker_template, taker_template = factories.action_templates()

    intent_map: Dict[str, policy_service.Intent] = {
        "alpha": policy_service.Intent(
            edge_bps=40.0,
            confidence=confidence,
            take_profit_bps=60.0,
            stop_loss_bps=25.0,
            selected_action="maker",
            action_templates=[maker_template, taker_template],
            approved=True,
            reason=None,
        ),
        "beta": policy_service.Intent(
            edge_bps=18.0,
            confidence=confidence,
            take_profit_bps=45.0,
            stop_loss_bps=20.0,
            selected_action="taker",
            action_templates=[maker_template, taker_template],
            approved=True,
            reason=None,
        ),
        "gamma": policy_service.Intent(
            edge_bps=28.0,
            confidence=confidence,
            take_profit_bps=55.0,
            stop_loss_bps=22.0,
            selected_action="maker",
            action_templates=[maker_template, taker_template],
            approved=True,
            reason=None,
        ),
    }

    monkeypatch.setattr(
        policy_service,
        "predict_intent",
        lambda **kwargs: intent_map[kwargs.get("account_id", "alpha")],
    )
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
        payload = response.json()
        return Decimal(str(payload["bps"]))

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", fake_fetch_effective_fee)

    class _Limits:
        def __init__(self, *, notional_cap: float) -> None:
            self.account_id = "stub"
            self.max_daily_loss = 250_000.0
            self.fee_budget = 75_000.0
            self.max_nav_pct_per_trade = 0.35
            self.notional_cap = notional_cap
            self.cooldown_minutes = 0

    limits_map = {
        "alpha": _Limits(notional_cap=150_000.0),
        "beta": _Limits(notional_cap=120_000.0),
        "gamma": _Limits(notional_cap=30_000.0),
    }

    monkeypatch.setattr(
        risk_service,
        "_load_account_limits",
        lambda account_id: limits_map[account_id],
    )

    async def fake_evaluate(context: risk_service.RiskEvaluationContext) -> risk_service.RiskValidationResponse:
        limits = limits_map[context.request.account_id]
        intent = context.request.intent
        max_qty = min(intent.quantity, limits.notional_cap / intent.price)
        return risk_service.RiskValidationResponse(pass_=True, reasons=[], adjusted_qty=max_qty)

    monkeypatch.setattr(risk_service, "_evaluate", fake_evaluate)
    monkeypatch.setattr(risk_service, "_refresh_usage_from_fills", lambda *_, **__: None)

    # ------------------------------------------------------------------
    # Trading intents for three accounts.
    # ------------------------------------------------------------------
    account_payloads = [
        {
            "account_id": "alpha",
            "order_id": "ALPHA-1",
            "instrument": "BTC-USD",
            "side": "buy",
            "quantity": 0.2,
            "price": 30050.0,
            "features": [0.12, 0.18, 0.23],
        },
        {
            "account_id": "beta",
            "order_id": "BETA-2",
            "instrument": "BTC-USD",
            "side": "buy",
            "quantity": 0.3,
            "price": 29980.0,
            "features": [0.04, 0.08, 0.16],
        },
        {
            "account_id": "gamma",
            "order_id": "GAMMA-3",
            "instrument": "ETH-USD",
            "side": "sell",
            "quantity": 1.5,
            "price": 29950.0,
            "features": [0.33, 0.22, 0.11],
        },
    ]

    results: Dict[str, Dict[str, Any]] = {}

    for payload in account_payloads:
        account_id = payload["account_id"]
        instrument = payload["instrument"]
        side = payload["side"]
        order_id = payload["order_id"]
        requested_qty = float(payload["quantity"])
        requested_price = float(payload["price"])

        timescale = TimescaleAdapter(account_id=account_id)
        KafkaNATSAdapter.reset(account_id)

        stage_start = time.perf_counter()

        policy_request = factories.policy_decision_request(
            account_id=account_id,
            order_id=order_id,
            instrument=instrument,
            side=side.upper(),
            quantity=requested_qty,
            price=requested_price,
            features=list(payload["features"]),
        )
        policy_response_raw = policy_client.post(
            "/policy/decide",
            json=policy_request.model_dump(mode="json"),
            headers={"X-Account-ID": account_id},
        )
        policy_response_raw.raise_for_status()
        policy_decision = PolicyDecisionResponse.model_validate(policy_response_raw.json())

        metrics_module.observe_policy_inference_latency(7.5, service="policy")
        audit_logger.log_audit(
            actor="policy",
            action="policy.intent",
            entity=account_id,
            before={"requested_qty": requested_qty},
            after={"selected_action": policy_decision.selected_action},
            ip_hash=audit_logger.hash_ip("127.0.0.1"),
        )

        trade_intent = risk_service.TradeIntent(
            policy_id=order_id,
            instrument_id=instrument,
            side=side,
            quantity=requested_qty,
            price=requested_price,
        )
        portfolio_state = risk_service.AccountPortfolioState(
            net_asset_value=2_000_000.0,
            notional_exposure=125_000.0,
            realized_daily_loss=0.0,
            fees_paid=0.0,
            instrument_exposure={instrument: 50_000.0},
        )
        risk_request = risk_service.RiskValidationRequest(
            account_id=account_id,
            intent=trade_intent,
            portfolio_state=portfolio_state,
        )
        with override_admin_auth(
            risk_client.app, risk_service.require_admin_account, account_id
        ) as headers:
            risk_headers = {**headers, "X-Account-ID": account_id}
            risk_response_http = risk_client.post(
                "/risk/validate",
                json=risk_request.model_dump(by_alias=True, mode="json"),
                headers=risk_headers,
            )
        risk_response_http.raise_for_status()
        risk_decision = risk_service.RiskValidationResponse.model_validate(risk_response_http.json())

        approved_qty = float(risk_decision.adjusted_qty or trade_intent.quantity)
        metrics_module.observe_risk_validation_latency(5.1, service="risk")
        audit_logger.log_audit(
            actor="risk",
            action="risk.validation",
            entity=account_id,
            before={"requested_qty": requested_qty},
            after={"approved_qty": approved_qty},
            ip_hash=audit_logger.hash_ip("127.0.0.1"),
        )

        notional = approved_qty * requested_price
        timescale.record_usage(notional)
        timescale.record_instrument_exposure(instrument, notional)

        fee_bps = asyncio.run(
            fake_fetch_effective_fee(
                account_id, instrument.replace("-", "/"), policy_decision.selected_action, abs(notional)
            )
        )

        order_response = asyncio.run(
            kraken_mock_server.add_order(
                pair=instrument.replace("-", "/"),
                side=side,
                volume=approved_qty,
                price=None,
                ordertype="market",
                account=account_id,
                userref=order_id,
            )
        )

        fills = order_response.get("fills", [])
        if fills:
            filled_qty = sum(float(fill["volume"]) for fill in fills)
            avg_price = sum(float(fill["price"]) * float(fill["volume"]) for fill in fills) / filled_qty
        else:
            filled_qty = 0.0
            avg_price = requested_price

        if side == "buy":
            pnl = (requested_price - avg_price) * filled_qty
        else:
            pnl = (avg_price - requested_price) * filled_qty
        fee_paid = abs(avg_price * filled_qty) * fee_bps / 10_000.0

        metrics_module.record_oms_latency(
            account=account_id,
            symbol=instrument,
            transport="rest",
            latency_ms=12.4,
            service="oms",
        )
        metrics_module.increment_oms_child_orders_total(
            account=account_id,
            symbol=instrument,
            transport="rest",
            service="oms",
        )
        metrics_module.increment_trades_submitted(service="sequencer")

        elapsed_ms = (time.perf_counter() - stage_start) * 1000.0
        metrics_module.set_pipeline_latency(elapsed_ms, service="sequencer")

        fill_payload = {
            "order_id": order_response["order"]["order_id"],
            "instrument": instrument,
            "filled_qty": filled_qty,
            "avg_price": avg_price,
            "pnl": pnl,
            "fee_paid": fee_paid,
        }
        timescale.record_fill(fill_payload)
        timescale.record_daily_usage(loss=max(-pnl, 0.0), fee=fee_paid)

        if pnl < 0:
            safe_mode.controller.enter(reason="pnl_limit", actor="sequencer")

        KafkaNATSAdapter(account_id=account_id).publish(
            "sequencer.fill",
            {"order_id": order_id, "pnl": pnl, "fee_bps": fee_bps},
        )

        audit_logger.log_audit(
            actor="oms",
            action="oms.order",
            entity=account_id,
            before={"approved_qty": approved_qty},
            after={"filled_qty": filled_qty, "avg_price": avg_price},
            ip_hash=audit_logger.hash_ip("127.0.0.1"),
        )
        audit_logger.log_audit(
            actor="pnl",
            action="pnl.recorded",
            entity=account_id,
            before={"fee_bps": fee_bps},
            after={"pnl": pnl, "fee_paid": fee_paid},
            ip_hash=audit_logger.hash_ip("127.0.0.1"),
        )

        results[account_id] = {
            "policy": policy_decision,
            "risk": risk_decision,
            "fill": fill_payload,
            "fee_bps": fee_bps,
            "requested_qty": requested_qty,
            "approved_qty": approved_qty,
            "pnl": pnl,
        }

    # ------------------------------------------------------------------
    # Validate outcomes across the pipeline.
    # ------------------------------------------------------------------
    assert set(results) == {"alpha", "beta", "gamma"}

    assert results["alpha"]["policy"].approved is True
    assert results["beta"]["policy"].selected_action == "taker"
    assert results["gamma"]["risk"].adjusted_qty < results["gamma"]["requested_qty"]

    assert results["alpha"]["pnl"] > 0
    assert results["beta"]["pnl"] < 0
    assert results["gamma"]["pnl"] >= 0

    for account_id, outcome in results.items():
        fills = TimescaleAdapter(account_id=account_id).events()["fills"]
        assert fills, f"expected fills recorded for {account_id}"
        recorded = fills[0]["payload"]
        assert pytest.approx(recorded["fee_paid"]) == outcome["fill"]["fee_paid"]
        usage = TimescaleAdapter(account_id=account_id).get_daily_usage()
        assert usage["fee"] >= outcome["fill"]["fee_paid"]

    status = safe_mode.controller.status()
    assert status.active is True
    assert status.reason == "pnl_limit"
    log_entries = safe_mode.get_safe_mode_log()
    assert any(entry["reason"] == "pnl_limit" for entry in log_entries)

    metrics_payload = generate_latest(metrics_module._REGISTRY)
    assert b"trades_submitted_total" in metrics_payload

    submitted_total = metrics_module._REGISTRY.get_sample_value(
        "trades_submitted_total", {"service": "sequencer"}
    )
    policy_latency = metrics_module._REGISTRY.get_sample_value(
        "policy_inference_latency_count", {"service": "policy"}
    )
    risk_latency = metrics_module._REGISTRY.get_sample_value(
        "risk_validation_latency_count", {"service": "risk"}
    )
    oms_latency = metrics_module._REGISTRY.get_sample_value(
        "oms_submit_latency_count", {"service": "oms", "transport": "rest"}
    )
    safe_mode_triggers = metrics_module._REGISTRY.get_sample_value(
        "safe_mode_triggers_total", {"service": "safe-mode", "reason": "pnl_limit"}
    )

    assert submitted_total == pytest.approx(3.0)
    assert policy_latency == pytest.approx(3.0)
    assert risk_latency == pytest.approx(3.0)
    assert oms_latency == pytest.approx(3.0)
    assert safe_mode_triggers >= 1.0

    audit_log_path = Path(os.getenv("AUDIT_CHAIN_LOG", ""))
    assert audit_log_path.exists()
    with audit_log_path.open("r", encoding="utf-8") as fh:
        records = [json.loads(line) for line in fh if line.strip()]

    assert len(records) == 12
    expected_actions = [
        "policy.intent",
        "risk.validation",
        "oms.order",
        "pnl.recorded",
    ] * 3
    assert [record["action"] for record in records] == expected_actions
    assert audit_logger.verify_audit_chain() is True
    assert len(audit_statements) == 12

    warm_start_accounts = {
        account_id: _StubAccount(account_id, len(TimescaleAdapter(account_id=account_id).events()["fills"]))
        for account_id in results
    }
    manager = _StubManager(warm_start_accounts)
    warm_start = WarmStartCoordinator(lambda: manager, lookback_seconds=0.1, poll_timeout=0.1)

    async def _fake_replay_fills(self, account: _StubAccount) -> int:  # type: ignore[override]
        return account._fills

    async def _fake_replay_trades(self, account: _StubAccount) -> int:  # type: ignore[override]
        return account._fills

    monkeypatch.setattr(warm_start, "_replay_account_fills", _fake_replay_fills.__get__(warm_start))
    monkeypatch.setattr(warm_start, "_replay_account_trades", _fake_replay_trades.__get__(warm_start))

    asyncio.run(warm_start.run(accounts=list(results)))
    warm_status = asyncio.run(warm_start.status())
    assert warm_status["orders_resynced"] == 3
    assert warm_status["fills_replayed"] >= 3

