from __future__ import annotations

from typing import Dict, Tuple

import services.risk.engine as risk_engine
from services.common.schemas import (
    FeeBreakdown,
    PolicyDecisionPayload,
    PolicyDecisionRequest,
    PortfolioState,
    RiskIntentMetrics,
    RiskIntentPayload,
    RiskValidationRequest,
)


class _StubTimescale:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id

    def load_risk_config(self) -> Dict[str, object]:
        return {}

    def open_positions(self) -> Dict[str, float]:
        return {"BTC-USD": 10_000.0}

    def record_event(self, event_type: str, payload: Dict[str, object]) -> None:  # pragma: no cover
        del event_type, payload

    def record_daily_usage(self, loss: float, fee: float) -> None:  # pragma: no cover
        del loss, fee

    def record_instrument_exposure(self, instrument: str, notional: float) -> None:  # pragma: no cover
        del instrument, notional


class _StubUniverse:
    def approved_instruments(self) -> Tuple[str, ...]:
        return ("BTC-USD",)


def _build_validation_request() -> RiskValidationRequest:
    fee = FeeBreakdown(currency="USD", maker=0.0, taker=0.0)
    decision_request = PolicyDecisionRequest(
        account_id="acct",
        order_id="abc-123",
        instrument="BTC-USD",
        side="BUY",
        quantity=1.0,
        price=25_000.0,
        fee=fee,
        take_profit_bps=100.0,
        stop_loss_bps=200.0,
    )
    intent_payload = RiskIntentPayload(
        policy_decision=PolicyDecisionPayload(request=decision_request, response=None),
        metrics=RiskIntentMetrics(
            net_exposure=0.0,
            gross_notional=25_000.0,
            projected_loss=0.0,
            projected_fee=0.0,
            var_95=0.0,
            spread_bps=5.0,
            latency_ms=10.0,
        ),
    )
    portfolio_state = PortfolioState(
        nav=100_000.0,
        loss_to_date=0.0,
        fee_to_date=0.0,
        available_cash=500.0,
    )
    return RiskValidationRequest(
        account_id="acct",
        intent=intent_payload,
        portfolio_state=portfolio_state,
        instrument="BTC-USD",
        net_exposure=0.0,
        gross_notional=25_000.0,
        projected_loss=0.0,
        projected_fee=0.0,
        var_95=0.0,
        spread_bps=5.0,
        latency_ms=10.0,
        fee=fee,
    )


def test_live_balances_update_exposure_and_stop_loss() -> None:
    request = _build_validation_request()

    def _load_balances(account_id: str) -> Tuple[Dict[str, float], float]:
        assert account_id == "acct"
        return {"BTC-USD": 10_000.0}, 400.0

    engine = risk_engine.RiskEngine(
        account_id="acct",
        timescale=_StubTimescale("acct"),
        universe_source=_StubUniverse(),
        balance_loader=_load_balances,
    )

    decision = engine.validate(request)

    assert decision.valid is True
    assert decision.stop_loss == 24_600.0

    exposures = request.portfolio_state.instrument_exposure
    assert exposures["BTC-USD"] == 10_000.0
    assert request.net_exposure == 35_000.0
