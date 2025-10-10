from typing import List

import services.hedge.hedge_service as hedge_service


def _build_service(*, state_path=None, **kwargs: object) -> hedge_service.HedgeService:
    if state_path is not None:
        store = hedge_service.HedgeOverrideStateStore(
            history_limit=200,
            state_path=state_path,
        )
        kwargs.setdefault("state_store", store)
    return hedge_service.HedgeService(**kwargs)


def _evaluate(service: hedge_service.HedgeService, **metrics: object) -> hedge_service.HedgeDecision:
    payload = hedge_service.HedgeMetricsRequest(**metrics)
    return service.evaluate(payload)


def test_auto_target_scales_with_risk_profile(tmp_path) -> None:
    service = _build_service(
        min_auto_target_pct=20.0,
        max_auto_target_pct=95.0,
        state_path=tmp_path / "state.json",
    )

    low = _evaluate(
        service,
        volatility=0.2,
        drawdown=0.05,
        stablecoin_price=1.0,
    )
    medium = _evaluate(
        service,
        volatility=1.1,
        drawdown=0.35,
        stablecoin_price=1.0,
    )
    high = _evaluate(
        service,
        volatility=2.5,
        drawdown=0.7,
        stablecoin_price=1.0,
    )

    decisions: List[float] = [
        low.diagnostics.adjusted_target_pct,
        medium.diagnostics.adjusted_target_pct,
        high.diagnostics.adjusted_target_pct,
    ]
    assert decisions == sorted(decisions)
    assert decisions[0] >= 20.0
    assert decisions[-1] >= 90.0

    components_low = low.diagnostics.components
    components_high = high.diagnostics.components
    assert components_low["combined_risk_score"] < components_high["combined_risk_score"]

    deviation_guard = _evaluate(
        service,
        volatility=0.6,
        drawdown=0.3,
        stablecoin_price=1.04,
    )
    assert deviation_guard.diagnostics.guard_triggered is True
    assert "Stablecoin" in deviation_guard.diagnostics.guard_reason
    assert deviation_guard.diagnostics.adjusted_target_pct >= service._guard_floor_pct  # type: ignore[attr-defined]


def test_drawdown_breach_recommends_kill_switch(tmp_path) -> None:
    service = _build_service(
        drawdown_kill_threshold=0.5,
        state_path=tmp_path / "state.json",
    )

    decision = _evaluate(
        service,
        volatility=0.8,
        drawdown=0.55,
        stablecoin_price=1.0,
    )

    assert decision.diagnostics.kill_switch_recommended is True
    assert decision.diagnostics.kill_switch_reason is not None
    assert "55" in decision.diagnostics.kill_switch_reason
    assert decision.reason == decision.diagnostics.guard_reason

    subsequent = _evaluate(
        service,
        volatility=0.8,
        drawdown=0.6,
        stablecoin_price=1.0,
    )
    assert subsequent.diagnostics.kill_switch_recommended is True
    assert subsequent.diagnostics.adjusted_target_pct >= decision.diagnostics.adjusted_target_pct


def test_kill_switch_handler_invoked_once_and_rearmed(tmp_path) -> None:
    triggers: List[str] = []

    def _handler(
        metrics: hedge_service.HedgeMetricsRequest,
        diagnostics: hedge_service.HedgeDiagnostics,
    ) -> None:
        record = f"{metrics.account_id or 'unknown'}:{diagnostics.kill_switch_reason}"
        triggers.append(record)

    store = hedge_service.HedgeOverrideStateStore(history_limit=10, state_path=tmp_path / "state.json")
    service = _build_service(
        state_store=store,
        drawdown_kill_threshold=0.4,
        drawdown_recovery_threshold=0.2,
        kill_switch_handler=_handler,
    )

    _evaluate(
        service,
        volatility=0.6,
        drawdown=0.45,
        stablecoin_price=1.0,
        account_id="Alpha",
    )
    assert len(triggers) == 1

    _evaluate(
        service,
        volatility=0.7,
        drawdown=0.6,
        stablecoin_price=1.0,
        account_id="Alpha",
    )
    assert len(triggers) == 1

    _evaluate(
        service,
        volatility=0.4,
        drawdown=0.3,
        stablecoin_price=1.0,
        account_id="Alpha",
    )
    assert len(triggers) == 1

    _evaluate(
        service,
        volatility=0.3,
        drawdown=0.15,
        stablecoin_price=1.0,
        account_id="Alpha",
    )
    assert len(triggers) == 1

    _evaluate(
        service,
        volatility=0.8,
        drawdown=0.5,
        stablecoin_price=1.0,
        account_id="Alpha",
    )
    assert len(triggers) == 2
    assert triggers[-1].startswith("Alpha:")
