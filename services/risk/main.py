
from fastapi import Body, Depends, FastAPI, HTTPException, status
from typing import Any, Mapping

import time

from services.common.schemas import (
    FeeBreakdown,
    PolicyDecisionPayload,
    PolicyDecisionRequest,
    PortfolioState,
    RiskIntentMetrics,
    RiskIntentPayload,
    RiskValidationRequest,
    RiskValidationResponse,
)
from services.common.security import require_admin_account
from services.risk.engine import RiskEngine
from services.risk.circuit_breakers import router as circuit_router
from services.risk.cvar_forecast import router as cvar_router
from services.risk.nav_forecaster import router as nav_router
from services.risk.diversification_allocator import router as diversification_router
from services.risk.pretrade_sanity import PRETRADE_SANITY, router as pretrade_router

from metrics import (
    increment_trade_rejection,
    metric_context,
    observe_risk_validation_latency,
    record_fees_nav_pct,
    setup_metrics,
    traced_span,
)
from shared.health import setup_health_checks
from shared.spot import normalize_spot_symbol

try:  # pragma: no cover - pydantic may be unavailable in some environments
    from pydantic import ValidationError
except Exception:  # pragma: no cover - fallback when pydantic is absent
    ValidationError = Exception  # type: ignore[assignment]

app = FastAPI(title="Risk Service")
setup_metrics(app, service_name="risk-service")
setup_health_checks(
    app,
    {
        "risk_engine": lambda: RiskEngine(account_id="__health__"),
    },
)


app.include_router(cvar_router)
app.include_router(circuit_router)
app.include_router(nav_router)
app.include_router(pretrade_router)
app.include_router(diversification_router)


def _coerce_side(value: Any) -> str:
    candidate = (str(value or "BUY")).strip().upper()
    if candidate not in {"BUY", "SELL"}:
        return "BUY"
    return candidate


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:  # pragma: no cover - defensive fallback
        return float(default)


def _build_fallback_request(
    payload: Mapping[str, Any],
    *,
    account_id: str,
) -> RiskValidationRequest:
    instrument = str(payload.get("instrument") or "BTC-USD")
    normalized_instrument = normalize_spot_symbol(instrument) or "BTC-USD"

    raw_fee = payload.get("fee") or {}
    fee = FeeBreakdown(
        currency=str(raw_fee.get("currency") or "USD"),
        maker=_coerce_float(raw_fee.get("maker")),
        taker=_coerce_float(raw_fee.get("taker")),
    )

    gross_notional = _coerce_float(payload.get("gross_notional"))
    net_exposure = _coerce_float(payload.get("net_exposure"), default=gross_notional)
    projected_loss = _coerce_float(payload.get("projected_loss"))
    projected_fee = _coerce_float(payload.get("projected_fee"), default=fee.taker)
    var_95 = _coerce_float(payload.get("var_95"))
    spread_bps = _coerce_float(payload.get("spread_bps"))
    latency_ms = _coerce_float(payload.get("latency_ms"))

    metrics = RiskIntentMetrics(
        net_exposure=net_exposure,
        gross_notional=gross_notional or max(net_exposure, 0.0),
        projected_loss=projected_loss,
        projected_fee=projected_fee,
        var_95=var_95,
        spread_bps=spread_bps,
        latency_ms=latency_ms,
    )

    quantity = _coerce_float(payload.get("quantity"), default=max(metrics.gross_notional, 1.0))
    price = _coerce_float(payload.get("price"), default=max(metrics.gross_notional, 1.0))
    if price <= 0:
        price = 1.0
    if quantity <= 0:
        quantity = max(metrics.gross_notional / price, 1.0)

    decision_request = PolicyDecisionRequest(
        account_id=str(payload.get("account_id") or account_id),
        order_id=str(payload.get("order_id") or "fallback-order"),
        instrument=normalized_instrument,
        side=_coerce_side(payload.get("side")),
        quantity=quantity,
        price=price,
        fee=fee,
        take_profit_bps=_coerce_float(payload.get("take_profit_bps"), default=50.0),
        stop_loss_bps=_coerce_float(payload.get("stop_loss_bps"), default=50.0),
    )

    intent = RiskIntentPayload(
        policy_decision=PolicyDecisionPayload(request=decision_request, response=None),
        metrics=metrics,
    )

    portfolio_state = PortfolioState(
        nav=_coerce_float(payload.get("nav"), default=max(metrics.gross_notional, 1.0) or 1.0),
        loss_to_date=_coerce_float(payload.get("loss_to_date"), default=projected_loss),
        fee_to_date=_coerce_float(payload.get("fee_to_date"), default=projected_fee),
        available_cash=_coerce_float(payload.get("available_cash")),
    )

    return RiskValidationRequest(
        account_id=decision_request.account_id,
        intent=intent,
        portfolio_state=portfolio_state,
        instrument=normalized_instrument,
        net_exposure=net_exposure,
        gross_notional=metrics.gross_notional,
        projected_loss=projected_loss,
        projected_fee=projected_fee,
        var_95=var_95,
        spread_bps=spread_bps,
        latency_ms=latency_ms,
        fee=fee,
    )


def _coerce_validation_request(
    payload: Any,
    *,
    account_id: str,
) -> RiskValidationRequest:
    if isinstance(payload, RiskValidationRequest):
        return payload
    if not isinstance(payload, Mapping):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Request body must be a mapping",
        )
    try:
        return RiskValidationRequest(**payload)
    except ValidationError as exc:
        missing_keys = {"intent", "portfolio_state"} - set(payload.keys())
        if not missing_keys:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid risk validation payload",
            ) from exc
        return _build_fallback_request(payload, account_id=account_id)


@app.post("/risk/validate", response_model=RiskValidationResponse)
def validate_risk(

    payload: Mapping[str, Any] = Body(...),
    account_id: str = Depends(require_admin_account),
) -> RiskValidationResponse:
    request = _coerce_validation_request(payload, account_id=account_id)
    if request.account_id != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )

    sanity_decision = PRETRADE_SANITY.evaluate_validation_request(request)
    if not sanity_decision.permitted:
        return RiskValidationResponse(
            valid=False,
            reasons=sanity_decision.reasons,
            fee=request.fee,
        )

    engine = RiskEngine(account_id=account_id)
    raw_symbol = request.instrument or request.intent.policy_decision.request.instrument
    symbol = str(raw_symbol)
    with traced_span("risk.validate", account_id=account_id, symbol=symbol):
        start = time.perf_counter()
        decision = engine.validate(request)
    observe_risk_validation_latency((time.perf_counter() - start) * 1000.0)
    metrics_ctx = metric_context(account_id=account_id, symbol=symbol)
    if not decision.valid:
        increment_trade_rejection(account_id, symbol, context=metrics_ctx)

    nav = float(request.portfolio_state.nav) if request.portfolio_state.nav else 0.0
    fees = float(request.portfolio_state.fee_to_date) if request.portfolio_state.fee_to_date else 0.0
    fees_pct = (fees / nav * 100.0) if nav else 0.0
    record_fees_nav_pct(account_id, symbol, fees_pct, context=metrics_ctx)

    return decision

