
from fastapi import Depends, FastAPI, HTTPException, status

import time

from services.common.schemas import RiskValidationRequest, RiskValidationResponse
from services.common.security import require_admin_account
from services.risk.engine import RiskEngine
from services.risk.circuit_breakers import router as circuit_router
from services.risk.cvar_forecast import router as cvar_router
from services.risk.nav_forecaster import router as nav_router

from metrics import (
    increment_trade_rejection,
    observe_risk_validation_latency,
    record_fees_nav_pct,
    setup_metrics,
    traced_span,
)

app = FastAPI(title="Risk Service")
setup_metrics(app, service_name="risk-service")


app.include_router(cvar_router)
app.include_router(circuit_router)
app.include_router(nav_router)


@app.post("/risk/validate", response_model=RiskValidationResponse)
def validate_risk(

    request: RiskValidationRequest,
    account_id: str = Depends(require_admin_account),
) -> RiskValidationResponse:
    if request.account_id != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )

    engine = RiskEngine(account_id=account_id)
    raw_symbol = request.instrument or request.intent.policy_decision.request.instrument
    symbol = str(raw_symbol)
    with traced_span("risk.validate", account_id=account_id, symbol=symbol):
        start = time.perf_counter()
        decision = engine.validate(request)
    observe_risk_validation_latency((time.perf_counter() - start) * 1000.0)
    if not decision.valid:
        increment_trade_rejection(account_id, symbol)

    nav = float(request.portfolio_state.nav) if request.portfolio_state.nav else 0.0
    fees = float(request.portfolio_state.fee_to_date) if request.portfolio_state.fee_to_date else 0.0
    fees_pct = (fees / nav * 100.0) if nav else 0.0
    record_fees_nav_pct(account_id, symbol, fees_pct)

    return decision

