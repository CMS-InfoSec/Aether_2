
from fastapi import Depends, FastAPI, HTTPException, status

from services.common.adapters import TimescaleAdapter
from services.common.schemas import RiskValidationRequest, RiskValidationResponse
from services.common.security import require_admin_account

app = FastAPI(title="Risk Service")



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

    timescale = TimescaleAdapter(account_id=account_id)
    notional = request.gross_notional
    within_limits = timescale.check_limits(notional)
    reasons = [] if within_limits else ["Projected usage exceeds configured limits"]

    if within_limits:
        timescale.record_usage(notional)

    return RiskValidationResponse(valid=within_limits, reasons=reasons, fee=request.fee)

