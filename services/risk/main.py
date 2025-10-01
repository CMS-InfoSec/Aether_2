"""FastAPI application for the risk service."""
from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from services.common import config
from services.common.schemas import RiskValidationRequest, RiskValidationResponse
from services.common.security import require_admin_account

app = FastAPI(title="Risk Service", version="0.1.0")


@app.post("/risk/validate", response_model=RiskValidationResponse)
def validate_risk(
    request: RiskValidationRequest, account_id: str = Depends(require_admin_account)
) -> RiskValidationResponse:
    if request.account_id != account_id:
        raise HTTPException(status_code=400, detail="Account isolation attributes do not match header context")

    config.get_timescale_session(account_id)

    limit = 1_000_000 * (1 if request.fee_tier == "standard" else 2)
    valid = abs(request.net_exposure) <= limit
    reason = None if valid else "Exposure exceeds configured account limit"

    return RiskValidationResponse(
        account_id=account_id,
        isolation_segment=request.isolation_segment,
        valid=valid,
        reason=reason,
        fee_tier=request.fee_tier,
    )


__all__ = ["app", "validate_risk"]
