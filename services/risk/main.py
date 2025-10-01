
from fastapi import Depends, FastAPI, HTTPException, status

from services.common.schemas import RiskValidationRequest, RiskValidationResponse
from services.common.security import require_admin_account
from services.risk.engine import RiskEngine

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

    engine = RiskEngine(account_id=account_id)
    decision = engine.validate(request)
    return decision

