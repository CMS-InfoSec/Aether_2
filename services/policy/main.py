
from fastapi import Depends, FastAPI, HTTPException, status

from services.common.adapters import KafkaNATSAdapter
from services.common.schemas import PolicyDecisionRequest, PolicyDecisionResponse
from services.common.security import require_admin_account

app = FastAPI(title="Policy Service")



@app.post("/policy/decide", response_model=PolicyDecisionResponse)
def decide_policy(

    request: PolicyDecisionRequest,
    account_id: str = Depends(require_admin_account),
) -> PolicyDecisionResponse:
    if request.account_id != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )

    kafka = KafkaNATSAdapter(account_id=account_id)
    approved = request.quantity * request.price <= 500_000
    reason = None if approved else "Order notional exceeds policy limit"
    fee_payload = request.fee.model_dump()

    kafka.publish(
        topic="policy.decisions",
        payload={
            "order_id": request.order_id,
            "approved": approved,
            "reason": reason,
            "fee": fee_payload,
        },
    )

    return PolicyDecisionResponse(
        approved=approved,
        reason=reason,
        effective_fee=request.fee if approved else request.fee,
    )

