"""FastAPI application for the policy service."""
from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from services.common import config
from services.common.schemas import PolicyDecisionRequest, PolicyDecisionResponse
from services.common.security import require_admin_account

app = FastAPI(title="Policy Service", version="0.1.0")


@app.post("/policy/decide", response_model=PolicyDecisionResponse)
def decide_policy(
    request: PolicyDecisionRequest, account_id: str = Depends(require_admin_account)
) -> PolicyDecisionResponse:
    """Evaluate whether an order candidate complies with policy guardrails."""
    if request.account_id != account_id:
        raise HTTPException(status_code=400, detail="Account isolation attributes do not match header context")

    # Establish dependencies to mirror production wiring.
    config.get_redis_client(account_id)
    config.get_feast_client(account_id)

    notional = abs(request.quantity * request.price)
    total_fees = notional * 0.001 if request.include_fees else 0.0
    approved = total_fees <= notional * 0.05
    reason = "" if approved else "Estimated fees exceed configured policy threshold"

    return PolicyDecisionResponse(
        account_id=account_id,
        isolation_segment=request.isolation_segment,
        approved=approved,
        reason=reason,
        fee_tier=request.fee_tier,
        total_fees=total_fees,
    )


__all__ = ["app", "decide_policy"]
