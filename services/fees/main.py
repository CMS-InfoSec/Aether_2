from datetime import datetime, timezone

from fastapi import Depends, FastAPI

from services.common.adapters import RedisFeastAdapter
from services.common.schemas import FeeBreakdown, FeeScheduleResponse
from services.common.security import require_admin_account

app = FastAPI(title="Fees Service")


@app.get("/fees/effective", response_model=FeeScheduleResponse)
def get_effective_fees(account_id: str = Depends(require_admin_account)) -> FeeScheduleResponse:
    redis = RedisFeastAdapter(account_id=account_id)
    default_fee = redis.fee_override("default") or {"currency": "USD", "maker": 0.1, "taker": 0.2}
    fee = FeeBreakdown(**default_fee)

    return FeeScheduleResponse(
        account_id=account_id,
        effective_from=datetime.now(timezone.utc),
        fee=fee,
    )
