from fastapi import Depends, FastAPI

from services.common.adapters import RedisFeastAdapter
from services.common.schemas import ApprovedUniverseResponse, FeeBreakdown
from services.common.security import require_admin_account

app = FastAPI(title="Universe Service")


@app.get("/universe/approved", response_model=ApprovedUniverseResponse)
def approved_universe(account_id: str = Depends(require_admin_account)) -> ApprovedUniverseResponse:
    redis = RedisFeastAdapter(account_id=account_id)
    instruments = redis.approved_instruments()
    fee_overrides = {}
    for instrument in instruments:
        override = redis.fee_override(instrument)
        if override:
            fee_overrides[instrument] = FeeBreakdown(
                currency=override.get("currency", "USD"),
                maker=override.get("maker", 0.0),
                taker=override.get("taker", 0.0),
            )

    return ApprovedUniverseResponse(account_id=account_id, instruments=instruments, fee_overrides=fee_overrides)
