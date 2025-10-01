"""FastAPI application for the fees service."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import Depends, FastAPI

from services.common import config
from services.common.schemas import FeeBreakdown, FeesEffectiveResponse
from services.common.security import require_admin_account

app = FastAPI(title="Fees Service", version="0.1.0")


@app.get("/fees/effective", response_model=FeesEffectiveResponse)
def get_effective_fees(
    isolation_segment: str,
    fee_tier: str = "standard",
    account_id: str = Depends(require_admin_account),
) -> FeesEffectiveResponse:
    config.get_redis_client(account_id)

    fee_schedule = FeeBreakdown(
        maker_bps=5.0 if fee_tier == "standard" else 3.0,
        taker_bps=7.0 if fee_tier == "standard" else 5.5,
        rebates_bps=1.0 if fee_tier != "standard" else 0.0,
    )

    return FeesEffectiveResponse(
        account_id=account_id,
        isolation_segment=isolation_segment,
        fee_schedule=fee_schedule,
        effective_at=datetime.now(tz=timezone.utc),
    )


__all__ = ["app", "get_effective_fees"]
