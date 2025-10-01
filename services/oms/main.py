"""FastAPI application for the OMS service."""
from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from services.common import config
from services.common.schemas import OMSPlaceRequest, OMSPlaceResponse
from services.common.security import require_admin_account

app = FastAPI(title="OMS Service", version="0.1.0")


@app.post("/oms/place", response_model=OMSPlaceResponse)
def place_order(
    request: OMSPlaceRequest, account_id: str = Depends(require_admin_account)
) -> OMSPlaceResponse:
    if request.account_id != account_id:
        raise HTTPException(status_code=400, detail="Account isolation attributes do not match header context")

    config.get_kafka_producer(account_id)
    config.get_nats_producer(account_id)

    if request.side.lower() not in {"buy", "sell"}:
        raise HTTPException(status_code=422, detail="Side must be 'buy' or 'sell'")

    accepted = True
    status = "accepted" if accepted else "rejected"

    return OMSPlaceResponse(
        account_id=account_id,
        isolation_segment=request.isolation_segment,
        status=status,
        accepted=accepted,
        fee_tier=request.fee_tier,
    )


__all__ = ["app", "place_order"]
