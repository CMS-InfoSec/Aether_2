from fastapi import Depends, FastAPI, HTTPException, status

from services.common.adapters import KafkaNATSAdapter
from services.common.schemas import OrderPlacementRequest, OrderPlacementResponse
from services.common.security import require_admin_account

app = FastAPI(title="OMS Service")


@app.post("/oms/place", response_model=OrderPlacementResponse)
def place_order(
    request: OrderPlacementRequest,
    account_id: str = Depends(require_admin_account),
) -> OrderPlacementResponse:
    if request.account_id != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )

    kafka = KafkaNATSAdapter(account_id=account_id)
    kafka.publish(
        topic="oms.orders",
        payload={
            "order_id": request.order_id,
            "instrument": request.instrument,
            "side": request.side,
            "quantity": request.quantity,
        },
    )

    venue = "kraken" if request.instrument.endswith("USD") else "kraken-intl"

    return OrderPlacementResponse(accepted=True, routed_venue=venue, fee=request.fee)
