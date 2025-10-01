"""FastAPI application for the universe service."""
from __future__ import annotations

from fastapi import Depends, FastAPI

from services.common import config
from services.common.schemas import UniverseApprovedResponse
from services.common.security import require_admin_account

app = FastAPI(title="Universe Service", version="0.1.0")


@app.get("/universe/approved", response_model=UniverseApprovedResponse)
def get_universe(
    isolation_segment: str,
    fee_tier: str = "standard",
    account_id: str = Depends(require_admin_account),
) -> UniverseApprovedResponse:
    feast_client = config.get_feast_client(account_id)

    base_symbols = ["BTC-USD", "ETH-USD", "SOL-USD"]
    if fee_tier != "standard":
        base_symbols.append("ATOM-USD")
    scoped_symbols = [f"{feast_client.account_namespace}:{symbol}" for symbol in base_symbols]

    return UniverseApprovedResponse(
        account_id=account_id,
        isolation_segment=isolation_segment,
        symbols=scoped_symbols,
        fee_tier=fee_tier,
    )


__all__ = ["app", "get_universe"]
