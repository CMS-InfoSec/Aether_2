"""FastAPI application for the secrets service."""
from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from services.common import config
from services.common.schemas import KrakenSecretRequest, KrakenSecretResponse
from services.common.security import require_admin_account

app = FastAPI(title="Secrets Service", version="0.1.0")


@app.post("/secrets/kraken", response_model=KrakenSecretResponse)
def get_kraken_secret(
    request: KrakenSecretRequest, account_id: str = Depends(require_admin_account)
) -> KrakenSecretResponse:
    if request.account_id != account_id:
        raise HTTPException(status_code=400, detail="Account isolation attributes do not match header context")

    try:
        credentials = config.get_kraken_credentials(account_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return KrakenSecretResponse(
        account_id=account_id,
        isolation_segment=request.isolation_segment,
        key=credentials.key,
        secret=credentials.secret,
    )


__all__ = ["app", "get_kraken_secret"]
