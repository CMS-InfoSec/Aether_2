from fastapi import Depends, FastAPI, HTTPException, status

from services.common.adapters import KrakenSecretManager
from services.common.schemas import KrakenCredentialRequest, KrakenCredentialResponse, FeeBreakdown
from services.common.security import require_admin_account

app = FastAPI(title="Secrets Service")


@app.post("/secrets/kraken", response_model=KrakenCredentialResponse)
def get_kraken_credentials(
    request: KrakenCredentialRequest,
    account_id: str = Depends(require_admin_account),
) -> KrakenCredentialResponse:
    if request.account_id != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )

    manager = KrakenSecretManager(account_id=account_id)
    try:
        credentials = manager.get_credentials()
    except PermissionError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    fee = FeeBreakdown(currency="USD", maker=0.0, taker=0.0)
    return KrakenCredentialResponse(
        account_id=account_id,
        api_key=credentials["api_key"],
        api_secret=credentials["api_secret"],
        fee=fee,
    )
