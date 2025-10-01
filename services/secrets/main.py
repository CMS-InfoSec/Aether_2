
from fastapi import Depends, FastAPI, HTTPException, Query, status

from services.common.adapters import KrakenSecretManager
from services.common.schemas import (
    KrakenSecretRotationRequest,
    KrakenSecretRotationResponse,
    KrakenSecretStatusResponse,
)
from services.common.security import require_admin_account, require_mfa_context
from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger

app = FastAPI(title="Secrets Service")

_audit_store = AuditLogStore()
_audit_logger = TimescaleAuditLogger(_audit_store)
_auditor = SensitiveActionRecorder(_audit_logger)


@app.post("/secrets/kraken/rotate", response_model=KrakenSecretRotationResponse)
def rotate_kraken_secret(
    request: KrakenSecretRotationRequest,
    account_id: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
) -> KrakenSecretRotationResponse:
    if request.account_id != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )

    manager = KrakenSecretManager(account_id=account_id)
    rotation = manager.rotate_credentials(api_key=request.api_key, api_secret=request.api_secret)

    _auditor.record(
        action="kraken.secret.rotate",
        actor_id=account_id,
        before=rotation["before"],
        after=rotation["after"],
    )

    return KrakenSecretRotationResponse(
        account_id=account_id,
        secret_name=rotation["secret_name"],
        rotated_at=rotation["rotated_at"],
    )


@app.get("/secrets/kraken/status", response_model=KrakenSecretStatusResponse)
def kraken_secret_status(
    account_id: str = Query(..., description="Trading account identifier"),
    header_account: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
) -> KrakenSecretStatusResponse:
    if account_id != header_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and query parameter.",
        )

    manager = KrakenSecretManager(account_id=account_id)
    status_payload = manager.status()
    if not status_payload:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No Kraken credential rotation metadata found.",
        )

    return KrakenSecretStatusResponse(
        account_id=account_id,
        secret_name=status_payload["secret_name"],
        created_at=status_payload["created_at"],
        rotated_at=status_payload["rotated_at"],
    )

