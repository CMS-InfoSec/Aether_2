
from datetime import datetime, timezone
from typing import Any, Callable, Dict, TypeVar, cast

from fastapi import Depends, FastAPI, HTTPException, Query, status

from metrics import setup_metrics
from starlette.middleware.trustedhost import TrustedHostMiddleware

from services.common.adapters import KrakenSecretManager
from services.common.schemas import (
    KrakenCredentialRequest,
    KrakenCredentialResponse,
    KrakenSecretStatusResponse,
)
from services.common.security import require_admin_account, require_mfa_context
from services.secrets.middleware import ForwardedSchemeMiddleware, TRUSTED_HOSTS
from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger

app = FastAPI(title="Secrets Service")
setup_metrics(app, service_name="secrets-gateway")
app.add_middleware(TrustedHostMiddleware, allowed_hosts=TRUSTED_HOSTS)
app.add_middleware(ForwardedSchemeMiddleware)

_audit_store = AuditLogStore()
_audit_logger = TimescaleAuditLogger(_audit_store)
_auditor = SensitiveActionRecorder(_audit_logger)


RouteFn = TypeVar("RouteFn", bound=Callable[..., Any])


def _app_post(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``app.post`` to satisfy static type checking."""

    return cast(Callable[[RouteFn], RouteFn], app.post(*args, **kwargs))


def _app_get(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``app.get`` to satisfy static type checking."""

    return cast(Callable[[RouteFn], RouteFn], app.get(*args, **kwargs))


def _normalize_metadata(metadata: Dict[str, Any], *, fallback_secret_name: str) -> Dict[str, Any]:
    normalized = dict(metadata)
    secret_name = normalized.get("secret_name", fallback_secret_name)
    created_at = normalized.get("created_at")
    rotated_at = normalized.get("rotated_at")

    if created_at is None and rotated_at is None:
        now = datetime.now(timezone.utc)
        created_at = rotated_at = now
    else:
        if created_at is None:
            created_at = rotated_at
        if rotated_at is None:
            rotated_at = created_at

    normalized["secret_name"] = secret_name
    normalized["created_at"] = created_at
    normalized["rotated_at"] = rotated_at
    return normalized


@_app_post("/secrets/kraken", response_model=KrakenCredentialResponse)
def upsert_kraken_secret(
    request: KrakenCredentialRequest,
    account_id: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
) -> KrakenCredentialResponse:
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
        after=rotation["metadata"],
    )

    metadata = _normalize_metadata(rotation.get("metadata", {}), fallback_secret_name=manager.secret_name)

    return KrakenCredentialResponse(
        account_id=account_id,
        secret_name=metadata["secret_name"],
        created_at=metadata["created_at"],
        rotated_at=metadata["rotated_at"],
    )


@_app_get("/secrets/kraken/status", response_model=KrakenSecretStatusResponse)
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

    metadata = _normalize_metadata(status_payload, fallback_secret_name=manager.secret_name)

    return KrakenSecretStatusResponse(
        account_id=account_id,
        secret_name=metadata["secret_name"],
        created_at=metadata["created_at"],
        rotated_at=metadata["rotated_at"],
    )

