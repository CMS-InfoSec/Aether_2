"""FastAPI service for managing Kraken API secrets in Kubernetes."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, SecretStr

try:  # pragma: no cover - optional dependency for runtime environment
    from kubernetes import client, config
    from kubernetes.client import CoreV1Api
    from kubernetes.client.rest import ApiException
except ImportError:  # pragma: no cover - fallback for testing environments
    client = None  # type: ignore
    config = None  # type: ignore

    class CoreV1Api:  # type: ignore
        """Placeholder to satisfy type-checkers when kubernetes is unavailable."""

        ...

    class ApiException(Exception):  # type: ignore
        """Placeholder ``ApiException`` when kubernetes is unavailable."""

        def __init__(self, status: int = 500, reason: str | None = None) -> None:
            super().__init__(reason)
            self.status = status

from services.common.security import require_admin_account, require_mfa_context
from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger


LOGGER = logging.getLogger(__name__)

app = FastAPI(title="Kraken Secrets Service")

_audit_store = AuditLogStore()
_audit_logger = TimescaleAuditLogger(_audit_store)
_auditor = SensitiveActionRecorder(_audit_logger)

KRAKEN_SECRET_NAMESPACE = "aether-secrets"
ANNOTATION_CREATED_AT = "aether.kraken/createdAt"
ANNOTATION_ROTATED_AT = "aether.kraken/lastRotatedAt"


class KrakenSecretRequest(BaseModel):
    """Payload for rotating Kraken API credentials."""

    account_id: str = Field(..., description="Trading account identifier")
    api_key: SecretStr = Field(..., description="Kraken API key")
    api_secret: SecretStr = Field(..., description="Kraken API secret")


class KrakenSecretResponse(BaseModel):
    """Response payload after rotating credentials."""

    account_id: str = Field(..., description="Trading account identifier")
    secret_name: str = Field(..., description="Kubernetes secret name")
    last_rotated_at: datetime = Field(..., description="Timestamp of the latest rotation")


class KrakenSecretStatus(BaseModel):
    """Status payload describing the stored secret."""

    account_id: str = Field(..., description="Trading account identifier")
    secret_name: str = Field(..., description="Kubernetes secret name")
    last_rotated_at: datetime = Field(..., description="Timestamp of the latest rotation")


def ensure_secure_transport(request: Request) -> None:
    """Reject requests that are not routed through TLS."""

    scheme = request.url.scheme
    if scheme.lower() != "https":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="TLS termination required (https only).",
        )


def _load_kubernetes_configuration() -> None:
    if config is None:  # pragma: no cover - handled in tests via monkeypatching
        return
    try:  # pragma: no cover - depends on deployment environment
        config.load_incluster_config()
    except Exception:
        try:  # pragma: no cover - fallback for local development
            config.load_kube_config()
        except Exception:
            LOGGER.debug("Kubernetes configuration could not be loaded", exc_info=True)


def get_core_v1_api() -> CoreV1Api:
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kubernetes client is not available",
        )
    _load_kubernetes_configuration()
    return client.CoreV1Api()


def _secret_name(account_id: str) -> str:
    return f"kraken-keys-{account_id}"


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _extract_metadata(secret: Any) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}
    annotations: Dict[str, str] = {}
    raw_metadata = getattr(secret, "metadata", None)
    if raw_metadata is not None:
        annotations = getattr(raw_metadata, "annotations", None) or {}
        metadata_name = getattr(raw_metadata, "name", None)
        if metadata_name:
            metadata["secret_name"] = metadata_name
    metadata["created_at"] = _parse_timestamp(annotations.get(ANNOTATION_CREATED_AT))
    metadata["last_rotated_at"] = _parse_timestamp(annotations.get(ANNOTATION_ROTATED_AT))
    return metadata


def _serialize_metadata_for_audit(metadata: Dict[str, Any]) -> Dict[str, Any]:
    audit_payload: Dict[str, Any] = {}
    for key, value in metadata.items():
        if isinstance(value, datetime):
            audit_payload[key] = value.isoformat()
        else:
            audit_payload[key] = value
    return audit_payload


def _read_secret_metadata(api: CoreV1Api, *, name: str, namespace: str) -> Dict[str, Any]:
    try:
        secret = api.read_namespaced_secret(name=name, namespace=namespace)
    except ApiException as exc:
        if getattr(exc, "status", None) == 404:
            return {}
        LOGGER.error("Failed to read Kubernetes secret %s/%s", namespace, name, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to read secret metadata",
        ) from exc
    except Exception as exc:  # pragma: no cover - protective guard
        LOGGER.exception("Unexpected error reading secret %s/%s", namespace, name)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to read secret metadata",
        ) from exc
    return _extract_metadata(secret)


def _upsert_secret(
    api: CoreV1Api,
    *,
    account_id: str,
    api_key: str,
    api_secret: str,
    namespace: str,
    existing_metadata: Dict[str, Any],
) -> Dict[str, Any]:
    secret_name = _secret_name(account_id)
    now = datetime.now(timezone.utc)
    created_at = existing_metadata.get("created_at") or now
    annotations = {
        ANNOTATION_CREATED_AT: created_at.isoformat(),
        ANNOTATION_ROTATED_AT: now.isoformat(),
    }
    patch_body = {
        "metadata": {"annotations": annotations},
        "stringData": {"api_key": api_key, "api_secret": api_secret},
        "type": "Opaque",
    }

    try:
        api.patch_namespaced_secret(name=secret_name, namespace=namespace, body=patch_body)
    except ApiException as exc:
        if getattr(exc, "status", None) != 404:
            LOGGER.error(
                "Failed to patch Kubernetes secret %s/%s", namespace, secret_name, exc_info=True
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Unable to update Kubernetes secret",
            ) from exc
        secret_manifest = {
            "metadata": {
                "name": secret_name,
                "namespace": namespace,
                "annotations": annotations,
            },
            "stringData": {"api_key": api_key, "api_secret": api_secret},
            "type": "Opaque",
        }
        try:
            api.create_namespaced_secret(namespace=namespace, body=secret_manifest)
        except Exception as create_exc:
            LOGGER.error(
                "Failed to create Kubernetes secret %s/%s",
                namespace,
                secret_name,
                exc_info=True,
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Unable to create Kubernetes secret",
            ) from create_exc
    except Exception as exc:  # pragma: no cover - additional safety
        LOGGER.exception("Unexpected error updating secret %s/%s", namespace, secret_name)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to update Kubernetes secret",
        ) from exc

    return {
        "secret_name": secret_name,
        "created_at": created_at,
        "last_rotated_at": now,
    }


@app.post(
    "/secrets/kraken",
    response_model=KrakenSecretResponse,
    dependencies=[Depends(ensure_secure_transport)],
)
def rotate_kraken_secret(
    payload: KrakenSecretRequest,
    actor_account: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
    api: CoreV1Api = Depends(get_core_v1_api),
) -> KrakenSecretResponse:
    if payload.account_id != actor_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload",
        )

    api_key = payload.api_key.get_secret_value()
    api_secret = payload.api_secret.get_secret_value()

    existing_metadata = _read_secret_metadata(
        api, name=_secret_name(actor_account), namespace=KRAKEN_SECRET_NAMESPACE
    )
    before_for_audit = _serialize_metadata_for_audit(existing_metadata)

    updated_metadata = _upsert_secret(
        api,
        account_id=actor_account,
        api_key=api_key,
        api_secret=api_secret,
        namespace=KRAKEN_SECRET_NAMESPACE,
        existing_metadata=existing_metadata,
    )

    _auditor.record(
        action="kraken.secret.rotate",
        actor_id=actor_account,
        before=before_for_audit,
        after=_serialize_metadata_for_audit(updated_metadata),
    )

    return KrakenSecretResponse(
        account_id=actor_account,
        secret_name=updated_metadata["secret_name"],
        last_rotated_at=updated_metadata["last_rotated_at"],
    )


@app.get(
    "/secrets/kraken/status",
    response_model=KrakenSecretStatus,
    dependencies=[Depends(ensure_secure_transport)],
)
def kraken_secret_status(
    account_id: str = Query(..., description="Trading account identifier"),
    actor_account: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
    api: CoreV1Api = Depends(get_core_v1_api),
) -> KrakenSecretStatus:
    if account_id != actor_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and query parameter",
        )

    metadata = _read_secret_metadata(
        api, name=_secret_name(account_id), namespace=KRAKEN_SECRET_NAMESPACE
    )
    if not metadata:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No rotation metadata found for account",
        )
    rotated_at = metadata.get("last_rotated_at")
    if rotated_at is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Rotation timestamp unavailable",
        )

    return KrakenSecretStatus(
        account_id=account_id,
        secret_name=metadata.get("secret_name", _secret_name(account_id)),
        last_rotated_at=rotated_at,
    )


__all__ = [
    "app",
    "KrakenSecretRequest",
    "KrakenSecretResponse",
    "KrakenSecretStatus",
]

