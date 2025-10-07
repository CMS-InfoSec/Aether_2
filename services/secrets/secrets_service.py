"""FastAPI service for managing Kraken API secrets in Kubernetes."""
from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import logging
import os
import re
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, Field, SecretStr, validator

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

from services.common.security import (
    require_admin_account,
    require_dual_director_confirmation,
    require_mfa_context,
)
from services.oms.kraken_rest import KrakenRESTClient, KrakenRESTError
from services.oms.rate_limit_guard import RateLimitGuard
from services.secrets.middleware import (
    ForwardedSchemeMiddleware,
    TRUSTED_HOSTS,
    TRUSTED_PROXY_CLIENTS,
)
from services.secrets.secure_secrets import (
    EnvelopeEncryptor,
    EncryptedSecretEnvelope,
    LocalKMSEmulator,
    MasterKeyPersistenceError,
    SecretsMetadataStore,
)
from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger
from shared.audit_hooks import load_audit_hooks


_AUDIT_HOOKS = load_audit_hooks()
chain_log_audit = _AUDIT_HOOKS.log
if _AUDIT_HOOKS.hash_ip is not None:
    audit_chain_hash_ip = _AUDIT_HOOKS.hash_ip
else:

    def audit_chain_hash_ip(_: Optional[str]) -> Optional[str]:  # type: ignore[override]
        return None

try:  # pragma: no cover - OMS watcher is optional in some runtimes
    from services.oms.oms_kraken import KrakenCredentialWatcher
except Exception:  # pragma: no cover - fallback when OMS package unavailable
    KrakenCredentialWatcher = None  # type: ignore[misc, assignment]


LOGGER = logging.getLogger(__name__)
SECRETS_LOGGER = logging.getLogger("secrets_log")

app = FastAPI(title="Kraken Secrets Service")
app.add_middleware(TrustedHostMiddleware, allowed_hosts=TRUSTED_HOSTS)
app.add_middleware(ForwardedSchemeMiddleware)

_audit_store = AuditLogStore()
_audit_logger = TimescaleAuditLogger(_audit_store)
_auditor = SensitiveActionRecorder(_audit_logger)

_MASTER_KEY_ROTATION_INTERVAL = timedelta(days=90)
def _load_master_key_material() -> bytes:
    """Load the base64-encoded master key from the deployment secret."""

    raw_key = os.getenv("SECRET_ENCRYPTION_KEY") or os.getenv("LOCAL_KMS_MASTER_KEY")
    if not raw_key:
        raise RuntimeError(
            "Secrets service encryption key is not configured; set SECRET_ENCRYPTION_KEY"
        )
    try:
        return base64.b64decode(raw_key)
    except (ValueError, binascii.Error) as exc:
        raise RuntimeError("Secrets service encryption key must be base64 encoded") from exc


def _build_encryptor() -> EnvelopeEncryptor:
    master_key = _load_master_key_material()
    kms = LocalKMSEmulator(
        master_key=master_key,
        rotation_interval=_MASTER_KEY_ROTATION_INTERVAL,
    )
    return EnvelopeEncryptor(kms)


_encryptor = _build_encryptor()
_MASTER_KEY_CHECK_INTERVAL = timedelta(hours=6)

_KRAKEN_VALIDATION_ENDPOINT = "/private/Balance"
_KRAKEN_VALIDATION_RATE_LIMIT = RateLimitGuard()


async def _master_key_rotation_loop() -> None:
    while True:
        try:
            record = _encryptor.rotate_master_key_if_due()
            if record is not None:
                SECRETS_LOGGER.info(
                    "master_key_rotated",  # pragma: no cover - logging only
                    extra={
                        "master_key_rotation": {
                            "master_key_id": record.master_key_id,
                            "rotated_at": record.rotated_at.isoformat(),
                            "version": record.version,
                        }
                    },
                )
        except MasterKeyPersistenceError as exc:
            LOGGER.error("Master key rotation persistence failed: %s", exc)
            kms = getattr(_encryptor, "_kms", None)
            kms_key_id = getattr(kms, "key_id", "unknown")
            SECRETS_LOGGER.error(
                "master_key_rotation_failed",  # pragma: no cover - logging only
                extra={
                    "master_key_rotation_error": {
                        "error": str(exc),
                        "kms_key_id": kms_key_id,
                    }
                },
            )
        except Exception:  # pragma: no cover - defensive guard for background loop
            LOGGER.exception("Background master key rotation loop failed")
        await asyncio.sleep(_MASTER_KEY_CHECK_INTERVAL.total_seconds())


@app.on_event("startup")
async def _start_master_key_scheduler() -> None:
    app.state.master_key_rotation_task = asyncio.create_task(_master_key_rotation_loop())


@app.on_event("shutdown")
async def _stop_master_key_scheduler() -> None:
    task = getattr(app.state, "master_key_rotation_task", None)
    if task is None:
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


def _trigger_hot_reload(account_id: str) -> None:
    """Signal the OMS credential watcher to refresh without downtime."""

    if KrakenCredentialWatcher is None:  # pragma: no cover - optional runtime dependency
        LOGGER.debug("KrakenCredentialWatcher unavailable; skipping hot-reload for %s", account_id)
        return
    try:
        watcher = KrakenCredentialWatcher.instance(account_id)
    except Exception:  # pragma: no cover - watcher instantiation issues
        LOGGER.exception("Failed to obtain credential watcher for %s", account_id)
        return
    try:
        watcher.trigger_refresh()
    except Exception:  # pragma: no cover - watcher errors are non-fatal
        LOGGER.exception("Failed to trigger OMS credential refresh for %s", account_id)


def _log_secret_rotation(*, account_id: str, actor: str, rotated_at: datetime) -> None:
    payload = {
        "account_id": account_id,
        "actor": actor,
        "ts": rotated_at.isoformat(),
    }
    SECRETS_LOGGER.info("credential_rotation", extra={"secret_rotation": payload})


def _post_rotation_hooks(
    *,
    account_id: str,
    actor: str,
    rotated_at: datetime,
    background_tasks: BackgroundTasks | None = None,
) -> None:
    if background_tasks is not None:
        background_tasks.add_task(_trigger_hot_reload, account_id)
    else:
        _trigger_hot_reload(account_id)
    _log_secret_rotation(account_id=account_id, actor=actor, rotated_at=rotated_at)


def _encrypt_credentials(
    *, account_id: str, api_key: str, api_secret: str
) -> Tuple[EncryptedSecretEnvelope, SecretsMetadataStore, Dict[str, str]]:
    envelope = _encryptor.encrypt_credentials(
        account_id,
        api_key=api_key,
        api_secret=api_secret,
    )
    store = SecretsMetadataStore(account_id=account_id)
    return envelope, store, envelope.to_secret_data()


def _perform_secure_rotation(
    *,
    account_id: str,
    api_key: str,
    api_secret: str,
    api: CoreV1Api,
    request: Request,
    approvals: Tuple[str, str],
    actor: str,
) -> Dict[str, Any]:
    existing_metadata = _read_secret_metadata(
        api, name=_secret_name(account_id), namespace=KRAKEN_SECRET_NAMESPACE
    )
    before_for_audit = _serialize_metadata_for_audit(existing_metadata)

    envelope, meta_store, secret_data = _encrypt_credentials(
        account_id=account_id,
        api_key=api_key,
        api_secret=api_secret,
    )

    updated_metadata = _upsert_secret(
        api,
        account_id=account_id,
        secret_data=secret_data,
        namespace=KRAKEN_SECRET_NAMESPACE,
        existing_metadata=existing_metadata,
    )

    ip_address = request.client.host if request.client else None
    hashed_ip = _hash_ip(ip_address)
    chain_ip_hash = audit_chain_hash_ip(ip_address)

    recorded_meta = meta_store.record_rotation(
        kms_key_id=envelope.kms_key_id,
        last_rotated=updated_metadata["last_rotated_at"],
        actor=actor,
        approvers=approvals,
        ip_hash=hashed_ip,
        master_key_id=envelope.master_key_id,
        master_key_rotated_at=envelope.master_key_rotated_at,
    )

    audit_after = {
        "account_id": account_id,
        "actor": actor,
        "secret_name": updated_metadata["secret_name"],
        "rotated_at": updated_metadata["last_rotated_at"].isoformat(),
        "kms_key_id": envelope.kms_key_id,
        "ip_hash": hashed_ip,
        "approvals": list(recorded_meta.get("approvers", [])),
        "master_key_id": envelope.master_key_id,
        "master_key_rotated_at": envelope.master_key_rotated_at.isoformat(),
    }

    chain_audit_after = dict(audit_after)
    chain_audit_after["ip_hash"] = chain_ip_hash

    _auditor.record(
        action="kraken.secret.rotate",
        actor_id=actor,
        before=before_for_audit,
        after=audit_after,
    )

    if chain_log_audit is not None:
        try:
            chain_log_audit(
                actor=actor,
                action="secret.kraken.rotate",
                entity=f"kraken:{account_id}",
                before=before_for_audit,
                after=chain_audit_after,
                ip_hash=chain_ip_hash,
            )
        except Exception:  # pragma: no cover - defensive best effort
            LOGGER.exception(
                "Failed to record tamper-evident audit log for Kraken rotation on %s", account_id
            )

    LOGGER.info(
        "OMS watcher note: Kraken credentials rotated with envelope encryption",  # pragma: no cover - log only
        extra={
            "account_id": account_id,
            "secret_name": updated_metadata["secret_name"],
            "kms_key_id": envelope.kms_key_id,
            "master_key_id": envelope.master_key_id,
        },
    )

    return {
        "secret_name": updated_metadata["secret_name"],
        "last_rotated_at": updated_metadata["last_rotated_at"],
        "kms_key_id": envelope.kms_key_id,
        "metadata": recorded_meta,
    }

KRAKEN_SECRET_NAMESPACE = "aether-secrets"
ANNOTATION_CREATED_AT = "aether.kraken/createdAt"
ANNOTATION_ROTATED_AT = "aether.kraken/lastRotatedAt"


def _hash_ip(value: Optional[str]) -> str:
    if not value:
        return "anonymous"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return digest


@app.exception_handler(RequestValidationError)
async def handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    LOGGER.warning(
        "Validation error for Kraken secret request",
        extra={"path": str(request.url.path)},
    )
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": "Invalid request payload"},
    )


def _mask_identifier(value: str, *, prefix: str = "anon") -> str:
    if not value:
        return f"{prefix}:<empty>"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"{prefix}:{digest[:8]}"


def _is_authentication_error(message: str) -> bool:
    lowered = message.lower()
    markers = (
        "eapi:invalid key",
        "eapi:invalid signature",
        "eapi:invalid nonce",
        "eapi:permission denied",
        "egeneral:permission denied",
        "eauth:",
    )
    return any(marker in lowered for marker in markers)


async def _validate_kraken_credentials_async(
    api_key: str,
    api_secret: str,
    account_reference: str,
) -> bool:
    async def _credentials() -> Dict[str, Any]:
        return {"api_key": api_key, "api_secret": api_secret}

    client = KrakenRESTClient(credential_getter=_credentials)
    validated = False
    await _KRAKEN_VALIDATION_RATE_LIMIT.acquire(
        account_reference,
        _KRAKEN_VALIDATION_ENDPOINT,
        transport="rest",
        urgent=False,
    )
    try:
        payload = await client.balance()
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict):
            raise KrakenRESTError("Kraken REST payload missing balance result")
        validated = True
        return True
    finally:
        await _KRAKEN_VALIDATION_RATE_LIMIT.release(
            account_reference,
            transport="rest",
            successful=validated,
        )
        await client.close()


def _run_validation(api_key: str, api_secret: str, account_reference: str) -> bool:
    loop = asyncio.new_event_loop()
    previous_loop: Optional[asyncio.AbstractEventLoop]
    try:
        try:
            previous_loop = asyncio.get_event_loop()
        except RuntimeError:
            previous_loop = None
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(
            _validate_kraken_credentials_async(api_key, api_secret, account_reference)
        )
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        asyncio.set_event_loop(previous_loop)


def validate_kraken_credentials(
    api_key: str,
    api_secret: str,
    *,
    account_id: Optional[str] = None,
) -> bool:
    account_reference = account_id or _mask_identifier(api_key)
    try:
        return _run_validation(api_key, api_secret, account_reference)
    except HTTPException:
        raise
    except KrakenRESTError as exc:
        message = str(exc)
        if _is_authentication_error(message):
            LOGGER.warning(
                "Kraken credential validation rejected for %s: %s",
                account_reference,
                message,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Kraken API rejected the provided credentials.",
            ) from exc
        LOGGER.error(
            "Kraken credential validation failed for %s due to REST error: %s",
            account_reference,
            message,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Kraken API validation request failed.",
        ) from exc
    except Exception as exc:  # pragma: no cover - unexpected failure path
        LOGGER.exception(
            "Unexpected error validating Kraken credentials for %s", account_reference
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unexpected error validating Kraken credentials.",
        ) from exc


_ACCOUNT_ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{2,63}$")


def _ensure_no_whitespace(value: str, field_name: str) -> str:
    if not value or value.strip() != value or any(ch.isspace() for ch in value):
        raise ValueError(f"Invalid {field_name} format")
    return value


class KrakenSecretRequest(BaseModel):
    """Payload for rotating Kraken API credentials."""

    account_id: str = Field(..., description="Trading account identifier")
    api_key: SecretStr = Field(..., description="Kraken API key")
    api_secret: SecretStr = Field(..., description="Kraken API secret")

    @staticmethod
    def _ensure(pattern: re.Pattern[str], value: str, field_name: str) -> str:
        if not pattern.match(value):
            raise ValueError(f"Invalid {field_name} format")
        return value

    @validator("account_id")
    def validate_account_id(cls, value: str) -> str:  # noqa: D401 - short description is clear
        return cls._ensure(_ACCOUNT_ID_PATTERN, value, "account_id")

    @validator("api_key")
    def validate_api_key(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        _ensure_no_whitespace(raw, "api_key")
        if len(raw) < 6:
            raise ValueError("Invalid api_key format")
        return value

    @validator("api_secret")
    def validate_api_secret(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        _ensure_no_whitespace(raw, "api_secret")
        if len(raw) < 10:
            raise ValueError("Invalid api_secret format")
        return value


class KrakenSecretStatus(BaseModel):
    """Status payload describing the stored secret."""

    account_id: str = Field(..., description="Trading account identifier")
    secret_name: str = Field(..., description="Kubernetes secret name")
    last_rotated_at: datetime = Field(..., description="Timestamp of the latest rotation")


class SecretRotationResponse(BaseModel):
    """Response payload for the generic rotation endpoint."""

    account_id: str = Field(..., description="Trading account identifier")
    secret_name: str = Field(..., description="Kubernetes secret name")
    last_rotated_at: datetime = Field(..., description="Timestamp of the latest rotation")
    kms_key_id: str = Field(..., description="Identifier of the KMS data key")


class SecretStatusResponse(BaseModel):
    """Status payload for the generic credential endpoint."""

    account_id: str = Field(..., description="Trading account identifier")
    last_rotated_at: datetime = Field(..., description="Timestamp of the latest rotation")


class EncryptedRotationResponse(BaseModel):
    """Response payload for the encrypted rotation endpoint."""

    secret_name: str
    last_rotated_at: datetime
    kms_key_id: str
    secrets_meta: Dict[str, Any]

    @validator("secrets_meta", pre=True)
    def _normalize_metadata(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, datetime):
                    result[key] = item.isoformat()
                else:
                    result[key] = item
        return result


class KrakenForceRotateRequest(BaseModel):
    """Request payload for manually updating rotation metadata."""

    account_id: str = Field(..., description="Trading account identifier")

    @validator("account_id")
    def validate_account_id(cls, value: str) -> str:
        return KrakenSecretRequest._ensure(_ACCOUNT_ID_PATTERN, value, "account_id")


class RotationAuditEntry(BaseModel):
    """History entry describing an individual secret rotation."""

    account_id: str
    actor: str
    approvers: List[str]
    kms_key_id: str
    master_key_id: str
    last_rotated_at: datetime
    master_key_rotated_at: datetime
    recorded_at: datetime
    ip_hash: str


def ensure_secure_transport(request: Request) -> None:
    """Reject requests that are not routed through TLS."""

    scope = request.scope
    scheme = str(scope.get("scheme", "")).lower()
    forwarded_scheme = scope.get("aether_forwarded_scheme")

    if not forwarded_scheme and scheme == "https":
        return

    if (
        forwarded_scheme == "https"
        and request.client is not None
        and request.client.host in TRUSTED_PROXY_CLIENTS
    ):
        return

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


def _fetch_secret(api: CoreV1Api, *, name: str, namespace: str) -> Any | None:
    try:
        return api.read_namespaced_secret(name=name, namespace=namespace)
    except ApiException as exc:
        if getattr(exc, "status", None) == 404:
            return None
        LOGGER.error("Failed to fetch Kubernetes secret %s/%s", namespace, name, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to read secret metadata",
        ) from exc
    except Exception as exc:  # pragma: no cover - defensive guard
        LOGGER.exception("Unexpected error fetching secret %s/%s", namespace, name)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to read secret metadata",
        ) from exc


def _apply_rotation_annotation(
    api: CoreV1Api,
    *,
    name: str,
    namespace: str,
    annotations: Dict[str, str],
) -> None:
    patch_body = {"metadata": {"annotations": annotations}}
    try:
        api.patch_namespaced_secret(name=name, namespace=namespace, body=patch_body)
    except ApiException as exc:
        if getattr(exc, "status", None) == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Kraken credentials secret not found",
            ) from exc
        LOGGER.error(
            "Failed to update annotations on Kubernetes secret %s/%s", namespace, name, exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to update Kubernetes secret",
        ) from exc
    except Exception as exc:  # pragma: no cover - additional guard
        LOGGER.exception("Unexpected error updating annotations for %s/%s", namespace, name)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to update Kubernetes secret",
        ) from exc


def _upsert_secret(
    api: CoreV1Api,
    *,
    account_id: str,
    secret_data: Dict[str, str],
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
        "data": secret_data,
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
            "data": secret_data,
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
    "/secrets/rotate",
    response_model=SecretRotationResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(ensure_secure_transport)],
)
def rotate_secret(
    payload: KrakenSecretRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    *,
    actor_account: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
    director_approvals: Tuple[str, str] = Depends(require_dual_director_confirmation),
    api: CoreV1Api = Depends(get_core_v1_api),
) -> Response:
    if payload.account_id != actor_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload",
        )

    api_key = payload.api_key.get_secret_value()
    api_secret = payload.api_secret.get_secret_value()

    validate_kraken_credentials(
        api_key,
        api_secret,
        account_id=actor_account,
    )

    rotation = _perform_secure_rotation(
        account_id=actor_account,
        api_key=api_key,
        api_secret=api_secret,
        api=api,
        request=request,
        approvals=director_approvals,
        actor=actor_account,
    )

    rotated_at = rotation["last_rotated_at"]
    if isinstance(rotated_at, datetime):
        rotation_ts = rotated_at
    else:
        rotation_ts = datetime.now(timezone.utc)
    _post_rotation_hooks(
        account_id=actor_account,
        actor=actor_account,
        rotated_at=rotation_ts,
        background_tasks=background_tasks,
    )

    response_payload = SecretRotationResponse(
        account_id=actor_account,
        secret_name=rotation["secret_name"],
        last_rotated_at=rotation_ts,
        kms_key_id=rotation["kms_key_id"],
    )
    headers = {
        "X-OMS-Watcher": "Credentials rotated; OMS hot-reload triggered.",
    }
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=jsonable_encoder(response_payload),
        headers=headers,
    )


@app.post(
    "/secrets/kraken",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(ensure_secure_transport)],
)
def rotate_kraken_secret(
    payload: KrakenSecretRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    *,
    actor_account: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
    director_approvals: Tuple[str, str] = Depends(require_dual_director_confirmation),
    api: CoreV1Api = Depends(get_core_v1_api),
) -> Response:
    if payload.account_id != actor_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload",
        )

    api_key = payload.api_key.get_secret_value()
    api_secret = payload.api_secret.get_secret_value()

    validate_kraken_credentials(
        api_key,
        api_secret,
        account_id=actor_account,
    )

    rotation = _perform_secure_rotation(
        account_id=actor_account,
        api_key=api_key,
        api_secret=api_secret,
        api=api,
        request=request,
        approvals=director_approvals,
        actor=actor_account,
    )

    rotated_at = rotation["last_rotated_at"]
    if isinstance(rotated_at, datetime):
        rotation_ts = rotated_at
    else:
        rotation_ts = datetime.now(timezone.utc)
    _post_rotation_hooks(
        account_id=actor_account,
        actor=actor_account,
        rotated_at=rotation_ts,
        background_tasks=background_tasks,
    )

    response = Response(status_code=status.HTTP_204_NO_CONTENT)
    response.headers[
        "X-OMS-Watcher"
    ] = "Kraken credentials rotated; OMS hot-reloads when secret annotations change."
    return response


@app.post(
    "/secrets/rotate_encrypted",
    response_model=EncryptedRotationResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(ensure_secure_transport)],
)
def rotate_encrypted_secret(
    payload: KrakenSecretRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    *,
    actor_account: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
    director_approvals: Tuple[str, str] = Depends(require_dual_director_confirmation),
    api: CoreV1Api = Depends(get_core_v1_api),
) -> Response:
    if payload.account_id != actor_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload",
        )

    api_key = payload.api_key.get_secret_value()
    api_secret = payload.api_secret.get_secret_value()

    validate_kraken_credentials(
        api_key,
        api_secret,
        account_id=actor_account,
    )

    rotation = _perform_secure_rotation(
        account_id=actor_account,
        api_key=api_key,
        api_secret=api_secret,
        api=api,
        request=request,
        approvals=director_approvals,
        actor=actor_account,
    )

    rotated_at = rotation["last_rotated_at"]
    if isinstance(rotated_at, datetime):
        rotation_ts = rotated_at
    else:
        rotation_ts = datetime.now(timezone.utc)
    _post_rotation_hooks(
        account_id=actor_account,
        actor=actor_account,
        rotated_at=rotation_ts,
        background_tasks=background_tasks,
    )

    response = EncryptedRotationResponse(
        secret_name=rotation["secret_name"],
        last_rotated_at=rotation_ts,
        kms_key_id=rotation["kms_key_id"],
        secrets_meta=rotation["metadata"],
    )

    headers = {
        "X-OMS-Watcher": "Kraken credentials rotated with envelope encryption; OMS watchers notified.",
    }
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=jsonable_encoder(response),
        headers=headers,
    )


@app.post(
    "/secrets/kraken/force_rotate",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(ensure_secure_transport)],
)
def force_rotate_kraken_secret(
    payload: KrakenForceRotateRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    *,
    actor_account: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
    api: CoreV1Api = Depends(get_core_v1_api),
) -> Response:
    if payload.account_id != actor_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload",
        )

    secret_name = _secret_name(actor_account)
    secret = _fetch_secret(api, name=secret_name, namespace=KRAKEN_SECRET_NAMESPACE)
    if secret is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Kraken credentials not found for account",
        )

    metadata = _extract_metadata(secret)
    now = datetime.now(timezone.utc)
    created_at = metadata.get("created_at") or now
    if isinstance(created_at, datetime):
        created_iso = created_at.isoformat()
    elif isinstance(created_at, str):
        created_iso = created_at
    else:
        created_iso = now.isoformat()

    raw_annotations = getattr(getattr(secret, "metadata", None), "annotations", None) or {}
    updated_annotations = dict(raw_annotations)
    updated_annotations[ANNOTATION_CREATED_AT] = created_iso
    updated_annotations[ANNOTATION_ROTATED_AT] = now.isoformat()

    _apply_rotation_annotation(
        api,
        name=secret_name,
        namespace=KRAKEN_SECRET_NAMESPACE,
        annotations=updated_annotations,
    )

    _post_rotation_hooks(
        account_id=actor_account,
        actor=actor_account,
        rotated_at=now,
        background_tasks=background_tasks,
    )

    before_for_audit = _serialize_metadata_for_audit(metadata)
    after_for_audit = {
        "account_id": actor_account,
        "actor": actor_account,
        "secret_name": secret_name,
        "rotated_at": now.isoformat(),
        "ip_hash": _hash_ip(request.client.host if request.client else None),
    }

    _auditor.record(
        action="kraken.secret.force_rotate",
        actor_id=actor_account,
        before=before_for_audit,
        after=after_for_audit,
    )

    LOGGER.info(
        "Force rotation triggered for Kraken credentials; OMS watchers notified",
        extra={"account_id": actor_account, "secret_name": secret_name},
    )

    response = Response(status_code=status.HTTP_204_NO_CONTENT)
    response.headers[
        "X-OMS-Watcher"
    ] = "Kraken credentials force-rotated; OMS hot-reloads when secret annotations change."
    return response


@app.get(
    "/secrets/status",
    response_model=SecretStatusResponse,
    dependencies=[Depends(ensure_secure_transport)],
)
def secret_status(
    account_id: str = Query(..., description="Trading account identifier"),
    actor_account: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
    api: CoreV1Api = Depends(get_core_v1_api),
) -> SecretStatusResponse:
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
    if not isinstance(rotated_at, datetime):
        try:
            rotated_at = datetime.fromisoformat(str(rotated_at))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Rotation metadata corrupt",
            ) from None

    return SecretStatusResponse(account_id=account_id, last_rotated_at=rotated_at)


@app.get(
    "/secrets/audit",
    response_model=List[RotationAuditEntry],
    dependencies=[Depends(ensure_secure_transport)],
)
def secret_rotation_audit(
    account_id: str = Query(..., description="Trading account identifier"),
    actor_account: str = Depends(require_admin_account),
    _: str = Depends(require_mfa_context),
) -> List[RotationAuditEntry]:
    if account_id != actor_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and query parameter",
        )

    history = SecretsMetadataStore.history(account_id)
    audit_entries: List[RotationAuditEntry] = []
    for entry in history:
        last_rotated = entry.get("last_rotated")
        if isinstance(last_rotated, datetime):
            last_rotated_at = last_rotated
        else:
            try:
                last_rotated_at = datetime.fromisoformat(str(last_rotated))
            except ValueError:
                last_rotated_at = datetime.now(timezone.utc)

        master_rotated = entry.get("master_key_rotated_at", last_rotated_at)
        if isinstance(master_rotated, datetime):
            master_key_rotated_at = master_rotated
        else:
            try:
                master_key_rotated_at = datetime.fromisoformat(str(master_rotated))
            except ValueError:
                master_key_rotated_at = last_rotated_at

        recorded_at = entry.get("ts", datetime.now(timezone.utc))
        if isinstance(recorded_at, datetime):
            ts = recorded_at
        else:
            try:
                ts = datetime.fromisoformat(str(recorded_at))
            except ValueError:
                ts = datetime.now(timezone.utc)

        audit_entries.append(
            RotationAuditEntry(
                account_id=entry.get("account_id", account_id),
                actor=entry.get("actor", actor_account),
                approvers=list(entry.get("approvers") or []),
                kms_key_id=entry.get("kms_key_id", "unknown"),
                master_key_id=entry.get("master_key_id", "unknown"),
                last_rotated_at=last_rotated_at,
                master_key_rotated_at=master_key_rotated_at,
                recorded_at=ts,
                ip_hash=entry.get("ip_hash", "anonymous"),
            )
        )

    return audit_entries


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


@app.post(
    "/secrets/kraken/test",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(ensure_secure_transport)],
)
def test_kraken_secret(
    payload: KrakenSecretRequest,
    _: str = Depends(require_admin_account),
    __: str = Depends(require_mfa_context),
) -> Response:
    api_key = payload.api_key.get_secret_value()
    api_secret = payload.api_secret.get_secret_value()
    validate_kraken_credentials(
        api_key,
        api_secret,
        account_id=payload.account_id,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


__all__ = [
    "app",
    "KrakenSecretRequest",
    "KrakenSecretStatus",
    "SecretRotationResponse",
    "SecretStatusResponse",
    "EncryptedRotationResponse",
    "KrakenForceRotateRequest",
    "RotationAuditEntry",
    "rotate_secret",
    "rotate_kraken_secret",
    "rotate_encrypted_secret",
    "force_rotate_kraken_secret",
    "secret_status",
    "secret_rotation_audit",
    "kraken_secret_status",
    "test_kraken_secret",
    "validate_kraken_credentials",
]

