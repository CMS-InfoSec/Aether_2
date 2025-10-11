"""FastAPI service for managing encrypted Kraken API credentials in Kubernetes."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
import hashlib

try:  # pragma: no cover - prefer the real cryptography implementation
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError:  # pragma: no cover - lightweight environments
    AESGCM = None  # type: ignore[assignment]
from fastapi import Depends, FastAPI, HTTPException, Header, Query, Request, status
from fastapi.responses import JSONResponse
from kubernetes import client, config
from kubernetes.client import ApiException
from kubernetes.config.config_exception import ConfigException
from pydantic import BaseModel, Field, validator

from services.secrets.signing import sign_kraken_request
from shared.k8s import ANNOTATION_ROTATED_AT as K8S_ROTATED_AT
from shared.audit_hooks import AuditEvent, load_audit_hooks
from shared.runtime_checks import ensure_insecure_default_flag_disabled


LOGGER = logging.getLogger(__name__)
SECRETS_LOGGER = logging.getLogger("secrets_log")

_INSECURE_DEFAULTS_FLAG = "SECRETS_ALLOW_INSECURE_DEFAULTS"
_DEFAULT_TEST_TOKEN = "local-dev-token"

ensure_insecure_default_flag_disabled(_INSECURE_DEFAULTS_FLAG)

_INITIAL_BACKOFF_SECONDS = 0.5
_MAX_BACKOFF_SECONDS = 8.0
_MAX_INITIALIZATION_ATTEMPTS = 5


class _InitializationState:
    """Tracks startup initialization status for dependency injection."""

    def __init__(self) -> None:
        self.settings: Optional[Settings] = None
        self.cipher: Optional[SecretCipher] = None
        self.initialization_error: Optional[BaseException] = None
        self.lock = asyncio.Lock()


STATE = _InitializationState()



def _parse_authorized_token_labels(raw_tokens: str) -> Dict[str, str]:
    tokens: Dict[str, str] = {}
    for entry in raw_tokens.split(","):
        entry = entry.strip()
        if not entry:
            continue
        token, _, label = entry.partition(":")
        token = token.strip()
        if not token:
            continue
        label = label.strip() or "system"
        tokens[token] = label
    if not tokens:
        raise ValueError("at least one authorized token must be configured")
    return tokens


def _load_authorized_tokens_from_env() -> Dict[str, str]:
    raw_tokens = os.getenv("KRAKEN_SECRETS_AUTH_TOKENS", "")
    try:
        tokens = _parse_authorized_token_labels(raw_tokens)
    except ValueError as exc:  # pragma: no cover - configuration error path
        raise RuntimeError(
            "KRAKEN_SECRETS_AUTH_TOKENS environment variable must define at least one token"
        ) from exc
    return tokens



class Settings(BaseModel):
    kubernetes_namespace: str = Field(
        default_factory=lambda: os.getenv("KRAKEN_SECRET_NAMESPACE", "default")
    )
    encryption_key_b64: str = Field(..., alias="SECRET_ENCRYPTION_KEY")
    kraken_api_url: str = Field(
        default_factory=lambda: os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
    )
    authorized_token_labels: Dict[str, str] = Field(
        default_factory=_load_authorized_tokens_from_env,
        alias="KRAKEN_SECRETS_AUTH_TOKENS",
    )

    authorized_token_ids: Tuple[str, ...] = Field(
        ..., alias="SECRETS_SERVICE_AUTH_TOKENS"
    )


    class Config:
        allow_population_by_field_name = True

    @validator("authorized_token_labels", pre=True, allow_reuse=True)
    def _normalize_authorized_token_labels(  # type: ignore[override]
        cls, value: Any
    ) -> Dict[str, str]:
        if isinstance(value, str):
            return _parse_authorized_token_labels(value)
        if isinstance(value, dict):
            cleaned = {
                str(token).strip(): str(label).strip() or "system"
                for token, label in value.items()
                if str(token).strip()
            }
            if not cleaned:
                raise ValueError("at least one authorized token must be configured")
            return cleaned
        raise ValueError("authorized token labels must be provided as a string or mapping")

    @validator("authorized_token_ids", pre=True, allow_reuse=True)
    def _split_tokens(cls, value: Any) -> Tuple[str, ...]:  # type: ignore[override]
        if isinstance(value, str):
            tokens = [item.strip() for item in value.split(",") if item.strip()]
        elif isinstance(value, (list, tuple, set)):
            tokens = [str(item).strip() for item in value if str(item).strip()]
        else:
            raise ValueError("authorized tokens must be provided as a string or sequence")

        if not tokens:
            raise ValueError("at least one authorized token must be configured")

        return tuple(tokens)


def _insecure_defaults_enabled() -> bool:
    if os.getenv(_INSECURE_DEFAULTS_FLAG) == "1":
        ensure_insecure_default_flag_disabled(_INSECURE_DEFAULTS_FLAG)
        return True
    return "pytest" in sys.modules


def _default_token_labels(tokens: str) -> Dict[str, str]:
    candidates = [token.strip() for token in tokens.split(",") if token.strip()]
    if not candidates:
        candidates = [_DEFAULT_TEST_TOKEN]
    label_spec = ",".join(f"{token}:local" for token in candidates)
    return _parse_authorized_token_labels(label_spec)


def load_settings() -> Settings:
    allow_insecure = _insecure_defaults_enabled()

    secret_key = os.getenv("SECRET_ENCRYPTION_KEY")
    if not secret_key:
        if not allow_insecure:
            raise RuntimeError("SECRET_ENCRYPTION_KEY environment variable must be set")
        generated = base64.b64encode(os.urandom(32)).decode("ascii")
        LOGGER.warning(
            "SECRET_ENCRYPTION_KEY missing; generated ephemeral key for local testing"
        )
        secret_key = generated
        os.environ.setdefault("SECRET_ENCRYPTION_KEY", secret_key)

    tokens = os.getenv("SECRETS_SERVICE_AUTH_TOKENS")
    if not tokens:
        if not allow_insecure:
            raise RuntimeError("SECRETS_SERVICE_AUTH_TOKENS environment variable must be set")
        tokens = _DEFAULT_TEST_TOKEN
        LOGGER.warning(
            "SECRETS_SERVICE_AUTH_TOKENS missing; using insecure default token for local testing"
        )
        os.environ.setdefault("SECRETS_SERVICE_AUTH_TOKENS", tokens)

    raw_labels = os.getenv("KRAKEN_SECRETS_AUTH_TOKENS")
    if raw_labels:
        token_labels = _parse_authorized_token_labels(raw_labels)
    elif allow_insecure:
        token_labels = _default_token_labels(tokens)
        os.environ.setdefault(
            "KRAKEN_SECRETS_AUTH_TOKENS",
            ",".join(f"{token}:{label}" for token, label in token_labels.items()),
        )
    else:
        token_labels = _load_authorized_tokens_from_env()

    return Settings(
        SECRET_ENCRYPTION_KEY=secret_key,
        SECRETS_SERVICE_AUTH_TOKENS=tokens,
        authorized_token_labels=token_labels,
    )


SETTINGS: Optional[Settings] = None


def load_kubernetes_config() -> None:
    try:
        config.load_incluster_config()
        LOGGER.info("Loaded in-cluster Kubernetes configuration")
    except ConfigException:
        config.load_kube_config()
        LOGGER.info("Loaded local Kubernetes configuration")
    else:
        try:
            config.load_kube_config()
            LOGGER.debug("Loaded local kubeconfig alongside in-cluster config")
        except ConfigException as exc:
            LOGGER.debug(
                "Local kubeconfig unavailable during initialization; retrying with backoff"
            )
            raise exc


def _decode_encryption_key(key_b64: str) -> bytes:
    try:
        key = base64.b64decode(key_b64)
    except (ValueError, binascii.Error):
        raise RuntimeError("SECRET_ENCRYPTION_KEY must be valid base64")
    if len(key) not in {16, 24, 32}:
        raise RuntimeError("SECRET_ENCRYPTION_KEY must decode to 16, 24, or 32 bytes")
    return key


class _FallbackCipher:
    """Deterministic cipher used when AESGCM is unavailable under insecure defaults."""

    def __init__(self, key: bytes) -> None:
        from cryptography.fernet import Fernet  # type: ignore[import-not-found]

        digest = hashlib.sha256(key).digest()
        encoded = base64.urlsafe_b64encode(digest)
        self._fernet = Fernet(encoded)

    def encrypt(self, plaintext: bytes, associated_data: bytes) -> bytes:
        prefix = len(associated_data).to_bytes(2, "big") + associated_data
        return self._fernet.encrypt(prefix + plaintext)

    def decrypt(self, payload: bytes, associated_data: bytes) -> bytes:
        data = self._fernet.decrypt(payload)
        assoc_len = int.from_bytes(data[:2], "big")
        associated = data[2 : 2 + assoc_len]
        if associated != associated_data:
            raise ValueError("associated data mismatch")
        return data[2 + assoc_len :]


class SecretCipher:
    """Encrypts and decrypts payloads using AES-GCM."""

    def __init__(self, key: bytes) -> None:
        self._aesgcm: AESGCM | None
        self._fallback: _FallbackCipher | None

        if AESGCM is not None:
            self._aesgcm = AESGCM(key)
            self._fallback = None
        else:
            if not _insecure_defaults_enabled():
                raise RuntimeError(
                    "cryptography AESGCM implementation is required; set "
                    "SECRETS_ALLOW_INSECURE_DEFAULTS=1 to activate the fallback"
                )
            self._aesgcm = None
            self._fallback = _FallbackCipher(key)

    def encrypt(self, plaintext: bytes, associated_data: bytes) -> bytes:
        if self._aesgcm is not None:
            nonce = os.urandom(12)
            ciphertext = self._aesgcm.encrypt(nonce, plaintext, associated_data)
            return nonce + ciphertext
        assert self._fallback is not None  # for type checkers
        return self._fallback.encrypt(plaintext, associated_data)

    def decrypt(self, payload: bytes, associated_data: bytes) -> bytes:
        if self._aesgcm is not None:
            nonce, ciphertext = payload[:12], payload[12:]
            return self._aesgcm.decrypt(nonce, ciphertext, associated_data)
        assert self._fallback is not None  # for type checkers
        return self._fallback.decrypt(payload, associated_data)



CIPHER: Optional[SecretCipher] = None



class KrakenSecretManager:
    """Handles Kubernetes interactions for Kraken API secrets."""

    SECRET_DATA_KEY = "credentials"
    LAST_ROTATED_KEY = "aether.io/last-rotated"
    ROTATION_ACTOR_KEY = "aether.io/rotated-by"
    OMS_RELOAD_KEY = "oms.aether.io/reload"

    def __init__(self, namespace: str, *, cipher: Optional[SecretCipher] = None) -> None:
        self._namespace = namespace
        self._client = client.CoreV1Api()
        self._cipher = cipher

    @property
    def namespace(self) -> str:
        """Return the Kubernetes namespace backing the credential secret."""

        return self._namespace

    def canonical_secret_name(self, account_id: str) -> str:
        """Return the deterministic Kubernetes secret name for an account."""

        return self._secret_name(account_id)

    @staticmethod
    def _secret_name(account_id: str) -> str:
        return f"kraken-keys-{account_id}"

    def _get_cipher(self) -> SecretCipher:
        cipher = self._cipher or CIPHER
        if cipher is None:
            raise RuntimeError("Secret encryption cipher is not initialized")
        return cipher

    def upsert_secret(self, account_id: str, payload: Dict[str, str], actor: str) -> Dict[str, str]:
        secret_name = self._secret_name(account_id)
        now = datetime.now(timezone.utc).isoformat()
        serialized = json.dumps(payload).encode("utf-8")
        encrypted = self._get_cipher().encrypt(
            serialized, associated_data=account_id.encode("utf-8")
        )
        encoded = base64.b64encode(encrypted).decode("utf-8")

        body: Dict[str, Any] = {
            "metadata": {
                "name": secret_name,
                "annotations": {
                    self.LAST_ROTATED_KEY: now,
                    self.ROTATION_ACTOR_KEY: actor,
                    self.OMS_RELOAD_KEY: now,
                },
            },
            "type": "Opaque",
            "data": {self.SECRET_DATA_KEY: encoded},
        }

        try:
            LOGGER.info("Patching secret %s for account %s", secret_name, account_id)
            self._client.patch_namespaced_secret(
                name=secret_name,
                namespace=self._namespace,
                body=body,
            )
        except ApiException as exc:
            if exc.status == 404:
                LOGGER.info("Secret %s not found; creating new secret", secret_name)
                self._client.create_namespaced_secret(namespace=self._namespace, body=body)
            else:
                LOGGER.exception("Failed to upsert secret for account %s", account_id)
                raise

        return {"secret_name": secret_name, "last_rotated": now}

    def get_secret(self, account_id: str) -> client.V1Secret:
        secret_name = self._secret_name(account_id)
        try:
            return self._client.read_namespaced_secret(secret_name, self._namespace)
        except ApiException as exc:
            if exc.status == 404:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Secret for account {account_id} was not found",
                )
            LOGGER.exception("Failed to read secret for account %s", account_id)
            raise

    def get_status(self, account_id: str) -> Dict[str, str]:
        secret = self.get_secret(account_id)
        annotations = secret.metadata.annotations or {}
        last_rotated = (
            annotations.get(self.LAST_ROTATED_KEY)
            or annotations.get(K8S_ROTATED_AT)
            or ""
        )
        actor = annotations.get(self.ROTATION_ACTOR_KEY)
        return {
            "secret_name": secret.metadata.name,
            "last_rotated": last_rotated,
            "rotated_by": actor,
        }

    def get_decrypted_credentials(self, account_id: str) -> Dict[str, str]:
        secret = self.get_secret(account_id)
        data = secret.data or {}
        encoded = data.get(self.SECRET_DATA_KEY)
        if not encoded:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Secret payload missing credentials",
            )
        try:
            encrypted = base64.b64decode(encoded)
            decrypted = self._get_cipher().decrypt(
                encrypted, associated_data=account_id.encode("utf-8")
            )
            return json.loads(decrypted.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to decrypt credentials for account %s", account_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Unable to decrypt credentials",
            ) from exc


secret_manager: Optional[KrakenSecretManager] = None


def _is_initialized() -> bool:
    return SETTINGS is not None and CIPHER is not None and secret_manager is not None


async def initialize_dependencies(force: bool = False) -> None:
    global SETTINGS, CIPHER, secret_manager

    if not force and _is_initialized():
        return

    async with STATE.lock:
        if not force and _is_initialized():
            return

        attempts = 0
        backoff = _INITIAL_BACKOFF_SECONDS

        while attempts < _MAX_INITIALIZATION_ATTEMPTS:
            try:
                loaded_settings = load_settings()
                load_kubernetes_config()
                cipher = SecretCipher(
                    _decode_encryption_key(loaded_settings.encryption_key_b64)
                )

                SETTINGS = loaded_settings
                CIPHER = cipher
                STATE.settings = loaded_settings
                STATE.cipher = cipher

                if secret_manager is None or force:
                    secret_manager = KrakenSecretManager(
                        loaded_settings.kubernetes_namespace, cipher=cipher
                    )

                STATE.initialization_error = None
                LOGGER.info("Kraken secrets service initialized successfully")
                return
            except (ConfigException, ApiException) as exc:
                attempts += 1
                STATE.initialization_error = exc
                LOGGER.warning(
                    "Attempt %s/%s to initialize Kraken secrets service failed: %s",
                    attempts,
                    _MAX_INITIALIZATION_ATTEMPTS,
                    exc,
                )
                if attempts >= _MAX_INITIALIZATION_ATTEMPTS:
                    LOGGER.error(
                        "Unable to initialize Kraken secrets service after %s attempts; "
                        "returning 503 until configuration succeeds",
                        _MAX_INITIALIZATION_ATTEMPTS,
                    )
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF_SECONDS)
            except Exception as exc:  # noqa: BLE001
                STATE.initialization_error = exc
                LOGGER.exception(
                    "Unexpected error during Kraken secrets service initialization"
                )
                break


class KrakenSecretRequest(BaseModel):
    account_id: str = Field(..., min_length=1)
    api_key: str = Field(..., min_length=1)
    api_secret: str = Field(..., min_length=1)
    actor: str = Field(default="unknown")


class KrakenTestRequest(BaseModel):
    account_id: str = Field(..., min_length=1)


class SecretsStatusResponse(BaseModel):
    """Response payload summarising the current Kraken credential state."""

    last_rotated_at: Optional[str] = Field(
        default=None,
        description="ISO-8601 timestamp for the most recent credential rotation.",
    )
    last_rotated_by: Optional[str] = Field(
        default=None,
        description="Identifier for the actor that last rotated the credentials.",
    )
    status: Optional[str] = Field(
        default=None,
        description="Human friendly status string describing the credential freshness.",
    )


class SecretsAuditEntry(BaseModel):
    """Audit log entry capturing a single credential rotation event."""

    actor: str = Field(..., description="Actor responsible for the rotation event.")
    rotated_at: str = Field(
        ..., description="ISO-8601 timestamp describing when rotation completed."
    )
    notes: Optional[str] = Field(
        default=None,
        description="Optional contextual note associated with the rotation event.",
    )


app = FastAPI(title="Kraken Secrets Service", version="1.0.0")


@app.on_event("startup")
async def startup_event() -> None:
    await initialize_dependencies()


def redact_secret(value: str) -> str:
    if len(value) <= 4:
        return "***"
    return f"{value[:4]}***"


def log_rotation(account_id: str, actor: str, timestamp: str) -> None:
    SECRETS_LOGGER.info(
        "kraken secret rotated",
        extra={"account_id": account_id, "actor": actor, "ts": timestamp},
    )
    

def _ensure_iso_timestamp(value: Any) -> Optional[str]:
    """Normalize arbitrary timestamp payloads into ISO-8601 strings."""

    if value is None:
        return None
    if isinstance(value, datetime):
        normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return normalized.astimezone(timezone.utc).isoformat()
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _extract_secret_context(secret: Any) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    """Return name, namespace, and annotations for the provided secret object."""

    metadata: Any
    if isinstance(secret, dict):
        metadata = secret.get("metadata")
    else:
        metadata = getattr(secret, "metadata", None)

    name: Optional[str] = None
    namespace: Optional[str] = None
    annotations: Dict[str, Any] = {}

    if isinstance(metadata, dict):
        name = metadata.get("name")
        namespace = metadata.get("namespace")
        raw_annotations = metadata.get("annotations")
        if isinstance(raw_annotations, dict):
            annotations = dict(raw_annotations)
    elif metadata is not None:
        name = getattr(metadata, "name", None)
        namespace = getattr(metadata, "namespace", None)
        raw_annotations = getattr(metadata, "annotations", None)
        if isinstance(raw_annotations, dict):
            annotations = dict(raw_annotations)
        elif raw_annotations is not None:
            annotations = dict(getattr(raw_annotations, "__dict__", {}))

    return name, namespace, annotations


_UNAVAILABLE_MESSAGE = "Kraken secrets service configuration is unavailable"


def _require_settings() -> Settings:
    if SETTINGS is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_UNAVAILABLE_MESSAGE,
        )
    return SETTINGS


async def kraken_get_balance(api_key: str, api_secret: str) -> Dict[str, Any]:
    nonce = str(int(time.time() * 1000))
    path = "/0/private/Balance"
    payload = {"nonce": nonce}
    body, signature = sign_kraken_request(path, payload, api_secret)

    headers = {
        "API-Key": api_key,
        "API-Sign": signature,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    settings = _require_settings()
    url = f"{settings.kraken_api_url}{path}"
    async with httpx.AsyncClient(timeout=10.0) as client_session:
        response = await client_session.post(url, data=body, headers=headers)
    response.raise_for_status()
    return response.json()


async def get_secret_manager() -> KrakenSecretManager:
    if not _is_initialized():
        await initialize_dependencies()

    if not _is_initialized() or secret_manager is None:
        error = STATE.initialization_error
        if isinstance(error, (ConfigException, ApiException)):
            LOGGER.warning(
                "Kraken secrets service configuration pending due to Kubernetes error: %s",
                error,
            )
        elif error is not None:
            LOGGER.error(
                "Kraken secrets service unavailable after initialization failure",
                exc_info=error,
            )
        else:
            LOGGER.warning("Kraken secrets service initialization pending")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_UNAVAILABLE_MESSAGE,
        )

    assert secret_manager is not None  # for type-checkers
    return secret_manager



def _extract_bearer_token(header_value: Optional[str]) -> Optional[str]:
    if not header_value:
        return None
    value = header_value.strip()
    if not value:
        return None
    if value.lower().startswith("bearer "):
        return value[7:].strip()
    return value


async def require_authorized_caller(
    authorization: Optional[str] = Header(default=None),
) -> str:
    token = _extract_bearer_token(authorization)
    if token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid authorization token",
        )
    if SETTINGS is None:
        await initialize_dependencies()

    settings = _require_settings()
    actor = settings.authorized_token_labels.get(token)
    if actor is None or token not in settings.authorized_token_ids:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Caller is not authorized to access Kraken secrets",
        )
    return actor



@app.post("/secrets/kraken", status_code=status.HTTP_201_CREATED)
async def store_kraken_secret(
    payload: KrakenSecretRequest,
    request: Request,

    manager: KrakenSecretManager = Depends(get_secret_manager),
    authorized_actor: str = Depends(require_authorized_caller),
) -> JSONResponse:
    masked_key = redact_secret(payload.api_key)
    LOGGER.info(
        "Received request to rotate Kraken secret for account %s using key %s",
        payload.account_id,
        masked_key,
    )
    LOGGER.debug(
        "Kraken secret rotation authorized for account %s by %s",
        payload.account_id,
        authorized_actor,
    )

    request_actor = (payload.actor or "").strip()
    if request_actor and request_actor.lower() != "unknown" and request_actor != authorized_actor:
        LOGGER.warning(
            "Actor mismatch for Kraken secret rotation on account %s: payload actor %s does not match authorized actor %s",
            payload.account_id,
            request_actor,
            authorized_actor,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Actor identity does not match provided credentials",
        )

    verified_actor = authorized_actor

    audit_hooks = load_audit_hooks()
    before_snapshot: Dict[str, Any] = {}
    if audit_hooks.log is not None:
        try:
            before_snapshot = manager.get_status(payload.account_id)
        except HTTPException as exc:
            if exc.status_code != status.HTTP_404_NOT_FOUND:
                raise
        except ApiException as exc:
            if exc.status != 404:
                raise

    LOGGER.info(
        "Validating Kraken credentials for account %s using key %s", payload.account_id, masked_key
    )
    try:
        validation_response = await kraken_get_balance(
            api_key=payload.api_key,
            api_secret=payload.api_secret,
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        LOGGER.warning(
            "Kraken API returned status %s while validating credentials for account %s",
            status_code,
            payload.account_id,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Kraken API responded with an error",
        ) from exc
    except httpx.HTTPError as exc:
        LOGGER.exception(
            "HTTP error during Kraken credential validation for account %s", payload.account_id
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to reach Kraken API",
        ) from exc

    validation_errors = validation_response.get("error")
    if validation_errors:
        LOGGER.warning(
            "Kraken credential validation failed for account %s using key %s: %s",
            payload.account_id,
            masked_key,
            validation_errors,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Kraken credentials",
        )

    LOGGER.info("Kraken credentials validated for account %s", payload.account_id)

    try:
        result = manager.upsert_secret(
            account_id=payload.account_id,
            payload={"api_key": payload.api_key, "api_secret": payload.api_secret},
            actor=verified_actor,
        )
    except ApiException as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to update Kubernetes secret",
        ) from exc

    log_rotation(payload.account_id, verified_actor, result["last_rotated"])

    audit_after = dict(result)
    audit_after["actor"] = verified_actor
    event = AuditEvent(
        actor=verified_actor,
        action="secret.kraken.rotate",
        entity=payload.account_id,
        before=before_snapshot,
        after=audit_after,
        ip_address=request.client.host if request.client else None,
    )
    event.log_with_fallback(
        audit_hooks,
        LOGGER,
        failure_message=(
            f"Failed to record audit log for Kraken secret rotation for {payload.account_id}"
        ),
        disabled_message=(
            f"Audit logging disabled; skipping secret.kraken.rotate for {payload.account_id}"
        ),
    )

    return JSONResponse(status_code=status.HTTP_201_CREATED, content=result)


@app.get("/secrets/kraken/status")
async def kraken_secret_status(
    account_id: str = Query(..., min_length=1),
    manager: KrakenSecretManager = Depends(get_secret_manager),
    authorized_actor: str = Depends(require_authorized_caller),
) -> Dict[str, str]:
    LOGGER.info("Status requested for Kraken secret %s by %s", account_id, authorized_actor)
    status_payload = manager.get_status(account_id)
    return status_payload


@app.get("/secrets/status", response_model=SecretsStatusResponse)
async def secrets_status(
    manager: KrakenSecretManager = Depends(get_secret_manager),
    authorized_actor: str = Depends(require_authorized_caller),
) -> SecretsStatusResponse:
    LOGGER.info("Secrets UI status requested by %s", authorized_actor)

    try:
        secret = manager.get_secret(authorized_actor)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_404_NOT_FOUND:
            LOGGER.info(
                "No stored Kraken credentials for account %s during status lookup",
                authorized_actor,
            )
            return SecretsStatusResponse(
                status="No Kraken credentials are currently stored for this account.",
            )
        raise

    secret_name, _, annotations = _extract_secret_context(secret)
    last_rotated = _ensure_iso_timestamp(
        annotations.get(manager.LAST_ROTATED_KEY) or annotations.get(K8S_ROTATED_AT)
    )
    rotated_by_raw = annotations.get(manager.ROTATION_ACTOR_KEY)
    rotated_by = (rotated_by_raw or "").strip() or None

    canonical_name = secret_name or manager.canonical_secret_name(authorized_actor)
    if last_rotated:
        if rotated_by:
            status_message = (
                f"Secret {canonical_name} rotated at {last_rotated} by {rotated_by}."
            )
        else:
            status_message = f"Secret {canonical_name} rotated at {last_rotated}."
    else:
        status_message = None

    return SecretsStatusResponse(
        last_rotated_at=last_rotated,
        last_rotated_by=rotated_by,
        status=status_message,
    )


@app.get("/secrets/audit", response_model=List[SecretsAuditEntry])
async def secrets_audit(
    manager: KrakenSecretManager = Depends(get_secret_manager),
    authorized_actor: str = Depends(require_authorized_caller),
) -> List[SecretsAuditEntry]:
    LOGGER.info("Secrets UI audit requested by %s", authorized_actor)

    try:
        secret = manager.get_secret(authorized_actor)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_404_NOT_FOUND:
            LOGGER.info(
                "No stored Kraken credentials for account %s during audit lookup",
                authorized_actor,
            )
            return []
        raise

    secret_name, namespace, annotations = _extract_secret_context(secret)
    last_rotated = _ensure_iso_timestamp(
        annotations.get(manager.LAST_ROTATED_KEY) or annotations.get(K8S_ROTATED_AT)
    )
    rotated_by_raw = annotations.get(manager.ROTATION_ACTOR_KEY)
    rotated_by = (rotated_by_raw or "").strip() or authorized_actor
    rotated_at = last_rotated or datetime.now(timezone.utc).isoformat()

    canonical_name = secret_name or manager.canonical_secret_name(authorized_actor)
    location = namespace or manager.namespace
    oms_reload = _ensure_iso_timestamp(annotations.get(manager.OMS_RELOAD_KEY))

    notes_parts = [f"Secret {canonical_name} stored in namespace {location}."]
    if oms_reload:
        notes_parts.append(f"OMS watchers notified at {oms_reload}.")
    notes = " ".join(notes_parts)

    return [
        SecretsAuditEntry(
            actor=rotated_by,
            rotated_at=rotated_at,
            notes=notes,
        )
    ]


@app.post("/secrets/kraken/test")
async def test_kraken_credentials(
    payload: KrakenTestRequest,

    manager: KrakenSecretManager = Depends(get_secret_manager),
    authorized_actor: str = Depends(require_authorized_caller),

) -> Dict[str, Any]:
    LOGGER.info(
        "Testing Kraken credentials for account %s authorized by %s",
        payload.account_id,
        authorized_actor,
    )
    credentials = manager.get_decrypted_credentials(payload.account_id)

    try:
        response = await kraken_get_balance(
            api_key=credentials["api_key"],
            api_secret=credentials["api_secret"],
        )
    except httpx.HTTPStatusError as exc:
        LOGGER.warning(
            "Kraken API returned status %s for account %s", exc.response.status_code, payload.account_id
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Kraken API responded with an error",
        ) from exc
    except httpx.HTTPError as exc:
        LOGGER.exception("HTTP error during Kraken credential validation for account %s", payload.account_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to reach Kraken API",
        ) from exc

    if response.get("error"):
        LOGGER.warning(
            "Kraken API validation failed for account %s with masked key %s",
            payload.account_id,
            redact_secret(credentials.get("api_key", "")),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Kraken credentials",
        )

    LOGGER.info("Kraken credentials validated for account %s", payload.account_id)
    return {"result": "success", "data": response.get("result", {})}


__all__ = ["app", "require_authorized_caller", "get_secret_manager"]

