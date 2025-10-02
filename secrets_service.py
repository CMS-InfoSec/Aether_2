"""FastAPI service for managing encrypted Kraken API credentials in Kubernetes."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, Tuple

import httpx

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status, Header

from fastapi.responses import JSONResponse
from kubernetes import client, config
from kubernetes.client import ApiException
from kubernetes.config.config_exception import ConfigException
from pydantic import BaseModel, Field, validator
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

try:  # pragma: no cover - optional audit dependency
    from common.utils.audit_logger import hash_ip, log_audit
except Exception:  # pragma: no cover - degrade gracefully
    log_audit = None  # type: ignore[assignment]

    def hash_ip(_: Optional[str]) -> Optional[str]:  # type: ignore[override]
        return None


LOGGER = logging.getLogger(__name__)
SECRETS_LOGGER = logging.getLogger("secrets_log")

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


DEFAULT_ADMIN_ACCOUNTS = {"company", "director-1", "director-2"}


def _csv_env(var_name: str, *, default: Optional[Set[str]] = None) -> Set[str]:
    raw = os.getenv(var_name, "")
    values = {item.strip() for item in raw.split(",") if item.strip()}
    if values:
        return values
    return set() if default is None else set(default)


class Settings(BaseModel):
    kubernetes_namespace: str = Field(
        default_factory=lambda: os.getenv("KRAKEN_SECRET_NAMESPACE", "default")
    )
    encryption_key_b64: str = Field(..., alias="SECRET_ENCRYPTION_KEY")
    kraken_api_url: str = Field(
        default_factory=lambda: os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
    )

    authorized_tokens: Tuple[str, ...] = Field(..., alias="SECRETS_SERVICE_AUTH_TOKENS")


    class Config:
        allow_population_by_field_name = True

    @validator("authorized_tokens", pre=True)
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


def load_settings() -> Settings:
    secret_key = os.getenv("SECRET_ENCRYPTION_KEY")
    if not secret_key:
        raise RuntimeError("SECRET_ENCRYPTION_KEY environment variable must be set")
    tokens = os.getenv("SECRETS_SERVICE_AUTH_TOKENS")
    if not tokens:
        raise RuntimeError("SECRETS_SERVICE_AUTH_TOKENS environment variable must be set")
    return Settings(
        SECRET_ENCRYPTION_KEY=secret_key,
        SECRETS_SERVICE_AUTH_TOKENS=tokens,
    )


SETTINGS: Optional[Settings] = None


def load_kubernetes_config() -> None:
    try:
        config.load_incluster_config()
        LOGGER.info("Loaded in-cluster Kubernetes configuration")
    except ConfigException:
        config.load_kube_config()
        LOGGER.info("Loaded local Kubernetes configuration")


def _decode_encryption_key(key_b64: str) -> bytes:
    try:
        key = base64.b64decode(key_b64)
    except (ValueError, binascii.Error):
        raise RuntimeError("SECRET_ENCRYPTION_KEY must be valid base64")
    if len(key) not in {16, 24, 32}:
        raise RuntimeError("SECRET_ENCRYPTION_KEY must decode to 16, 24, or 32 bytes")
    return key


class SecretCipher:
    """Encrypts and decrypts payloads using AES-GCM."""

    def __init__(self, key: bytes) -> None:
        self._aesgcm = AESGCM(key)

    def encrypt(self, plaintext: bytes, associated_data: bytes) -> bytes:
        nonce = os.urandom(12)
        ciphertext = self._aesgcm.encrypt(nonce, plaintext, associated_data)
        return nonce + ciphertext

    def decrypt(self, payload: bytes, associated_data: bytes) -> bytes:
        nonce, ciphertext = payload[:12], payload[12:]
        return self._aesgcm.decrypt(nonce, ciphertext, associated_data)



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
        last_rotated = annotations.get(self.LAST_ROTATED_KEY, "")
        return {
            "secret_name": secret.metadata.name,
            "last_rotated": last_rotated,
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


def sign_kraken_request(path: str, data: Dict[str, Any], api_secret: str) -> Tuple[str, str]:
    import hashlib
    import hmac
    from urllib.parse import urlencode

    post_data = urlencode(data)
    encoded = (data["nonce"] + post_data).encode()
    message = hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(api_secret), path.encode() + message, hashlib.sha512)
    signature = base64.b64encode(mac.digest()).decode()
    return post_data, signature


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


def authorize_request(
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    admin_id: Optional[str] = Header(default=None, alias="X-Admin-ID"),
) -> str:
    if admin_id:
        if admin_id in SETTINGS.authorized_admins:
            return admin_id
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin identity is not authorized to manage Kraken secrets.",
        )

    if authorization:
        if not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authorization header must use the Bearer scheme.",
            )
        token = authorization.partition(" ")[2].strip()
        if token and token in SETTINGS.service_tokens:
            return f"service:{token}"
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Service token is not authorized to access Kraken secrets.",
        )

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication is required to access Kraken secrets.",
    )


@app.post("/secrets/kraken", status_code=status.HTTP_201_CREATED)
async def store_kraken_secret(
    payload: KrakenSecretRequest,
    request: Request,

    _: str = Depends(require_authorized_caller),

    manager: KrakenSecretManager = Depends(get_secret_manager),
) -> JSONResponse:
    masked_key = redact_secret(payload.api_key)
    LOGGER.info(
        "Received request to rotate Kraken secret for account %s using key %s",
        payload.account_id,
        masked_key,
    )

    before_snapshot: Dict[str, Any] = {}
    if log_audit is not None:
        try:
            before_snapshot = manager.get_status(payload.account_id)
        except HTTPException as exc:
            if exc.status_code != status.HTTP_404_NOT_FOUND:
                raise
        except ApiException as exc:
            if exc.status != 404:
                raise

    try:
        result = manager.upsert_secret(
            account_id=payload.account_id,
            payload={"api_key": payload.api_key, "api_secret": payload.api_secret},
            actor=payload.actor,
        )
    except ApiException as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to update Kubernetes secret",
        ) from exc

    log_rotation(payload.account_id, payload.actor, result["last_rotated"])

    if log_audit is not None:
        try:
            audit_after = dict(result)
            audit_after["actor"] = payload.actor
            log_audit(
                actor=payload.actor,
                action="secret.kraken.rotate",
                entity=payload.account_id,
                before=before_snapshot,
                after=audit_after,
                ip_hash=hash_ip(request.client.host if request.client else None),
            )
        except Exception:  # pragma: no cover - defensive best effort
            LOGGER.exception(
                "Failed to record audit log for Kraken secret rotation for %s",
                payload.account_id,
            )

    return JSONResponse(status_code=status.HTTP_201_CREATED, content=result)


@app.get("/secrets/kraken/status")
async def kraken_secret_status(
    account_id: str = Query(..., min_length=1),

    _: str = Depends(require_authorized_caller),

    manager: KrakenSecretManager = Depends(get_secret_manager),
) -> Dict[str, str]:
    LOGGER.info("Status requested for Kraken secret %s", account_id)
    status_payload = manager.get_status(account_id)
    return status_payload


@app.post("/secrets/kraken/test")
async def test_kraken_credentials(
    payload: KrakenTestRequest,

    _: str = Depends(require_authorized_caller),

    manager: KrakenSecretManager = Depends(get_secret_manager),
) -> Dict[str, Any]:
    LOGGER.info("Testing Kraken credentials for account %s", payload.account_id)
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

