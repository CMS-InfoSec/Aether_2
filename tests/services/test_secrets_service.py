import asyncio
import base64
import importlib
import sys
import types

import pytest


@pytest.fixture
def secrets_service_module(monkeypatch):
    # Ensure a clean import each time
    sys.modules.pop("secrets_service", None)

    dummy_httpx = types.ModuleType("httpx")
    monkeypatch.setitem(sys.modules, "httpx", dummy_httpx)

    dummy_client = types.ModuleType("kubernetes.client")

    class _CoreV1Api:  # pragma: no cover - stubbed dependency
        def __init__(self, *args, **kwargs) -> None:
            pass

    dummy_client.CoreV1Api = _CoreV1Api
    dummy_client.ApiException = Exception

    dummy_config = types.ModuleType("kubernetes.config")

    def _noop(*args, **kwargs) -> None:  # pragma: no cover - stubbed dependency
        return None

    dummy_config.load_incluster_config = _noop
    dummy_config.load_kube_config = _noop

    dummy_config_exception = types.ModuleType("kubernetes.config.config_exception")

    class _ConfigException(Exception):
        pass

    dummy_config_exception.ConfigException = _ConfigException
    dummy_config.config_exception = dummy_config_exception

    dummy_kubernetes = types.ModuleType("kubernetes")
    dummy_kubernetes.client = dummy_client
    dummy_kubernetes.config = dummy_config

    monkeypatch.setitem(sys.modules, "kubernetes", dummy_kubernetes)
    monkeypatch.setitem(sys.modules, "kubernetes.client", dummy_client)
    monkeypatch.setitem(sys.modules, "kubernetes.config", dummy_config)
    monkeypatch.setitem(
        sys.modules, "kubernetes.config.config_exception", dummy_config_exception
    )

    dummy_crypto = types.ModuleType("cryptography")
    dummy_crypto_hazmat = types.ModuleType("cryptography.hazmat")
    dummy_crypto_primitives = types.ModuleType("cryptography.hazmat.primitives")
    dummy_crypto_ciphers = types.ModuleType(
        "cryptography.hazmat.primitives.ciphers"
    )
    dummy_crypto_aead = types.ModuleType(
        "cryptography.hazmat.primitives.ciphers.aead"
    )

    class _AESGCM:  # pragma: no cover - stubbed dependency
        def __init__(self, key: bytes) -> None:
            self._key = key

        def encrypt(self, nonce: bytes, plaintext: bytes, associated_data: bytes) -> bytes:
            return plaintext

        def decrypt(self, nonce: bytes, payload: bytes, associated_data: bytes) -> bytes:
            return payload

    dummy_crypto_aead.AESGCM = _AESGCM
    dummy_crypto_ciphers.aead = dummy_crypto_aead
    dummy_crypto_primitives.ciphers = dummy_crypto_ciphers
    dummy_crypto_hazmat.primitives = dummy_crypto_primitives
    dummy_crypto.hazmat = dummy_crypto_hazmat

    monkeypatch.setitem(sys.modules, "cryptography", dummy_crypto)
    monkeypatch.setitem(sys.modules, "cryptography.hazmat", dummy_crypto_hazmat)
    monkeypatch.setitem(
        sys.modules, "cryptography.hazmat.primitives", dummy_crypto_primitives
    )
    monkeypatch.setitem(
        sys.modules, "cryptography.hazmat.primitives.ciphers", dummy_crypto_ciphers
    )
    monkeypatch.setitem(
        sys.modules,
        "cryptography.hazmat.primitives.ciphers.aead",
        dummy_crypto_aead,
    )

    module = importlib.import_module("secrets_service")
    return module


@pytest.fixture
def configured_settings(monkeypatch, secrets_service_module):
    encryption_key = base64.b64encode(b"x" * 32).decode("utf-8")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", encryption_key)
    monkeypatch.setenv("SECRETS_SERVICE_AUTH_TOKENS", "token-1,token-2")
    monkeypatch.setenv("KRAKEN_SECRETS_AUTH_TOKENS", "token-1:alpha,token-2:beta")

    settings = secrets_service_module.load_settings()
    monkeypatch.setattr(secrets_service_module, "SETTINGS", settings, raising=False)
    return settings, secrets_service_module


def test_require_authorized_caller_accepts_known_token(configured_settings):
    settings, secrets_service = configured_settings
    assert settings.authorized_token_ids == ("token-1", "token-2")
    assert settings.authorized_token_labels["token-1"] == "alpha"

    actor = asyncio.run(secrets_service.require_authorized_caller("Bearer token-1"))
    assert actor == "alpha"


def test_require_authorized_caller_rejects_unknown_token(configured_settings):
    _, secrets_service = configured_settings
    with pytest.raises(secrets_service.HTTPException) as excinfo:
        asyncio.run(secrets_service.require_authorized_caller("Bearer unknown"))
    assert excinfo.value.status_code == secrets_service.status.HTTP_403_FORBIDDEN


def test_require_authorized_caller_returns_503_when_settings_unavailable(
    monkeypatch, secrets_service_module
):
    secrets_service = secrets_service_module

    monkeypatch.delenv("SECRET_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("SECRETS_SERVICE_AUTH_TOKENS", raising=False)
    monkeypatch.delenv("KRAKEN_SECRETS_AUTH_TOKENS", raising=False)

    monkeypatch.setattr(secrets_service, "SETTINGS", None, raising=False)
    monkeypatch.setattr(secrets_service, "CIPHER", None, raising=False)
    monkeypatch.setattr(secrets_service, "secret_manager", None, raising=False)
    secrets_service.STATE.initialization_error = None

    call_count = {"value": 0}

    async def fake_initialize(force: bool = False) -> None:
        call_count["value"] += 1

    monkeypatch.setattr(secrets_service, "initialize_dependencies", fake_initialize)

    with pytest.raises(secrets_service.HTTPException) as excinfo:
        asyncio.run(secrets_service.require_authorized_caller("Bearer token-1"))

    assert excinfo.value.status_code == secrets_service.status.HTTP_503_SERVICE_UNAVAILABLE
    assert call_count["value"] == 1


def test_require_authorized_caller_returns_503_when_kubernetes_unavailable(
    monkeypatch, secrets_service_module
):
    secrets_service = secrets_service_module

    encryption_key = base64.b64encode(b"x" * 32).decode("utf-8")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", encryption_key)
    monkeypatch.setenv("SECRETS_SERVICE_AUTH_TOKENS", "token-1")
    monkeypatch.setenv("KRAKEN_SECRETS_AUTH_TOKENS", "token-1:alpha")

    monkeypatch.setattr(secrets_service, "SETTINGS", None, raising=False)
    monkeypatch.setattr(secrets_service, "CIPHER", None, raising=False)
    monkeypatch.setattr(secrets_service, "secret_manager", None, raising=False)
    secrets_service.STATE.initialization_error = None

    async def immediate_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(secrets_service.asyncio, "sleep", immediate_sleep)

    def fail_kubernetes_config() -> None:
        raise secrets_service.ConfigException("kubernetes unavailable")

    monkeypatch.setattr(
        secrets_service, "load_kubernetes_config", fail_kubernetes_config
    )

    call_count = {"value": 0}
    original_initialize = secrets_service.initialize_dependencies

    async def wrapped_initialize(force: bool = False) -> None:
        call_count["value"] += 1
        await original_initialize(force=force)

    monkeypatch.setattr(
        secrets_service, "initialize_dependencies", wrapped_initialize
    )

    with pytest.raises(secrets_service.HTTPException) as excinfo:
        asyncio.run(secrets_service.require_authorized_caller("Bearer token-1"))

    assert excinfo.value.status_code == secrets_service.status.HTTP_503_SERVICE_UNAVAILABLE
    assert call_count["value"] == 1
    assert isinstance(secrets_service.STATE.initialization_error, Exception)
