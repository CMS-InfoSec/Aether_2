"""Unit tests for the top-level auth service module."""

from __future__ import annotations

import asyncio
import base64
import importlib
import json
import sys
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace

import pytest


def _clear_auth_service_module() -> None:
    sys.modules.pop("auth_service", None)


def _install_dependency_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install lightweight stubs for optional third-party dependencies."""

    if "httpx" not in sys.modules:
        httpx_stub = ModuleType("httpx")

        class _Client:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def __enter__(self) -> "_Client":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def get(self, *args: object, **kwargs: object) -> SimpleNamespace:
                return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {})

        class _AsyncClient:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            async def __aenter__(self) -> "_AsyncClient":
                return self

            async def __aexit__(self, exc_type, exc, tb) -> bool:
                return False

            async def post(self, *args: object, **kwargs: object) -> SimpleNamespace:
                return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {})

            async def get(self, *args: object, **kwargs: object) -> SimpleNamespace:
                return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {})

        httpx_stub.Client = _Client
        httpx_stub.AsyncClient = _AsyncClient
        httpx_stub.HTTPStatusError = Exception
        monkeypatch.setitem(sys.modules, "httpx", httpx_stub)

    if "pyotp" not in sys.modules:
        pyotp_stub = ModuleType("pyotp")

        class _TOTP:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def verify(self, *args: object, **kwargs: object) -> bool:
                return True

            def provisioning_uri(self, *args: object, **kwargs: object) -> str:
                return "otpauth://totp/placeholder"

        pyotp_stub.TOTP = _TOTP
        pyotp_stub.random_base32 = lambda: "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
        monkeypatch.setitem(sys.modules, "pyotp", pyotp_stub)


def _decode_jwt_payload(token: str) -> dict[str, object]:
    header_b64, payload_b64, _signature = token.split(".")
    padding = "=" * (-len(payload_b64) % 4)
    decoded = base64.urlsafe_b64decode(payload_b64 + padding).decode("utf-8")
    return json.loads(decoded)


def test_auth_service_requires_jwt_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """Importing the service without a configured secret should fail fast."""

    monkeypatch.delenv("AUTH_JWT_SECRET", raising=False)
    _install_dependency_stubs(monkeypatch)
    _clear_auth_service_module()

    with pytest.raises(RuntimeError):
        importlib.import_module("auth_service")

    _clear_auth_service_module()


@pytest.fixture
def auth_service(monkeypatch: pytest.MonkeyPatch):
    """Load the auth_service module with a deterministic secret."""

    def loader(secret: str = "test-secret"):
        monkeypatch.setenv("AUTH_JWT_SECRET", secret)
        _install_dependency_stubs(monkeypatch)
        _clear_auth_service_module()
        module = importlib.import_module("auth_service")
        return module

    return loader


def test_authenticate_emits_role_specific_token(
    monkeypatch: pytest.MonkeyPatch, auth_service
) -> None:
    """The issued JWT should include the caller's resolved role."""

    module = auth_service()

    async def fake_exchange_code(**_: object) -> dict[str, str]:
        return {"access_token": "token"}

    async def fake_fetch_userinfo(**_: object) -> dict[str, object]:
        return {"email": "user@example.com", "role": "auditor"}

    class DummyMFA:
        def verify(self, *, user_id: str, method: str, code: str) -> bool:
            assert user_id == "user@example.com"
            assert method == "totp"
            return True

    class DummySessions:
        def create(self, *, user_id: str, mfa_verified: bool):
            return SimpleNamespace(
                session_token="session-token",
                user_id=user_id,
                mfa_verified=mfa_verified,
                ts=datetime.now(timezone.utc),
            )

    async def fake_persist_session(repo: DummySessions, *, user_id: str):
        return repo.create(user_id=user_id, mfa_verified=True)

    monkeypatch.setattr(module, "_exchange_code", fake_exchange_code)
    monkeypatch.setattr(module, "_fetch_userinfo", fake_fetch_userinfo)
    monkeypatch.setattr(module, "_persist_session", fake_persist_session)

    providers = {
        "microsoft": module.OIDCProvider(
            name="microsoft",
            discovery_url="https://example.com/oidc",
            client_id="client",
            client_secret="secret",
        )
    }

    payload = module.LoginRequest(
        provider="microsoft",
        code="auth-code",
        redirect_uri="https://example.com/callback",
        mfa_method="totp",
        mfa_code="123456",
    )

    response = asyncio.run(
        module.authenticate(
            payload,
            providers=providers,
            mfa=DummyMFA(),
            sessions=DummySessions(),
        )
    )

    assert response.role == "auditor"
    claims = _decode_jwt_payload(response.access_token)
    assert claims["role"] == "auditor"
    assert claims["sub"] == "user@example.com"
