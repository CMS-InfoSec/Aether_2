"""Unit tests for the top-level auth service module."""

from __future__ import annotations

import asyncio
import base64
import itertools
import importlib
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
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


def test_auth_service_requires_jwt_secret(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Importing the service without a configured secret should fail fast."""

    monkeypatch.setenv("AUTH_DATABASE_URL", f"sqlite:///{tmp_path/'auth.db'}")
    monkeypatch.delenv("AUTH_JWT_SECRET", raising=False)
    _install_dependency_stubs(monkeypatch)
    _clear_auth_service_module()

    with pytest.raises(RuntimeError):
        importlib.import_module("auth_service")

    _clear_auth_service_module()


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
services_init = ROOT / "services" / "__init__.py"
_services_spec = importlib.util.spec_from_file_location(
    "services", services_init, submodule_search_locations=[str(services_init.parent)]
)
if _services_spec and _services_spec.loader:
    _services_module = importlib.util.module_from_spec(_services_spec)
    _services_spec.loader.exec_module(_services_module)
    sys.modules["services"] = _services_module


@pytest.fixture
def auth_service(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory):
    """Load the auth_service module with a deterministic secret."""

    def loader(secret: str = "test-secret", database_url: str | None = None):
        db_path = database_url
        if db_path is None:
            directory = tmp_path_factory.mktemp("auth-db")
            db_path = f"sqlite:///{directory/'auth.db'}"
        monkeypatch.setenv("AUTH_DATABASE_URL", db_path)
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


def test_auth_service_requires_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """The service should fail fast if the shared database URL is missing or default."""

    monkeypatch.delenv("AUTH_DATABASE_URL", raising=False)
    monkeypatch.setenv("AUTH_JWT_SECRET", "test-secret")
    _install_dependency_stubs(monkeypatch)
    _clear_auth_service_module()

    with pytest.raises(RuntimeError):
        importlib.import_module("auth_service")

    _clear_auth_service_module()
    monkeypatch.setenv("AUTH_DATABASE_URL", "sqlite:///./auth_sessions.db")

    with pytest.raises(RuntimeError):
        importlib.import_module("auth_service")

    _clear_auth_service_module()


def test_sessions_shared_across_replicas(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory) -> None:
    """Multiple replicas should persist sessions to the same database."""

    db_path = tmp_path_factory.mktemp("auth-multi") / "sessions.db"
    database_url = f"sqlite:///{db_path}"
    monkeypatch.setenv("AUTH_DATABASE_URL", database_url)
    monkeypatch.setenv("AUTH_JWT_SECRET", "shared-secret")
    _install_dependency_stubs(monkeypatch)

    _clear_auth_service_module()
    replica_a = importlib.import_module("auth_service")
    repo_a = replica_a.SessionRepository(replica_a.SessionLocal)
    counter = itertools.count()

    def deterministic_token(_: int = 32) -> str:
        return f"token-{next(counter)}"

    monkeypatch.setattr(replica_a.secrets, "token_urlsafe", deterministic_token, raising=False)
    session_a = repo_a.create(user_id="shared@example.com", mfa_verified=True)

    _clear_auth_service_module()
    replica_b = importlib.import_module("auth_service")
    monkeypatch.setattr(replica_b.secrets, "token_urlsafe", deterministic_token, raising=False)

    with replica_b.SessionLocal() as session:
        stored = session.get(replica_b.AuthSession, session_a.session_token)
        assert stored is not None
        assert stored.user_id == "shared@example.com"
        assert stored.mfa_verified is True

    repo_b = replica_b.SessionRepository(replica_b.SessionLocal)
    session_b = repo_b.create(user_id="secondary@example.com", mfa_verified=False)

    with replica_a.SessionLocal() as session:
        fetched = session.get(replica_a.AuthSession, session_b.session_token)
        assert fetched is not None
        assert fetched.user_id == "secondary@example.com"
        assert fetched.mfa_verified is False


def test_session_tokens_survive_restart(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory) -> None:
    """Session tokens should remain valid across pod restarts."""

    db_path = tmp_path_factory.mktemp("auth-restart") / "sessions.db"
    monkeypatch.setenv("AUTH_DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("AUTH_JWT_SECRET", "restart-secret")
    _install_dependency_stubs(monkeypatch)

    _clear_auth_service_module()
    module = importlib.import_module("auth_service")
    repo = module.SessionRepository(module.SessionLocal)
    counter = itertools.count()

    def deterministic_token(_: int = 32) -> str:
        return f"token-{next(counter)}"

    monkeypatch.setattr(module.secrets, "token_urlsafe", deterministic_token, raising=False)
    record = repo.create(user_id="restart@example.com", mfa_verified=True)

    _clear_auth_service_module()
    restarted = importlib.import_module("auth_service")
    monkeypatch.setattr(restarted.secrets, "token_urlsafe", deterministic_token, raising=False)

    with restarted.SessionLocal() as session:
        persisted = session.get(restarted.AuthSession, record.session_token)
        assert persisted is not None
        assert persisted.user_id == "restart@example.com"
        assert persisted.mfa_verified is True
