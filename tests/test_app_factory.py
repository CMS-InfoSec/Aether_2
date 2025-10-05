from __future__ import annotations

import sys
import types

import pytest
from fastapi import APIRouter


def _install_package(name: str) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__path__ = []  # type: ignore[attr-defined]
    sys.modules[name] = module
    return module


def _install_module(name: str, module: types.ModuleType) -> None:
    sys.modules[name] = module
    package_name, _, attr = name.rpartition(".")
    if package_name:
        package = sys.modules.setdefault(package_name, _install_package(package_name))
        setattr(package, attr, module)


# Stub audit mode wiring to avoid pulling optional dependencies during import.
audit_mode = types.ModuleType("audit_mode")

def _configure_audit_mode(app) -> None:  # pragma: no cover - behaviour not under test
    app.audit_mode_configured = True

audit_mode.configure_audit_mode = _configure_audit_mode  # type: ignore[attr-defined]
_install_module("audit_mode", audit_mode)


# Minimal accounts service stub.
accounts_service = types.ModuleType("accounts.service")


class _StubAccountsService:
    def __init__(self, recorder) -> None:  # pragma: no cover - behaviour not under test
        self.recorder = recorder


accounts_service.AccountsService = _StubAccountsService  # type: ignore[attr-defined]
_install_module("accounts.service", accounts_service)


# Minimal auth routing stub.
auth_routes = types.ModuleType("auth.routes")

def _get_auth_service():  # pragma: no cover - overridden in create_app
    raise RuntimeError("dependency override not configured")


auth_routes.get_auth_service = _get_auth_service  # type: ignore[attr-defined]
auth_routes.router = APIRouter()
_install_module("auth.routes", auth_routes)


# Authentication service primitives for the application factory.
auth_service_module = types.ModuleType("auth.service")


class AdminRepositoryProtocol:  # pragma: no cover - structural stub
    pass


class AdminAccount:
    def __init__(
        self,
        *,
        admin_id: str,
        email: str,
        password_hash: str,
        mfa_secret: str,
        allowed_ips: set[str] | None = None,
    ) -> None:
        self.admin_id = admin_id
        self.email = email
        self.password_hash = password_hash
        self.mfa_secret = mfa_secret
        self.allowed_ips = allowed_ips


def hash_password(password: str) -> str:
    return f"hashed::{password}"


class InMemoryAdminRepository(AdminRepositoryProtocol):
    def __init__(self) -> None:
        self._admins: dict[str, object] = {}

    def add(self, admin) -> None:  # pragma: no cover - behaviour not under test
        self._admins[getattr(admin, "email", "")] = admin

    def delete(self, email: str) -> None:  # pragma: no cover - behaviour not under test
        self._admins.pop(email, None)

    def get_by_email(self, email: str):  # pragma: no cover - behaviour not under test
        return self._admins.get(email)


class SessionStoreProtocol:  # pragma: no cover - structural stub
    pass


class InMemorySessionStore(SessionStoreProtocol):
    def __init__(self, ttl_minutes: int = 60) -> None:
        self.ttl_minutes = ttl_minutes
        self._sessions: dict[str, object] = {}

    def create(self, admin_id: str):  # pragma: no cover - behaviour not under test
        token = f"token-{admin_id}"
        session = types.SimpleNamespace(token=token, admin_id=admin_id)
        self._sessions[token] = session
        return session

    def get(self, token: str):  # pragma: no cover - behaviour not under test
        return self._sessions.get(token)


class RedisSessionStore(SessionStoreProtocol):  # pragma: no cover - structural stub
    def __init__(self, client, ttl_minutes: int = 60) -> None:
        self.client = client
        self.ttl_minutes = ttl_minutes


def build_session_store_from_url(redis_url: str, *, ttl_minutes: int = 60) -> RedisSessionStore:
    return RedisSessionStore(client={"url": redis_url}, ttl_minutes=ttl_minutes)


class PostgresAdminRepository(AdminRepositoryProtocol):
    def __init__(self, dsn: str) -> None:  # pragma: no cover - behaviour not under test
        self.dsn = dsn


class AuthService:
    def __init__(self, repository: AdminRepositoryProtocol, sessions: SessionStoreProtocol) -> None:
        self.repository = repository
        self.sessions = sessions


# Backwards-compatible aliases used by the application module.
AdminRepository = InMemoryAdminRepository
SessionStore = InMemorySessionStore

auth_service_module.AdminRepositoryProtocol = AdminRepositoryProtocol  # type: ignore[attr-defined]
auth_service_module.InMemoryAdminRepository = InMemoryAdminRepository  # type: ignore[attr-defined]
auth_service_module.AdminAccount = AdminAccount  # type: ignore[attr-defined]
auth_service_module.hash_password = hash_password  # type: ignore[attr-defined]
auth_service_module.PostgresAdminRepository = PostgresAdminRepository  # type: ignore[attr-defined]
auth_service_module.AuthService = AuthService  # type: ignore[attr-defined]
auth_service_module.SessionStoreProtocol = SessionStoreProtocol  # type: ignore[attr-defined]
auth_service_module.InMemorySessionStore = InMemorySessionStore  # type: ignore[attr-defined]
auth_service_module.RedisSessionStore = RedisSessionStore  # type: ignore[attr-defined]
auth_service_module.build_session_store_from_url = build_session_store_from_url  # type: ignore[attr-defined]
auth_service_module.SessionStore = SessionStore  # type: ignore[attr-defined]
_install_module("auth.service", auth_service_module)


# Metrics wiring.
metrics_module = types.ModuleType("metrics")


def setup_metrics(app, *, service_name: str) -> None:  # pragma: no cover - behaviour not under test
    app.metrics_service_name = service_name


metrics_module.setup_metrics = setup_metrics  # type: ignore[attr-defined]
_install_module("metrics", metrics_module)


# Alerting stubs.
services_pkg = _install_package("services")

alert_manager_module = types.ModuleType("services.alert_manager")

def setup_alerting(app, *, alertmanager_url=None) -> None:  # pragma: no cover - behaviour not under test
    app.alertmanager_url = alertmanager_url


alert_manager_module.setup_alerting = setup_alerting  # type: ignore[attr-defined]
_install_module("services.alert_manager", alert_manager_module)

alerts_pkg = _install_package("services.alerts")
alert_dedupe_module = types.ModuleType("services.alerts.alert_dedupe")
alert_dedupe_module.router = APIRouter()

def setup_alert_dedupe(app, *, alertmanager_url=None) -> None:  # pragma: no cover - behaviour not under test
    app.alert_dedupe_url = alertmanager_url


alert_dedupe_module.setup_alert_dedupe = setup_alert_dedupe  # type: ignore[attr-defined]
_install_module("services.alerts.alert_dedupe", alert_dedupe_module)


# Shared audit primitives.
shared_pkg = _install_package("shared")
shared_audit_module = types.ModuleType("shared.audit")


class AuditLogStore:  # pragma: no cover - structural stub
    pass


class TimescaleAuditLogger:  # pragma: no cover - structural stub
    def __init__(self, store: AuditLogStore) -> None:
        self.store = store


class SensitiveActionRecorder:  # pragma: no cover - structural stub
    def __init__(self, logger: TimescaleAuditLogger) -> None:
        self.logger = logger


shared_audit_module.AuditLogStore = AuditLogStore  # type: ignore[attr-defined]
shared_audit_module.TimescaleAuditLogger = TimescaleAuditLogger  # type: ignore[attr-defined]
shared_audit_module.SensitiveActionRecorder = SensitiveActionRecorder  # type: ignore[attr-defined]
_install_module("shared.audit", shared_audit_module)

shared_correlation_module = types.ModuleType("shared.correlation")


class CorrelationIdMiddleware:  # pragma: no cover - structural stub
    pass


shared_correlation_module.CorrelationIdMiddleware = CorrelationIdMiddleware  # type: ignore[attr-defined]
_install_module("shared.correlation", shared_correlation_module)


# Scaling controller infrastructure.
scaling_controller_module = types.ModuleType("scaling_controller")


class _ScalingController:
    async def start(self) -> None:  # pragma: no cover - behaviour not under test
        return None

    async def stop(self) -> None:  # pragma: no cover - behaviour not under test
        return None


def build_scaling_controller_from_env() -> _ScalingController:
    return _ScalingController()


def configure_scaling_controller(controller: _ScalingController) -> None:  # pragma: no cover - behaviour not under test
    controller.configured = True


scaling_controller_module.build_scaling_controller_from_env = build_scaling_controller_from_env  # type: ignore[attr-defined]
scaling_controller_module.configure_scaling_controller = configure_scaling_controller  # type: ignore[attr-defined]
scaling_controller_module.router = APIRouter()
_install_module("scaling_controller", scaling_controller_module)


import app as app_module


def test_create_app_uses_postgres_repository_when_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_POSTGRES_DSN", "postgresql://example.com/admin")

    created: dict[str, str] = {}

    class DummyPostgresRepository(app_module.InMemoryAdminRepository):
        def __init__(self, dsn: str) -> None:
            super().__init__()
            created["dsn"] = dsn

    monkeypatch.setattr(app_module, "PostgresAdminRepository", DummyPostgresRepository)

    session_store = InMemorySessionStore()
    application = app_module.create_app(session_store=session_store)

    assert isinstance(application.state.admin_repository, DummyPostgresRepository)
    assert created["dsn"] == "postgresql://example.com/admin"


def test_create_app_normalizes_timescale_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_POSTGRES_DSN", "timescale://tenant:pass@example.com/admin")

    captured: dict[str, str] = {}

    class DummyPostgresRepository(app_module.InMemoryAdminRepository):
        def __init__(self, dsn: str) -> None:
            super().__init__()
            captured["dsn"] = dsn

    monkeypatch.setattr(app_module, "PostgresAdminRepository", DummyPostgresRepository)

    session_store = InMemorySessionStore()
    application = app_module.create_app(session_store=session_store)

    assert isinstance(application.state.admin_repository, DummyPostgresRepository)
    assert captured["dsn"].startswith("postgresql://")


def test_create_app_normalizes_sqlalchemy_style_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ADMIN_POSTGRES_DSN",
        "postgresql+psycopg://tenant:pass@example.com/admin",
    )

    captured: dict[str, str] = {}

    class DummyPostgresRepository(app_module.InMemoryAdminRepository):
        def __init__(self, dsn: str) -> None:
            super().__init__()
            captured["dsn"] = dsn

    monkeypatch.setattr(app_module, "PostgresAdminRepository", DummyPostgresRepository)

    session_store = InMemorySessionStore()
    application = app_module.create_app(session_store=session_store)

    assert isinstance(application.state.admin_repository, DummyPostgresRepository)
    assert captured["dsn"].startswith("postgresql://")


def test_create_app_normalizes_postgresql_scheme_casing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ADMIN_POSTGRES_DSN",
        "  PostgreSQL://tenant:pass@example.com/admin?sslmode=require  ",
    )

    captured: dict[str, str] = {}

    class DummyPostgresRepository(app_module.InMemoryAdminRepository):
        def __init__(self, dsn: str) -> None:
            super().__init__()
            captured["dsn"] = dsn

    monkeypatch.setattr(app_module, "PostgresAdminRepository", DummyPostgresRepository)

    session_store = InMemorySessionStore()
    application = app_module.create_app(session_store=session_store)

    assert isinstance(application.state.admin_repository, DummyPostgresRepository)
    assert captured["dsn"] == "postgresql://tenant:pass@example.com/admin?sslmode=require"


def test_create_app_requires_dsn_when_not_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ADMIN_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("ADMIN_DATABASE_DSN", raising=False)
    monkeypatch.delenv("ADMIN_DB_DSN", raising=False)


    application = app_module.create_app(session_store=InMemorySessionStore())

    assert isinstance(application.state.admin_repository, app_module.InMemoryAdminRepository)


def test_create_app_rejects_unsupported_admin_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_POSTGRES_DSN", "mysql://example.com/admin")

    with pytest.raises(RuntimeError, match="requires a Postgres/Timescale DSN"):
        app_module.create_app(session_store=InMemorySessionStore())


def test_create_app_rejects_blank_admin_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_POSTGRES_DSN", "   ")

    with pytest.raises(RuntimeError, match="requires a DSN with an explicit scheme"):
        app_module.create_app(session_store=InMemorySessionStore())


def test_create_app_requires_session_store_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SESSION_REDIS_URL", raising=False)
    monkeypatch.delenv("SESSION_STORE_URL", raising=False)
    monkeypatch.delenv("SESSION_BACKEND_DSN", raising=False)

    with pytest.raises(RuntimeError, match="Session store misconfigured"):
        app_module.create_app()

