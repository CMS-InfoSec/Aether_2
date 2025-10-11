from __future__ import annotations

import inspect
import os
import sys
import types

import pytest
from fastapi import APIRouter
from fastapi.testclient import TestClient


_INSTALLED_MODULES: list[tuple[str, types.ModuleType | None, str | None, str | None, object]] = []
_MISSING = object()


def _install_package(name: str) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__path__ = []  # type: ignore[attr-defined]
    sys.modules[name] = module
    return module


def _install_module(name: str, module: types.ModuleType) -> None:
    previous = sys.modules.get(name)
    package_name, _, attr = name.rpartition(".")
    package_attr = None
    if package_name:
        package = sys.modules.setdefault(package_name, _install_package(package_name))
        package_attr = getattr(package, attr, _MISSING)
        setattr(package, attr, module)
    sys.modules[name] = module
    _INSTALLED_MODULES.append((name, previous, package_name or None, attr or None, package_attr))


def teardown_module(module: types.ModuleType) -> None:  # pragma: no cover - pytest hook
    del module  # Only used for pytest signature compatibility.
    while _INSTALLED_MODULES:
        name, previous, package_name, attr, package_attr = _INSTALLED_MODULES.pop()
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
        if package_name and attr:
            package = sys.modules.get(package_name)
            if package is not None:
                if package_attr is _MISSING:
                    try:
                        delattr(package, attr)
                    except AttributeError:
                        pass
                else:
                    setattr(package, attr, package_attr)


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
        self._sessions: dict[str, object] = {}

    def get(self, token: str):  # pragma: no cover - behaviour not under test
        return self._sessions.get(token)


def build_session_store_from_url(redis_url: str, *, ttl_minutes: int = 60) -> RedisSessionStore:
    return RedisSessionStore(client={"url": redis_url}, ttl_minutes=ttl_minutes)


class PostgresAdminRepository(AdminRepositoryProtocol):
    def __init__(self, dsn: str) -> None:  # pragma: no cover - behaviour not under test
        self.dsn = dsn
        self._admins: dict[str, object] = {}

    def add(self, admin) -> None:  # pragma: no cover - behaviour not under test
        self._admins[getattr(admin, "email", "")] = admin

    def delete(self, email: str) -> None:  # pragma: no cover - behaviour not under test
        self._admins.pop(email, None)

    def get_by_email(self, email: str):  # pragma: no cover - behaviour not under test
        return self._admins.get(email)


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

hedge_pkg = _install_package("services.hedge")
hedge_service_module = types.ModuleType("services.hedge.hedge_service")


class _StubHedgeService:
    def health_status(self) -> dict[str, object]:  # pragma: no cover - simple data carrier
        return {
            "mode": "auto",
            "override_active": False,
            "history_depth": 0,
            "last_decision_at": None,
            "last_target_pct": None,
            "last_guard_triggered": False,
        }


_stub_hedge_service = _StubHedgeService()


def get_hedge_service() -> _StubHedgeService:  # pragma: no cover - dependency override
    return _stub_hedge_service


hedge_service_module.get_hedge_service = get_hedge_service  # type: ignore[attr-defined]
hedge_service_module.router = APIRouter()
_install_module("services.hedge.hedge_service", hedge_service_module)


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

shared_health_module = types.ModuleType("shared.health")


def setup_health_checks(app, checks=None):  # pragma: no cover - behaviour exercised via tests
    registry = dict(checks or {})

    @app.get("/healthz")
    async def _healthz():  # pragma: no cover - simple stub endpoint
        overall = "ok"
        results: dict[str, dict[str, object]] = {}
        for name, check in registry.items():
            try:
                result = check()
                if inspect.isawaitable(result):
                    result = await result
                entry: dict[str, object] = {"status": "ok"}
                if isinstance(result, dict):
                    entry.update(result)
            except Exception as exc:  # pragma: no cover - defensive default
                overall = "error"
                entry = {"status": "error", "error": str(exc)}
            results[name] = entry

        return {"status": overall, "checks": results}


shared_health_module.setup_health_checks = setup_health_checks  # type: ignore[attr-defined]
_install_module("shared.health", shared_health_module)

shared_session_config_module = types.ModuleType("shared.session_config")


def load_session_ttl_minutes(*, env=None, default: int = 60):  # pragma: no cover - helper stub
    source = env or os.environ
    raw = source.get("SESSION_TTL_MINUTES")
    if raw is None:
        if default <= 0:
            raise RuntimeError("Session TTL default must be a positive integer.")
        return default
    value = str(raw).strip()
    if not value:
        raise RuntimeError("SESSION_TTL_MINUTES must be a positive integer representing minutes.")
    try:
        ttl = int(value)
    except ValueError as exc:  # pragma: no cover - behaviour not under test
        raise RuntimeError("SESSION_TTL_MINUTES must be a positive integer representing minutes.") from exc
    if ttl <= 0:
        raise RuntimeError("SESSION_TTL_MINUTES must be a positive integer representing minutes.")
    return ttl


shared_session_config_module.load_session_ttl_minutes = load_session_ttl_minutes  # type: ignore[attr-defined]
_install_module("shared.session_config", shared_session_config_module)


# Scaling controller infrastructure.
scaling_controller_module = types.ModuleType("scaling_controller")


class _ScalingController:
    async def start(self) -> None:  # pragma: no cover - behaviour not under test
        return None

    async def stop(self) -> None:  # pragma: no cover - behaviour not under test
        return None

    @property
    def status(self) -> types.SimpleNamespace:  # pragma: no cover - behaviour not under test
        return types.SimpleNamespace(oms_replicas=1, gpu_nodes=0, pending_jobs=0)


def build_scaling_controller_from_env() -> _ScalingController:
    return _ScalingController()


def configure_scaling_controller(controller: _ScalingController) -> None:  # pragma: no cover - behaviour not under test
    controller.configured = True


class _NullGPUManager:
    async def list_gpu_nodes(self) -> list[str]:  # pragma: no cover - behaviour not under test
        return []

    async def provision_gpu_pool(self) -> list[str]:  # pragma: no cover - behaviour not under test
        return []

    async def deprovision_gpu_pool(self) -> None:  # pragma: no cover - behaviour not under test
        return None


scaling_controller_module.build_scaling_controller_from_env = build_scaling_controller_from_env  # type: ignore[attr-defined]
scaling_controller_module.configure_scaling_controller = configure_scaling_controller  # type: ignore[attr-defined]
scaling_controller_module.router = APIRouter()
scaling_controller_module.ScalingController = _ScalingController  # type: ignore[attr-defined]
scaling_controller_module.NullGPUManager = _NullGPUManager  # type: ignore[attr-defined]
_install_module("scaling_controller", scaling_controller_module)


import app as app_module


def _restore_modules() -> None:
    teardown_module(sys.modules[__name__])


_restore_modules()


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

    repository = app_module.InMemoryAdminRepository()
    application = app_module.create_app(
        admin_repository=repository, session_store=InMemorySessionStore()
    )

    assert application.state.admin_repository is repository


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
        app_module.create_app(admin_repository=app_module.InMemoryAdminRepository())


def test_create_app_rejects_non_integer_session_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://admin-platform-test")
    monkeypatch.setenv("SESSION_TTL_MINUTES", "not-a-number")

    with pytest.raises(RuntimeError, match="SESSION_TTL_MINUTES must be a positive integer"):
        app_module.create_app(admin_repository=app_module.InMemoryAdminRepository())


def test_create_app_rejects_non_positive_session_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://admin-platform-test")
    monkeypatch.setenv("SESSION_TTL_MINUTES", "0")

    with pytest.raises(RuntimeError, match="SESSION_TTL_MINUTES must be a positive integer"):
        app_module.create_app(admin_repository=app_module.InMemoryAdminRepository())


def test_create_app_rejects_blank_session_store_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SESSION_REDIS_URL", "   ")

    with pytest.raises(RuntimeError, match="Session store misconfigured"):
        app_module.create_app(admin_repository=app_module.InMemoryAdminRepository())


def test_create_app_rejects_memory_session_store_outside_tests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://admin-platform-test")
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    with pytest.raises(
        RuntimeError, match="memory:// DSNs are only supported when running tests"
    ):
        app_module.create_app(admin_repository=app_module.InMemoryAdminRepository())


def test_healthz_endpoint_reports_component_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_POSTGRES_DSN", "postgresql://example.com/admin")
    monkeypatch.setenv("SESSION_REDIS_URL", "redis://localhost/0")

    application = app_module.create_app()

    client = TestClient(application)
    response = client.get("/healthz")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    checks = payload["checks"]
    assert set(checks.keys()) == {
        "admin_repository",
        "session_store",
        "scaling_controller",
        "hedge_service",
    }
    assert all(entry["status"] == "ok" for entry in checks.values())
    hedge_status = checks["hedge_service"]
    assert hedge_status["override_active"] is False

