"""Application factory wiring services, middleware, and routers."""
from __future__ import annotations

import base64
import importlib
import logging
import os
import uuid
from typing import Optional

from fastapi import FastAPI

from audit_mode import configure_audit_mode
from accounts.service import AccountsService
from auth.routes import get_auth_service, router as auth_router
from auth.service import (
    AdminAccount,
    AdminRepositoryProtocol,
    AuthService,
    InMemoryAdminRepository,
    InMemorySessionStore,
    PostgresAdminRepository,
    SessionStoreProtocol,
    build_session_store_from_url,
    hash_password,
)
from metrics import setup_metrics
from services.alert_manager import setup_alerting
from services.alerts.alert_dedupe import router as alert_dedupe_router, setup_alert_dedupe
from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger
from shared.correlation import CorrelationIdMiddleware
from scaling_controller import (
    build_scaling_controller_from_env,
    configure_scaling_controller,
    router as scaling_router,
)
logger = logging.getLogger(__name__)
_ADMIN_REPOSITORY_HEALTHCHECK_EMAIL = "__admin_healthcheck__@aether.local"
_ADMIN_REPOSITORY_HEALTHCHECK_ID = "__admin_repository_healthcheck__"


def _generate_random_password() -> str:
    """Generate a high-entropy password for the sentinel admin record."""

    # 32 bytes provides a large keyspace while remaining URL-safe for storage/logging.
    raw = os.urandom(32)
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _generate_random_mfa_secret() -> str:
    """Generate a base32-encoded secret compatible with TOTP generators."""

    # Use 20 bytes (160 bits) to match typical TOTP secret entropy.
    raw = os.urandom(20)
    return base64.b32encode(raw).decode("ascii").rstrip("=")


def _build_admin_repository_from_env() -> AdminRepositoryProtocol:
    dsn_env_vars = (
        "ADMIN_POSTGRES_DSN",
        "ADMIN_DATABASE_DSN",
        "ADMIN_DB_DSN",
    )
    dsn = next((os.getenv(var) for var in dsn_env_vars if os.getenv(var)), None)
    if not dsn:
        raise RuntimeError(
            "A Postgres/Timescale DSN must be provided via ADMIN_POSTGRES_DSN, "
            "ADMIN_DATABASE_DSN, or ADMIN_DB_DSN."
        )

    normalized = dsn.lower()
    if normalized.startswith("postgres://"):
        dsn = "postgresql://" + dsn.split("://", 1)[1]
        normalized = dsn.lower()

    allowed_prefixes = (
        "postgresql://",
        "postgresql+psycopg://",
        "postgresql+psycopg2://",
        "timescale://",
    )
    if not normalized.startswith(allowed_prefixes):
        raise RuntimeError(
            "Admin repository requires a Postgres/Timescale DSN; "
            f"received '{dsn}'."
        )

    return PostgresAdminRepository(dsn)


def _verify_admin_repository(admin_repository: AdminRepositoryProtocol) -> None:
    """Persist and validate a sentinel admin record for startup verification."""


    sentinel = AdminAccount(
        admin_id=_ADMIN_REPOSITORY_HEALTHCHECK_ID,
        email=_ADMIN_REPOSITORY_HEALTHCHECK_EMAIL,
        password_hash=hash_password(_generate_random_password()),
        mfa_secret=_generate_random_mfa_secret(),
    )
    admin_repository.add(sentinel)
    stored = admin_repository.get_by_email(_ADMIN_REPOSITORY_HEALTHCHECK_EMAIL)
    if not stored or stored.admin_id != _ADMIN_REPOSITORY_HEALTHCHECK_ID:
        raise RuntimeError("Admin repository is not writable; startup verification failed.")



def _build_session_store_from_env() -> SessionStoreProtocol:
    ttl_minutes = int(os.getenv("SESSION_TTL_MINUTES", "60"))

    dsn_env_vars = (
        "SESSION_REDIS_URL",
        "SESSION_STORE_URL",
        "SESSION_BACKEND_DSN",
    )
    redis_url = next((os.getenv(var) for var in dsn_env_vars if os.getenv(var)), None)
    if not redis_url:
        joined = ", ".join(dsn_env_vars)
        raise RuntimeError(
            "Session store misconfigured: set one of "
            f"{joined} so the API can use the shared Redis backend"
        )
    if redis_url.startswith("memory://"):
        return InMemorySessionStore(ttl_minutes=ttl_minutes)

    return build_session_store_from_url(redis_url, ttl_minutes=ttl_minutes)


def _maybe_include_router(app: FastAPI, module: str, attribute: str) -> None:
    try:
        module_obj = importlib.import_module(module)
        router = getattr(module_obj, attribute)
    except Exception as exc:  # pragma: no cover - optional routes are best-effort
        logger.debug("Skipping router %s.%s due to %s", module, attribute, exc)
        return
    app.include_router(router)


def create_app(
    *,
    admin_repository: Optional[AdminRepositoryProtocol] = None,
    session_store: Optional[SessionStoreProtocol] = None,
) -> FastAPI:
    app = FastAPI(title="Aether Admin Platform")
    setup_metrics(app, service_name="admin-platform")
    app.add_middleware(CorrelationIdMiddleware)

    audit_store = AuditLogStore()
    audit_logger = TimescaleAuditLogger(audit_store)
    recorder = SensitiveActionRecorder(audit_logger)

    admin_repository = admin_repository or _build_admin_repository_from_env()
    _verify_admin_repository(admin_repository)
    session_store = session_store or _build_session_store_from_env()
    auth_service = AuthService(admin_repository, session_store)
    accounts_service = AccountsService(recorder)

    def _get_auth_service() -> AuthService:
        return auth_service

    app.dependency_overrides[get_auth_service] = _get_auth_service
    app.include_router(auth_router)
    _maybe_include_router(app, "services.report_service", "router")
    _maybe_include_router(app, "exposure_forecast", "router")
    _maybe_include_router(app, "alert_prioritizer", "router")
    app.include_router(alert_dedupe_router)

    _maybe_include_router(app, "services.models.meta_learner", "router")
    _maybe_include_router(app, "services.models.model_zoo", "router")
    _maybe_include_router(app, "multiformat_export", "router")
    _maybe_include_router(app, "compliance_pack", "router")
    _maybe_include_router(app, "pack_exporter", "router")
    _maybe_include_router(app, "services.system.health_service", "router")
    _maybe_include_router(app, "services.hedge.hedge_service", "router")


    scaling_controller = build_scaling_controller_from_env()
    configure_scaling_controller(scaling_controller)
    app.include_router(scaling_router)


    app.state.audit_store = audit_store
    app.state.audit_logger = audit_logger
    app.state.sensitive_recorder = recorder
    app.state.admin_repository = admin_repository
    app.state.session_store = session_store
    app.state.auth_service = auth_service
    app.state.accounts_service = accounts_service
    app.state.scaling_controller = scaling_controller

    configure_audit_mode(app)

    @app.on_event("startup")
    async def _start_scaling_controller() -> None:  # pragma: no cover - FastAPI lifecycle
        await scaling_controller.start()

    @app.on_event("shutdown")
    async def _stop_scaling_controller() -> None:  # pragma: no cover - FastAPI lifecycle
        await scaling_controller.stop()

    alertmanager_url = os.getenv("ALERTMANAGER_URL")
    setup_alerting(app, alertmanager_url=alertmanager_url)
    setup_alert_dedupe(app, alertmanager_url=alertmanager_url)

    return app


__all__ = ["create_app"]
