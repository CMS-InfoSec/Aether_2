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
    RedisSessionStore,
    SessionStoreProtocol,
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

    probe_suffix = uuid.uuid4().hex
    probe_email = f"__admin_healthcheck__+{probe_suffix}@aether.local"
    probe_id = f"admin-healthcheck::{probe_suffix}"
    probe_password = (uuid.uuid4().hex + uuid.uuid4().hex)
    probe_secret = base64.b32encode(os.urandom(10)).decode("utf-8").rstrip("=")

    sentinel = AdminAccount(
        admin_id=probe_id,
        email=probe_email,
        password_hash=hash_password(probe_password),
        mfa_secret=probe_secret,
    )
    try:
        admin_repository.add(sentinel)
        stored = admin_repository.get_by_email(probe_email)
        if not stored or stored.admin_id != probe_id:
            raise RuntimeError(
                "Admin repository is not writable; startup verification failed."
            )
    finally:
        try:
            admin_repository.delete(probe_email)
        except Exception:  # pragma: no cover - best-effort cleanup for startup probe
            logger.warning(
                "Failed to remove admin repository health check record for %s", probe_email
            )


def _build_session_store_from_env() -> SessionStoreProtocol:
    ttl_minutes = int(os.getenv("SESSION_TTL_MINUTES", "60"))
    redis_url = os.getenv("SESSION_REDIS_URL")
    if redis_url:
        try:  # pragma: no cover - import guarded for optional dependency resolution
            import redis
        except ImportError as exc:  # pragma: no cover - surfaced when dependency missing at runtime
            raise RuntimeError("redis package is required when SESSION_REDIS_URL is set") from exc
        client = redis.Redis.from_url(redis_url)
        return RedisSessionStore(client, ttl_minutes=ttl_minutes)
    return InMemorySessionStore(ttl_minutes=ttl_minutes)


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
