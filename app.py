"""Application factory wiring services, middleware, and routers."""
from __future__ import annotations

import os

from fastapi import FastAPI

from audit_mode import configure_audit_mode
from accounts.service import AccountsService
from auth.routes import get_auth_service, router as auth_router
from auth.service import AdminRepository, AuthService, SessionStore
from services.alert_manager import setup_alerting
from alert_prioritizer import router as alert_prioritizer_router
from services.report_service import router as reports_router
from multiformat_export import router as log_export_router

from services.models.meta_learner import router as meta_router
from services.models.model_zoo import router as models_router

from exposure_forecast import router as exposure_router
from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger
from shared.correlation import CorrelationIdMiddleware


def create_app() -> FastAPI:
    app = FastAPI(title="Aether Admin Platform")
    app.add_middleware(CorrelationIdMiddleware)

    audit_store = AuditLogStore()
    audit_logger = TimescaleAuditLogger(audit_store)
    recorder = SensitiveActionRecorder(audit_logger)

    admin_repository = AdminRepository()
    session_store = SessionStore()
    auth_service = AuthService(admin_repository, session_store)
    accounts_service = AccountsService(recorder)

    def _get_auth_service() -> AuthService:
        return auth_service

    app.dependency_overrides[get_auth_service] = _get_auth_service
    app.include_router(auth_router)
    app.include_router(reports_router)
    app.include_router(exposure_router)
    app.include_router(alert_prioritizer_router)

    app.include_router(models_router)

    app.include_router(log_export_router)



    app.state.audit_store = audit_store
    app.state.audit_logger = audit_logger
    app.state.sensitive_recorder = recorder
    app.state.admin_repository = admin_repository
    app.state.session_store = session_store
    app.state.auth_service = auth_service
    app.state.accounts_service = accounts_service

    configure_audit_mode(app)

    alertmanager_url = os.getenv("ALERTMANAGER_URL")
    setup_alerting(app, alertmanager_url=alertmanager_url)

    return app


__all__ = ["create_app"]
