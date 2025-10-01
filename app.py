"""Application factory wiring services, middleware, and routers."""
from __future__ import annotations

from fastapi import FastAPI

from accounts.service import AccountsService
from auth.routes import get_auth_service, router as auth_router
from auth.service import AdminRepository, AuthService, SessionStore
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

    app.state.audit_store = audit_store
    app.state.audit_logger = audit_logger
    app.state.sensitive_recorder = recorder
    app.state.admin_repository = admin_repository
    app.state.session_store = session_store
    app.state.auth_service = auth_service
    app.state.accounts_service = accounts_service

    return app


__all__ = ["create_app"]
