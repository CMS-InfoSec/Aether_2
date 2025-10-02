"""Administrative authentication service with MFA and session management."""
from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set

import pyotp
from prometheus_client import Counter

from metrics import _REGISTRY
from shared.correlation import get_correlation_id


logger = logging.getLogger(__name__)


_LOGIN_SUCCESS_COUNTER = Counter(
    "auth_login_success_total",
    "Total number of successful administrator login attempts.",
    registry=_REGISTRY,
)

_LOGIN_FAILURE_COUNTER = Counter(
    "auth_login_failure_total",
    "Total number of failed administrator login attempts by reason.",
    ["reason"],
    registry=_REGISTRY,
)

_MFA_DENIED_COUNTER = Counter(
    "auth_mfa_denied_total",
    "Total number of administrator logins denied due to MFA failure.",
    registry=_REGISTRY,
)


@dataclass
class AdminAccount:
    admin_id: str
    email: str
    password_hash: str
    mfa_secret: str
    allowed_ips: Optional[Set[str]] = None


class AdminRepository:
    """Simple in-memory repository for administrator accounts."""

    def __init__(self) -> None:
        self._admins: Dict[str, AdminAccount] = {}

    def add(self, admin: AdminAccount) -> None:
        self._admins[admin.email] = admin

    def get_by_email(self, email: str) -> Optional[AdminAccount]:
        return self._admins.get(email)


@dataclass
class Session:
    token: str
    admin_id: str
    created_at: datetime
    expires_at: datetime

    @property
    def is_active(self) -> bool:
        return datetime.now(timezone.utc) < self.expires_at


class SessionStore:
    """Tracks authenticated admin sessions."""

    def __init__(self, ttl_minutes: int = 60) -> None:
        self._sessions: Dict[str, Session] = {}
        self._ttl = timedelta(minutes=ttl_minutes)

    def create(self, admin_id: str) -> Session:
        now = datetime.now(timezone.utc)
        token = secrets.token_urlsafe(32)
        session = Session(
            token=token,
            admin_id=admin_id,
            created_at=now,
            expires_at=now + self._ttl,
        )
        self._sessions[token] = session
        return session

    def get(self, token: str) -> Optional[Session]:
        session = self._sessions.get(token)
        if session and not session.is_active:
            self._sessions.pop(token, None)
            return None
        return session


class AuthService:
    """Authenticates admins and enforces MFA and IP allow-listing."""

    def __init__(self, repository: AdminRepository, sessions: SessionStore) -> None:
        self._repository = repository
        self._sessions = sessions

    def _record_failure(
        self,
        *,
        reason: str,
        email: str,
        ip_address: Optional[str],
        admin_id: Optional[str] = None,
    ) -> None:
        correlation_id = get_correlation_id()
        extra = {
            "auth_event": "auth_login_failure",
            "auth_reason": reason,
            "auth_email": email,
            "auth_ip": ip_address,
            "correlation_id": correlation_id,
        }
        if admin_id:
            extra["auth_admin_id"] = admin_id
        logger.warning("Admin login failed", extra=extra)
        _LOGIN_FAILURE_COUNTER.labels(reason=reason).inc()
        if reason == "mfa_required":
            _MFA_DENIED_COUNTER.inc()

    def _verify_password(self, admin: AdminAccount, password: str) -> bool:
        candidate = hashlib.sha256(password.encode()).hexdigest()
        return hmac.compare_digest(candidate, admin.password_hash)

    def _verify_mfa(self, admin: AdminAccount, code: str) -> bool:
        totp = pyotp.TOTP(admin.mfa_secret)
        return totp.verify(code, valid_window=1)

    def _verify_ip(self, admin: AdminAccount, ip_address: Optional[str]) -> bool:
        if not admin.allowed_ips:
            return True
        return ip_address in admin.allowed_ips

    def login(
        self,
        *,
        email: str,
        password: str,
        mfa_code: str,
        ip_address: Optional[str] = None,
    ) -> Session:
        admin = self._repository.get_by_email(email)
        if admin is None:
            self._record_failure(
                reason="invalid_credentials",
                email=email,
                ip_address=ip_address,
                admin_id=None,
            )
            raise PermissionError("invalid credentials")
        if not self._verify_ip(admin, ip_address):
            self._record_failure(
                reason="ip_not_allowed",
                email=email,
                ip_address=ip_address,
                admin_id=admin.admin_id,
            )
            raise PermissionError("ip_not_allowed")
        if not self._verify_password(admin, password):
            self._record_failure(
                reason="invalid_credentials",
                email=email,
                ip_address=ip_address,
                admin_id=admin.admin_id,
            )
            raise PermissionError("invalid credentials")
        if not mfa_code or not self._verify_mfa(admin, mfa_code):
            self._record_failure(
                reason="mfa_required",
                email=email,
                ip_address=ip_address,
                admin_id=admin.admin_id,
            )
            raise PermissionError("mfa_required")
        session = self._sessions.create(admin.admin_id)
        _LOGIN_SUCCESS_COUNTER.inc()
        return session


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


__all__ = [
    "AdminAccount",
    "AdminRepository",
    "Session",
    "SessionStore",
    "AuthService",
    "hash_password",
]
