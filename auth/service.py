"""Administrative authentication service with MFA and session management."""
from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set

import pyotp


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
            raise PermissionError("invalid credentials")
        if not self._verify_ip(admin, ip_address):
            raise PermissionError("ip_not_allowed")
        if not self._verify_password(admin, password):
            raise PermissionError("invalid credentials")
        if not mfa_code or not self._verify_mfa(admin, mfa_code):
            raise PermissionError("mfa_required")
        return self._sessions.create(admin.admin_id)


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
