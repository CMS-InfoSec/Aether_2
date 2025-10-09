"""Administrative authentication service with MFA and session management."""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac

import os

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
from typing import Dict, Optional, Protocol, Set, runtime_checkable


import pyotp

from shared.correlation import get_correlation_id
from common.utils.redis import create_redis_from_url


try:  # pragma: no cover - optional dependency in some test environments
    from argon2 import PasswordHasher as _Argon2PasswordHasher, Type
    from argon2.exceptions import InvalidHash, VerificationError, VerifyMismatchError
except ImportError:  # pragma: no cover - fallback when argon2 is unavailable
    class Type:  # type: ignore[override]
        """Fallback stub mirroring the argon2 Type enum attributes used here."""

        ID = "argon2id"


    class VerificationError(ValueError):
        """Minimal stand-in for argon2's verification errors."""


    class InvalidHash(VerificationError):
        """Raised when an argon2 hash cannot be parsed."""

    class VerifyMismatchError(VerificationError):
        """Raised when a password does not match the stored hash."""

    class _FallbackPasswordHasher:
        """Safe fallback used when argon2 is not installed."""

        hash_prefix = "$pbkdf2-sha256$"

        def __init__(self, *args: object, iterations: int = 390000, **kwargs: object) -> None:
            self._iterations = iterations

        def hash(self, password: str) -> str:
            salt = os.urandom(16)
            derived = hashlib.pbkdf2_hmac(
                "sha256", password.encode("utf-8"), salt, self._iterations
            )
            return "".join(
                [
                    self.hash_prefix,
                    str(self._iterations),
                    "$",
                    base64.b64encode(salt).decode("ascii"),
                    "$",
                    base64.b64encode(derived).decode("ascii"),
                ]
            )

        def verify(self, hashed: str, password: str) -> bool:
            if not hashed.startswith(self.hash_prefix):
                raise InvalidHash("Unsupported password hash prefix")

            try:
                parts = hashed.split("$")
                _, prefix_name, iterations_str, salt_b64, derived_b64 = parts
            except ValueError as exc:
                raise InvalidHash("Malformed password hash") from exc

            if prefix_name != "pbkdf2-sha256":
                raise InvalidHash("Unexpected password hash algorithm")

            try:
                iterations = int(iterations_str)
            except ValueError as exc:
                raise InvalidHash("Invalid iteration count in password hash") from exc

            try:
                salt = base64.b64decode(salt_b64.encode("ascii"))
                expected = base64.b64decode(derived_b64.encode("ascii"))
            except (ValueError, binascii.Error) as exc:
                raise InvalidHash("Invalid base64 in stored password hash") from exc

            computed = hashlib.pbkdf2_hmac(
                "sha256", password.encode("utf-8"), salt, iterations
            )
            if not hmac.compare_digest(expected, computed):
                raise VerifyMismatchError("Password does not match stored hash")
            return True

        def needs_update(self, hashed: str) -> bool:
            """Return ``True`` when the stored hash should be upgraded."""

            if not hashed.startswith(self.hash_prefix):
                return False

            try:
                _, prefix_name, iterations_str, *_rest = hashed.split("$", 4)
            except ValueError:
                return False

            if prefix_name != "pbkdf2-sha256":
                return False

            try:
                iterations = int(iterations_str)
            except ValueError:
                return False

            return iterations < self._iterations


    PasswordHasher = _FallbackPasswordHasher  # type: ignore[assignment]
    _ARGON2_HASHER = PasswordHasher(type=getattr(Type, "ID", None))
else:
    PasswordHasher = _Argon2PasswordHasher
    _ARGON2_HASHER = PasswordHasher(type=Type.ID)


try:  # pragma: no cover - prometheus is optional outside production
    from prometheus_client import CollectorRegistry, Counter
except Exception:  # pragma: no cover - provide a no-op fallback
    class CollectorRegistry:  # type: ignore[override]
        def __init__(self) -> None:
            self._collectors: list[object] = []

    class Counter:  # type: ignore[override]
        def __init__(self, *args: object, **kwargs: object) -> None:
            self._value = 0.0

        def labels(self, **kwargs: object) -> "Counter":
            return self

        def inc(self, value: float = 1.0) -> None:
            self._value = getattr(self, "_value", 0.0) + value

        def set(self, value: float) -> None:
            self._value = value



logger = logging.getLogger(__name__)


_METRICS_REGISTRY: CollectorRegistry | None = None
_LOGIN_FAILURE_COUNTER: Counter
_MFA_DENIED_COUNTER: Counter
_LOGIN_SUCCESS_COUNTER: Counter


def _build_counter(name: str, documentation: str, labels: tuple[str, ...] = ()) -> Counter:
    kwargs: dict[str, object] = {}
    if _METRICS_REGISTRY is not None:
        kwargs["registry"] = _METRICS_REGISTRY
    try:
        return Counter(name, documentation, labels, **kwargs)
    except TypeError:  # pragma: no cover - fallback implementations may ignore labels
        return Counter(name, documentation, **kwargs)  # type: ignore[misc]


def _init_metrics(*, registry: CollectorRegistry | None = None) -> None:
    """Initialise or reset Prometheus counters used by the auth service."""

    global _METRICS_REGISTRY
    global _LOGIN_FAILURE_COUNTER
    global _MFA_DENIED_COUNTER
    global _LOGIN_SUCCESS_COUNTER

    if registry is None:
        shared_registry: CollectorRegistry | None
        try:
            from metrics import _REGISTRY as shared_registry  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover - metrics may be unavailable in minimal envs
            shared_registry = None

        registry = shared_registry

    _METRICS_REGISTRY = registry

    _LOGIN_FAILURE_COUNTER = _build_counter(
        "auth_login_failures_total",
        "Number of failed administrator authentication attempts.",
        ("reason",),
    )
    _MFA_DENIED_COUNTER = _build_counter(
        "auth_mfa_denied_total",
        "Number of administrator logins denied due to MFA.",
    )
    _LOGIN_SUCCESS_COUNTER = _build_counter(
        "auth_login_success_total",
        "Number of successful administrator logins.",
    )


_init_metrics()



def _password_needs_update(stored_hash: str) -> bool:
    """Determine whether the stored hash should be upgraded."""

    check_rehash = getattr(_ARGON2_HASHER, "check_needs_rehash", None)
    if callable(check_rehash):
        try:
            return bool(check_rehash(stored_hash))
        except (InvalidHash, AttributeError):
            return False

    needs_update = getattr(_ARGON2_HASHER, "needs_update", None)
    if callable(needs_update):
        try:
            return bool(needs_update(stored_hash))
        except (InvalidHash, AttributeError):
            return False

    return False


@dataclass
class AdminAccount:
    admin_id: str
    email: str
    password_hash: str
    mfa_secret: str
    allowed_ips: Optional[Set[str]] = None



@runtime_checkable
class AdminRepositoryProtocol(Protocol):
    """Minimal interface for persisting administrator accounts."""

    def add(self, admin: AdminAccount) -> None:  # pragma: no cover - Protocol definition
        ...

    def delete(self, email: str) -> None:  # pragma: no cover - Protocol definition
        ...

    def get_by_email(self, email: str) -> Optional[AdminAccount]:  # pragma: no cover - Protocol definition
        ...


class InMemoryAdminRepository(AdminRepositoryProtocol):

    """Simple in-memory repository for administrator accounts."""

    def __init__(self) -> None:
        self._admins: Dict[str, AdminAccount] = {}

    def add(self, admin: AdminAccount) -> None:
        self._admins[admin.email] = admin

    def delete(self, email: str) -> None:
        self._admins.pop(email, None)

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


@runtime_checkable
class SessionStoreProtocol(Protocol):
    """Abstraction for storing login sessions."""

    def create(self, admin_id: str) -> Session:  # pragma: no cover - Protocol definition
        ...

    def get(self, token: str) -> Optional[Session]:  # pragma: no cover - Protocol definition
        ...


def _generate_session_token() -> str:
    raw = os.urandom(32)
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


class InMemorySessionStore(SessionStoreProtocol):
    """Tracks authenticated admin sessions in memory."""

    def __init__(self, ttl_minutes: int = 60) -> None:
        self._sessions: Dict[str, Session] = {}
        self._ttl = timedelta(minutes=ttl_minutes)

    def create(self, admin_id: str) -> Session:
        now = datetime.now(timezone.utc)
        token = _generate_session_token()
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


class PostgresAdminRepository(AdminRepositoryProtocol):
    """PostgreSQL-backed implementation of the admin repository."""

    def __init__(self, dsn: str, *, psycopg_module=None) -> None:
        if psycopg_module is None:  # pragma: no cover - executed in production
            import psycopg

            psycopg_module = psycopg

        self._dsn = dsn
        self._psycopg = psycopg_module
        self._ensure_schema()

    def _connect(self):
        return self._psycopg.connect(self._dsn)

    def _ensure_schema(self) -> None:
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS admin_accounts (
                            email TEXT PRIMARY KEY,
                            admin_id TEXT NOT NULL,
                            password_hash TEXT NOT NULL,
                            mfa_secret TEXT NOT NULL,
                            allowed_ips TEXT
                        )
                        """
                    )
                conn.commit()
        except Exception:  # pragma: no cover - defensive logging for production observability
            logger.exception("Failed to ensure admin account schema is present")
            raise

    def add(self, admin: AdminAccount) -> None:
        allowed_ips = None
        if admin.allowed_ips:
            allowed_ips = json.dumps(sorted(admin.allowed_ips))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO admin_accounts (email, admin_id, password_hash, mfa_secret, allowed_ips)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (email) DO UPDATE
                    SET admin_id = EXCLUDED.admin_id,
                        password_hash = EXCLUDED.password_hash,
                        mfa_secret = EXCLUDED.mfa_secret,
                        allowed_ips = EXCLUDED.allowed_ips
                    """,
                    (
                        admin.email,
                        admin.admin_id,
                        admin.password_hash,
                        admin.mfa_secret,
                        allowed_ips,
                    ),
                )
            conn.commit()

    def delete(self, email: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM admin_accounts WHERE email = %s",
                    (email,),
                )
            conn.commit()

    def get_by_email(self, email: str) -> Optional[AdminAccount]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT email, admin_id, password_hash, mfa_secret, allowed_ips
                    FROM admin_accounts
                    WHERE email = %s
                    LIMIT 1
                    """,
                    (email,),
                )
                row = cur.fetchone()
        if not row:
            return None
        allowed_ips_raw = row[4]
        allowed_ips: Optional[Set[str]]
        if allowed_ips_raw:
            allowed_ips = set(json.loads(allowed_ips_raw))
        else:
            allowed_ips = None
        return AdminAccount(
            admin_id=row[1],
            email=row[0],
            password_hash=row[2],
            mfa_secret=row[3],
            allowed_ips=allowed_ips,
        )


class RedisSessionStore(SessionStoreProtocol):
    """Redis-backed implementation for persistent sessions."""

    def __init__(self, redis_client, ttl_minutes: int = 60) -> None:
        self._redis = redis_client
        self._ttl = timedelta(minutes=ttl_minutes)

    def create(self, admin_id: str) -> Session:
        now = datetime.now(timezone.utc)
        token = _generate_session_token()
        expires_at = now + self._ttl
        payload = {
            "token": token,
            "admin_id": admin_id,
            "created_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
        serialized = json.dumps(payload)
        ttl_seconds = int(self._ttl.total_seconds())
        self._redis.setex(token, ttl_seconds, serialized)
        return Session(
            token=token,
            admin_id=admin_id,
            created_at=now,
            expires_at=expires_at,
        )

    def get(self, token: str) -> Optional[Session]:
        data = self._redis.get(token)
        if data is None:
            return None
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        payload = json.loads(data)
        created_at = datetime.fromisoformat(payload["created_at"])
        expires_at = datetime.fromisoformat(payload["expires_at"])
        session = Session(
            token=payload["token"],
            admin_id=payload["admin_id"],
            created_at=created_at,
            expires_at=expires_at,
        )
        if not session.is_active:
            try:
                self._redis.delete(token)
            except AttributeError:  # pragma: no cover - fall back for minimal stubs
                pass
            return None
        return session


def build_session_store_from_url(redis_url: str, *, ttl_minutes: int = 60) -> SessionStoreProtocol:
    """Create a session store backed by Redis or a deterministic in-memory stub."""

    client, used_stub = create_redis_from_url(redis_url, decode_responses=True, logger=logger)
    if used_stub:
        logger.warning(
            "Redis dependency unavailable; using in-memory session store stub for %s", redis_url
        )
    return RedisSessionStore(client, ttl_minutes=ttl_minutes)


# Backwards-compatible aliases for legacy imports
AdminRepository = InMemoryAdminRepository
SessionStore = InMemorySessionStore


class AuthService:
    """Authenticates admins and enforces MFA and IP allow-listing."""

    def __init__(
        self,
        repository: AdminRepositoryProtocol,
        sessions: SessionStoreProtocol,
    ) -> None:
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
        stored_hash = admin.password_hash

        try:
            if _ARGON2_HASHER.verify(stored_hash, password):
                if _password_needs_update(stored_hash):
                    admin.password_hash = hash_password(password)
                    self._repository.add(admin)
                return True
        except VerifyMismatchError:
            return False
        except InvalidHash:
            pass
        except VerificationError:
            return False


        # Backwards compatibility with legacy SHA-256 hashes.
        candidate = hashlib.sha256(password.encode()).hexdigest()
        if hmac.compare_digest(candidate, stored_hash):
            # Upgrade legacy hash on successful login.
            admin.password_hash = hash_password(password)
            self._repository.add(admin)
            return True
        return False

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
    return _ARGON2_HASHER.hash(password)


__all__ = [
    "AdminAccount",
    "AdminRepositoryProtocol",
    "InMemoryAdminRepository",
    "PostgresAdminRepository",
    "AdminRepository",
    "Session",
    "SessionStoreProtocol",
    "InMemorySessionStore",
    "RedisSessionStore",
    "build_session_store_from_url",
    "SessionStore",
    "AuthService",
    "hash_password",
]
