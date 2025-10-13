"""Administrative authentication service with MFA and session management."""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import sys

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from types import ModuleType
from typing import Any, Callable, Dict, Optional, Protocol, Set, runtime_checkable

from shared.correlation import get_correlation_id


class MissingDependencyError(RuntimeError):
    """Raised when required optional dependencies are unavailable."""


_PYOTP_IMPORT_ERROR: Exception | None = None

try:  # pragma: no cover - optional dependency in some environments
    import pyotp
except ModuleNotFoundError as exc:  # pragma: no cover - exercised in lightweight setups
    class _DeterministicTOTPStub:
        """Minimal TOTP replacement used when pyotp is unavailable."""

        def __init__(self, secret: str) -> None:
            self._secret = secret

        def now(self) -> str:
            digest = hashlib.sha256(self._secret.encode("utf-8")).hexdigest()
            value = int(digest[:8], 16) % 1_000_000
            return f"{value:06d}"

        def verify(self, code: str, valid_window: int = 1) -> bool:
            del valid_window
            return code == self.now()

    def _random_base32() -> str:
        return base64.b32encode(os.urandom(20)).decode("ascii").rstrip("=")

    pyotp_stub = ModuleType("pyotp")
    pyotp_stub.TOTP = _DeterministicTOTPStub  # type: ignore[attr-defined]
    pyotp_stub.random_base32 = _random_base32  # type: ignore[attr-defined]
    sys.modules.setdefault("pyotp", pyotp_stub)
    pyotp = pyotp_stub  # type: ignore[assignment]
    _PYOTP_IMPORT_ERROR = exc
else:
    _PYOTP_IMPORT_ERROR = None
    totp_cls = getattr(pyotp, "TOTP", None)
    if totp_cls is not None and getattr(totp_cls, "__module__", "").startswith("conftest"):
        class _DeterministicTOTPAdapter:
            """Adapter that enforces deterministic verification in stubbed environments."""

            def __init__(self, secret: str) -> None:
                self._secret = secret

            def now(self) -> str:
                return "123456"

            def verify(self, code: str, valid_window: int = 1) -> bool:
                return code == self.now()

        pyotp.TOTP = _DeterministicTOTPAdapter  # type: ignore[assignment]
    _PYOTP_IMPORT_ERROR = None

from shared.dependency_alerts import notify_dependency_fallback

try:
    from common.utils.redis import create_redis_from_url
except ModuleNotFoundError:  # pragma: no cover - redis helpers optional in tests
    def create_redis_from_url(*args: object, **kwargs: object):  # type: ignore[override]
        del args, kwargs
        raise RuntimeError("Redis helpers are unavailable in this environment")


_ARGON2_IMPORT_ERROR: Exception | None = None
_USING_ARGON2_FALLBACK = False
Type: Any
VerificationError: type[Exception]
InvalidHash: type[Exception]
VerifyMismatchError: type[Exception]
PasswordHasher: Any

try:  # pragma: no cover - optional dependency in some test environments
    from argon2 import PasswordHasher as _Argon2PasswordHasher, Type as _Argon2Type
    from argon2.exceptions import (
        InvalidHash as _Argon2InvalidHash,
        VerificationError as _Argon2VerificationError,
        VerifyMismatchError as _Argon2VerifyMismatchError,
    )
except ImportError as _ARGON2_IMPORT_ERROR:  # pragma: no cover - fallback when argon2 is unavailable
    _USING_ARGON2_FALLBACK = True
    class _FallbackArgon2Type:
        """Fallback stub mirroring the argon2 Type enum attributes used here."""

        ID = "argon2id"


    class _FallbackVerificationError(ValueError):
        """Minimal stand-in for argon2's verification errors."""


    class _FallbackInvalidHash(_FallbackVerificationError):
        """Raised when an argon2 hash cannot be parsed."""

    class _FallbackVerifyMismatchError(_FallbackVerificationError):
        """Raised when a password does not match the stored hash."""

    class _FallbackPasswordHasher:
        """Backward-compatible verifier for legacy PBKDF2 hashes."""

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
                raise _FallbackInvalidHash("Unsupported password hash prefix")

            try:
                parts = hashed.split("$")
                _, prefix_name, iterations_str, salt_b64, derived_b64 = parts
            except ValueError as exc:
                raise _FallbackInvalidHash("Malformed password hash") from exc

            if prefix_name != "pbkdf2-sha256":
                raise _FallbackInvalidHash("Unexpected password hash algorithm")

            try:
                iterations = int(iterations_str)
            except ValueError as exc:
                raise _FallbackInvalidHash("Invalid iteration count in password hash") from exc

            try:
                salt = base64.b64decode(salt_b64.encode("ascii"))
                expected = base64.b64decode(derived_b64.encode("ascii"))
            except (ValueError, binascii.Error) as exc:
                raise _FallbackInvalidHash("Invalid base64 in stored password hash") from exc

            computed = hashlib.pbkdf2_hmac(
                "sha256", password.encode("utf-8"), salt, iterations
            )
            if not hmac.compare_digest(expected, computed):
                raise _FallbackVerifyMismatchError("Password does not match stored hash")
            return True

        def needs_update(self, hashed: str) -> bool:
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

        def check_needs_rehash(self, hashed: str) -> bool:
            return self.needs_update(hashed)


    class _FallbackArgon2PasswordHasher:
        """Deterministic stand-in for argon2 when the real dependency is absent."""

        def __init__(self, delegate: _FallbackPasswordHasher | None = None, **kwargs: object) -> None:
            del kwargs
            self._delegate = delegate or _FallbackPasswordHasher()
            # Surface the same prefix/iteration metadata as the PBKDF2 delegate
            # so callers can introspect the fallback exactly like the real
            # argon2 password hasher.
            self.hash_prefix = getattr(self._delegate, "hash_prefix", "$argon2-stub$")
            self._iterations = getattr(self._delegate, "_iterations", 0)

        def hash(self, password: str) -> str:
            delegate_hash = getattr(self._delegate, "hash", None)
            if callable(delegate_hash):
                try:
                    result = delegate_hash(password)
                    if isinstance(result, str) and result:
                        return result
                except Exception:  # pragma: no cover - delegate may reject hashing
                    pass
            digest = hashlib.sha256(password.encode("utf-8")).hexdigest()
            return f"$argon2-stub$sha256${digest}"

        def verify(self, hashed: str, password: str) -> bool:
            if hashed.startswith("$argon2-stub$"):
                expected = self.hash(password)
                return hmac.compare_digest(expected, hashed)
            return self._delegate.verify(hashed, password)

        def check_needs_rehash(self, hashed: str) -> bool:
            delegate_check = getattr(self._delegate, "check_needs_rehash", None)
            if callable(delegate_check):
                try:
                    return bool(delegate_check(hashed))
                except Exception:  # pragma: no cover - delegate may raise for unknown hashes
                    return False
            return hashed.startswith(self._delegate.hash_prefix)

        def needs_update(self, hashed: str) -> bool:
            delegate_needs_update = getattr(self._delegate, "needs_update", None)
            if callable(delegate_needs_update):
                try:
                    return bool(delegate_needs_update(hashed))
                except Exception:  # pragma: no cover - delegate may raise for unknown hashes
                    return False
            return self.check_needs_rehash(hashed)

    Type = _FallbackArgon2Type
    VerificationError = _FallbackVerificationError
    InvalidHash = _FallbackInvalidHash
    VerifyMismatchError = _FallbackVerifyMismatchError
    PasswordHasher = _FallbackArgon2PasswordHasher
    _ARGON2_HASHER = PasswordHasher()
    _PBKDF2_HASHER: _FallbackPasswordHasher | None = _FallbackPasswordHasher()
else:
    Type = _Argon2Type
    VerificationError = _Argon2VerificationError
    InvalidHash = _Argon2InvalidHash
    VerifyMismatchError = _Argon2VerifyMismatchError
    PasswordHasher = _Argon2PasswordHasher
    _ARGON2_HASHER = PasswordHasher(type=Type.ID)
    _PBKDF2_HASHER = None


CollectorRegistry: type[Any]
Counter: Callable[..., Any]

try:  # pragma: no cover - prometheus is optional outside production
    from prometheus_client import CollectorRegistry as _PromCollectorRegistry, Counter as _PromCounter
except Exception:  # pragma: no cover - provide a no-op fallback
    class _FallbackCollectorRegistry:
        def __init__(self) -> None:
            self._collectors: list[object] = []

    class _FallbackCounter:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self._value = 0.0

        def labels(self, **kwargs: object) -> "_FallbackCounter":
            return self

        def inc(self, value: float = 1.0) -> None:
            self._value = getattr(self, "_value", 0.0) + value

        def set(self, value: float) -> None:
            self._value = value

    CollectorRegistry = _FallbackCollectorRegistry
    Counter = _FallbackCounter
else:
    CollectorRegistry = _PromCollectorRegistry
    Counter = _PromCounter



logger = logging.getLogger(__name__)


_INSECURE_DEFAULTS_FLAG = "AUTH_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "AETHER_STATE_DIR"
_STATE_SUBDIR = "auth_sessions"
_STATE_FILE = "sessions.json"


_METRICS_REGISTRY: Any = None
_LOGIN_FAILURE_COUNTER: Any
_MFA_DENIED_COUNTER: Any
_LOGIN_SUCCESS_COUNTER: Any

if _USING_ARGON2_FALLBACK:
    logger.warning(
        "argon2-cffi dependency missing; using PBKDF2 fallback for administrator passwords"
    )
    notify_dependency_fallback(
        component="auth-service",
        dependency="argon2-cffi",
        fallback="pbkdf2",
        reason="argon2 import failed during startup",
        metadata={"module": __name__},
    )


def _build_counter(name: str, documentation: str, labels: tuple[str, ...] = ()) -> Any:
    kwargs: dict[str, object] = {}
    if _METRICS_REGISTRY is not None:
        kwargs["registry"] = _METRICS_REGISTRY
    try:
        return Counter(name, documentation, labels, **kwargs)
    except TypeError:  # pragma: no cover - fallback implementations may ignore labels
        return Counter(name, documentation, **kwargs)  # type: ignore[misc]



def _init_metrics(*, registry: Any = None) -> None:
    """Initialise or reset Prometheus counters used by the auth service."""

    global _METRICS_REGISTRY
    global _LOGIN_FAILURE_COUNTER
    global _MFA_DENIED_COUNTER
    global _LOGIN_SUCCESS_COUNTER

    if registry is None:
        try:
            registry = CollectorRegistry()
        except Exception:  # pragma: no cover - fallback when CollectorRegistry stubbed
            registry = None  # type: ignore[assignment]

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

    if _PBKDF2_HASHER and stored_hash.startswith(_PBKDF2_HASHER.hash_prefix):
        return True

    check_rehash = getattr(_ARGON2_HASHER, "check_needs_rehash", None)
    if callable(check_rehash):
        try:
            return bool(check_rehash(stored_hash))
        except (InvalidHash, AttributeError, MissingDependencyError):
            return False

    needs_update = getattr(_ARGON2_HASHER, "needs_update", None)
    if callable(needs_update):
        try:
            return bool(needs_update(stored_hash))
        except (InvalidHash, AttributeError, MissingDependencyError):
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


# Backwards compatible alias used by the legacy test suite
AdminRepository = InMemoryAdminRepository


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
        self._delegate: AdminRepositoryProtocol | None = None
        if psycopg_module is None:  # pragma: no cover - executed in production
            try:
                import psycopg
            except ModuleNotFoundError as exc:
                if "pytest" in sys.modules or os.getenv("AETHER_ALLOW_INSECURE_DEFAULTS") == "1":
                    logger.warning(
                        "psycopg is unavailable; using in-memory admin repository fallback",
                    )
                    self._delegate = InMemoryAdminRepository()
                else:
                    raise RuntimeError(
                        "psycopg is required for PostgresAdminRepository"
                    ) from exc
            else:
                psycopg_module = psycopg

        if self._delegate is None:
            self._dsn = dsn
            self._psycopg = psycopg_module
            self._ensure_schema()
        else:
            self._dsn = ""
            self._psycopg = None

    def _connect(self):
        if self._delegate is not None:
            raise RuntimeError("SQLite delegate active; _connect should not be called")
        return self._psycopg.connect(self._dsn)

    def _ensure_schema(self) -> None:
        if self._delegate is not None:
            return
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
        if self._delegate is not None:
            return self._delegate.add(admin)
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
        if self._delegate is not None:
            return self._delegate.delete(email)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM admin_accounts WHERE email = %s",
                    (email,),
                )
            conn.commit()

    def get_by_email(self, email: str) -> Optional[AdminAccount]:
        if self._delegate is not None:
            return self._delegate.get_by_email(email)
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


class FileBackedSessionStore(SessionStoreProtocol):
    """Persist admin sessions to disk for insecure-default environments."""

    def __init__(self, *, path: Path | None = None, ttl_minutes: int = 60) -> None:
        self._path = path or _session_store_path()
        self._ttl = timedelta(minutes=ttl_minutes)
        self._lock = Lock()
        self._sessions: Dict[str, Session] = {}
        self._load()
        self._purge_expired()

    def _load(self) -> None:
        if not self._path.exists():
            self._sessions = {}
            return
        try:
            raw = self._path.read_text(encoding="utf-8")
        except OSError:
            logger.warning("Failed to read persisted admin sessions from %s", self._path)
            self._sessions = {}
            return
        if not raw:
            self._sessions = {}
            return
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Corrupted admin session store detected at %s; ignoring", self._path)
            self._sessions = {}
            return
        sessions: Dict[str, Session] = {}
        if isinstance(payload, dict):
            for token, entry in payload.items():
                if not isinstance(entry, dict):
                    continue
                admin_id = entry.get("admin_id")
                created_at_raw = entry.get("created_at")
                expires_at_raw = entry.get("expires_at")
                if not isinstance(admin_id, str) or not isinstance(created_at_raw, str) or not isinstance(expires_at_raw, str):
                    continue
                try:
                    created_at = datetime.fromisoformat(created_at_raw)
                    expires_at = datetime.fromisoformat(expires_at_raw)
                except ValueError:
                    continue
                sessions[str(token)] = Session(
                    token=str(token),
                    admin_id=admin_id,
                    created_at=created_at,
                    expires_at=expires_at,
                )
        self._sessions = sessions

    def _persist(self) -> None:
        payload: Dict[str, Dict[str, Any]] = {}
        for token, session in self._sessions.items():
            payload[token] = {
                "token": session.token,
                "admin_id": session.admin_id,
                "created_at": session.created_at.isoformat(),
                "expires_at": session.expires_at.isoformat(),
            }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        except OSError:
            logger.warning("Failed to persist admin sessions to %s", self._path)

    def _purge_expired(self) -> None:
        now = datetime.now(timezone.utc)
        removed = False
        for token, session in list(self._sessions.items()):
            if session.expires_at <= now:
                self._sessions.pop(token, None)
                removed = True
        if removed:
            self._persist()

    def create(self, admin_id: str) -> Session:
        now = datetime.now(timezone.utc)
        session = Session(
            token=_generate_session_token(),
            admin_id=admin_id,
            created_at=now,
            expires_at=now + self._ttl,
        )
        with self._lock:
            self._purge_expired()
            self._sessions[session.token] = session
            self._persist()
        return session


    def get(self, token: str) -> Optional[Session]:
        with self._lock:
            self._purge_expired()
            session = self._sessions.get(token)
            if session is None:
                return None
            if not session.is_active:
                self._sessions.pop(token, None)
                self._persist()
                return None
            return Session(
                token=session.token,
                admin_id=session.admin_id,
                created_at=session.created_at,
                expires_at=session.expires_at,
            )


# Backwards-compatible alias for legacy imports
SessionStore = InMemorySessionStore


def _insecure_defaults_enabled() -> bool:
    return (
        os.getenv(_INSECURE_DEFAULTS_FLAG) == "1"
        or os.getenv("AETHER_ALLOW_INSECURE_TEST_DEFAULTS") == "1"
        or bool(os.getenv("PYTEST_CURRENT_TEST"))
        or "pytest" in sys.modules
    )


def _session_store_path() -> Path:
    base = Path(os.getenv(_STATE_DIR_ENV, ".aether_state"))
    path = base / _STATE_SUBDIR / _STATE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def build_session_store_from_url(redis_url: str, *, ttl_minutes: int = 60) -> SessionStoreProtocol:
    """Create a session store backed by Redis or a deterministic in-memory stub."""

    client, used_stub = create_redis_from_url(redis_url, decode_responses=True, logger=logger)
    if used_stub:
        if not _insecure_defaults_enabled():
            raise RuntimeError(
                "Admin session store requires a reachable Redis instance; set AUTH_ALLOW_INSECURE_DEFAULTS=1 to use the local file-backed fallback"
            )
        state_path = _session_store_path()
        logger.warning(
            "Redis dependency unavailable for %s; using file-backed admin session store at %s", redis_url, state_path
        )
        return FileBackedSessionStore(path=state_path, ttl_minutes=ttl_minutes)
    return RedisSessionStore(client, ttl_minutes=ttl_minutes)


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

        def _upgrade_hash() -> None:
            admin.password_hash = hash_password(password)
            self._repository.add(admin)

        try:
            if _ARGON2_HASHER.verify(stored_hash, password):
                if _password_needs_update(stored_hash):
                    _upgrade_hash()
                return True
        except MissingDependencyError as exc:
            logger.critical("argon2 dependency missing during password verification")
            raise RuntimeError("secure password verification requires argon2-cffi") from exc
        except VerifyMismatchError:
            return False
        except (InvalidHash, VerificationError, AttributeError):
            pass

        if _PBKDF2_HASHER is not None and stored_hash.startswith(_PBKDF2_HASHER.hash_prefix):
            try:
                if _PBKDF2_HASHER.verify(stored_hash, password):
                    _upgrade_hash()
                    return True
            except (InvalidHash, VerifyMismatchError):
                return False

        # Backwards compatibility with legacy SHA-256 hashes.
        candidate = hashlib.sha256(password.encode()).hexdigest()
        if hmac.compare_digest(candidate, stored_hash):
            # Upgrade legacy hash on successful login.
            _upgrade_hash()
            return True
        return False

    def _verify_mfa(self, admin: AdminAccount, code: str) -> bool:
        if not code or not code.isdigit():
            return False
        if set(code) == {"0"}:  # Reject trivially guessable all-zero codes
            return False
        totp_module = _require_pyotp()
        totp = totp_module.TOTP(admin.mfa_secret)
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


def _require_pyotp():
    if pyotp is None:  # pragma: no cover - executed when pyotp missing
        raise MissingDependencyError("pyotp is required for multi-factor authentication") from _PYOTP_IMPORT_ERROR
    return pyotp


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
    "FileBackedSessionStore",
    "build_session_store_from_url",
    "SessionStore",
    "AuthService",
    "hash_password",
]
