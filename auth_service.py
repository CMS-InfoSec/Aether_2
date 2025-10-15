"""FastAPI authentication service implementing SSO, MFA, and Builder.io Fusion integration.

This service exposes a single endpoint ``POST /auth/login`` which performs the following
workflow:

* Exchanges an OAuth2 authorization code for tokens with Microsoft Entra ID or Google
  Identity using OIDC discovery metadata.
* Enforces multi-factor authentication (MFA) with support for TOTP codes and SMS codes.
* Issues a short-lived JWT using the caller's resolved role for downstream services.
* Persists login sessions to the ``auth_sessions`` table for auditing and analytics.
* Returns Builder.io Fusion specific context needed by the frontend to complete the
  authentication flow.

Environment configuration
-------------------------

The service expects a number of environment variables so it can operate in different
deployments without code changes:

``MICROSOFT_OIDC_DISCOVERY``
    Discovery document URL for Microsoft Entra ID. Defaults to the common tenant.
``MICROSOFT_CLIENT_ID`` / ``MICROSOFT_CLIENT_SECRET``
    Credentials for the Microsoft OAuth2 application.
``GOOGLE_OIDC_DISCOVERY``
    Discovery document URL for Google Identity.
``GOOGLE_CLIENT_ID`` / ``GOOGLE_CLIENT_SECRET``
    Credentials for the Google OAuth2 client.
``AUTH_JWT_SECRET``
    Symmetric secret used to sign issued JWTs.
``AUTH_JWT_TTL_SECONDS``
    Optional override for the JWT expiration (defaults to one hour).
``MFA_TOTP_SECRETS``
    JSON object mapping user identifiers (email/subject) to their TOTP shared secret.
``MFA_SMS_STATIC_CODES``
    JSON object mapping user identifiers to the out-of-band SMS code currently accepted.
``AUTH_DATABASE_URL``
    SQLAlchemy connection string for persisting sessions (defaults to SQLite file).
``BUILDER_FUSION_SPACE_ID`` / ``BUILDER_FUSION_ENVIRONMENT``
    Metadata returned to the frontend so Builder.io Fusion can complete login.

The implementation focuses on clarity and testability. It caches OIDC discovery
metadata, isolates MFA verification strategies, and uses a minimal, dependency-free JWT
encoder to avoid adding new packages to the environment.
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict, Literal, Mapping, Optional

from contextlib import asynccontextmanager
from types import SimpleNamespace

from shared.common_bootstrap import ensure_httpx_ready

httpx = ensure_httpx_ready()

try:  # pragma: no cover - optional dependency used in production
    import pyotp
except ImportError:  # pragma: no cover - exercised in unit-only environments
    class _MissingPyOTP(SimpleNamespace):
        def __getattr__(self, name: str) -> Any:
            raise RuntimeError("pyotp is required for auth_service MFA operations")

    pyotp = _MissingPyOTP()

try:  # pragma: no cover - prefer the real FastAPI implementation
    from fastapi import Depends, FastAPI, HTTPException, Request, status
    from fastapi.concurrency import run_in_threadpool
    from fastapi.middleware.cors import CORSMiddleware
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[misc]
        CORSMiddleware,
        Depends,
        FastAPI,
        HTTPException,
        Request,
        run_in_threadpool,
        status,
    )
from pydantic import BaseModel, Field, HttpUrl

_SQLALCHEMY_AVAILABLE = True

try:  # pragma: no cover - optional dependency used in production
    from sqlalchemy import Boolean, Column, DateTime, String, create_engine
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.url import make_url
    from sqlalchemy.orm import Session as OrmSession
    from sqlalchemy.orm import declarative_base, sessionmaker
except ImportError:  # pragma: no cover - exercised in unit-only environments
    _SQLALCHEMY_AVAILABLE = False
    import sqlite3
    from pathlib import Path
    from urllib.parse import urlparse

    Engine = Any  # type: ignore[assignment]
    OrmSession = Any  # type: ignore[assignment]

    class _URL:
        def __init__(self, raw: str) -> None:
            self._raw = raw
            self._parsed = urlparse(raw)

        def get_backend_name(self) -> str:
            base, _, _driver = self._parsed.scheme.partition("+")
            return base or self._parsed.scheme

        @property
        def query(self) -> str:
            return self._parsed.query

    def make_url(url: str) -> _URL:  # type: ignore[override]
        return _URL(url)

    def _resolve_sqlite_path(url: str) -> str:
        parsed = urlparse(url)
        base, _, _driver = parsed.scheme.partition("+")
        if base not in {"sqlite"}:
            raise RuntimeError("Only sqlite URLs are supported without SQLAlchemy installed")

        raw_path = f"{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path
        if raw_path.startswith("//"):
            raw_path = raw_path[1:]

        if raw_path in {"", "/"}:
            return ":memory:"

        if raw_path.startswith("/"):
            resolved = Path(raw_path)
        else:
            resolved = Path(raw_path.lstrip("/"))
            if not resolved.is_absolute():
                resolved = Path.cwd() / resolved

        if str(resolved) != ":memory:":
            resolved.parent.mkdir(parents=True, exist_ok=True)

        return str(resolved)

    class Column:  # type: ignore[override]
        def __init__(
            self,
            column_type: Any,
            primary_key: bool = False,
            nullable: bool = True,
            default: Any | None = None,
            index: bool = False,
            **__: Any,
        ) -> None:
            if isinstance(column_type, type):
                try:
                    column_type = column_type()
                except Exception:
                    column_type = column_type  # type: ignore[assignment]
            self.type = column_type
            self.primary_key = primary_key
            self.nullable = nullable
            self.default = default
            self.index = index

    class Boolean:  # type: ignore[override]
        sqlite_type = "INTEGER"

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args
            self.kwargs = kwargs

    class DateTime:  # type: ignore[override]
        sqlite_type = "TEXT"

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args
            self.kwargs = kwargs

    class String:  # type: ignore[override]
        sqlite_type = "TEXT"

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args
            self.kwargs = kwargs

    class _Metadata:
        def __init__(self) -> None:
            self.schema: str | None = None
            self._models: list[type[Any]] = []

        def _register(self, model: type[Any]) -> None:
            if model not in self._models:
                self._models.append(model)

        def create_all(self, *, bind: "_SQLiteEngine" | None = None) -> None:
            if bind is None:
                return
            for model in self._models:
                bind._create_table(model)

    def declarative_base() -> type[Any]:  # type: ignore[override]
        metadata_obj = _Metadata()

        class _Base:
            metadata = metadata_obj

            def __init_subclass__(cls, **kwargs: Any) -> None:
                super().__init_subclass__(**kwargs)
                columns: dict[str, Column] = {}
                primary: str | None = None
                for name, value in cls.__dict__.items():
                    if isinstance(value, Column):
                        columns[name] = value
                        if value.primary_key and primary is None:
                            primary = name
                cls.__columns__ = columns  # type: ignore[attr-defined]
                cls.__primary_key__ = primary  # type: ignore[attr-defined]
                if not hasattr(cls, "__table__"):
                    cls.__table__ = SimpleNamespace(schema=None)
                metadata_obj._register(cls)

            def __init__(self, **kwargs: Any) -> None:
                for name, column in getattr(self, "__columns__", {}).items():
                    if name in kwargs:
                        value = kwargs[name]
                    elif column.default is not None:
                        value = column.default() if callable(column.default) else column.default
                    else:
                        value = None
                    setattr(self, name, value)

        return _Base

    class _SQLiteEngine:
        def __init__(self, url: str, **__: Any) -> None:
            self.url = url
            self.database = _resolve_sqlite_path(url)

        def dispose(self) -> None:
            return None

        def _connect(self) -> sqlite3.Connection:
            return sqlite3.connect(self.database)

        def _column_sql(self, name: str, column: Column) -> str:
            type_hint = getattr(column.type, "sqlite_type", "TEXT")
            parts = [name, type_hint]
            if column.primary_key:
                parts.append("PRIMARY KEY")
            if not column.nullable:
                parts.append("NOT NULL")
            return " ".join(parts)

        def _create_table(self, model: type[Any]) -> None:
            table = getattr(model, "__tablename__", None)
            if not table:
                return
            columns: dict[str, Column] = getattr(model, "__columns__", {})
            statements = [self._column_sql(name, column) for name, column in columns.items()]
            if not statements:
                return
            ddl = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(statements)})"
            with self._connect() as conn:
                conn.execute(ddl)
                for name, column in columns.items():
                    if column.index and not column.primary_key:
                        conn.execute(
                            f"CREATE INDEX IF NOT EXISTS idx_{table}_{name} ON {table} ({name})"
                        )
                conn.commit()

        def _serialise(self, column: Column, value: Any) -> Any:
            if value is None:
                return None
            if isinstance(column.type, DateTime) and isinstance(value, datetime):
                return value.isoformat()
            if isinstance(column.type, Boolean):
                return int(bool(value))
            return value

        def _fetch_row(self, model: type[Any], key: Any) -> sqlite3.Row | None:
            table = getattr(model, "__tablename__", None)
            primary = getattr(model, "__primary_key__", None)
            if not table or not primary:
                return None
            columns: dict[str, Column] = getattr(model, "__columns__", {})
            query = f"SELECT {', '.join(columns.keys())} FROM {table} WHERE {primary} = ?"
            with self._connect() as conn:
                conn.row_factory = sqlite3.Row
                return conn.execute(query, (key,)).fetchone()

        def _commit(self, instances: list[Any]) -> None:
            if not instances:
                return
            with self._connect() as conn:
                for instance in instances:
                    model = type(instance)
                    table = getattr(model, "__tablename__", None)
                    if not table:
                        continue
                    columns: dict[str, Column] = getattr(model, "__columns__", {})
                    names = []
                    values = []
                    placeholders = []
                    for name, column in columns.items():
                        names.append(name)
                        placeholders.append("?")
                        current = getattr(instance, name, None)
                        if current is None and column.default is not None:
                            current = column.default() if callable(column.default) else column.default
                            setattr(instance, name, current)
                        values.append(self._serialise(column, current))
                    sql = f"INSERT INTO {table} ({', '.join(names)}) VALUES ({', '.join(placeholders)})"
                    conn.execute(sql, values)
                conn.commit()

        def _refresh(self, instance: Any) -> bool:
            model = type(instance)
            primary = getattr(model, "__primary_key__", None)
            if not primary:
                return False
            key = getattr(instance, primary, None)
            if key is None:
                return False
            row = self._fetch_row(model, key)
            if row is None:
                return False
            columns: dict[str, Column] = getattr(model, "__columns__", {})
            for name, column in columns.items():
                value = row[name]
                if isinstance(column.type, DateTime) and isinstance(value, str):
                    try:
                        value = datetime.fromisoformat(value)
                    except ValueError:
                        pass
                elif isinstance(column.type, Boolean):
                    value = bool(value)
                setattr(instance, name, value)
            return True

    Engine = _SQLiteEngine  # type: ignore[assignment]

    class _SQLiteSession:
        def __init__(self, engine: _SQLiteEngine) -> None:
            self._engine = engine
            self._pending: list[Any] = []

        def __enter__(self) -> "_SQLiteSession":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            if exc_type:
                self._pending.clear()
            return False

        def add(self, instance: Any) -> None:
            self._pending.append(instance)

        def commit(self) -> None:
            self._engine._commit(self._pending)
            self._pending.clear()

        def refresh(self, instance: Any) -> None:
            self._engine._refresh(instance)

        def close(self) -> None:
            self._pending.clear()

        def get(self, model: type[Any], primary_key: Any) -> Any | None:
            primary = getattr(model, "__primary_key__", None)
            if primary is None:
                return None
            instance = model(**{primary: primary_key})
            found = self._engine._refresh(instance)
            if not found:
                return None
            return instance

    OrmSession = _SQLiteSession  # type: ignore[assignment]

    def create_engine(url: str, **kwargs: Any) -> _SQLiteEngine:  # type: ignore[override]
        return _SQLiteEngine(url, **kwargs)

    def sessionmaker(*, bind: _SQLiteEngine, **__: Any):  # type: ignore[override]
        engine = bind

        class _SessionFactory:
            def __call__(self) -> _SQLiteSession:
                return _SQLiteSession(engine)

            def close_all(self) -> None:
                engine.dispose()

        return _SessionFactory()

    _SQLALCHEMY_AVAILABLE = True

from services.auth import jwt_tokens
from shared.postgres import normalize_postgres_dsn, normalize_postgres_schema


logger = logging.getLogger("auth_service")


def _log_auth_failure(
    reason: str,
    *,
    account_id: Optional[str],
    client_ip: Optional[str],
    status_code: int,
    detail: str | None = None,
) -> None:
    """Emit a structured warning whenever authentication is denied."""

    detail_suffix = f" detail={detail}" if detail else ""
    logger.warning(
        "Authentication failure [%s]: account_id=%s ip=%s status=%s%s",
        reason,
        account_id or "unknown",
        client_ip or "unknown",
        status_code,
        detail_suffix,
    )
logging.basicConfig(level=logging.INFO)


def _require_env(name: str) -> str:
    """Return a required environment variable or raise a runtime error."""

    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} environment variable must be set before starting the auth service")
    return value


# ---------------------------------------------------------------------------
# Database layer
# ---------------------------------------------------------------------------


def _resolve_database_url() -> tuple[Optional[str], Optional[RuntimeError]]:
    raw_url = os.getenv("AUTH_DATABASE_URL")
    if raw_url is None:
        return None, RuntimeError(
            "AUTH_DATABASE_URL environment variable must be set before starting the auth service"
        )
    url = raw_url.strip()
    if not url:
        return None, RuntimeError(
            "AUTH_DATABASE_URL environment variable must be set before starting the auth service"
        )

    allow_sqlite = "pytest" in sys.modules

    try:
        normalized = normalize_postgres_dsn(
            url,
            allow_sqlite=allow_sqlite,
            label="Auth database DSN",
        )
    except RuntimeError as exc:
        return None, RuntimeError(str(exc))

    if normalized == "sqlite:///./auth_sessions.db":
        return None, RuntimeError(
            "AUTH_DATABASE_URL must point at the shared Postgres/Timescale cluster instead of the legacy SQLite default"
        )

    if not allow_sqlite and normalized.startswith("sqlite"):
        return None, RuntimeError(
            "AUTH_DATABASE_URL must use a PostgreSQL/Timescale-compatible scheme"
        )
    return normalized, None


def _engine_options(url: str) -> Dict[str, Any]:
    options: Dict[str, Any] = {"future": True, "pool_pre_ping": True}
    sa_url = make_url(url)
    backend = getattr(sa_url, "get_backend_name", None)
    if callable(backend):
        backend_name = backend()
    else:  # SQLAlchemy 2.0+ exposes ``backend_name`` instead of the helper
        backend_name = getattr(sa_url, "backend_name", sa_url.drivername)
    if backend_name.startswith("postgresql"):
        options.update(
            pool_size=int(os.getenv("AUTH_DATABASE_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("AUTH_DATABASE_MAX_OVERFLOW", "20")),
            pool_timeout=int(os.getenv("AUTH_DATABASE_POOL_TIMEOUT_SECONDS", "30")),
            pool_recycle=int(os.getenv("AUTH_DATABASE_POOL_RECYCLE_SECONDS", "300")),
        )
        sslmode = os.getenv("AUTH_DATABASE_SSLMODE", "require")
        if sslmode and "sslmode" not in sa_url.query:
            options["connect_args"] = {"sslmode": sslmode}
    return options


def _resolve_schema(url: str) -> Optional[str]:
    sa_url = make_url(url)
    backend = getattr(sa_url, "get_backend_name", None)
    if callable(backend):
        backend_name = backend()
    else:
        backend_name = getattr(sa_url, "backend_name", sa_url.drivername)
    if backend_name.startswith("postgresql"):
        override = os.getenv("AUTH_DATABASE_SCHEMA")
        if override is None:
            schema = "auth"
        else:
            if not override.strip():
                raise RuntimeError(
                    "AUTH_DATABASE_SCHEMA is set but empty; configure a valid schema identifier"
                )
            schema = override

        return normalize_postgres_schema(
            schema,
            label="Auth database schema",
            prefix_if_missing=None,
        )
    return None


ENGINE: Optional[Engine] = None
SessionLocal: Optional[sessionmaker[OrmSession]] = None

Base = declarative_base()


def _initialise_database(*, require: bool = False) -> Optional[sessionmaker[OrmSession]]:
    """Initialise the database engine and session factory if configuration is present."""

    global ENGINE, SessionLocal

    if SessionLocal is not None and ENGINE is not None:
        return SessionLocal

    url, error = _resolve_database_url()
    if error is not None or url is None:
        if require:
            raise error
        return None

    engine = create_engine(url, **_engine_options(url))
    schema = _resolve_schema(url)
    table = getattr(AuthSession, "__table__", None)
    if schema:
        Base.metadata.schema = schema
        if table is not None:
            table.schema = schema
    else:
        Base.metadata.schema = None
        if table is not None:
            table.schema = None

    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    Base.metadata.create_all(bind=engine)

    ENGINE = engine
    SessionLocal = session_factory
    return session_factory


_JWT_SECRET: Optional[str] = os.getenv("AUTH_JWT_SECRET") or None


def _initialise_jwt_secret(*, require: bool = False) -> Optional[str]:
    """Load the JWT signing secret from the environment."""

    global _JWT_SECRET

    secret = os.getenv("AUTH_JWT_SECRET")
    if not secret:
        if require:
            raise RuntimeError("AUTH_JWT_SECRET environment variable must be set before starting the auth service")
        _JWT_SECRET = None
        return None

    _JWT_SECRET = secret
    return secret


def _get_configured_jwt_secret() -> str:
    """Return the configured JWT secret, raising when unavailable."""

    secret = _JWT_SECRET or os.getenv("AUTH_JWT_SECRET")
    if not secret:
        raise RuntimeError("AUTH_JWT_SECRET environment variable must be set before issuing tokens")

    _initialise_jwt_secret()
    return secret


class AuthSession(Base):
    """SQLAlchemy model backing the ``auth_sessions`` table."""

    __tablename__ = "auth_sessions"

    session_token = Column(String(128), primary_key=True)
    user_id = Column(String(320), nullable=False, index=True)
    mfa_verified = Column(Boolean, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class SessionRepository:
    """Persists login sessions to the database."""

    def __init__(self, factory: sessionmaker[OrmSession]):
        self._factory = factory

    def create(self, *, user_id: str, mfa_verified: bool) -> AuthSession:
        token = secrets.token_urlsafe(32)
        record = AuthSession(
            session_token=token,
            user_id=user_id,
            mfa_verified=mfa_verified,
            ts=datetime.now(timezone.utc),
        )
        with self._factory() as session:
            session.add(record)
            session.commit()
            session.refresh(record)
        return record


# Attempt to initialise configuration eagerly when the environment is already populated.
if _SQLALCHEMY_AVAILABLE:
    _initialise_database()
_initialise_jwt_secret()


# ---------------------------------------------------------------------------
# OIDC helpers
# ---------------------------------------------------------------------------


class OIDCError(RuntimeError):
    """Raised when an OIDC interaction fails."""


@dataclass(slots=True)
class OIDCProvider:
    name: Literal["microsoft", "google"]
    discovery_url: str
    client_id: str
    client_secret: str

    @property
    def redirect_uri(self) -> Optional[str]:
        return os.getenv(f"{self.name.upper()}_REDIRECT_URI")


@lru_cache(maxsize=4)
def _load_discovery(url: str) -> Dict[str, Any]:
    logger.info("Fetching OIDC discovery metadata from %s", url)
    with httpx.Client(timeout=10.0) as client:
        response = client.get(url, timeout=10.0)
        response.raise_for_status()
        return response.json()


async def _exchange_code(*, provider: OIDCProvider, code: str, redirect_uri: str) -> Dict[str, Any]:
    metadata = _load_discovery(provider.discovery_url)
    token_endpoint = metadata.get("token_endpoint")
    if not token_endpoint:
        raise OIDCError("token_endpoint_missing")

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": provider.client_id,
        "client_secret": provider.client_secret,
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.post(token_endpoint, data=data)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:  # pragma: no cover - HTTP error mapping
            logger.warning("Token exchange failed: %s", exc.response.text)
            raise OIDCError("token_exchange_failed") from exc
        payload = response.json()
    if "access_token" not in payload:
        raise OIDCError("invalid_token_response")
    return payload


async def _fetch_userinfo(*, provider: OIDCProvider, access_token: str) -> Dict[str, Any]:
    metadata = _load_discovery(provider.discovery_url)
    userinfo_endpoint = metadata.get("userinfo_endpoint")
    if not userinfo_endpoint:
        raise OIDCError("userinfo_endpoint_missing")

    headers = {"Authorization": f"Bearer {access_token}"}
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(userinfo_endpoint, headers=headers)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:  # pragma: no cover - HTTP error mapping
            logger.warning("Fetching userinfo failed: %s", exc.response.text)
            raise OIDCError("userinfo_failed") from exc
        return response.json()


# ---------------------------------------------------------------------------
# MFA providers
# ---------------------------------------------------------------------------


def _load_json_env(name: str) -> Dict[str, str]:
    raw = os.getenv(name, "{}")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:  # pragma: no cover - configuration error
        raise RuntimeError(f"Invalid JSON payload in {name}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"Environment variable {name} must contain a JSON object")
    return {str(key): str(value) for key, value in data.items()}


class TotpMfaVerifier:
    """Verifies TOTP codes using per-user shared secrets."""

    def __init__(self, secrets_mapping: Mapping[str, str]):
        self._secrets = secrets_mapping

    def verify(self, user_id: str, code: str) -> bool:
        secret = self._secrets.get(user_id)
        if not secret:
            logger.warning("No TOTP secret configured for user %s", user_id)
            return False
        totp = pyotp.TOTP(secret)
        return bool(code) and totp.verify(code, valid_window=1)


class SmsMfaVerifier:
    """Verifies SMS codes using static mappings.

    In production the codes would be generated dynamically and stored in a
    datastore such as Redis or DynamoDB. For the purposes of this service we rely
    on a mapping provided via environment variables so the behaviour is
    deterministic under test.
    """

    def __init__(self, code_mapping: Mapping[str, str]):
        self._codes = code_mapping

    def verify(self, user_id: str, code: str) -> bool:
        expected = self._codes.get(user_id)
        if not expected:
            logger.warning("No SMS code configured for user %s", user_id)
            return False
        return bool(code) and secrets.compare_digest(expected, code)


class MFAVerifier:
    """Dispatches MFA verification based on the requested method."""

    def __init__(self) -> None:
        self._totp = TotpMfaVerifier(_load_json_env("MFA_TOTP_SECRETS"))
        self._sms = SmsMfaVerifier(_load_json_env("MFA_SMS_STATIC_CODES"))

    def verify(self, *, user_id: str, method: Literal["totp", "sms"], code: str) -> bool:
        if method == "totp":
            return self._totp.verify(user_id, code)
        if method == "sms":
            return self._sms.verify(user_id, code)
        raise ValueError(f"Unsupported MFA method: {method}")


# ---------------------------------------------------------------------------
# JWT handling
# ---------------------------------------------------------------------------



def create_jwt(
    *,
    subject: str,
    role: str,
    ttl_seconds: Optional[int] = None,
    secret: Optional[str] = None,
    claims: Optional[Mapping[str, Any]] = None,
) -> tuple[str, datetime]:
    """Compatibility wrapper around ``services.auth.jwt_tokens.create_jwt``.

    The auth service initialises and stores the JWT signing secret during
    application startup.  Downstream tests import ``auth_service.create_jwt`` to
    mint tokens without directly depending on the internal module structure, so
    this wrapper forwards to the shared helper while allowing an explicit secret
    override (used in unit tests) and optional custom claims.
    """

    return jwt_tokens.create_jwt(
        subject=subject,
        role=role,
        ttl_seconds=ttl_seconds,
        secret=secret or _get_configured_jwt_secret(),
        claims=claims,
    )


# ---------------------------------------------------------------------------
# Builder.io Fusion integration helper
# ---------------------------------------------------------------------------


class BuilderFusionPayload(BaseModel):
    """Metadata consumed by the Builder.io Fusion frontend."""

    space_id: Optional[str]
    environment: Optional[str]
    user_email: str
    provider: str


class LoginResponse(BaseModel):
    """Response body returned to the frontend after successful login."""

    access_token: str = Field(..., description="Signed JWT access token")
    token_type: Literal["bearer"] = Field("bearer", description="Token type indicator")
    expires_at: datetime = Field(..., description="Token expiration timestamp")
    role: str = Field(..., description="Role claim included in the token")
    session_token: str = Field(..., description="Identifier for the persisted auth session")
    builder_fusion: BuilderFusionPayload


class LoginRequest(BaseModel):
    """Payload expected by the ``POST /auth/login`` endpoint."""

    provider: Literal["microsoft", "google"]
    code: str = Field(..., description="Authorization code returned by the OIDC provider")
    redirect_uri: Optional[HttpUrl] = Field(
        None,
        description="Redirect URI used during the authorization request."
        " Defaults to the provider specific value when omitted.",
    )
    mfa_method: Literal["totp", "sms"]
    mfa_code: str = Field(..., min_length=3, description="One-time multi-factor authentication code")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


def _provider_registry() -> Dict[str, OIDCProvider]:
    return {
        "microsoft": OIDCProvider(
            name="microsoft",
            discovery_url=os.getenv(
                "MICROSOFT_OIDC_DISCOVERY",
                "https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration",
            ),
            client_id=os.getenv("MICROSOFT_CLIENT_ID", ""),
            client_secret=os.getenv("MICROSOFT_CLIENT_SECRET", ""),
        ),
        "google": OIDCProvider(
            name="google",
            discovery_url=os.getenv(
                "GOOGLE_OIDC_DISCOVERY",
                "https://accounts.google.com/.well-known/openid-configuration",
            ),
            client_id=os.getenv("GOOGLE_CLIENT_ID", ""),
            client_secret=os.getenv("GOOGLE_CLIENT_SECRET", ""),
        ),
    }


async def _persist_session(repo: SessionRepository, *, user_id: str) -> AuthSession:
    return await run_in_threadpool(repo.create, user_id=user_id, mfa_verified=True)


def _resolve_role(claims: Mapping[str, Any]) -> str:
    """Determine the role claim to embed in the issued JWT."""

    role_claim = claims.get("role")
    if isinstance(role_claim, str) and role_claim:
        return role_claim

    roles_claim = claims.get("roles")
    if isinstance(roles_claim, (list, tuple)):
        for candidate in roles_claim:
            if isinstance(candidate, str) and candidate:
                return candidate

    return os.getenv("AUTH_DEFAULT_ROLE", "admin")


async def authenticate(
    payload: LoginRequest,
    *,
    providers: Dict[str, OIDCProvider],
    mfa: MFAVerifier,
    sessions: SessionRepository,
    jwt_secret: str,
    client_ip: Optional[str] = None,
) -> LoginResponse:
    provider = providers.get(payload.provider)
    if not provider:
        _log_auth_failure(
            "unsupported_provider",
            account_id=None,
            client_ip=client_ip,
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=payload.provider,
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="unsupported_provider")
    redirect_uri = payload.redirect_uri or provider.redirect_uri
    if not redirect_uri:
        _log_auth_failure(
            "redirect_uri_missing",
            account_id=None,
            client_ip=client_ip,
            status_code=status.HTTP_400_BAD_REQUEST,
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="redirect_uri_missing")

    try:
        token_payload = await _exchange_code(provider=provider, code=payload.code, redirect_uri=str(redirect_uri))
        userinfo = await _fetch_userinfo(provider=provider, access_token=token_payload["access_token"])
    except OIDCError as exc:
        _log_auth_failure(
            "oidc_error",
            account_id=None,
            client_ip=client_ip,
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    user_id = str(userinfo.get("email") or userinfo.get("sub"))
    if not user_id:
        _log_auth_failure(
            "user_identity_missing",
            account_id=None,
            client_ip=client_ip,
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="user_identity_missing")

    if not mfa.verify(user_id=user_id, method=payload.mfa_method, code=payload.mfa_code):
        _log_auth_failure(
            "mfa_verification_failed",
            account_id=user_id,
            client_ip=client_ip,
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="mfa_verification_failed")

    role = _resolve_role(userinfo)

    token, expires_at = jwt_tokens.create_jwt(
        subject=user_id,
        role=role,
        secret=jwt_secret,
    )
    session = await _persist_session(sessions, user_id=user_id)

    builder_payload = BuilderFusionPayload(
        space_id=os.getenv("BUILDER_FUSION_SPACE_ID"),
        environment=os.getenv("BUILDER_FUSION_ENVIRONMENT"),
        user_email=user_id,
        provider=payload.provider,
    )
    return LoginResponse(
        access_token=token,
        token_type="bearer",
        expires_at=expires_at,
        role=role,
        session_token=session.session_token,
        builder_fusion=builder_payload,
    )


def get_application() -> FastAPI:
    providers = _provider_registry()
    mfa = MFAVerifier()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global SessionLocal, ENGINE
        session_factory: Optional[sessionmaker[OrmSession]] = None
        try:
            secret = _initialise_jwt_secret(require=True)
            session_factory = _initialise_database(require=True)
        except Exception:
            # Ensure partially initialised globals don't leak between startup attempts.
            SessionLocal = None
            if ENGINE is not None:
                ENGINE.dispose()
            ENGINE = None
            raise

        app.state.jwt_secret = secret
        app.state.session_repository = SessionRepository(session_factory)

        try:
            yield
        finally:
            app.state.__dict__.pop("session_repository", None)
            app.state.__dict__.pop("jwt_secret", None)

            close_all = getattr(session_factory, "close_all", None)
            if callable(close_all):
                close_all()

            SessionLocal = None
            if ENGINE is not None:
                ENGINE.dispose()
                ENGINE = None

    app = FastAPI(title="Aether Auth Service", version="1.0.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "https://builder.io",
            "https://app.builder.io",
            os.getenv("BUILDER_FUSION_ORIGIN", "https://fusion.builder.io"),
            os.getenv("ADDITIONAL_CORS_ORIGIN", ""),
        ],
        allow_credentials=True,
        allow_methods=["POST", "OPTIONS"],
        allow_headers=["*"],
    )

    def _session_repository_dependency(request: Request) -> SessionRepository:
        repo = getattr(request.app.state, "session_repository", None)
        if repo is None:
            raise RuntimeError("Auth session repository is not initialised; ensure service startup has executed")
        return repo

    def _jwt_secret_dependency(request: Request) -> str:
        secret = getattr(request.app.state, "jwt_secret", None)
        if not secret:
            raise RuntimeError("AUTH_JWT_SECRET environment variable must be configured before issuing tokens")
        return secret

    @app.post("/auth/login", response_model=LoginResponse, tags=["auth"])
    async def login_endpoint(
        payload: LoginRequest,
        request: Request,
        repo: SessionRepository = Depends(_session_repository_dependency),
        jwt_secret: str = Depends(_jwt_secret_dependency),
    ) -> LoginResponse:
        return await authenticate(
            payload,
            providers=providers,
            mfa=mfa,
            sessions=repo,
            jwt_secret=jwt_secret,
            client_ip=request.client.host if request.client else None,
        )

    return app


app = get_application()


__all__ = [
    "app",
    "authenticate",
    "AuthSession",
    "create_jwt",
    "BuilderFusionPayload",
    "LoginRequest",
    "LoginResponse",
    "MFAVerifier",
    "SessionRepository",
    "get_application",
]
