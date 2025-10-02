"""FastAPI authentication service implementing SSO, MFA, and Builder.io Fusion integration.

This service exposes a single endpoint ``POST /auth/login`` which performs the following
workflow:

* Exchanges an OAuth2 authorization code for tokens with Microsoft Entra ID or Google
  Identity using OIDC discovery metadata.
* Enforces multi-factor authentication (MFA) with support for TOTP codes and SMS codes.
* Issues a short-lived JWT where ``role=admin`` to integrate with downstream services.
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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict, Literal, Mapping, Optional

import httpx
import pyotp
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, HttpUrl
from sqlalchemy import Boolean, Column, DateTime, String, create_engine
from sqlalchemy.orm import Session as OrmSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from services.auth.jwt_tokens import create_jwt


logger = logging.getLogger("auth_service")
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Database layer
# ---------------------------------------------------------------------------


def _engine_options(url: str) -> Dict[str, Any]:
    options: Dict[str, Any] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


DATABASE_URL = os.getenv("AUTH_DATABASE_URL", "sqlite:///./auth_sessions.db")
ENGINE = create_engine(DATABASE_URL, **_engine_options(DATABASE_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)
Base = declarative_base()


class AuthSession(Base):
    """SQLAlchemy model backing the ``auth_sessions`` table."""

    __tablename__ = "auth_sessions"

    session_token = Column(String(128), primary_key=True)
    user_id = Column(String(320), nullable=False, index=True)
    mfa_verified = Column(Boolean, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


Base.metadata.create_all(bind=ENGINE)


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
    role: Literal["admin"] = Field("admin", description="Role claim included in the token")
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


async def authenticate(
    payload: LoginRequest,
    *,
    providers: Dict[str, OIDCProvider],
    mfa: MFAVerifier,
    sessions: SessionRepository,
) -> LoginResponse:
    provider = providers.get(payload.provider)
    if not provider:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="unsupported_provider")
    redirect_uri = payload.redirect_uri or provider.redirect_uri
    if not redirect_uri:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="redirect_uri_missing")

    try:
        token_payload = await _exchange_code(provider=provider, code=payload.code, redirect_uri=str(redirect_uri))
        userinfo = await _fetch_userinfo(provider=provider, access_token=token_payload["access_token"])
    except OIDCError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    user_id = str(userinfo.get("email") or userinfo.get("sub"))
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="user_identity_missing")

    if not mfa.verify(user_id=user_id, method=payload.mfa_method, code=payload.mfa_code):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="mfa_verification_failed")

    token, expires_at = create_jwt(subject=user_id)
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
        role="admin",
        session_token=session.session_token,
        builder_fusion=builder_payload,
    )


def get_application() -> FastAPI:
    app = FastAPI(title="Aether Auth Service", version="1.0.0")
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

    providers = _provider_registry()
    mfa = MFAVerifier()
    sessions = SessionRepository(SessionLocal)

    @app.post("/auth/login", response_model=LoginResponse, tags=["auth"])
    async def login_endpoint(payload: LoginRequest, repo: SessionRepository = Depends(lambda: sessions)) -> LoginResponse:
        return await authenticate(
            payload,
            providers=providers,
            mfa=mfa,
            sessions=repo,
        )

    return app


app = get_application()


__all__ = [
    "app",
    "authenticate",
    "AuthSession",
    "BuilderFusionPayload",
    "LoginRequest",
    "LoginResponse",
    "MFAVerifier",
    "SessionRepository",
    "get_application",
]
