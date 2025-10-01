"""FastAPI authentication service supporting OIDC, MFA, and JWT issuance.

This module exposes a small FastAPI application that can be embedded inside
other services or launched standalone.  It provides helper classes to interact
with OIDC providers (e.g. Microsoft and Google), enforces multi-factor
authentication (either TOTP or SMS based) before granting access, and issues
JWT tokens that include an ``admin`` role claim.  It also exposes a
``/auth/status`` endpoint that can be used by the UI to check whether a user is
currently authenticated.
"""
from __future__ import annotations

import base64
import dataclasses
import hashlib
import hmac
import json
import secrets
import time
from typing import Any, Dict, Literal, Optional
from urllib.parse import urlencode

import httpx
import pyotp
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field


MFAMethod = Literal["totp", "sms"]


class OIDCProviderConfig(BaseModel):
    """Configuration for a single OIDC provider."""

    name: str
    authorization_endpoint: str
    token_endpoint: str
    userinfo_endpoint: str
    client_id: str
    client_secret: str
    redirect_uri: str
    scope: str = "openid profile email"


class AuthSettings(BaseModel):
    """Runtime settings for the authentication service."""

    jwt_secret: str = Field(default_factory=lambda: secrets.token_urlsafe(32))
    jwt_issuer: str = "aether/auth-service"
    token_ttl_seconds: int = 3600


class OIDCCallbackRequest(BaseModel):
    """Payload expected from the OIDC redirect handler."""

    code: str
    state: Optional[str] = None
    nonce: Optional[str] = None
    mfa_method: MFAMethod = Field(default="totp")


class MFAVerificationRequest(BaseModel):
    """Payload for verifying a MFA challenge."""

    session_id: str
    method: MFAMethod
    code: str


class AuthorizationResponse(BaseModel):
    """Response object for initiating an OIDC authorization request."""

    authorization_url: str
    provider: str


class OIDCCallbackResponse(BaseModel):
    """Response returned when an OIDC callback succeeds but needs MFA."""

    session_id: str
    mfa_required: bool
    mfa_method: MFAMethod
    mfa_provisioning_uri: Optional[str] = None


class TokenResponse(BaseModel):
    """Response returned once MFA succeeds and a JWT is issued."""

    access_token: str
    token_type: str = "bearer"
    expires_in: int


class AuthStatusResponse(BaseModel):
    """Response for the ``/auth/status`` endpoint."""

    authenticated: bool
    claims: Optional[Dict[str, Any]] = None


@dataclasses.dataclass
class PendingSession:
    """Holds data between the OIDC callback and MFA verification."""

    user_info: Dict[str, Any]
    method: MFAMethod
    totp_secret: Optional[str]
    sms_code: Optional[str]
    expires_at: float


class MFAService:
    """Handles TOTP and SMS MFA challenges."""

    def __init__(self) -> None:
        self._totp_secrets: Dict[str, str] = {}
        self._sms_codes: Dict[str, tuple[str, float]] = {}

    def ensure_totp_secret(self, user_id: str) -> str:
        secret = self._totp_secrets.get(user_id)
        if not secret:
            secret = pyotp.random_base32()
            self._totp_secrets[user_id] = secret
        return secret

    def provisioning_uri(self, user_id: str, issuer: str) -> str:
        secret = self.ensure_totp_secret(user_id)
        return pyotp.TOTP(secret).provisioning_uri(name=user_id, issuer_name=issuer)

    def verify_totp(self, user_id: str, code: str) -> bool:
        secret = self._totp_secrets.get(user_id)
        if not secret:
            return False
        totp = pyotp.TOTP(secret)
        return totp.verify(code, valid_window=1)

    def generate_sms_code(self, user_id: str) -> str:
        code = f"{secrets.randbelow(1_000_000):06d}"
        self._sms_codes[user_id] = (code, time.time() + 300)
        # In a production system this is where we would integrate with an SMS
        # provider to deliver the message.  For now we simply store the code.
        return code

    def verify_sms(self, user_id: str, code: str) -> bool:
        entry = self._sms_codes.get(user_id)
        if not entry:
            return False
        stored_code, expires_at = entry
        if expires_at < time.time():
            del self._sms_codes[user_id]
            return False
        if secrets.compare_digest(stored_code, code):
            del self._sms_codes[user_id]
            return True
        return False


class AuthService:
    """Encapsulates core authentication logic."""

    def __init__(self, providers: Dict[str, OIDCProviderConfig], settings: AuthSettings) -> None:
        self.providers = providers
        self.settings = settings
        self._sessions: Dict[str, PendingSession] = {}
        self._mfa = MFAService()
        self._http_client = httpx.AsyncClient(timeout=10.0)

    async def close(self) -> None:
        await self._http_client.aclose()

    def _get_provider(self, name: str) -> OIDCProviderConfig:
        try:
            return self.providers[name.lower()]
        except KeyError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown provider") from exc

    async def build_authorization_url(self, provider_name: str, state: Optional[str] = None, nonce: Optional[str] = None) -> str:
        provider = self._get_provider(provider_name)
        query = {
            "client_id": provider.client_id,
            "response_type": "code",
            "scope": provider.scope,
            "redirect_uri": provider.redirect_uri,
        }
        if state:
            query["state"] = state
        if nonce:
            query["nonce"] = nonce
        return f"{provider.authorization_endpoint}?{urlencode(query)}"

    async def exchange_code_for_tokens(self, provider_name: str, code: str) -> Dict[str, Any]:
        provider = self._get_provider(provider_name)
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": provider.redirect_uri,
            "client_id": provider.client_id,
            "client_secret": provider.client_secret,
        }
        response = await self._http_client.post(provider.token_endpoint, data=payload)
        if response.status_code >= 400:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OIDC token exchange failed")
        return response.json()

    async def fetch_user_info(self, provider_name: str, access_token: str) -> Dict[str, Any]:
        provider = self._get_provider(provider_name)
        headers = {"Authorization": f"Bearer {access_token}"}
        response = await self._http_client.get(provider.userinfo_endpoint, headers=headers)
        if response.status_code >= 400:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Failed to retrieve user info")
        return response.json()

    def _create_pending_session(self, user_info: Dict[str, Any], method: MFAMethod) -> tuple[PendingSession, Optional[str]]:
        user_id = user_info.get("email") or user_info.get("sub")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User info missing identifier")

        if method == "totp":
            secret = self._mfa.ensure_totp_secret(user_id)
            provisioning_uri = self._mfa.provisioning_uri(user_id, self.settings.jwt_issuer)
            session = PendingSession(
                user_info=user_info,
                method=method,
                totp_secret=secret,
                sms_code=None,
                expires_at=time.time() + 600,
            )
            return session, provisioning_uri
        if method == "sms":
            code = self._mfa.generate_sms_code(user_id)
            session = PendingSession(
                user_info=user_info,
                method=method,
                totp_secret=None,
                sms_code=code,
                expires_at=time.time() + 600,
            )
            return session, None
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported MFA method")

    def create_pending_session(self, user_info: Dict[str, Any], method: MFAMethod) -> OIDCCallbackResponse:
        session, provisioning_uri = self._create_pending_session(user_info, method)
        session_id = secrets.token_urlsafe(24)
        self._sessions[session_id] = session
        return OIDCCallbackResponse(
            session_id=session_id,
            mfa_required=True,
            mfa_method=method,
            mfa_provisioning_uri=provisioning_uri,
        )

    def verify_mfa_and_issue_token(self, session_id: str, method: MFAMethod, code: str) -> TokenResponse:
        session = self._sessions.get(session_id)
        if not session or session.expires_at < time.time():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session expired or not found")

        user_id = session.user_info.get("email") or session.user_info.get("sub")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session missing user identifier")

        if method != session.method:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="MFA method mismatch")

        if method == "totp":
            if not self._mfa.verify_totp(user_id, code):
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid TOTP code")
        elif method == "sms":
            if not self._mfa.verify_sms(user_id, code):
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid SMS code")
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported MFA method")

        token = self._issue_admin_token(session.user_info)
        del self._sessions[session_id]
        return TokenResponse(access_token=token, expires_in=self.settings.token_ttl_seconds)

    def _issue_admin_token(self, user_info: Dict[str, Any]) -> str:
        now = int(time.time())
        payload = {
            "sub": user_info.get("sub") or user_info.get("email"),
            "email": user_info.get("email"),
            "name": user_info.get("name"),
            "role": "admin",
            "iat": now,
            "exp": now + self.settings.token_ttl_seconds,
            "iss": self.settings.jwt_issuer,
        }
        return self._encode_jwt(payload)

    def _encode_segment(self, data: Any) -> str:
        if isinstance(data, bytes):
            raw = data
        else:
            raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    def _encode_jwt(self, payload: Dict[str, Any]) -> str:
        header = {"alg": "HS256", "typ": "JWT"}
        header_b64 = self._encode_segment(header)
        payload_b64 = self._encode_segment(payload)
        signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
        signature = hmac.new(self.settings.jwt_secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
        signature_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")
        return f"{header_b64}.{payload_b64}.{signature_b64}"

    def decode_jwt(self, token: str) -> Dict[str, Any]:
        try:
            header_b64, payload_b64, signature_b64 = token.split(".")
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid token format") from exc

        signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
        expected_sig = hmac.new(self.settings.jwt_secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
        actual_sig = self._decode_segment(signature_b64)
        if not hmac.compare_digest(expected_sig, actual_sig):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token signature invalid")

        payload = json.loads(self._decode_segment(payload_b64))
        exp = payload.get("exp")
        if exp is not None and int(exp) < int(time.time()):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
        return payload

    @staticmethod
    def _decode_segment(segment: str) -> bytes:
        padding = "=" * (-len(segment) % 4)
        return base64.urlsafe_b64decode(segment + padding)


# Default provider configuration placeholders.  In production these values should
# come from environment variables or a secure configuration service.
default_providers = {
    "google": OIDCProviderConfig(
        name="google",
        authorization_endpoint="https://accounts.google.com/o/oauth2/v2/auth",
        token_endpoint="https://oauth2.googleapis.com/token",
        userinfo_endpoint="https://openidconnect.googleapis.com/v1/userinfo",
        client_id="GOOGLE_CLIENT_ID",
        client_secret="GOOGLE_CLIENT_SECRET",
        redirect_uri="https://localhost/auth/google/callback",
    ),
    "microsoft": OIDCProviderConfig(
        name="microsoft",
        authorization_endpoint="https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
        token_endpoint="https://login.microsoftonline.com/common/oauth2/v2.0/token",
        userinfo_endpoint="https://graph.microsoft.com/oidc/userinfo",
        client_id="MICROSOFT_CLIENT_ID",
        client_secret="MICROSOFT_CLIENT_SECRET",
        redirect_uri="https://localhost/auth/microsoft/callback",
    ),
}

settings = AuthSettings()
auth_service = AuthService(default_providers, settings)
app = FastAPI(title="Authentication Service", version="1.0.0")


async def get_auth_service() -> AuthService:
    return auth_service


@app.get("/auth/oidc/{provider}", response_model=AuthorizationResponse)
async def start_oidc_flow(provider: str, state: Optional[str] = None, nonce: Optional[str] = None, service: AuthService = Depends(get_auth_service)) -> AuthorizationResponse:
    """Returns an authorization URL for the requested OIDC provider."""

    authorization_url = await service.build_authorization_url(provider, state=state, nonce=nonce)
    return AuthorizationResponse(authorization_url=authorization_url, provider=provider)


@app.post("/auth/oidc/{provider}/callback", response_model=OIDCCallbackResponse)
async def oidc_callback(provider: str, request: OIDCCallbackRequest, service: AuthService = Depends(get_auth_service)) -> OIDCCallbackResponse:
    """Processes the OIDC callback, stores a pending session, and enforces MFA."""

    tokens = await service.exchange_code_for_tokens(provider, request.code)
    access_token = tokens.get("access_token")
    if not access_token:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Missing access token from provider")

    user_info = await service.fetch_user_info(provider, access_token)
    return service.create_pending_session(user_info, request.mfa_method)


@app.post("/auth/mfa/verify", response_model=TokenResponse)
async def verify_mfa(request: MFAVerificationRequest, service: AuthService = Depends(get_auth_service)) -> TokenResponse:
    """Verifies the MFA challenge and returns an admin JWT token."""

    return service.verify_mfa_and_issue_token(request.session_id, request.method, request.code)


@app.get("/auth/status", response_model=AuthStatusResponse)
async def auth_status(authorization: Optional[str] = Header(default=None), service: AuthService = Depends(get_auth_service)) -> AuthStatusResponse:
    """Returns authentication status for the caller based on their JWT."""

    if not authorization or not authorization.lower().startswith("bearer "):
        return AuthStatusResponse(authenticated=False)
    token = authorization.split(" ", 1)[1]
    try:
        claims = service.decode_jwt(token)
    except HTTPException:
        return AuthStatusResponse(authenticated=False)
    return AuthStatusResponse(authenticated=True, claims=claims)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await auth_service.close()
