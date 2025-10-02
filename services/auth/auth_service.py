"""FastAPI authentication service integrating Azure AD OIDC with TOTP MFA.

This module exposes a small FastAPI application capable of handling an
enterprise-grade authentication flow backed by Azure Active Directory
(Azure AD / Microsoft 365).  The login process relies on OIDC for the
initial identity verification and then enforces a time-based one-time
password (TOTP) challenge.  Successful MFA verification results in a
short-lived JWT (15 minutes) tagged with the caller's role and an
accompanying refresh token which can be traded in for new access tokens
when the old one expires.

The service exposes a handful of endpoints:

* ``GET /auth/login`` - returns the Azure authorization URL to redirect users.
* ``POST /auth/callback`` - processes the authorization code and starts MFA.
* ``POST /auth/mfa/totp`` - verifies the TOTP code and issues tokens.
* ``POST /auth/refresh`` - exchanges a refresh token for a new access token.
* ``GET /auth/status`` - reports whether the caller holds a valid JWT and returns the claims.
* ``POST /auth/logout`` - revokes refresh tokens and ends the session.

The implementation intentionally keeps state in-memory for simplicity.  In a
real deployment a persistent cache (Redis, SQL, etc.) should be used.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import os
import secrets
from typing import Any, Dict, Optional

import httpx
import pyotp
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field, SecretStr


class AzureOIDCSettings(BaseModel):
    """Azure Active Directory specific OIDC configuration."""

    tenant_id: str = Field(..., description="Azure AD tenant identifier")
    client_id: str = Field(..., description="Azure AD application (client) ID")
    client_secret: SecretStr = Field(..., description="Azure AD client secret")
    redirect_uri: str = Field(..., description="Redirect URI registered with Azure")
    scope: str = Field(
        default="openid profile email offline_access",
        description="OIDC scopes requested from Azure AD",
    )

    @property
    def authorization_endpoint(self) -> str:
        return f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/authorize"

    @property
    def token_endpoint(self) -> str:
        return f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

    @property
    def userinfo_endpoint(self) -> str:
        return "https://graph.microsoft.com/oidc/userinfo"


class AuthSettings(BaseModel):
    """Runtime configuration for token issuance, MFA and role mapping."""

    jwt_secret: SecretStr = Field(..., description="HS256 secret used for signing access tokens")
    jwt_issuer: str = Field(default="aether/auth", description="JWT issuer claim")
    access_token_ttl_seconds: int = Field(default=900, description="Access token lifetime (15 minutes)")
    refresh_token_ttl_seconds: int = Field(default=86400, description="Refresh token lifetime (1 day)")
    auditor_accounts: tuple[str, ...] = Field(
        default_factory=tuple,
        description=(
            "Accounts (email/UPN) that should receive the read-only 'auditor' role."
        ),
    )


class AuthorizationURLResponse(BaseModel):
    authorization_url: str


class OIDCCallbackRequest(BaseModel):
    code: str
    state: Optional[str] = None
    nonce: Optional[str] = None


class OIDCCallbackResponse(BaseModel):
    session_id: str
    mfa_required: bool = True
    totp_provisioning_uri: str


class MFAVerificationRequest(BaseModel):
    session_id: str
    totp_code: str


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class RefreshRequest(BaseModel):
    refresh_token: str


class AuthStatusResponse(BaseModel):
    authenticated: bool
    claims: Optional[Dict[str, Any]] = None


class LogoutRequest(BaseModel):
    refresh_token: Optional[str] = None


class _PendingSession(BaseModel):
    user_info: Dict[str, Any]
    totp_secret: str
    expires_at: float


class _RefreshTokenState(BaseModel):
    user_id: str
    role: str
    expires_at: float


class MFAService:
    """Handles storage and verification of per-user TOTP secrets."""

    def __init__(self) -> None:
        self._totp_secrets: Dict[str, str] = {}

    def ensure_totp_secret(self, user_id: str) -> str:
        secret = self._totp_secrets.get(user_id)
        if not secret:
            secret = pyotp.random_base32()
            self._totp_secrets[user_id] = secret
        return secret

    def provisioning_uri(self, user_id: str, issuer: str) -> str:
        secret = self.ensure_totp_secret(user_id)
        totp = pyotp.TOTP(secret)
        return totp.provisioning_uri(name=user_id, issuer_name=issuer)

    def verify(self, user_id: str, code: str) -> bool:
        secret = self._totp_secrets.get(user_id)
        if not secret:
            return False
        totp = pyotp.TOTP(secret)
        return totp.verify(code, valid_window=1)


class AuthService:
    """Encapsulates Azure AD OIDC authentication and token issuance."""

    def __init__(self, oidc: AzureOIDCSettings, settings: AuthSettings) -> None:
        self.oidc = oidc
        self.settings = settings
        self._mfa = MFAService()
        self._sessions: Dict[str, _PendingSession] = {}
        self._refresh_tokens: Dict[str, _RefreshTokenState] = {}
        self._http_client = httpx.AsyncClient(timeout=10.0)
        self._auditor_accounts = {acct.lower() for acct in self.settings.auditor_accounts}

    async def close(self) -> None:
        await self._http_client.aclose()

    async def build_authorization_url(self, state: Optional[str] = None, nonce: Optional[str] = None) -> str:
        query: Dict[str, str] = {
            "client_id": self.oidc.client_id,
            "response_type": "code",
            "redirect_uri": self.oidc.redirect_uri,
            "scope": self.oidc.scope,
        }
        if state:
            query["state"] = state
        if nonce:
            query["nonce"] = nonce
        encoded = str(httpx.QueryParams(query))
        return f"{self.oidc.authorization_endpoint}?{encoded}"

    async def exchange_code_for_tokens(self, code: str) -> Dict[str, Any]:
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.oidc.redirect_uri,
            "client_id": self.oidc.client_id,
            "client_secret": self.oidc.client_secret.get_secret_value(),
        }
        response = await self._http_client.post(self.oidc.token_endpoint, data=data)
        if response.status_code >= 400:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Failed to exchange authorization code")
        return response.json()

    async def fetch_user_info(self, access_token: str) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {access_token}"}
        response = await self._http_client.get(self.oidc.userinfo_endpoint, headers=headers)
        if response.status_code >= 400:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Failed to fetch user info")
        return response.json()

    def start_mfa_session(self, user_info: Dict[str, Any]) -> OIDCCallbackResponse:
        user_id = user_info.get("email") or user_info.get("preferred_username") or user_info.get("sub")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="OIDC user info missing identifier")

        secret = self._mfa.ensure_totp_secret(user_id)
        provisioning_uri = self._mfa.provisioning_uri(user_id, self.settings.jwt_issuer)
        session_id = secrets.token_urlsafe(32)
        expires_at = time.time() + 600  # 10 minutes window to finish MFA
        self._sessions[session_id] = _PendingSession(user_info=user_info, totp_secret=secret, expires_at=expires_at)
        return OIDCCallbackResponse(session_id=session_id, totp_provisioning_uri=provisioning_uri)

    def _issue_tokens(self, user_info: Dict[str, Any], role: Optional[str] = None) -> TokenPair:
        now = int(time.time())
        expires_at = now + self.settings.access_token_ttl_seconds
        resolved_role = role or self._resolve_role(user_info)
        permissions = self._permissions_for_role(resolved_role)
        payload = {
            "iss": self.settings.jwt_issuer,
            "iat": now,
            "exp": expires_at,
            "sub": user_info.get("sub") or user_info.get("email"),
            "email": user_info.get("email"),
            "name": user_info.get("name") or user_info.get("given_name"),
            "role": resolved_role,
            "permissions": sorted(permissions),
            "read_only": resolved_role == "auditor",
        }
        access_token = self._encode_jwt(payload)
        refresh_token = secrets.token_urlsafe(48)
        refresh_expires = now + self.settings.refresh_token_ttl_seconds
        self._refresh_tokens[refresh_token] = _RefreshTokenState(
            user_id=payload["sub"], role=resolved_role, expires_at=refresh_expires
        )
        return TokenPair(access_token=access_token, refresh_token=refresh_token, expires_in=self.settings.access_token_ttl_seconds)

    def _resolve_role(self, user_info: Dict[str, Any]) -> str:
        candidate = (
            user_info.get("email")
            or user_info.get("preferred_username")
            or user_info.get("sub")
            or ""
        )
        normalized = str(candidate).strip().lower()
        if normalized and normalized in self._auditor_accounts:
            return "auditor"
        return "admin"

    @staticmethod
    def _permissions_for_role(role: str) -> set[str]:
        base_permissions = {
            "view_reports",
            "view_logs",
            "view_trades",
        }
        if role == "auditor":
            return base_permissions
        if role == "admin":
            return base_permissions | {"place_orders", "modify_configs"}
        return set()

    def _encode_jwt(self, payload: Dict[str, Any]) -> str:
        header = {"alg": "HS256", "typ": "JWT"}
        header_b64 = self._b64encode(header)
        payload_b64 = self._b64encode(payload)
        signing_input = f"{header_b64}.{payload_b64}".encode()
        signature = hmac.new(
            self.settings.jwt_secret.get_secret_value().encode(),
            signing_input,
            hashlib.sha256,
        ).digest()
        signature_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
        return f"{header_b64}.{payload_b64}.{signature_b64}"

    def decode_jwt(self, token: str) -> Dict[str, Any]:
        try:
            header_b64, payload_b64, signature_b64 = token.split(".")
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid token format") from exc

        signing_input = f"{header_b64}.{payload_b64}".encode()
        expected_signature = hmac.new(
            self.settings.jwt_secret.get_secret_value().encode(),
            signing_input,
            hashlib.sha256,
        ).digest()
        actual_signature = self._b64decode(signature_b64)
        if not hmac.compare_digest(expected_signature, actual_signature):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token signature")

        payload = json.loads(self._b64decode(payload_b64))
        if payload.get("exp") and int(payload["exp"]) < int(time.time()):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
        role = payload.get("role")
        if role not in {"admin", "auditor"}:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Unsupported role")
        return payload

    def verify_totp_and_issue_tokens(self, session_id: str, totp_code: str) -> TokenPair:
        session = self._sessions.get(session_id)
        if not session or session.expires_at < time.time():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session expired or not found")

        user_id = session.user_info.get("email") or session.user_info.get("preferred_username") or session.user_info.get("sub")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Session missing user identifier")

        if not self._mfa.verify(user_id, totp_code):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid TOTP code")

        del self._sessions[session_id]
        return self._issue_tokens(session.user_info)

    def refresh_access_token(self, refresh_token: str) -> TokenPair:
        state = self._refresh_tokens.get(refresh_token)
        if not state or state.expires_at < time.time():
            self._refresh_tokens.pop(refresh_token, None)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token")

        # Keep the refresh token one-time use by rotating it
        self._refresh_tokens.pop(refresh_token, None)
        user_info = {"sub": state.user_id, "email": state.user_id}
        return self._issue_tokens(user_info, role=state.role)

    def logout(self, refresh_token: Optional[str], token: Optional[str]) -> None:
        if refresh_token:
            self._refresh_tokens.pop(refresh_token, None)

        if token:
            try:
                payload = self.decode_jwt(token)
            except HTTPException:
                return
            user_id = payload.get("sub")
            if user_id:
                for rt, rt_state in list(self._refresh_tokens.items()):
                    if rt_state.user_id == user_id:
                        self._refresh_tokens.pop(rt, None)

    @staticmethod
    def _b64encode(data: Any) -> str:
        if isinstance(data, bytes):
            raw = data
        else:
            raw = json.dumps(data, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    @staticmethod
    def _b64decode(segment: str) -> bytes:
        padding = "=" * (-len(segment) % 4)
        return base64.urlsafe_b64decode(segment + padding)


# Default settings rely on environment variables to encourage secure deployments.
# In unit tests or local experiments these can be overridden easily.


def _load_default_auth_service() -> AuthService:
    oidc = AzureOIDCSettings(
        tenant_id=os.getenv("AZURE_AD_TENANT_ID", "common"),
        client_id=os.getenv("AZURE_AD_CLIENT_ID", "YOUR_CLIENT_ID"),
        client_secret=SecretStr(os.getenv("AZURE_AD_CLIENT_SECRET", "development-secret")),
        redirect_uri=os.getenv("AZURE_AD_REDIRECT_URI", "http://localhost:8000/auth/callback"),
    )
    auditors_env = os.getenv("AUTH_AUDITOR_ACCOUNTS", "")
    auditor_accounts = tuple(
        account.strip()
        for account in auditors_env.split(",")
        if account.strip()
    )
    settings = AuthSettings(
        jwt_secret=SecretStr(os.getenv("AUTH_JWT_SECRET", secrets.token_urlsafe(32))),
        jwt_issuer=os.getenv("AUTH_JWT_ISSUER", "aether/auth"),
        auditor_accounts=auditor_accounts,
    )
    return AuthService(oidc=oidc, settings=settings)


auth_service = _load_default_auth_service()
app = FastAPI(title="Auth Service", version="1.0.0")


async def get_auth_service() -> AuthService:
    return auth_service


@app.get("/auth/login", response_model=AuthorizationURLResponse)
async def start_login(state: Optional[str] = None, nonce: Optional[str] = None, service: AuthService = Depends(get_auth_service)) -> AuthorizationURLResponse:
    url = await service.build_authorization_url(state=state, nonce=nonce)
    return AuthorizationURLResponse(authorization_url=url)


@app.post("/auth/callback", response_model=OIDCCallbackResponse)
async def oidc_callback(payload: OIDCCallbackRequest, service: AuthService = Depends(get_auth_service)) -> OIDCCallbackResponse:
    tokens = await service.exchange_code_for_tokens(payload.code)
    access_token = tokens.get("access_token")
    if not access_token:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Azure AD did not return an access token")
    user_info = await service.fetch_user_info(access_token)
    return service.start_mfa_session(user_info)


@app.post("/auth/mfa/totp", response_model=TokenPair)
async def verify_totp(request: MFAVerificationRequest, service: AuthService = Depends(get_auth_service)) -> TokenPair:
    return service.verify_totp_and_issue_tokens(request.session_id, request.totp_code)


@app.post("/auth/refresh", response_model=TokenPair)
async def refresh_token(request: RefreshRequest, service: AuthService = Depends(get_auth_service)) -> TokenPair:
    return service.refresh_access_token(request.refresh_token)


@app.get("/auth/status", response_model=AuthStatusResponse)
async def auth_status(authorization: Optional[str] = Header(default=None), service: AuthService = Depends(get_auth_service)) -> AuthStatusResponse:
    if not authorization or not authorization.lower().startswith("bearer "):
        return AuthStatusResponse(authenticated=False)
    token = authorization.split(" ", 1)[1]
    try:
        claims = service.decode_jwt(token)
    except HTTPException:
        return AuthStatusResponse(authenticated=False)
    return AuthStatusResponse(authenticated=True, claims=claims)


@app.post("/auth/logout")
async def logout(request: LogoutRequest, authorization: Optional[str] = Header(default=None), service: AuthService = Depends(get_auth_service)) -> Dict[str, str]:
    token = None
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1]
    service.logout(request.refresh_token, token)
    return {"status": "logged_out"}


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await auth_service.close()
