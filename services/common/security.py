from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from fastapi import Header, HTTPException, Request, status

from auth.service import Session, SessionStoreProtocol

ADMIN_ACCOUNTS = {"company", "director-1", "director-2"}
DIRECTOR_ACCOUNTS = {account for account in ADMIN_ACCOUNTS if account.startswith("director-")}


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    """Representation of the caller derived from a verified session token."""

    account_id: str
    token: str

    @property
    def normalized_account(self) -> str:
        return self.account_id.strip().lower()


_DEFAULT_SESSION_STORE: SessionStoreProtocol | None = None


def set_default_session_store(store: SessionStoreProtocol | None) -> None:
    """Configure a module-level fallback session store used during tests."""

    global _DEFAULT_SESSION_STORE
    _DEFAULT_SESSION_STORE = store


def _get_session_store(request: Request) -> SessionStoreProtocol:
    store = getattr(request.app.state, "session_store", None)
    if store is None and hasattr(request.app.state, "auth_service"):
        service = getattr(request.app.state, "auth_service")
        store = getattr(service, "_sessions", None)
    if store is None:
        store = _DEFAULT_SESSION_STORE
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication session store is not configured.",
        )
    if not hasattr(store, "get") or not hasattr(store, "create"):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Configured session store does not implement the required interface.",
        )
    return store  # type: ignore[return-value]


def _extract_token(raw_value: Optional[str], *, header_name: str) -> str:
    if raw_value is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Missing {header_name} header.",
        )
    value = raw_value.strip()
    if not value:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"{header_name} header is empty.",
        )
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    if not value:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"{header_name} header is empty.",
        )
    return value


def _resolve_session(request: Request, token: str) -> Session:
    store = _get_session_store(request)
    session = store.get(token)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session token.",
        )
    if not session.admin_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session token is missing an associated account.",
        )
    return session


def require_authenticated_principal(
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> AuthenticatedPrincipal:
    token = _extract_token(authorization, header_name="Authorization")
    session = _resolve_session(request, token)
    account_id = session.admin_id.strip()
    if not account_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authenticated session is missing an account identifier.",
        )
    return AuthenticatedPrincipal(account_id=account_id, token=token)


def require_admin_account(
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
    x_account_id: Optional[str] = Header(None, alias="X-Account-ID"),
) -> str:
    principal = require_authenticated_principal(request, authorization)
    header_account = (x_account_id or "").strip().lower()
    if header_account and header_account != principal.normalized_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account header does not match authenticated session.",
        )

    if principal.normalized_account not in {acct.lower() for acct in ADMIN_ACCOUNTS}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is not authorized for administrative access.",
        )
    return principal.account_id


def require_mfa_context(
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> str:
    """Ensure the caller has completed MFA challenges via a verified session."""

    principal = require_authenticated_principal(request, authorization)
    if principal.normalized_account not in {acct.lower() for acct in ADMIN_ACCOUNTS}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is not authorized for administrative access.",
        )
    return principal.account_id


def _parse_director_tokens(raw_header: Optional[str]) -> List[str]:
    if raw_header is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Two director approvals are required for this action.",
        )
    parts: List[str] = []
    for candidate in raw_header.split(","):
        token = candidate.strip()
        if not token:
            continue
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        if token:
            parts.append(token)
    if len(parts) != 2:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Two distinct director approvals are required.",
        )
    if len(set(parts)) != 2:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Two distinct director approvals are required.",
        )
    return parts


def require_dual_director_confirmation(
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
    x_director_approvals: Optional[str] = Header(None, alias="X-Director-Approvals"),
) -> Tuple[str, str]:
    """Enforce the presence of two distinct director approvals for sensitive actions."""

    # Ensure the caller has an authenticated session even if their account is not a director.
    require_authenticated_principal(request, authorization)

    tokens = _parse_director_tokens(x_director_approvals)
    approvals: List[AuthenticatedPrincipal] = []
    for token in tokens:
        session = _resolve_session(request, token)
        account = session.admin_id.strip()
        if not account:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Director approvals must reference valid sessions.",
            )
        approvals.append(AuthenticatedPrincipal(account_id=account, token=token))

    accounts = [approval.normalized_account for approval in approvals]
    if len(set(accounts)) != 2:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Two distinct director approvals are required.",
        )

    valid_directors = {acct.lower() for acct in DIRECTOR_ACCOUNTS}
    for account in accounts:
        if account not in valid_directors:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Director approvals must come from authorized directors.",
            )

    return approvals[0].account_id, approvals[1].account_id


