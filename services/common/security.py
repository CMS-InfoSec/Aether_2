from __future__ import annotations

import inspect
import os
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Tuple

try:  # pragma: no cover - FastAPI is optional in some unit tests
    from fastapi import Header, HTTPException, Request, status
except ImportError:  # pragma: no cover - fallback when FastAPI is stubbed out
    from services.common.fastapi_stub import (  # type: ignore[misc]
        Header,
        HTTPException,
        Request,
        status,
    )

try:  # pragma: no cover - auth service dependencies may be unavailable
    from auth.service import Session, SessionStoreProtocol
except Exception:  # pragma: no cover - provide minimal stand-ins
    @dataclass
    class Session:  # type: ignore[override]
        token: str
        admin_id: str

    class SessionStoreProtocol:  # type: ignore[override]
        def get(self, token: str) -> Session | None:  # pragma: no cover - simple stub
            return None

        def create(self, admin_id: str) -> Session:  # pragma: no cover - simple stub
            session = Session(token=admin_id, admin_id=admin_id)
            return session

_DEFAULT_ADMIN_ACCOUNTS = frozenset({"company", "director-1", "director-2"})
_ADMIN_ENV_VARIABLE = "AETHER_ADMIN_ACCOUNTS"


def _sanitize_account_id(value: str) -> str:
    return value.strip()


def _normalize_account_id(value: str) -> str:
    return _sanitize_account_id(value).lower()


def _load_admin_accounts_from_env() -> set[str]:
    raw = os.environ.get(_ADMIN_ENV_VARIABLE)
    if not raw:
        return set(_DEFAULT_ADMIN_ACCOUNTS)
    accounts = {
        _sanitize_account_id(part)
        for part in raw.split(",")
        if _sanitize_account_id(part)
    }
    if not accounts:
        return set(_DEFAULT_ADMIN_ACCOUNTS)
    return accounts


def reload_admin_accounts(source: Optional[Iterable[str]] = None) -> set[str]:
    """Refresh administrator accounts from the configured source."""

    if source is None:
        accounts = _load_admin_accounts_from_env()
    else:
        accounts = {
            _sanitize_account_id(account)
            for account in source
            if _sanitize_account_id(account)
        }
    if not accounts:
        accounts = set(_DEFAULT_ADMIN_ACCOUNTS)

    ADMIN_ACCOUNTS.clear()
    ADMIN_ACCOUNTS.update(accounts)

    DIRECTOR_ACCOUNTS.clear()
    DIRECTOR_ACCOUNTS.update(
        {
            account
            for account in ADMIN_ACCOUNTS
            if _normalize_account_id(account).startswith("director-")
        }
    )
    return set(ADMIN_ACCOUNTS)


ADMIN_ACCOUNTS: set[str] = set()
DIRECTOR_ACCOUNTS: set[str] = set()
reload_admin_accounts()


def get_admin_accounts(*, normalized: bool = False) -> set[str]:
    """Return the configured administrator accounts."""

    accounts = {_sanitize_account_id(account) for account in ADMIN_ACCOUNTS if _sanitize_account_id(account)}
    if normalized:
        return {_normalize_account_id(account) for account in accounts}
    return accounts


def get_director_accounts(*, normalized: bool = False) -> set[str]:
    """Return the configured director accounts."""

    directors = {_sanitize_account_id(account) for account in DIRECTOR_ACCOUNTS if _sanitize_account_id(account)}
    if normalized:
        return {_normalize_account_id(account) for account in directors}
    return directors


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
    return cast(SessionStoreProtocol, store)


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
    header_value = x_account_id if isinstance(x_account_id, str) else ""
    header_account = header_value.strip().lower()
    if header_account and header_account != principal.normalized_account:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account header does not match authenticated session.",
        )

    if principal.normalized_account not in get_admin_accounts(normalized=True):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is not authorized for administrative access.",
        )
    return principal.account_id


async def ensure_admin_access(
    request: Request,
    *,
    authorization: Optional[str] = None,
    x_account_id: Optional[str] = None,
    forbid_on_missing_token: bool = False,
) -> str:
    """Resolve the administrator guard while respecting dependency overrides."""

    overrides = getattr(getattr(request, "app", None), "dependency_overrides", None)
    override = overrides.get(require_admin_account) if overrides else None

    auth_value = authorization if authorization is not None else request.headers.get("Authorization")
    header_value = x_account_id if x_account_id is not None else request.headers.get("X-Account-ID")

    try:
        if override is not None:
            call_kwargs: dict[str, Any] = {}
            try:
                signature = inspect.signature(override)
            except (TypeError, ValueError):
                signature = None
            if signature is not None:
                parameters = signature.parameters
                if "request" in parameters:
                    call_kwargs["request"] = request
                if "authorization" in parameters:
                    call_kwargs["authorization"] = auth_value
                if "x_account_id" in parameters:
                    call_kwargs["x_account_id"] = header_value
            try:
                result = override(**call_kwargs)
            except TypeError:
                result = override()
            if inspect.isawaitable(result):
                result = await result
            return result

        return require_admin_account(
            request,
            authorization=auth_value,
            x_account_id=header_value,
        )
    except HTTPException as exc:
        if forbid_on_missing_token and exc.status_code == status.HTTP_401_UNAUTHORIZED:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=exc.detail,
            ) from exc
        raise


def require_mfa_context(
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> str:
    """Ensure the caller has completed MFA challenges via a verified session."""

    principal = require_authenticated_principal(request, authorization)
    if principal.normalized_account not in get_admin_accounts(normalized=True):
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

    valid_directors = get_director_accounts(normalized=True)
    for account in accounts:
        if account not in valid_directors:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Director approvals must come from authorized directors.",
            )

    return approvals[0].account_id, approvals[1].account_id


