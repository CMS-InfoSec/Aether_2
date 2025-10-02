"""FastAPI authorization dependency and decorators."""

from __future__ import annotations

import base64
import inspect
import json
import logging
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional, Sequence, Tuple, TypeVar

from fastapi import HTTPException, Request


logger = logging.getLogger(__name__)


try:  # pragma: no cover - optional integration point for centralised access logging.
    from access_log import access_log  # type: ignore
except Exception:  # pragma: no cover - fallback when helper is unavailable.
    _access_logger = logging.getLogger("access")

    def access_log(  # type: ignore[override]
        user_id: str,
        account_id: str,
        endpoint: str,
        ts: str,
        reason: str = "scope_violation",
    ) -> None:
        """Fallback access logging that emits structured denial events."""

        _access_logger.warning(
            "access_denied",
            extra={
                "user_id": user_id,
                "account_id": account_id,
                "endpoint": endpoint,
                "ts": ts,
                "reason": reason,
            },
        )


_TCallable = TypeVar("_TCallable", bound=Callable[..., Any])


class AuthenticatedUser(dict):
    """Lightweight mapping representing an authenticated principal."""

    def __init__(self, *, user_id: str, claims: Mapping[str, Any]) -> None:
        super().__init__(claims)
        self["user_id"] = user_id
        self.id = user_id

    @property
    def user_id(self) -> str:
        return self["user_id"]


class BearerTokenError(HTTPException):
    """Specialised HTTP error raised when the bearer token cannot be processed."""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=401, detail=detail)


def _decode_segment(segment: str) -> bytes:
    padding = "=" * (-len(segment) % 4)
    try:
        return base64.urlsafe_b64decode(f"{segment}{padding}".encode("ascii"))
    except Exception as exc:  # pragma: no cover - invalid tokens trigger auth failure.
        raise BearerTokenError("Malformed bearer token") from exc


def _decode_jwt(token: str) -> Mapping[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        raise BearerTokenError("Malformed bearer token")
    header_segment, payload_segment = parts[0], parts[1]
    _ = _decode_segment(header_segment)  # header currently unused but validates encoding.
    payload_raw = _decode_segment(payload_segment)
    try:
        payload: Mapping[str, Any] = json.loads(payload_raw.decode("utf-8"))
    except Exception as exc:  # pragma: no cover - invalid tokens trigger auth failure.
        raise BearerTokenError("Malformed bearer token") from exc
    return payload


def _extract_bearer_token(request: Request) -> str:
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.lower().startswith("bearer "):
        raise BearerTokenError("Missing bearer token")
    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        raise BearerTokenError("Missing bearer token")
    return token


def _coerce_account_scopes(raw: Any) -> Tuple[str, ...]:
    if raw is None:
        return tuple()
    if isinstance(raw, str):
        return (raw,)
    if isinstance(raw, Mapping):
        return tuple(str(value) for value in raw.values())
    if isinstance(raw, Iterable):
        scopes: list[str] = []
        for item in raw:
            if item is None:
                continue
            scopes.append(str(item))
        return tuple(scopes)
    return tuple()


async def authz_dependency(request: Request) -> AuthenticatedUser:
    """FastAPI dependency that validates bearer tokens and attaches user context."""

    token = _extract_bearer_token(request)
    payload = _decode_jwt(token)

    user_id = str(payload.get("sub") or payload.get("user_id") or "").strip()
    if not user_id:
        raise BearerTokenError("Bearer token missing subject claim")

    scopes = _coerce_account_scopes(payload.get("account_scopes"))

    user = AuthenticatedUser(user_id=user_id, claims=payload)

    request.scope["user"] = user
    request.scope["account_scopes"] = scopes
    request.state.user = user
    request.state.account_scopes = scopes
    setattr(request, "account_scopes", scopes)

    account_id = request.path_params.get("account_id")
    if account_id is None:
        account_id = request.query_params.get("account_id")
    if account_id is not None and str(account_id) not in scopes:
        _log_denied_attempt(request, user_id, str(account_id))
        raise HTTPException(status_code=403, detail="Insufficient account scope")

    return user


def _log_denied_attempt(request: Request, user_id: str, account_id: str) -> None:
    endpoint = request.url.path
    timestamp = datetime.now(timezone.utc).isoformat()
    try:
        access_log(user_id, account_id, endpoint, timestamp, reason="scope_violation")
    except Exception as exc:  # pragma: no cover - logging must not block authz decisions.
        logger.warning("Failed to emit access log entry", exc_info=exc)


def _resolve_request(args: Tuple[Any, ...], kwargs: MutableMapping[str, Any]) -> Request:
    request = kwargs.get("request")
    if isinstance(request, Request):
        return request
    for arg in args:
        if isinstance(arg, Request):
            return arg
    raise RuntimeError("@requires_account_scope decorated endpoint must receive a Request")


def _ensure_scope(request: Request, account_id: Optional[str]) -> None:
    if account_id is None:
        account_id = request.path_params.get("account_id")
    if account_id is None:
        account_id = request.query_params.get("account_id")
    if not account_id:
        raise HTTPException(status_code=400, detail="Account identifier missing")

    scopes: Sequence[str] = getattr(request, "account_scopes", ())
    if not scopes:
        scopes = getattr(request.state, "account_scopes", ())

    account_id_str = str(account_id)
    if account_id_str not in scopes:
        user_obj: Any
        try:
            user_obj = request.user  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover - parity with Starlette behaviour when user missing.
            user_obj = None
        if user_obj is None:
            user_obj = getattr(request.state, "user", None)

        user_id: Optional[str] = None
        if hasattr(user_obj, "user_id"):
            user_id = getattr(user_obj, "user_id")  # type: ignore[assignment]
        elif isinstance(user_obj, Mapping):
            raw_id = user_obj.get("user_id")
            if raw_id is not None:
                user_id = str(raw_id)

        user_id_str = user_id or "unknown"
        _log_denied_attempt(request, user_id_str, account_id_str)
        raise HTTPException(status_code=403, detail="Insufficient account scope")


def requires_account_scope(func: _TCallable) -> _TCallable:
    """Decorator ensuring the caller holds scope for the requested account."""

    is_coroutine = inspect.iscoroutinefunction(func)

    @wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        request = _resolve_request(args, kwargs)
        account_id = kwargs.get("account_id")
        _ensure_scope(request, account_id)
        return await func(*args, **kwargs)  # type: ignore[misc]

    @wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        request = _resolve_request(args, kwargs)
        account_id = kwargs.get("account_id")
        _ensure_scope(request, account_id)
        return func(*args, **kwargs)

    if is_coroutine or hasattr(func, "__await__") or getattr(func, "_is_coroutine", False):
        return async_wrapper  # type: ignore[return-value]
    return sync_wrapper  # type: ignore[return-value]


__all__ = [
    "AuthenticatedUser",
    "authz_dependency",
    "requires_account_scope",
]
