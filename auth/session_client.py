"""HTTP client and cache for administrator session tokens.

The OMS exchange adapter requires an administrator session token that matches the
``account_id`` of the request so the downstream OMS API can authorise the
operation.  This module provides a small HTTP client for the authentication
service's session endpoint along with a caching manager that keeps tokens fresh
for repeated calls.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Protocol

try:
    import httpx
except Exception:  # pragma: no cover - optional dependency
    httpx = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SessionToken:
    """Represents a bearer token issued by the session service."""

    token: str
    expires_at: Optional[float] = None

    def is_valid(self, *, now: float, leeway: float) -> bool:
        """Return ``True`` when the token is still considered valid."""

        if not self.token:
            return False
        if self.expires_at is None:
            return True
        return (self.expires_at - leeway) > now


class SessionClientProtocol(Protocol):
    """Protocol describing a client capable of issuing admin session tokens."""

    async def fetch_session(self, account_id: str) -> SessionToken:  # pragma: no cover - protocol
        ...


def _coerce_expires_at(payload: Mapping[str, Any], *, now: float) -> Optional[float]:
    raw_expires = payload.get("expires_at") or payload.get("expiry")
    if raw_expires is not None:
        if isinstance(raw_expires, (int, float)):
            return float(raw_expires)
        if isinstance(raw_expires, str):
            try:
                parsed = datetime.fromisoformat(raw_expires.replace("Z", "+00:00"))
            except ValueError:
                try:
                    return float(raw_expires)
                except (TypeError, ValueError):
                    return None
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()

    raw_ttl = payload.get("expires_in") or payload.get("ttl") or payload.get("ttl_seconds")
    if raw_ttl is not None:
        try:
            ttl = float(raw_ttl)
        except (TypeError, ValueError):
            return None
        return now + ttl
    return None


class HttpSessionClient(SessionClientProtocol):
    """HTTP client that fetches administrator sessions from the auth service."""

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        endpoint_template: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        resolved_base = (base_url or os.getenv("SESSION_SERVICE_URL", "")).strip()
        self._base_url = resolved_base.rstrip("/")
        self._endpoint = (endpoint_template or os.getenv("SESSION_SERVICE_ENDPOINT") or "/sessions/admin/{account_id}").strip()
        self._timeout = timeout if timeout is not None else float(os.getenv("SESSION_SERVICE_TIMEOUT", "2.0"))

    def _build_url(self, account_id: str) -> str:
        if not self._base_url:
            raise RuntimeError("Session service URL is not configured (set SESSION_SERVICE_URL)")
        path = self._endpoint.format(account_id=account_id)
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self._base_url}{path}"

    async def fetch_session(self, account_id: str) -> SessionToken:
        url = self._build_url(account_id)
        if httpx is None:
            raise RuntimeError("httpx is required to fetch admin sessions")
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(url)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:  # pragma: no cover - passthrough for observability
            logger.error("Session service request failed for %s: %s", account_id, exc)
            raise
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("Session service returned non-JSON payload") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("Session service returned unexpected payload type")

        token_value = str(payload.get("token") or payload.get("session_token") or "").strip()
        if not token_value:
            raise RuntimeError("Session service response missing token value")
        now = time.time()
        expires_at = _coerce_expires_at(payload, now=now)
        return SessionToken(token=token_value, expires_at=expires_at)


class AdminSessionManager:
    """Caches administrator session tokens and refreshes them as needed."""

    def __init__(
        self,
        client: SessionClientProtocol,
        *,
        refresh_leeway: float = 30.0,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self._client = client
        self._refresh_leeway = max(refresh_leeway, 0.0)
        self._clock = clock or time.time
        self._cache: Dict[str, SessionToken] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    async def token_for_account(self, account_id: str) -> str:
        normalized = account_id.strip()
        if not normalized:
            raise ValueError("account_id must be provided")

        cached = self._cache.get(normalized)
        now = self._clock()
        if cached and cached.is_valid(now=now, leeway=self._refresh_leeway):
            return cached.token

        lock = self._locks.setdefault(normalized, asyncio.Lock())
        async with lock:
            now = self._clock()
            cached = self._cache.get(normalized)
            if cached and cached.is_valid(now=now, leeway=self._refresh_leeway):
                return cached.token

            fresh = await self._client.fetch_session(normalized)
            if not fresh.token:
                raise RuntimeError("Session service returned an empty token")
            if fresh.expires_at is not None and fresh.expires_at <= now:
                raise RuntimeError("Session service returned an already expired token")
            self._cache[normalized] = fresh
            logger.debug("Refreshed admin session token for %s", normalized)
            return fresh.token

    def invalidate(self, account_id: Optional[str] = None) -> None:
        if account_id is None:
            self._cache.clear()
            return
        self._cache.pop(account_id.strip(), None)


_DEFAULT_MANAGER: Optional[AdminSessionManager] = None


def build_default_session_manager() -> AdminSessionManager:
    base_url = os.getenv("SESSION_SERVICE_URL") or os.getenv("AUTH_SESSION_URL")
    if not base_url:
        raise RuntimeError("Session service URL is not configured")
    client = HttpSessionClient(base_url=base_url)
    return AdminSessionManager(client)


def get_default_session_manager() -> AdminSessionManager:
    global _DEFAULT_MANAGER
    if _DEFAULT_MANAGER is None:
        _DEFAULT_MANAGER = build_default_session_manager()
    return _DEFAULT_MANAGER


def set_default_session_manager(manager: Optional[AdminSessionManager]) -> None:
    global _DEFAULT_MANAGER
    _DEFAULT_MANAGER = manager


__all__ = [
    "AdminSessionManager",
    "HttpSessionClient",
    "SessionClientProtocol",
    "SessionToken",
    "build_default_session_manager",
    "get_default_session_manager",
    "set_default_session_manager",
]

