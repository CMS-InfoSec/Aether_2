"""Shared middleware utilities for the secrets services."""
from __future__ import annotations

import os
from typing import Any, Awaitable, Callable, Iterable, MutableMapping, Sequence

try:  # pragma: no cover - Starlette is optional in lightweight environments
    from starlette.datastructures import Headers
    from starlette.types import ASGIApp, Receive, Scope, Send
except ModuleNotFoundError:  # pragma: no cover - provide a minimal fallback
    Scope = MutableMapping[str, Any]
    Receive = Callable[[], Awaitable[dict]]
    Send = Callable[[dict], Awaitable[None]]
    ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

    class Headers:  # type: ignore[override]
        """Small stand-in that exposes the ``.get`` API Starlette normally provides."""

        def __init__(self, *, scope: Scope | None = None, headers: Iterable[tuple[str, str]] | None = None) -> None:
            raw_items: list[tuple[str, str]] = []
            if headers is not None:
                raw_items = list(headers)
            elif scope is not None:
                scope_headers = scope.get("headers") or []
                raw_items = [
                    (
                        key.decode("latin-1") if isinstance(key, (bytes, bytearray)) else str(key),
                        value.decode("latin-1") if isinstance(value, (bytes, bytearray)) else str(value),
                    )
                    for key, value in scope_headers
                ]

            self._headers = {key.lower(): value for key, value in raw_items}

        def get(self, key: str, default: str | None = None) -> str | None:
            return self._headers.get(key.lower(), default)

TRUSTED_HOSTS: Sequence[str] = (
    "risk.aether.local",
    "*.aether.svc.cluster.local",
    "localhost",
    "127.0.0.1",
    "testserver",
)


def _load_trusted_proxy_clients() -> tuple[str, ...]:
    """Return the set of proxy client hosts whose scheme overrides are trusted."""

    value = os.getenv("SECRETS_TRUSTED_PROXY_CLIENTS")
    if not value:
        return ("127.0.0.1", "::1", "testclient")

    clients = tuple(host.strip() for host in value.split(",") if host.strip())
    return clients or ("127.0.0.1", "::1", "testclient")


TRUSTED_PROXY_CLIENTS: tuple[str, ...] = _load_trusted_proxy_clients()


class ForwardedSchemeMiddleware:
    """ASGI middleware that normalizes the scheme from ingress headers."""

    def __init__(self, app: ASGIApp, header_names: Iterable[str] | None = None) -> None:
        self.app = app
        self.header_names = tuple(
            header.lower() for header in (header_names or ("x-forwarded-proto", "x-forwarded-scheme"))
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        scheme_override: str | None = None
        original_scheme = scope.get("scheme")
        normalized_original = original_scheme.lower() if isinstance(original_scheme, str) else None

        for header in self.header_names:
            value = headers.get(header)
            if value:
                forwarded = value.split(",")[0].strip()
                if forwarded:
                    scheme_override = forwarded.lower()
                    break

        if scheme_override and scheme_override != normalized_original:
            scope = dict(scope)
            scope["aether_original_scheme"] = normalized_original
            scope["aether_forwarded_scheme"] = scheme_override
            scope["scheme"] = scheme_override

        await self.app(scope, receive, send)


__all__ = [
    "ForwardedSchemeMiddleware",
    "TRUSTED_HOSTS",
    "TRUSTED_PROXY_CLIENTS",
]
