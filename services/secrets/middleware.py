"""Shared middleware utilities for the secrets services."""
from __future__ import annotations

import os
from typing import Iterable, Sequence

from starlette.datastructures import Headers

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

    def __init__(self, app, header_names: Iterable[str] | None = None) -> None:
        self.app = app
        self.header_names = tuple(
            header.lower() for header in (header_names or ("x-forwarded-proto", "x-forwarded-scheme"))
        )

    async def __call__(self, scope, receive, send):  # type: ignore[override]
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
