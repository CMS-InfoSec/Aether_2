"""Shared middleware utilities for the secrets services."""
from __future__ import annotations

from typing import Iterable, Sequence

from starlette.datastructures import Headers

TRUSTED_HOSTS: Sequence[str] = (
    "risk.aether.local",
    "*.aether.svc.cluster.local",
    "localhost",
    "127.0.0.1",
    "testserver",
)


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

        for header in self.header_names:
            value = headers.get(header)
            if value:
                forwarded = value.split(",")[0].strip()
                if forwarded:
                    scheme_override = forwarded.lower()
                    break

        if scheme_override:
            scope = dict(scope)
            scope["scheme"] = scheme_override

        await self.app(scope, receive, send)


__all__ = [
    "ForwardedSchemeMiddleware",
    "TRUSTED_HOSTS",
]
