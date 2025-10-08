"""Small stub of Starlette's :mod:`trustedhost` middleware."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

__all__ = ["TrustedHostMiddleware"]


def _normalize_hosts(hosts: Iterable[str] | None, *, default: Sequence[str]) -> tuple[str, ...]:
    if hosts is None:
        return tuple(default)
    return tuple(str(host) for host in hosts)


@dataclass
class TrustedHostMiddleware:
    """Records configuration for tests without performing any filtering."""

    app: object | None = None
    allowed_hosts: tuple[str, ...] = ("*",)
    disallowed_hosts: tuple[str, ...] = ()

    def __init__(
        self,
        app: object,
        *,
        allowed_hosts: Iterable[str] | None = None,
        disallowed_hosts: Iterable[str] | None = None,
    ) -> None:
        self.app = app
        self.allowed_hosts = _normalize_hosts(allowed_hosts, default=("*",))
        self.disallowed_hosts = _normalize_hosts(disallowed_hosts, default=())
