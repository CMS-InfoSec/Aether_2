"""Typed helpers for loading optional audit logging dependencies."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, Mapping, Optional, Protocol


class AuditCallable(Protocol):
    """Protocol describing the signature of :func:`log_audit`."""

    def __call__(
        self,
        actor: str,
        action: str,
        entity: str,
        before: Mapping[str, Any],
        after: Mapping[str, Any],
        ip_hash: Optional[str],
    ) -> None:
        """Emit an audit entry."""


HashIpCallable = Callable[[Optional[str]], Optional[str]]


def _hash_ip_fallback(value: Optional[str]) -> Optional[str]:
    """Mirror :func:`common.utils.audit_logger.hash_ip` when the module is absent."""

    if value is None:
        return None

    stripped = value.strip()
    if not stripped:
        return None

    return hashlib.sha256(stripped.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AuditHooks:
    """Container describing the optional audit logging callbacks."""

    log: AuditCallable | None
    hash_ip: HashIpCallable


@lru_cache(maxsize=1)
def load_audit_hooks() -> AuditHooks:
    """Return the configured audit logging helpers if available.

    The core services rely on :mod:`common.utils.audit_logger` when it is
    installed alongside the application.  When the optional dependency is
    missing (for example in stripped-down test environments) the services
    should continue to function with audit logging disabled.  This helper
    encapsulates that import guard while retaining type information for
    downstream modules.  The resolved hooks are cached so repeated callers
    avoid importing the optional dependency multiple times; test suites can
    force a reload by invoking :func:`load_audit_hooks.cache_clear`.
    """

    try:  # pragma: no cover - import guarded for optional dependency
        from common.utils.audit_logger import hash_ip, log_audit
    except Exception:  # pragma: no cover - degrade gracefully when unavailable
        return AuditHooks(log=None, hash_ip=_hash_ip_fallback)

    return AuditHooks(log=log_audit, hash_ip=hash_ip)


__all__ = ["AuditHooks", "AuditCallable", "HashIpCallable", "load_audit_hooks"]

