"""Typed helpers for loading optional audit logging dependencies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


AuditCallable = Callable[..., None]
HashIpCallable = Callable[[Optional[str]], Optional[str]]


@dataclass(frozen=True)
class AuditHooks:
    """Container describing the optional audit logging callbacks."""

    log: AuditCallable | None
    hash_ip: HashIpCallable


def load_audit_hooks() -> AuditHooks:
    """Return the configured audit logging helpers if available.

    The core services rely on :mod:`common.utils.audit_logger` when it is
    installed alongside the application.  When the optional dependency is
    missing (for example in stripped-down test environments) the services
    should continue to function with audit logging disabled.  This helper
    encapsulates that import guard while retaining type information for
    downstream modules.
    """

    try:  # pragma: no cover - import guarded for optional dependency
        from common.utils.audit_logger import hash_ip, log_audit
    except Exception:  # pragma: no cover - degrade gracefully when unavailable

        def _hash_ip_passthrough(value: Optional[str]) -> Optional[str]:
            return None

        return AuditHooks(log=None, hash_ip=_hash_ip_passthrough)

    return AuditHooks(log=log_audit, hash_ip=hash_ip)


__all__ = ["AuditHooks", "AuditCallable", "HashIpCallable", "load_audit_hooks"]

