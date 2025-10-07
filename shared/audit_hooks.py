"""Typed helpers for loading optional audit logging dependencies."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Callable, Optional


AuditCallable = Callable[..., None]
HashIpCallable = Callable[[Optional[str]], Optional[str]]


def _hash_ip_fallback(value: Optional[str]) -> Optional[str]:
    """Return a deterministic hash for IP values when the audit module is absent."""

    if not value:
        return "anonymous"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return digest


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
        return AuditHooks(log=None, hash_ip=_hash_ip_fallback)

    return AuditHooks(log=log_audit, hash_ip=hash_ip)


__all__ = ["AuditHooks", "AuditCallable", "HashIpCallable", "load_audit_hooks"]

