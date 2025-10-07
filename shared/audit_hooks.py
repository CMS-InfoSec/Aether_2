"""Typed helpers for loading optional audit logging dependencies."""

from __future__ import annotations

import hashlib
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, Iterator, Mapping, Optional, Protocol, Tuple


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


LOGGER = logging.getLogger("shared.audit_hooks")


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

    def resolve_ip_hash(
        self,
        *,
        ip_address: Optional[str],
        ip_hash: Optional[str],
    ) -> Tuple[Optional[str], bool, Optional[Exception]]:
        """Return a hashed IP, tracking whether a fallback hash was required."""

        if ip_hash is not None:
            return ip_hash, False, None

        if ip_address is None:
            return None, False, None

        try:
            return self.hash_ip(ip_address), False, None
        except Exception as exc:  # pragma: no cover - exercised via tests
            fallback_hash = _hash_ip_fallback(ip_address)
            return fallback_hash, True, exc

    def log_event(
        self,
        *,
        actor: str,
        action: str,
        entity: str,
        before: Mapping[str, Any],
        after: Mapping[str, Any],
        ip_address: Optional[str] = None,
        ip_hash: Optional[str] = None,
    ) -> bool:
        """Record an audit entry when the optional logger is available.

        The helper mirrors :func:`common.utils.audit_logger.log_audit` while
        deferring hashing logic to the configured :func:`hash_ip` callback.  It
        returns ``True`` when an audit logger handled the event and ``False``
        when the optional dependency is absent.
        """

        log_callable = self.log
        if log_callable is None:
            return False

        resolved_ip_hash, _hash_fallback, hash_error = self.resolve_ip_hash(
            ip_address=ip_address,
            ip_hash=ip_hash,
        )

        if hash_error is not None:
            LOGGER.error(
                "Audit hash_ip callable failed; using fallback hash.",
                exc_info=(type(hash_error), hash_error, hash_error.__traceback__),
            )

        log_callable(
            actor=actor,
            action=action,
            entity=entity,
            before=before,
            after=after,
            ip_hash=resolved_ip_hash,
        )
        return True


_AUDIT_HOOK_OVERRIDE: AuditHooks | None = None


@lru_cache(maxsize=1)
def _load_audit_hooks_from_dependency() -> AuditHooks:
    """Resolve the optional audit logging helpers from the shared module."""

    try:  # pragma: no cover - import guarded for optional dependency
        from common.utils.audit_logger import hash_ip, log_audit
    except Exception:  # pragma: no cover - degrade gracefully when unavailable
        return AuditHooks(log=None, hash_ip=_hash_ip_fallback)

    return AuditHooks(log=log_audit, hash_ip=hash_ip)


def load_audit_hooks() -> AuditHooks:
    """Return the configured audit logging helpers if available.

    The core services rely on :mod:`common.utils.audit_logger` when it is
    installed alongside the application.  When the optional dependency is
    missing (for example in stripped-down test environments) the services
    should continue to function with audit logging disabled.  This helper
    encapsulates that import guard while retaining type information for
    downstream modules.  The resolved hooks are cached so repeated callers
    avoid importing the optional dependency multiple times; test suites can
    force a reload by invoking :func:`reset_audit_hooks_cache` (or the legacy
    :func:`load_audit_hooks.cache_clear`).  During tests callers can override
    the resolved hooks temporarily via :func:`temporary_audit_hooks`.
    """

    override = _AUDIT_HOOK_OVERRIDE
    if override is not None:
        return override

    return _load_audit_hooks_from_dependency()


def reset_audit_hooks_cache() -> None:
    """Clear cached audit hooks and remove any active overrides."""

    global _AUDIT_HOOK_OVERRIDE
    _AUDIT_HOOK_OVERRIDE = None
    _load_audit_hooks_from_dependency.cache_clear()


@contextmanager
def temporary_audit_hooks(hooks: AuditHooks) -> Iterator[None]:
    """Temporarily override the resolved audit hooks.

    The override applies to subsequent :func:`load_audit_hooks` calls within
    the ``with`` block, allowing tests to simulate the optional dependency
    being present or absent without mutating import state globally.  Nested
    overrides restore the previous hooks when exiting their respective
    contexts.
    """

    global _AUDIT_HOOK_OVERRIDE
    previous = _AUDIT_HOOK_OVERRIDE
    _AUDIT_HOOK_OVERRIDE = hooks
    try:
        yield
    finally:
        _AUDIT_HOOK_OVERRIDE = previous


def _build_audit_log_extra(
    *,
    actor: str,
    action: str,
    entity: str,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    ip_address: Optional[str],
    ip_hash: Optional[str],
    context: Optional[Mapping[str, Any]],
    hash_fallback: bool,
) -> dict[str, Any]:
    """Construct structured logging metadata for audit fallbacks."""

    if context is not None:
        extra = dict(context)
        if hash_fallback:
            audit_metadata = extra.get("audit")
            if isinstance(audit_metadata, Mapping):
                audit_copy = dict(audit_metadata)
                audit_copy.setdefault("hash_fallback", True)
                extra["audit"] = audit_copy
            else:
                extra.setdefault("hash_fallback", True)
        return extra

    return {
        "audit": {
            "actor": actor,
            "action": action,
            "entity": entity,
            "before": dict(before),
            "after": dict(after),
            "ip_address": ip_address,
            "ip_hash": ip_hash,
            "hash_fallback": hash_fallback,
        }
    }


def log_event_with_fallback(
    hooks: AuditHooks,
    logger: logging.Logger,
    *,
    actor: str,
    action: str,
    entity: str,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    ip_address: Optional[str] = None,
    ip_hash: Optional[str] = None,
    failure_message: str,
    disabled_message: Optional[str] = None,
    disabled_level: int = logging.DEBUG,
    context: Optional[Mapping[str, Any]] = None,
) -> bool:
    """Emit an audit event while shielding callers from optional failures.

    The helper wraps :meth:`AuditHooks.log_event` to provide consistent
    fallback logging across services.  When the optional audit dependency is
    absent the function logs ``disabled_message`` (when provided) at
    ``disabled_level`` with structured metadata describing the attempted
    event.  When the audit logger raises an unexpected exception the error is
    recorded using :meth:`logging.Logger.exception`, the same metadata is
    attached via ``extra`` for downstream processors, and ``False`` is returned
    to signal that no entry was written.
    """

    resolved_ip_hash, hash_fallback, hash_error = hooks.resolve_ip_hash(
        ip_address=ip_address,
        ip_hash=ip_hash,
    )

    log_extra = _build_audit_log_extra(
        actor=actor,
        action=action,
        entity=entity,
        before=before,
        after=after,
        ip_address=ip_address,
        ip_hash=resolved_ip_hash,
        context=context,
        hash_fallback=hash_fallback,
    )

    if hash_error is not None:
        logger.error(
            "Audit hash_ip callable failed; using fallback hash.",
            extra=log_extra,
            exc_info=(type(hash_error), hash_error, hash_error.__traceback__),
        )

    try:
        handled = hooks.log_event(
            actor=actor,
            action=action,
            entity=entity,
            before=before,
            after=after,
            ip_address=ip_address,
            ip_hash=resolved_ip_hash,
        )
    except Exception:
        logger.exception(failure_message, extra=log_extra)
        return False

    if not handled and disabled_message is not None:
        logger.log(disabled_level, disabled_message, extra=log_extra)

    return handled


__all__ = [
    "AuditHooks",
    "AuditCallable",
    "HashIpCallable",
    "load_audit_hooks",
    "log_event_with_fallback",
    "reset_audit_hooks_cache",
    "temporary_audit_hooks",
]


def _cache_clear_wrapper() -> None:
    reset_audit_hooks_cache()


def _cache_info_wrapper() -> Any:
    return _load_audit_hooks_from_dependency.cache_info()


load_audit_hooks.cache_clear = _cache_clear_wrapper  # type: ignore[attr-defined]
load_audit_hooks.cache_info = _cache_info_wrapper  # type: ignore[attr-defined]

