"""Typed helpers for loading optional audit logging dependencies."""

from __future__ import annotations

import hashlib
import logging
from contextlib import contextmanager
from dataclasses import dataclass, replace
from functools import lru_cache
from typing import Any, Callable, Iterator, Mapping, Optional, Protocol, cast


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

ContextFactory = Callable[[], Optional[Mapping[str, Any]]]


_EVENT_CONTEXT_SENTINEL = object()


@dataclass(frozen=True)
class AuditEvent:
    """Structured payload describing an audit event to be recorded."""

    actor: str
    action: str
    entity: str
    before: Mapping[str, Any]
    after: Mapping[str, Any]
    ip_address: Optional[str] = None
    ip_hash: Optional[str] = None
    context: Optional[Mapping[str, Any]] = None

    def log_with_fallback(
        self,
        hooks: "AuditHooks",
        logger: logging.Logger,
        *,
        failure_message: str,
        disabled_message: Optional[str] = None,
        disabled_level: int = logging.DEBUG,
        context: Optional[Mapping[str, Any]] | object = _EVENT_CONTEXT_SENTINEL,
        context_factory: ContextFactory | None = None,
        resolved_ip_hash: "ResolvedIpHash" | None = None,
    ) -> "AuditLogResult":
        """Log the event using :func:`log_audit_event_with_fallback`.

        Callers may override the structured fallback context by supplying a
        mapping via ``context`` or defer construction entirely with
        ``context_factory``.  When neither argument is provided the event's
        stored ``context`` is reused.
        """

        context_mapping: Optional[Mapping[str, Any]]
        if context is _EVENT_CONTEXT_SENTINEL:
            if context_factory is None:
                context_mapping = self.context
            else:
                context_mapping = None
        else:
            context_mapping = cast(Optional[Mapping[str, Any]], context)

        return log_audit_event_with_fallback(
            hooks,
            logger,
            self,
            failure_message=failure_message,
            disabled_message=disabled_message,
            disabled_level=disabled_level,
            context=context_mapping,
            context_factory=context_factory,
            resolved_ip_hash=resolved_ip_hash,
        )

    def with_context(
        self,
        context: Optional[Mapping[str, Any]],
        *,
        merge: bool = True,
    ) -> "AuditEvent":
        """Return a copy of the event with updated structured context.

        When ``merge`` is ``True`` (the default) the supplied ``context`` is
        merged with any existing context, allowing callers to add supplemental
        metadata without mutating the original mapping.  Passing ``merge=False``
        replaces the stored context instead.  Supplying ``None`` while merging
        leaves the event unchanged, whereas ``merge=False`` and ``None`` clears
        the stored context entirely.
        """

        if context is None:
            if merge:
                return self
            return replace(self, context=None)

        if not merge or self.context is None:
            return replace(self, context=context)

        merged_context = dict(self.context)
        merged_context.update(context)
        return replace(self, context=merged_context)


@dataclass(frozen=True)
class AuditLogResult:
    """Structured metadata describing the outcome of an audit logging attempt."""

    handled: bool
    ip_hash: Optional[str]
    hash_fallback: bool
    hash_error: Optional[Exception]
    log_error: Optional[Exception] = None

    def __bool__(self) -> bool:  # pragma: no cover - exercised implicitly via truthiness
        return self.handled


LOGGER = logging.getLogger("shared.audit_hooks")


@dataclass(frozen=True)
class ResolvedIpHash:
    """Structured response describing the resolved IP hash state."""

    value: Optional[str]
    fallback: bool
    error: Optional[Exception]


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
    ) -> ResolvedIpHash:
        """Return a hashed IP, tracking whether a fallback hash was required."""

        if ip_hash is not None:
            return ResolvedIpHash(value=ip_hash, fallback=False, error=None)

        if ip_address is None:
            return ResolvedIpHash(value=None, fallback=False, error=None)

        try:
            return ResolvedIpHash(
                value=self.hash_ip(ip_address),
                fallback=False,
                error=None,
            )
        except Exception as exc:  # pragma: no cover - exercised via tests
            fallback_hash = _hash_ip_fallback(ip_address)
            return ResolvedIpHash(value=fallback_hash, fallback=True, error=exc)

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
        resolved_ip_hash: ResolvedIpHash | None = None,
    ) -> AuditLogResult:
        """Record an audit entry when the optional logger is available.

        The helper mirrors :func:`common.utils.audit_logger.log_audit` while
        deferring hashing logic to the configured :func:`hash_ip` callback.  It
        returns an :class:`AuditLogResult` describing whether the optional
        dependency handled the event and capturing any fallback metadata.  When
        ``resolved_ip_hash`` is supplied the method will reuse the precomputed
        hash outcome instead of invoking :meth:`resolve_ip_hash` again, allowing
        callers to avoid duplicate hashing or double-logging of failures.
        """

        log_callable = self.log
        if log_callable is None:
            resolved = resolved_ip_hash
            if resolved is None:
                resolved = ResolvedIpHash(
                    value=ip_hash,
                    fallback=False,
                    error=None,
                )
            return AuditLogResult(
                handled=False,
                ip_hash=resolved.value,
                hash_fallback=resolved.fallback,
                hash_error=resolved.error,
            )

        computed_resolved = False
        resolved = resolved_ip_hash
        if resolved is None:
            computed_resolved = True
            resolved = self.resolve_ip_hash(
                ip_address=ip_address,
                ip_hash=ip_hash,
            )

        if resolved.error is not None and computed_resolved:
            LOGGER.error(
                "Audit hash_ip callable failed; using fallback hash.",
                extra=_build_audit_log_extra(
                    actor=actor,
                    action=action,
                    entity=entity,
                    before=before,
                    after=after,
                    ip_address=ip_address,
                    ip_hash=resolved.value,
                    context=None,
                    hash_fallback=resolved.fallback,
                    hash_error=resolved.error,
                ),
                exc_info=(
                    type(resolved.error),
                    resolved.error,
                    resolved.error.__traceback__,
                ),
            )

        log_callable(
            actor=actor,
            action=action,
            entity=entity,
            before=before,
            after=after,
            ip_hash=resolved.value,
        )
        return AuditLogResult(
            handled=True,
            ip_hash=resolved.value,
            hash_fallback=resolved.fallback,
            hash_error=resolved.error,
        )


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


def _build_hash_error_metadata(hash_error: Exception) -> Mapping[str, str]:
    """Return structured metadata describing a hashing failure."""

    message = str(hash_error)
    if not message:
        message = repr(hash_error)

    return {
        "type": type(hash_error).__name__,
        "message": message,
    }


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
    hash_error: Optional[Exception],
) -> dict[str, Any]:
    """Construct structured logging metadata for audit fallbacks."""

    error_metadata: Optional[Mapping[str, str]] = None
    if hash_error is not None:
        error_metadata = _build_hash_error_metadata(hash_error)

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
        if error_metadata is not None:
            audit_metadata = extra.get("audit")
            if isinstance(audit_metadata, Mapping):
                audit_copy = dict(audit_metadata)
                audit_copy.setdefault("hash_error", error_metadata)
                extra["audit"] = audit_copy
            else:
                extra.setdefault("hash_error", error_metadata)
        return extra

    audit_payload: dict[str, Any] = {
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
    if error_metadata is not None:
        audit_section = audit_payload["audit"]
        audit_section.setdefault("hash_error", error_metadata)

    return audit_payload


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
    context_factory: ContextFactory | None = None,
    resolved_ip_hash: ResolvedIpHash | None = None,
) -> AuditLogResult:
    """Emit an audit event while shielding callers from optional failures.

    The helper wraps :meth:`AuditHooks.log_event` to provide consistent
    fallback logging across services.  When the optional audit dependency is
    absent the function logs ``disabled_message`` (when provided) at
    ``disabled_level`` with structured metadata describing the attempted
    event.  When the audit logger raises an unexpected exception the error is
    recorded using :meth:`logging.Logger.exception`, the same metadata is
    attached via ``extra`` for downstream processors, and an
    :class:`AuditLogResult` with ``handled`` set to ``False`` is returned to
    signal that no entry was written.

    Callers that need to reuse previously resolved hash metadata can supply
    ``resolved_ip_hash``.  Doing so skips the internal resolution step while
    preserving the structured fallback logging semantics, allowing services to
    compute hashes once per request and fan the result out to multiple audit
    events without recomputing (or re-logging) failures.  When the structured
    fallback ``context`` is expensive to build, ``context_factory`` can be
    provided to defer that work until a fallback log entry is actually
    required.
    """

    resolved = resolved_ip_hash
    if resolved is None:
        resolved = hooks.resolve_ip_hash(
            ip_address=ip_address,
            ip_hash=ip_hash,
        )

    log_extra: dict[str, Any] | None = None
    context_value = context
    context_evaluated = context_value is not None or context_factory is None

    def ensure_log_extra() -> Mapping[str, Any]:
        nonlocal log_extra, context_value, context_evaluated
        if log_extra is None:
            if not context_evaluated and context_factory is not None:
                context_value = context_factory()
                context_evaluated = True
            log_extra = _build_audit_log_extra(
                actor=actor,
                action=action,
                entity=entity,
                before=before,
                after=after,
                ip_address=ip_address,
                ip_hash=resolved.value,
                context=context_value,
                hash_fallback=resolved.fallback,
                hash_error=resolved.error,
            )
        return log_extra

    if resolved.error is not None:
        logger.error(
            "Audit hash_ip callable failed; using fallback hash.",
            extra=ensure_log_extra(),
            exc_info=(
                type(resolved.error),
                resolved.error,
                resolved.error.__traceback__,
            ),
        )

    try:
        result = hooks.log_event(
            actor=actor,
            action=action,
            entity=entity,
            before=before,
            after=after,
            ip_address=ip_address,
            ip_hash=resolved.value,
            resolved_ip_hash=resolved,
        )
    except Exception as exc:
        logger.exception(failure_message, extra=ensure_log_extra())
        return AuditLogResult(
            handled=False,
            ip_hash=resolved.value,
            hash_fallback=resolved.fallback,
            hash_error=resolved.error,
            log_error=exc,
        )

    if not result.handled and disabled_message is not None:
        logger.log(disabled_level, disabled_message, extra=ensure_log_extra())

    combined_hash_fallback = resolved.fallback or result.hash_fallback
    combined_hash_error = resolved.error or result.hash_error

    return AuditLogResult(
        handled=result.handled,
        ip_hash=result.ip_hash or resolved.value,
        hash_fallback=combined_hash_fallback,
        hash_error=combined_hash_error,
        log_error=result.log_error,
    )


def log_audit_event_with_fallback(
    hooks: AuditHooks,
    logger: logging.Logger,
    event: AuditEvent,
    *,
    failure_message: str,
    disabled_message: Optional[str] = None,
    disabled_level: int = logging.DEBUG,
    context: Optional[Mapping[str, Any]] | object = _EVENT_CONTEXT_SENTINEL,
    context_factory: ContextFactory | None = None,
    resolved_ip_hash: ResolvedIpHash | None = None,
) -> AuditLogResult:
    """Convenience wrapper for logging :class:`AuditEvent` instances."""
    if context is _EVENT_CONTEXT_SENTINEL:
        if context_factory is None:
            context_mapping = event.context
        else:
            context_mapping = None
    else:
        context_mapping = cast(Optional[Mapping[str, Any]], context)

    return log_event_with_fallback(
        hooks,
        logger,
        actor=event.actor,
        action=event.action,
        entity=event.entity,
        before=event.before,
        after=event.after,
        ip_address=event.ip_address,
        ip_hash=event.ip_hash,
        failure_message=failure_message,
        disabled_message=disabled_message,
        disabled_level=disabled_level,
        context=context_mapping,
        context_factory=context_factory,
        resolved_ip_hash=resolved_ip_hash,
    )


__all__ = [
    "AuditHooks",
    "AuditEvent",
    "AuditLogResult",
    "ResolvedIpHash",
    "AuditCallable",
    "HashIpCallable",
    "ContextFactory",
    "load_audit_hooks",
    "log_audit_event_with_fallback",
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

