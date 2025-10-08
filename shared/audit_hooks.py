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

FallbackExtraFactory = Callable[[], Optional[Mapping[str, Any]]]


_EVENT_CONTEXT_SENTINEL = object()
_EVENT_FALLBACK_EXTRA_SENTINEL = object()


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
    context_factory: ContextFactory | None = None
    fallback_extra: Optional[Mapping[str, Any]] = None
    fallback_extra_factory: FallbackExtraFactory | None = None

    def to_payload(
        self,
        *,
        include_ip_address: bool = True,
        include_context: bool = False,
        resolved_ip_hash: "ResolvedIpHash" | None = None,
        use_context_factory: bool = False,
        include_hash_metadata: bool = False,
        include_fallback_extra: bool = False,
        use_fallback_extra_factory: bool = False,
        resolved_fallback_extra: "ResolvedFallbackExtra" | None = None,
        include_fallback_extra_metadata: bool = False,
    ) -> dict[str, Any]:
        """Return a serialisable representation of the audit event.

        The payload mirrors the arguments consumed by
        :func:`common.utils.audit_logger.log_audit`, making it suitable for
        structured logging, downstream storage, or API responses.  ``before``
        and ``after`` mappings are copied into plain dictionaries to avoid
        exposing mutable references, and callers can opt out of including the
        raw IP address via ``include_ip_address``.  When ``include_context`` is
        ``True`` any eagerly stored context mapping is duplicated as well.  By
        default the method only reuses eagerly stored context to avoid
        triggering expensive lazy builders; pass ``use_context_factory=True`` to
        evaluate :attr:`context_factory` when no stored mapping is available.
        A pre-resolved hash can be supplied so the method reuses the computed
        value instead of the event's cached ``ip_hash``.  When
        ``include_hash_metadata`` is ``True`` the payload also includes whether
        hashing required a fallback and any associated error metadata.  For
        accurate metadata callers should supply ``resolved_ip_hash`` from
        :meth:`AuditHooks.resolve_ip_hash` (or a compatible pre-computed
        result); otherwise the hash metadata defaults to a non-fallback state.

        Additional structured metadata that is only required for fallback
        logging can be included via ``include_fallback_extra``.  The returned
        mapping is copied so callers do not mutate the stored payload, and
        ``use_fallback_extra_factory`` controls whether the optional
        :attr:`fallback_extra_factory` should run when no eager mapping is
        available.  Supplying ``resolved_fallback_extra`` allows callers to
        reuse previously captured fallback extra metadata—such as from
        :meth:`resolve_fallback_extra_metadata`—without re-evaluating factories.
        When ``include_fallback_extra_metadata`` is ``True`` the payload
        includes whether fallback extra resolution was evaluated and any
        associated error metadata.  Factory execution is still opt-in via
        ``use_fallback_extra_factory`` so callers remain in control of expensive
        metadata generation.
        """

        resolved_hash = self.ip_hash if resolved_ip_hash is None else resolved_ip_hash.value

        payload: dict[str, Any] = {
            "actor": self.actor,
            "action": self.action,
            "entity": self.entity,
            "before": dict(self.before),
            "after": dict(self.after),
            "ip_hash": resolved_hash,
        }

        if include_ip_address:
            payload["ip_address"] = self.ip_address

        if include_context:
            context_mapping: Optional[Mapping[str, Any]] = self.context
            if context_mapping is None and use_context_factory and self.context_factory is not None:
                context_mapping = self.context_factory()
            if context_mapping is not None:
                payload["context"] = dict(context_mapping)

        if include_hash_metadata:
            resolved_metadata = resolved_ip_hash
            if resolved_metadata is None:
                resolved_metadata = ResolvedIpHash(
                    value=resolved_hash,
                    fallback=False,
                    error=None,
                )

            payload["hash_fallback"] = resolved_metadata.fallback
            if resolved_metadata.error is not None:
                payload["hash_error"] = _build_hash_error_metadata(resolved_metadata.error)

        fallback_extra_value: Optional[Mapping[str, Any]] = self.fallback_extra
        fallback_extra_error: Optional[Exception] = None
        fallback_extra_evaluated = fallback_extra_value is not None

        if resolved_fallback_extra is not None:
            if fallback_extra_value is None:
                fallback_extra_value = resolved_fallback_extra.value
            fallback_extra_error = resolved_fallback_extra.error
            fallback_extra_evaluated = (
                fallback_extra_evaluated or resolved_fallback_extra.evaluated
            )
        elif (
            (include_fallback_extra or include_fallback_extra_metadata)
            and fallback_extra_value is None
            and use_fallback_extra_factory
            and self.fallback_extra_factory is not None
        ):
            try:
                fallback_extra_value = self.fallback_extra_factory()
            except Exception as exc:  # pragma: no cover - exercised via tests
                fallback_extra_error = exc
                fallback_extra_value = None
            finally:
                fallback_extra_evaluated = True

        if include_fallback_extra and fallback_extra_value is not None:
            payload["fallback_extra"] = dict(fallback_extra_value)

        if include_fallback_extra_metadata:
            metadata = resolved_fallback_extra
            if metadata is None:
                metadata = ResolvedFallbackExtra(
                    value=fallback_extra_value,
                    error=fallback_extra_error,
                    evaluated=fallback_extra_evaluated,
                )

            payload["fallback_extra_evaluated"] = metadata.evaluated
            if metadata.error is not None:
                payload["fallback_extra_error"] = _describe_exception(metadata.error)

        return payload

    def resolve_ip_hash(self, hooks: "AuditHooks") -> "ResolvedIpHash":
        """Resolve the event's hashed IP using the provided hooks.

        The helper delegates to :meth:`AuditHooks.resolve_ip_hash` so callers
        can pre-compute the hash outcome (including any fallback metadata)
        before handing control to :func:`log_event_with_fallback` or the
        service-level wrappers.  The stored ``ip_hash`` is respected when
        present, avoiding redundant hashing, while ``ip_address`` is hashed on
        demand and retains the shared error-handling semantics.
        """

        return hooks.resolve_ip_hash(
            ip_address=self.ip_address,
            ip_hash=self.ip_hash,
        )

    def with_resolved_ip_hash(
        self,
        resolved: "ResolvedIpHash",
        *,
        drop_ip_address: bool = False,
    ) -> "AuditEvent":
        """Return a copy of the event with an updated resolved IP hash.

        The helper is useful when callers pre-compute hashed IP metadata via
        :meth:`resolve_ip_hash` and wish to persist the result before logging.
        Supplying ``drop_ip_address=True`` clears the raw address from the
        event so only the hashed value remains, which can help avoid retaining
        sensitive data in long-lived structures.  When the resolved value and
        resulting IP address match the current state the original instance is
        returned to preserve object identity.
        """

        next_hash = resolved.value
        next_ip = None if drop_ip_address else self.ip_address

        if next_hash == self.ip_hash and next_ip == self.ip_address:
            return self

        return replace(self, ip_hash=next_hash, ip_address=next_ip)

    def ensure_resolved_ip_hash(
        self,
        hooks: "AuditHooks",
        *,
        drop_ip_address: bool = False,
    ) -> tuple["AuditEvent", "ResolvedIpHash"]:
        """Return an updated event alongside the resolved IP hash metadata.

        The helper delegates to :meth:`resolve_ip_hash` to compute the hashed
        address (including fallback metadata) and persists the result on the
        event via :meth:`with_resolved_ip_hash`.  Callers can request that the
        raw IP address be cleared after hashing by supplying
        ``drop_ip_address=True``—useful when the event should avoid retaining
        the unhashed value beyond the resolution step.  The original event is
        returned when no state changes are required, preserving object
        identity.
        """

        resolved = self.resolve_ip_hash(hooks)
        return self.with_resolved_ip_hash(
            resolved,
            drop_ip_address=drop_ip_address,
        ), resolved

    def ensure_resolved_metadata(
        self,
        hooks: "AuditHooks",
        *,
        drop_ip_address: bool = False,
        use_context_factory: bool = True,
        drop_context_factory: bool = False,
        refresh_context: bool = False,
    ) -> tuple["AuditEvent", "ResolvedIpHash", "ResolvedContext"]:
        """Return an updated event with resolved hash and context metadata.

        The helper combines :meth:`ensure_resolved_ip_hash` and
        :meth:`resolve_context_metadata` so callers that need both outcomes can
        update the event once and receive the structured metadata required for
        logging.  Keyword arguments mirror the underlying helpers, allowing
        callers to drop the raw IP address, control whether context factories
        execute, refresh cached context, or discard factories after use.  The
        resulting event reflects any requested updates (such as clearing the IP
        address or context factory) and is returned alongside the resolved hash
        and context metadata.
        """

        updated_event, resolved_hash = self.ensure_resolved_ip_hash(
            hooks,
            drop_ip_address=drop_ip_address,
        )

        updated_event, resolved_context = updated_event.resolve_context_metadata(
            use_factory=use_context_factory,
            drop_factory=drop_context_factory,
            refresh=refresh_context,
        )

        return updated_event, resolved_hash, resolved_context

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
        resolved_context: "ResolvedContext" | None = None,
        fallback_extra: Optional[Mapping[str, Any]] | object = _EVENT_FALLBACK_EXTRA_SENTINEL,
        fallback_extra_factory: FallbackExtraFactory | None = None,
        resolved_fallback_extra: "ResolvedFallbackExtra" | None = None,
    ) -> "AuditLogResult":
        """Log the event using :func:`log_audit_event_with_fallback`.

        Callers may override the structured fallback context by supplying a
        mapping via ``context`` or defer construction entirely with
        ``context_factory``.  When neither argument is provided the event's
        stored ``context`` or ``context_factory`` is reused.  Additional
        structured metadata for fallback logging can be supplied via
        ``fallback_extra`` without mutating the event payload, allowing callers
        to attach correlation identifiers or request details to disabled/error
        log entries.  Lazy metadata builders can be supplied via
        ``fallback_extra_factory`` so expensive payloads are only evaluated when
        a fallback log entry is emitted.  When neither ``fallback_extra`` nor
        ``fallback_extra_factory`` is provided the event's stored
        ``fallback_extra`` or ``fallback_extra_factory`` is reused.
        """

        effective_factory = context_factory
        context_mapping: Optional[Mapping[str, Any]]
        if context is _EVENT_CONTEXT_SENTINEL:
            if effective_factory is None and self.context is not None:
                context_mapping = self.context
            else:
                if effective_factory is None:
                    effective_factory = self.context_factory
                context_mapping = None
        else:
            context_mapping = cast(Optional[Mapping[str, Any]], context)

        effective_fallback_factory = fallback_extra_factory
        if fallback_extra is _EVENT_FALLBACK_EXTRA_SENTINEL:
            fallback_extra_mapping = self.fallback_extra
            if effective_fallback_factory is None:
                effective_fallback_factory = self.fallback_extra_factory
        else:
            fallback_extra_mapping = cast(Optional[Mapping[str, Any]], fallback_extra)

        return log_audit_event_with_fallback(
            hooks,
            logger,
            self,
            failure_message=failure_message,
            disabled_message=disabled_message,
            disabled_level=disabled_level,
            context=context_mapping,
            context_factory=effective_factory,
            resolved_ip_hash=resolved_ip_hash,
            resolved_context=resolved_context,
            fallback_extra=fallback_extra_mapping,
            fallback_extra_factory=effective_fallback_factory,
            resolved_fallback_extra=resolved_fallback_extra,
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

    def with_context_factory(
        self,
        context_factory: ContextFactory | None,
        *,
        preserve_context: bool = False,
    ) -> "AuditEvent":
        """Return a copy of the event with an updated context factory.

        Replacing the context factory typically invalidates any eagerly stored
        context payload, so this helper clears ``context`` by default.  Callers
        that wish to retain the existing context can pass
        ``preserve_context=True``.  Supplying the same factory without
        preserving the context simply clears the stored mapping when present,
        allowing callers to drop cached metadata without constructing a new
        factory.
        """

        if context_factory is self.context_factory:
            if preserve_context or self.context is None:
                return self
            return replace(self, context=None)

        if preserve_context:
            return replace(self, context_factory=context_factory)

        return replace(self, context=None, context_factory=context_factory)

    def with_fallback_extra(
        self,
        fallback_extra: Optional[Mapping[str, Any]],
        *,
        merge: bool = True,
    ) -> "AuditEvent":
        """Return a copy of the event with updated fallback metadata."""

        if fallback_extra is None:
            if merge:
                return self
            return replace(self, fallback_extra=None)

        if not merge or self.fallback_extra is None:
            return replace(self, fallback_extra=fallback_extra)

        merged_extra = dict(self.fallback_extra)
        merged_extra.update(fallback_extra)
        return replace(self, fallback_extra=merged_extra)

    def with_fallback_extra_factory(
        self,
        fallback_extra_factory: FallbackExtraFactory | None,
        *,
        preserve_fallback_extra: bool = False,
    ) -> "AuditEvent":
        """Return a copy of the event with an updated fallback extra factory."""

        if fallback_extra_factory is self.fallback_extra_factory:
            if preserve_fallback_extra or self.fallback_extra is None:
                return self
            return replace(self, fallback_extra=None)

        if preserve_fallback_extra:
            return replace(self, fallback_extra_factory=fallback_extra_factory)

        return replace(
            self,
            fallback_extra=None,
            fallback_extra_factory=fallback_extra_factory,
        )

    def _resolve_context_internal(
        self,
        *,
        use_factory: bool,
        drop_factory: bool,
        refresh: bool,
        capture_errors: bool,
    ) -> tuple["AuditEvent", Optional[Mapping[str, Any]], Optional[Exception], bool]:
        """Return context resolution metadata while reusing the core logic."""

        context_value: Optional[Mapping[str, Any]]
        if refresh:
            context_value = None
        else:
            context_value = self.context

        factory = self.context_factory
        new_factory = factory

        evaluated = False
        if context_value is not None and not refresh:
            evaluated = True

        error: Optional[Exception] = None

        if use_factory and context_value is None and factory is not None:
            evaluated = True
            try:
                context_value = factory()
            except Exception as exc:
                if not capture_errors:
                    raise
                error = exc
                context_value = None
        elif capture_errors and (not use_factory):
            evaluated = True

        if drop_factory and new_factory is not None:
            new_factory = None

        updates: dict[str, Any] = {}

        if refresh:
            if self.context is not context_value:
                updates["context"] = context_value
        elif context_value is not self.context:
            updates["context"] = context_value

        if new_factory is not factory:
            updates["context_factory"] = new_factory

        if updates:
            updated_event = replace(self, **updates)
        else:
            updated_event = self

        if capture_errors and (factory is None):
            evaluated = True

        return updated_event, context_value, error, evaluated

    def resolve_context(
        self,
        *,
        use_factory: bool = True,
        drop_factory: bool = False,
        refresh: bool = False,
    ) -> tuple["AuditEvent", Optional[Mapping[str, Any]]]:
        """Return an updated event alongside the resolved structured context.

        The method ensures the event has an eagerly stored context mapping and
        returns the value that should be used when logging or serialising the
        event.  When ``refresh`` is ``True`` any cached context is cleared prior
        to resolution so the factory can be re-evaluated.  Context factories are
        invoked lazily—only when ``use_factory`` is ``True`` and there is no
        stored mapping.  Pass ``drop_factory=True`` to clear the factory on the
        returned event after it has been evaluated (or skipped), which is useful
        when callers want to avoid repeated work after capturing the context
        once.  The original instance is returned when no state changes are
        required.
        """

        updated_event, context_value, _, _ = self._resolve_context_internal(
            use_factory=use_factory,
            drop_factory=drop_factory,
            refresh=refresh,
            capture_errors=False,
        )

        return updated_event, context_value

    def resolve_context_metadata(
        self,
        *,
        use_factory: bool = True,
        drop_factory: bool = False,
        refresh: bool = False,
    ) -> tuple["AuditEvent", "ResolvedContext"]:
        """Return an updated event and structured context resolution metadata.

        The helper mirrors :meth:`resolve_context` but also captures whether a
        context factory was evaluated and records any exception raised during
        resolution.  Callers can pass the returned :class:`ResolvedContext`
        directly to :func:`log_event_with_fallback` (or
        :meth:`AuditEvent.log_with_fallback`) to avoid re-evaluating expensive
        factories and to surface context factory failures alongside other
        fallback metadata.
        """

        updated_event, context_value, error, evaluated = self._resolve_context_internal(
            use_factory=use_factory,
            drop_factory=drop_factory,
            refresh=refresh,
            capture_errors=True,
        )

        return updated_event, ResolvedContext(
            value=context_value,
            error=error,
            evaluated=evaluated,
        )

    def _resolve_fallback_extra_internal(
        self,
        *,
        use_factory: bool,
        drop_factory: bool,
        drop_extra: bool,
        refresh: bool,
        capture_errors: bool,
    ) -> tuple["AuditEvent", Optional[Mapping[str, Any]], Optional[Exception], bool]:
        """Return fallback extra resolution metadata while reusing core logic."""

        extra_value: Optional[Mapping[str, Any]]
        if refresh:
            extra_value = None
        else:
            extra_value = self.fallback_extra

        factory = self.fallback_extra_factory
        new_factory = factory

        evaluated = False
        if extra_value is not None and not refresh:
            evaluated = True

        error: Optional[Exception] = None

        if use_factory and extra_value is None and factory is not None:
            evaluated = True
            try:
                extra_value = factory()
            except Exception as exc:
                if not capture_errors:
                    raise
                error = exc
                extra_value = None
        elif capture_errors and (not use_factory):
            evaluated = True

        returned_extra = extra_value

        if drop_factory and new_factory is not None:
            new_factory = None

        updates: dict[str, Any] = {}

        if drop_extra and extra_value is not None:
            extra_value = None

        if refresh:
            if self.fallback_extra is not extra_value:
                updates["fallback_extra"] = extra_value
        elif extra_value is not self.fallback_extra:
            updates["fallback_extra"] = extra_value

        if new_factory is not factory:
            updates["fallback_extra_factory"] = new_factory

        if updates:
            updated_event = replace(self, **updates)
        else:
            updated_event = self

        if capture_errors and (factory is None):
            evaluated = True

        return updated_event, returned_extra, error, evaluated

    def resolve_fallback_extra(
        self,
        *,
        use_factory: bool = True,
        drop_factory: bool = False,
        drop_extra: bool = False,
        refresh: bool = False,
    ) -> tuple["AuditEvent", Optional[Mapping[str, Any]]]:
        """Return an updated event alongside the resolved fallback metadata.

        When ``drop_extra`` is ``True`` the resolved mapping is returned but
        cleared from the event, allowing callers to capture the payload once
        without retaining it on the instance.
        """

        updated_event, extra_value, _, _ = self._resolve_fallback_extra_internal(
            use_factory=use_factory,
            drop_factory=drop_factory,
            drop_extra=drop_extra,
            refresh=refresh,
            capture_errors=False,
        )

        return updated_event, extra_value

    def resolve_fallback_extra_metadata(
        self,
        *,
        use_factory: bool = True,
        drop_factory: bool = False,
        drop_extra: bool = False,
        refresh: bool = False,
    ) -> tuple["AuditEvent", "ResolvedFallbackExtra"]:
        """Return an updated event and structured fallback extra metadata.

        ``drop_extra=True`` mirrors :meth:`resolve_fallback_extra` by clearing
        the stored mapping after resolution while still returning the value via
        :class:`ResolvedFallbackExtra`.
        """

        updated_event, extra_value, error, evaluated = self._resolve_fallback_extra_internal(
            use_factory=use_factory,
            drop_factory=drop_factory,
            drop_extra=drop_extra,
            refresh=refresh,
            capture_errors=True,
        )

        return updated_event, ResolvedFallbackExtra(
            value=extra_value,
            error=error,
            evaluated=evaluated,
        )

    def with_actor(self, actor: str) -> "AuditEvent":
        """Return a copy of the event with an updated actor value."""

        if actor == self.actor:
            return self

        return replace(self, actor=actor)

    def with_action(self, action: str) -> "AuditEvent":
        """Return a copy of the event with an updated action value."""

        if action == self.action:
            return self

        return replace(self, action=action)

    def with_entity(self, entity: str) -> "AuditEvent":
        """Return a copy of the event with an updated entity value."""

        if entity == self.entity:
            return self

        return replace(self, entity=entity)

    def with_before(
        self,
        before: Mapping[str, Any],
        *,
        merge: bool = False,
    ) -> "AuditEvent":
        """Return a copy of the event with updated ``before`` metadata."""

        if merge:
            merged = dict(self.before)
            updated = False
            for key, value in before.items():
                if key not in merged or merged[key] != value:
                    updated = True
                merged[key] = value

            if not updated:
                return self

            return replace(self, before=merged)

        if before is self.before or before == self.before:
            return self

        return replace(self, before=before)

    def with_after(
        self,
        after: Mapping[str, Any],
        *,
        merge: bool = False,
    ) -> "AuditEvent":
        """Return a copy of the event with updated ``after`` metadata."""

        if merge:
            merged = dict(self.after)
            updated = False
            for key, value in after.items():
                if key not in merged or merged[key] != value:
                    updated = True
                merged[key] = value

            if not updated:
                return self

            return replace(self, after=merged)

        if after is self.after or after == self.after:
            return self

        return replace(self, after=after)

    def with_ip_address(
        self,
        ip_address: Optional[str],
        *,
        preserve_hash: bool = False,
    ) -> "AuditEvent":
        """Return a copy of the event with an updated IP address.

        By default the stored ``ip_hash`` is cleared whenever the IP address is
        changed to avoid accidentally reusing hashes derived from a different
        address.  Callers can keep the existing hash by passing
        ``preserve_hash=True``—useful when the hash originates from an external
        source.  Supplying the same IP address returns the original instance
        unless the hash needs to be cleared.
        """

        if ip_address == self.ip_address:
            if preserve_hash or self.ip_hash is None:
                return self
            return replace(self, ip_hash=None)

        next_hash = self.ip_hash if preserve_hash else None
        return replace(self, ip_address=ip_address, ip_hash=next_hash)

    def with_ip_hash(self, ip_hash: Optional[str]) -> "AuditEvent":
        """Return a copy of the event with an updated IP hash."""

        if ip_hash == self.ip_hash:
            return self

        return replace(self, ip_hash=ip_hash)


@dataclass(frozen=True)
class AuditLogResult:
    """Structured metadata describing the outcome of an audit logging attempt.

    The result exposes whether the optional audit dependency handled the
    request as well as the resolved hash metadata used during logging.  When
    fallback logging occurs the ``context`` field contains the structured
    payload that was emitted (if any) alongside any factory errors captured in
    ``context_error``.  ``fallback_logged`` signals whether a fallback record
    was emitted (for example due to disabled audit logging, hashing failures,
    or context factory errors), helping callers surface additional telemetry.
    ``context_evaluated`` indicates whether fallback context resolution
    completed—either because an eager mapping was already available, the
    factory executed, or there was no factory to invoke—allowing callers to
    detect when expensive context builders were triggered.  ``fallback_extra``
    captures the structured logging metadata that was passed to the logger
    whenever a fallback entry was emitted so callers can reuse (or inspect)
    exactly what was recorded without reconstituting the payload.  When lazy
    fallback metadata factories are used, ``fallback_extra_evaluated`` reflects
    whether the callable executed, and ``fallback_extra_error`` captures any
    exception raised while constructing the payload so callers can surface
    degraded telemetry.
    """

    handled: bool
    ip_hash: Optional[str]
    hash_fallback: bool
    hash_error: Optional[Exception]
    context: Optional[Mapping[str, Any]] = None
    log_error: Optional[Exception] = None
    context_error: Optional[Exception] = None
    context_evaluated: bool = False
    fallback_logged: bool = False
    fallback_extra: Optional[Mapping[str, Any]] = None
    fallback_extra_error: Optional[Exception] = None
    fallback_extra_evaluated: bool = False

    def __bool__(self) -> bool:  # pragma: no cover - exercised implicitly via truthiness
        return self.handled


@dataclass(frozen=True)
class ResolvedContext:
    """Structured metadata describing context resolution state."""

    value: Optional[Mapping[str, Any]]
    error: Optional[Exception]
    evaluated: bool


@dataclass(frozen=True)
class ResolvedFallbackExtra:
    """Structured metadata describing fallback extra resolution state."""

    value: Optional[Mapping[str, Any]]
    error: Optional[Exception]
    evaluated: bool


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
                context_evaluated=False,
                fallback_logged=False,
            )

        computed_resolved = False
        resolved = resolved_ip_hash
        if resolved is None:
            computed_resolved = True
            resolved = self.resolve_ip_hash(
                ip_address=ip_address,
                ip_hash=ip_hash,
            )

        fallback_logged = False
        fallback_extra: Optional[Mapping[str, Any]] = None

        if resolved.error is not None and computed_resolved:
            log_extra_payload = _build_audit_log_extra(
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
            )
            LOGGER.error(
                "Audit hash_ip callable failed; using fallback hash.",
                extra=log_extra_payload,
                exc_info=(
                    type(resolved.error),
                    resolved.error,
                    resolved.error.__traceback__,
                ),
            )
            fallback_logged = True
            fallback_extra = dict(log_extra_payload)

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
            context_evaluated=False,
            fallback_logged=fallback_logged,
            fallback_extra=fallback_extra,
            fallback_extra_evaluated=fallback_extra is not None,
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


def _describe_exception(error: Exception) -> Mapping[str, str]:
    """Return structured metadata describing an exception."""

    message = str(error)
    if not message:
        message = repr(error)

    return {
        "type": type(error).__name__,
        "message": message,
    }


def _build_hash_error_metadata(hash_error: Exception) -> Mapping[str, str]:
    """Return structured metadata describing a hashing failure."""

    return _describe_exception(hash_error)


def _snapshot_context_mapping(
    context: Optional[Mapping[str, Any]],
    *,
    copy: bool,
) -> Optional[Mapping[str, Any]]:
    """Return structured context metadata, optionally copying the mapping."""

    if context is None:
        return None

    if not copy:
        return context

    return dict(context)


def _combine_fallback_extra(
    primary: Optional[Mapping[str, Any]],
    secondary: Optional[Mapping[str, Any]],
) -> Optional[Mapping[str, Any]]:
    """Return a merged view of fallback logging metadata."""

    if primary is None and secondary is None:
        return None

    if primary is None:
        return None if secondary is None else dict(secondary)

    if secondary is None:
        return dict(primary)

    combined: dict[str, Any] = dict(secondary)
    for key, value in primary.items():
        if key == "audit" and isinstance(value, Mapping):
            existing = combined.get("audit")
            if isinstance(existing, Mapping):
                merged = dict(existing)
                merged.update(dict(value))
                combined["audit"] = merged
                continue
        combined[key] = dict(value) if isinstance(value, Mapping) else value

    return combined


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
    context_error: Optional[Exception] = None,
) -> dict[str, Any]:
    """Construct structured logging metadata for audit fallbacks."""

    error_metadata: Optional[Mapping[str, str]] = None
    if hash_error is not None:
        error_metadata = _build_hash_error_metadata(hash_error)

    context_error_metadata: Optional[Mapping[str, str]] = None
    if context_error is not None:
        context_error_metadata = _describe_exception(context_error)

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
        if context_error_metadata is not None:
            extra.setdefault("audit_context_error", context_error_metadata)
        return extra

    resolved_metadata = ResolvedIpHash(
        value=ip_hash,
        fallback=hash_fallback,
        error=hash_error,
    )
    audit_payload: dict[str, Any] = {
        "audit": AuditEvent(
            actor=actor,
            action=action,
            entity=entity,
            before=before,
            after=after,
            ip_address=ip_address,
            ip_hash=ip_hash,
        ).to_payload(
            resolved_ip_hash=resolved_metadata,
            include_hash_metadata=True,
        )
    }
    if context_error_metadata is not None:
        audit_payload.setdefault("audit_context_error", context_error_metadata)
    if error_metadata is not None:
        audit_payload["audit"].setdefault("hash_error", error_metadata)

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
    resolved_context: "ResolvedContext" | None = None,
    fallback_extra: Optional[Mapping[str, Any]] = None,
    fallback_extra_factory: FallbackExtraFactory | None = None,
    resolved_fallback_extra: "ResolvedFallbackExtra" | None = None,
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
    required.  Supplying ``resolved_context`` allows callers to pre-resolve
    structured context metadata—including capturing factory exceptions—so the
    helper can reuse the outcome without re-invoking the factory during
    fallback logging.  The returned :class:`AuditLogResult` exposes the
    structured context mapping that was resolved for fallback logging when one
    is available, allowing callers to inspect the payload without re-evaluating
    expensive factories.  ``context_evaluated`` reflects whether context
    resolution completed (for example because a factory ran, eager context was
    available, or no factory was provided), enabling callers to detect when
    deferred work was triggered.  Additional structured metadata for fallback
    logging can be supplied via ``fallback_extra`` so services can attach
    correlation identifiers or request context without mutating the base audit
    payload.  Expensive fallback metadata can be deferred by providing
    ``fallback_extra_factory``; the callable is only invoked when a fallback log
    entry is emitted.  Any factory exception is captured in the returned
    :class:`AuditLogResult` and recorded via the provided ``logger`` alongside
    the structured fallback payload.  When callers have already resolved the
    fallback metadata (or captured prior failures) they can supply
    ``resolved_fallback_extra`` to skip re-evaluating factories while
    preserving the recorded error context.
    """

    resolved = resolved_ip_hash
    if resolved is None:
        resolved = hooks.resolve_ip_hash(
            ip_address=ip_address,
            ip_hash=ip_hash,
        )

    log_extra: dict[str, Any] | None = None
    effective_context_factory = context_factory
    context_value = context
    context_error: Exception | None = None
    if resolved_context is not None:
        if context is None:
            context_value = resolved_context.value
        context_error = resolved_context.error

    context_evaluated = (
        context_value is not None
        or effective_context_factory is None
        or (resolved_context.evaluated if resolved_context is not None else False)
    )
    context_error_logged = False
    fallback_logged = False

    fallback_extra_value = fallback_extra
    fallback_extra_error: Exception | None = None
    fallback_extra_error_logged = False
    fallback_extra_evaluated = fallback_extra is not None

    if resolved_fallback_extra is not None:
        if fallback_extra_value is None:
            fallback_extra_value = resolved_fallback_extra.value
        fallback_extra_error = resolved_fallback_extra.error
        fallback_extra_evaluated = (
            fallback_extra_evaluated or resolved_fallback_extra.evaluated
        )

    def ensure_log_extra() -> Mapping[str, Any]:
        nonlocal log_extra, context_value, context_evaluated
        nonlocal context_error, context_error_logged, effective_context_factory
        nonlocal fallback_logged, fallback_extra_value, fallback_extra_error
        nonlocal fallback_extra_error_logged, fallback_extra_evaluated
        if log_extra is None:
            if not context_evaluated and effective_context_factory is not None:
                try:
                    context_value = effective_context_factory()
                except Exception as exc:  # pragma: no cover - exercised via tests
                    context_error = exc
                    context_value = None
                finally:
                    effective_context_factory = None
                    context_evaluated = True
            base_extra = _build_audit_log_extra(
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
                context_error=context_error,
            )
            base_extra = dict(base_extra)
            if not fallback_extra_evaluated and fallback_extra_factory is not None:
                try:
                    fallback_extra_value = fallback_extra_factory()
                except Exception as exc:  # pragma: no cover - exercised via tests
                    fallback_extra_error = exc
                    fallback_extra_value = None
                finally:
                    fallback_extra_evaluated = True
            if fallback_extra_value is not None:
                merged = _combine_fallback_extra(
                    fallback_extra_value,
                    base_extra,
                )
                assert merged is not None  # _combine_fallback_extra returns mapping when inputs provided
                log_extra = dict(merged)
            else:
                log_extra = base_extra
                fallback_extra_evaluated = True
            if fallback_extra_error is not None:
                log_extra = dict(log_extra)
                log_extra.setdefault(
                    "audit_fallback_extra_error",
                    _describe_exception(fallback_extra_error),
                )
                if not fallback_extra_error_logged:
                    logger.exception(
                        "Audit fallback extra factory raised; omitting custom metadata.",
                        extra=log_extra,
                        exc_info=(
                            type(fallback_extra_error),
                            fallback_extra_error,
                            fallback_extra_error.__traceback__,
                        ),
                    )
                    fallback_extra_error_logged = True
                    fallback_logged = True
            if context_error is not None and not context_error_logged:
                logger.exception(
                    "Audit fallback context factory raised; omitting context metadata.",
                    extra=log_extra,
                    exc_info=(
                        type(context_error),
                        context_error,
                        context_error.__traceback__,
                    ),
                )
                context_error_logged = True
                fallback_logged = True
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
        fallback_logged = True

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
            context=_snapshot_context_mapping(context_value, copy=True),
            log_error=exc,
            context_error=context_error,
            context_evaluated=context_evaluated,
            fallback_logged=True,
            fallback_extra=dict(log_extra) if log_extra is not None else None,
            fallback_extra_error=fallback_extra_error,
            fallback_extra_evaluated=fallback_extra_evaluated,
        )

    if not result.handled and disabled_message is not None:
        logger.log(disabled_level, disabled_message, extra=ensure_log_extra())
        fallback_logged = True

    combined_hash_fallback = resolved.fallback or result.hash_fallback
    combined_hash_error = resolved.error or result.hash_error

    context_snapshot = result.context
    if context_snapshot is None and context_value is not None and context_evaluated:
        context_snapshot = _snapshot_context_mapping(
            context_value,
            copy=log_extra is not None,
        )

    wrapper_fallback_extra = (
        dict(log_extra) if fallback_logged and log_extra is not None else None
    )
    combined_fallback_extra = _combine_fallback_extra(
        result.fallback_extra,
        wrapper_fallback_extra,
    )

    combined_fallback_extra_error = result.fallback_extra_error or fallback_extra_error
    combined_fallback_extra_evaluated = (
        result.fallback_extra_evaluated or fallback_extra_evaluated
    )

    return AuditLogResult(
        handled=result.handled,
        ip_hash=result.ip_hash or resolved.value,
        hash_fallback=combined_hash_fallback,
        hash_error=combined_hash_error,
        context=context_snapshot,
        log_error=result.log_error,
        context_error=result.context_error or context_error,
        context_evaluated=result.context_evaluated or context_evaluated,
        fallback_logged=result.fallback_logged or fallback_logged,
        fallback_extra=combined_fallback_extra,
        fallback_extra_error=combined_fallback_extra_error,
        fallback_extra_evaluated=combined_fallback_extra_evaluated,
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
    resolved_context: ResolvedContext | None = None,
    fallback_extra: Optional[Mapping[str, Any]] | object = _EVENT_FALLBACK_EXTRA_SENTINEL,
    fallback_extra_factory: FallbackExtraFactory | None = None,
    resolved_fallback_extra: "ResolvedFallbackExtra" | None = None,
) -> AuditLogResult:
    """Convenience wrapper for logging :class:`AuditEvent` instances.

    ``fallback_extra`` mirrors the similarly named argument on
    :func:`log_event_with_fallback`, allowing callers to attach additional
    structured metadata (for example correlation identifiers) to any fallback
    log entries emitted during the call.  ``fallback_extra_factory`` allows
    callers to defer construction of custom fallback metadata until a fallback
    log entry is required.  When callers have already resolved fallback extra
    metadata they can supply ``resolved_fallback_extra`` to reuse the cached
    structure and propagate prior factory errors without re-evaluating the
    callable.
    """
    effective_factory = context_factory
    if context is _EVENT_CONTEXT_SENTINEL:
        if effective_factory is None and event.context is not None:
            context_mapping = event.context
        else:
            if effective_factory is None:
                effective_factory = event.context_factory
            context_mapping = None
    else:
        context_mapping = cast(Optional[Mapping[str, Any]], context)

    effective_fallback_factory = fallback_extra_factory
    if fallback_extra is _EVENT_FALLBACK_EXTRA_SENTINEL:
        fallback_extra_mapping = event.fallback_extra
        if effective_fallback_factory is None:
            effective_fallback_factory = event.fallback_extra_factory
    else:
        fallback_extra_mapping = cast(Optional[Mapping[str, Any]], fallback_extra)

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
        context_factory=effective_factory,
        resolved_ip_hash=resolved_ip_hash,
        resolved_context=resolved_context,
        fallback_extra=fallback_extra_mapping,
        fallback_extra_factory=effective_fallback_factory,
        resolved_fallback_extra=resolved_fallback_extra,
    )


__all__ = [
    "AuditHooks",
    "AuditEvent",
    "AuditLogResult",
    "ResolvedIpHash",
    "ResolvedContext",
    "ResolvedFallbackExtra",
    "AuditCallable",
    "HashIpCallable",
    "ContextFactory",
    "FallbackExtraFactory",
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

