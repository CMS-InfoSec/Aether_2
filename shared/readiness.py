"""Reusable readiness probes for core infrastructure dependencies.

The helpers provided here are designed to be reused by FastAPI and background
services that expose a ``/readyz`` endpoint.  Each probe accepts an existing
client object or an optional *provider* callback used to lazily acquire a
resource.  Providers may return the resource directly or an awaitable.  The
probes are intentionally lightweight and enforce per-step timeouts using
:func:`asyncio.wait_for` to keep Kubernetes readiness checks responsive.

Typical usage::

    from shared import readiness

    async def readyz() -> None:
        await readiness.postgres_read_probe(provider=get_reader)
        await readiness.postgres_write_probe(provider=get_writer)
        await readiness.redis_ping_probe(provider=get_redis)
        await readiness.kafka_metadata_probe(provider=get_kafka_admin)

Where ``get_reader`` and similar callbacks return the configured dependency
clients.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol, TypeVar

__all__ = [
    "DEFAULT_PROBE_TIMEOUT",
    "Provider",
    "ReadinessProbeError",
    "ProbeTimeout",
    "ProviderNotConfigured",
    "ProbeFailure",
    "postgres_read_probe",
    "postgres_write_probe",
    "redis_ping_probe",
    "kafka_metadata_probe",
]

T = TypeVar("T")
DEFAULT_PROBE_TIMEOUT = 3.0
"""Default timeout in seconds for dependency probes.

The helper functions accept a ``timeout`` argument which defaults to three
seconds.  This value works well for Kubernetes readiness probes that typically
allow five seconds for the HTTP handler.  Services with stricter SLAs can lower
this value while still leveraging the shared error handling.
"""

Provider = Callable[[], Awaitable[T] | T]
"""Signature for provider callbacks that supply dependency clients."""


class ReadinessProbeError(RuntimeError):
    """Base error type raised when readiness probes fail."""


@dataclass(slots=True)
class ProbeTimeout(ReadinessProbeError):
    """Raised when an awaited operation exceeds the configured timeout."""

    dependency: str
    """Human readable name for the dependency being probed."""

    timeout: float
    """Timeout limit (seconds) that was exceeded."""

    step: str
    """Description of the specific step that timed out."""

    def __post_init__(self) -> None:  # pragma: no cover - repr glue
        super().__init__(
            f"{self.dependency} probe timed out after {self.timeout:.1f}s while {self.step}."
        )


class ProviderNotConfigured(ReadinessProbeError):
    """Raised when a probe cannot resolve a dependency client."""

    def __init__(self, dependency: str) -> None:
        super().__init__(
            f"{dependency} probe requires either an explicit client instance or a provider callback."
        )
        self.dependency = dependency


class ProbeFailure(ReadinessProbeError):
    """Raised when a probe receives an unexpected response from a dependency."""

    def __init__(self, dependency: str, message: str) -> None:
        super().__init__(f"{dependency} probe failed: {message}")
        self.dependency = dependency
        self.message = message


class SupportsPostgresRead(Protocol):
    """Minimal async interface required for the Postgres read probe."""

    async def fetchval(self, query: str, *args: Any, timeout: float | None = None) -> Any:
        ...


class SupportsPostgresExecute(Protocol):
    """Minimal async interface required for the Postgres write probe."""

    async def execute(self, query: str, *args: Any, timeout: float | None = None) -> Any:
        ...

    async def fetchval(self, query: str, *args: Any, timeout: float | None = None) -> Any:
        ...


class SupportsRedisPing(Protocol):
    """Minimal async interface for Redis clients used in readiness probes."""

    async def ping(self) -> Any:
        ...


class SupportsKafkaMetadata(Protocol):
    """Minimal async interface for Kafka metadata clients."""

    async def list_topics(self) -> Any:
        ...


async def _maybe_await(value: Awaitable[T] | T) -> T:
    if inspect.isawaitable(value):  # pragma: no branch - fast path
        return await value  # type: ignore[return-value]
    return value  # type: ignore[return-value]


async def _within_timeout(coro: Awaitable[T], dependency: str, step: str, timeout: float) -> T:
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError as exc:  # pragma: no cover - defensive surface
        raise ProbeTimeout(dependency, timeout, step) from exc


async def _resolve_dependency(
    dependency: str,
    client: T | None,
    provider: Provider[T] | None,
    timeout: float,
) -> T:
    if client is not None:
        return client
    if provider is None:
        raise ProviderNotConfigured(dependency)

    try:
        provided = provider()
    except Exception as exc:  # pragma: no cover - defensive surface
        raise ProbeFailure(dependency, f"provider callback raised {exc!r}") from exc

    return await _within_timeout(
        _maybe_await(provided),
        dependency,
        "acquiring dependency",
        timeout,
    )


async def postgres_read_probe(
    client: SupportsPostgresRead | None = None,
    *,
    provider: Provider[SupportsPostgresRead] | None = None,
    query: str = "SELECT 1",
    timeout: float = DEFAULT_PROBE_TIMEOUT,
) -> Any:
    """Execute a lightweight read against Postgres.

    Parameters
    ----------
    client / provider:
        Pass an existing async client or a provider callback returning one.
    query:
        SQL statement executed via ``fetchval``. ``SELECT 1`` is used by
        default and is safe for replicas.
    timeout:
        Maximum number of seconds allowed for acquiring the client and executing
        the query.  A :class:`ProbeTimeout` is raised when the limit is
        exceeded.
    """

    dependency = "Postgres (read)"
    executor = await _resolve_dependency(dependency, client, provider, timeout)
    try:
        return await _within_timeout(
            executor.fetchval(query, timeout=timeout),
            dependency,
            "executing read query",
            timeout,
        )
    except ProbeTimeout:
        raise
    except Exception as exc:  # pragma: no cover - defensive surface
        raise ProbeFailure(dependency, f"read query failed with {exc!r}") from exc


async def postgres_write_probe(
    client: SupportsPostgresExecute | None = None,
    *,
    provider: Provider[SupportsPostgresExecute] | None = None,
    read_only_query: str = "SELECT pg_is_in_recovery()",
    timeout: float = DEFAULT_PROBE_TIMEOUT,
) -> None:
    """Validate write readiness against Postgres.

    The default probe checks ``pg_is_in_recovery`` and raises when the server is
    in recovery (read-only) mode.  Deployments that require stronger guarantees
    can supply a custom ``read_only_query`` that exercises a service-owned table
    or stored procedure.
    """

    dependency = "Postgres (write)"
    executor = await _resolve_dependency(dependency, client, provider, timeout)
    try:
        recovery_flag = await _within_timeout(
            executor.fetchval(read_only_query, timeout=timeout),
            dependency,
            "verifying write capability",
            timeout,
        )
    except ProbeTimeout:
        raise
    except Exception as exc:  # pragma: no cover - defensive surface
        raise ProbeFailure(dependency, f"write readiness query failed with {exc!r}") from exc

    normalized = str(recovery_flag).strip().lower()
    if normalized in {"true", "t", "1", "yes"}:
        raise ProbeFailure(dependency, "database is reporting read-only mode")


async def redis_ping_probe(
    client: SupportsRedisPing | None = None,
    *,
    provider: Provider[SupportsRedisPing] | None = None,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
) -> Any:
    """Send a ``PING`` command to Redis and return the response."""

    dependency = "Redis"
    redis = await _resolve_dependency(dependency, client, provider, timeout)
    try:
        return await _within_timeout(
            redis.ping(),
            dependency,
            "pinging",
            timeout,
        )
    except ProbeTimeout:
        raise
    except Exception as exc:  # pragma: no cover - defensive surface
        raise ProbeFailure(dependency, f"ping failed with {exc!r}") from exc


async def kafka_metadata_probe(
    client: SupportsKafkaMetadata | None = None,
    *,
    provider: Provider[SupportsKafkaMetadata] | None = None,
    metadata_fetcher: Callable[[SupportsKafkaMetadata], Awaitable[Any] | Any] | None = None,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
) -> Any:
    """Fetch Kafka topic metadata to ensure brokers are reachable.

    Parameters
    ----------
    metadata_fetcher:
        Optional callable used to retrieve metadata from the client.  When not
        provided, :meth:`list_topics` is invoked.
    """

    dependency = "Kafka"
    kafka = await _resolve_dependency(dependency, client, provider, timeout)

    fetcher = metadata_fetcher or (lambda obj: obj.list_topics())
    try:
        metadata = fetcher(kafka)
    except Exception as exc:  # pragma: no cover - defensive surface
        raise ProbeFailure(dependency, f"metadata fetcher raised {exc!r}") from exc

    try:
        return await _within_timeout(
            _maybe_await(metadata),
            dependency,
            "fetching metadata",
            timeout,
        )
    except ProbeTimeout:
        raise
    except Exception as exc:  # pragma: no cover - defensive surface
        raise ProbeFailure(dependency, f"metadata retrieval failed with {exc!r}") from exc
