"""Helper utilities for wiring shared readiness probes across services."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable

from fastapi import FastAPI

from . import readiness

try:  # pragma: no cover - SQLAlchemy is optional in lightweight environments
    from sqlalchemy import text as _sa_text
except Exception:  # pragma: no cover - fall back to passthrough when unavailable
    _sa_text = None


__all__ = [
    "SQLAlchemyProbeClient",
    "session_factory_from_state",
    "engine_from_state",
    "using_sqlite",
    "verify_session_store",
]


async def _run_in_thread(func: Callable[[], Any]) -> Any:
    """Execute blocking *func* in a worker thread and return the result."""

    try:  # pragma: no branch - Python 3.9+ fast path
        to_thread = asyncio.to_thread
    except AttributeError:  # pragma: no cover - Python < 3.9 fallback
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func)
    return await to_thread(func)


class SQLAlchemyProbeClient:
    """Adapter exposing async helpers compatible with readiness probes."""

    def __init__(self, session_factory: Callable[[], Any]) -> None:
        self._session_factory = session_factory

    async def fetchval(self, query: str, *args: Any, timeout: float | None = None) -> Any:
        del args, timeout  # readiness helpers enforce per-step timeouts

        def _execute() -> Any:
            session = self._session_factory()
            try:
                statement = _sa_text(query) if callable(_sa_text) else query
                result = session.execute(statement)
                scalar = getattr(result, "scalar", None)
                return scalar() if callable(scalar) else result
            finally:
                if hasattr(session, "close"):
                    session.close()

        return await _run_in_thread(_execute)

    async def execute(self, query: str, *args: Any, timeout: float | None = None) -> Any:
        del args, timeout

        def _execute() -> Any:
            session = self._session_factory()
            try:
                statement = _sa_text(query) if callable(_sa_text) else query
                session.execute(statement)
                commit = getattr(session, "commit", None)
                if callable(commit):
                    commit()
            except Exception:
                rollback = getattr(session, "rollback", None)
                if callable(rollback):
                    rollback()
                raise
            finally:
                if hasattr(session, "close"):
                    session.close()

        return await _run_in_thread(_execute)


def session_factory_from_state(
    app: FastAPI,
    *,
    attribute: str = "db_sessionmaker",
    dependency_name: str = "database",
) -> Callable[[], Any]:
    """Resolve a SQLAlchemy session factory stored on *app.state*."""

    session_factory = getattr(app.state, attribute, None)
    if session_factory is None:
        raise readiness.ProviderNotConfigured(dependency_name)
    if not callable(session_factory):  # pragma: no cover - defensive guard
        raise readiness.ProviderNotConfigured(dependency_name)
    return session_factory


def engine_from_state(app: FastAPI, *, attribute: str = "db_engine") -> Any:
    """Return a database engine stored on *app.state* when available."""

    return getattr(app.state, attribute, None)


def using_sqlite(engine: Any) -> bool:
    """Return ``True`` when *engine* represents a SQLite backend."""

    if engine is None:
        return False
    dialect = getattr(engine, "dialect", None)
    backend = getattr(dialect, "name", "") if dialect is not None else ""
    return str(backend).lower() == "sqlite"


async def verify_session_store(store: Any) -> None:
    """Execute readiness checks against a configured session store."""

    if store is None:
        raise readiness.ProviderNotConfigured("Session store")

    redis_client = getattr(store, "_redis", None)
    if redis_client is not None:
        await readiness.redis_ping_probe(client=redis_client)
        return

    ping = getattr(store, "ping", None)
    if callable(ping):
        await readiness.redis_ping_probe(client=store)  # type: ignore[arg-type]
        return

    getter = getattr(store, "get", None)
    if callable(getter):
        try:
            result = getter("__readyz_probe__")
            if inspect.isawaitable(result):
                await result  # pragma: no cover - defensive async guard
        except Exception as exc:  # pragma: no cover - defensive surface
            raise readiness.ProbeFailure(
                "Session store", f"probe lookup failed with {exc!r}"
            ) from exc
        return

    raise readiness.ProbeFailure(
        "Session store", "configured store does not expose a usable interface"
    )
