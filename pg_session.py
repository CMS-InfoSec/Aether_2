"""PostgreSQL session utilities for FastAPI services.

This module exposes :func:`db_session`, a context manager tailored for
FastAPI endpoints that need to execute SQL queries scoped by authorization
middleware.  The context manager expects the FastAPI ``Request`` to expose a
``db_sessionmaker`` attribute on ``app.state`` (commonly a ``sessionmaker``
instance) and an ``account_scopes`` iterable on ``request.state`` populated by
authorization middleware.  When invoked, it applies the caller's account scopes
via ``set_config`` so that PostgreSQL row-level security policies are enforced
for the duration of the request.  The configuration flag is reset when the
connection is returned to the pool to avoid scope leakage across requests.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Iterator, Sequence, Tuple, cast

try:  # pragma: no cover - FastAPI is optional in some unit test environments
    from fastapi import Request
except Exception:  # pragma: no cover - fallback to satisfy type checkers when FastAPI missing
    Request = Any  # type: ignore[assignment]
try:  # pragma: no cover - SQLAlchemy may be unavailable in lightweight test environments
    from sqlalchemy.engine import Connection
    from sqlalchemy.orm import Session, sessionmaker
except Exception:  # pragma: no cover - provide fallbacks when SQLAlchemy is missing
    Connection = Any  # type: ignore[assignment]
    Session = Any  # type: ignore[assignment]

    class sessionmaker:  # type: ignore[override]
        """Minimal stub allowing isinstance checks when SQLAlchemy is absent."""

        def __call__(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - defensive fallback
            raise RuntimeError("sqlalchemy is required for database session management")

logger = logging.getLogger(__name__)

_SET_SCOPE_SQL = "SELECT set_config('app.account_scopes', %(scopes)s, true)"
_RESET_SCOPE_SQL = "RESET app.account_scopes"

SessionFactory = Callable[[], Session]


def _resolve_session_factory(request: Request) -> SessionFactory:
    """Return the configured SQLAlchemy session factory for *request*."""

    factory = getattr(request.app.state, "db_sessionmaker", None)
    if factory is None:
        raise RuntimeError(
            "FastAPI application state is missing 'db_sessionmaker'. "
            "Configure request.app.state.db_sessionmaker with a sessionmaker instance."
        )
    if isinstance(factory, sessionmaker):
        return cast(SessionFactory, factory)
    if callable(factory):
        return cast(SessionFactory, factory)
    raise RuntimeError(
        "Configured 'db_sessionmaker' is not callable. Ensure it is a sessionmaker or factory function."
    )


def _normalize_account_scopes(scopes: object) -> Tuple[str, ...]:
    """Normalise account scopes from the request state into a tuple of strings."""

    if scopes is None:
        raise RuntimeError(
            "Authorization middleware did not attach account scopes to the request state."
        )

    if isinstance(scopes, str):
        candidates: Iterable[object] = (scopes,)
    else:
        try:
            candidates = cast(Iterable[object], scopes)
        except TypeError as exc:  # pragma: no cover - defensive programming
            raise RuntimeError("Account scopes must be an iterable of strings.") from exc

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        if raw is None:
            continue
        scope = str(raw).strip()
        if not scope or scope in seen:
            continue
        normalized.append(scope)
        seen.add(scope)

    if not normalized:
        raise RuntimeError("Request did not include any usable account scopes.")

    return tuple(normalized)


def _session_in_transaction(session: Session) -> bool:
    """Return ``True`` when *session* currently owns an active transaction."""

    in_transaction = getattr(session, "in_transaction", None)
    if callable(in_transaction):
        try:
            return bool(in_transaction())
        except Exception:  # pragma: no cover - defensive guard against driver quirks
            logger.debug("Failed to determine transaction state via in_transaction", exc_info=True)
    get_transaction = getattr(session, "get_transaction", None)
    if callable(get_transaction):
        try:
            return get_transaction() is not None
        except Exception:  # pragma: no cover - defensive guard against driver quirks
            logger.debug("Failed to determine transaction state via get_transaction", exc_info=True)
    # Fall back to assuming the session is in a transaction; better to err on the
    # side of committing/rolling back than skipping it entirely.
    return True


def _set_account_scopes(connection: Connection, scopes: Sequence[str]) -> bool:
    """Apply the caller's account scopes on the given *connection*."""

    if connection.dialect.name != "postgresql":
        logger.debug(
            "Skipping account scope configuration because dialect is %s", connection.dialect.name
        )
        return False
    joined = ",".join(scopes)
    connection.exec_driver_sql(_SET_SCOPE_SQL, {"scopes": joined})
    return True


def _reset_account_scopes(connection: Connection) -> None:
    """Reset the account scope configuration on *connection*."""

    if connection.dialect.name != "postgresql":
        return
    try:
        reset_conn = connection.execution_options(isolation_level="AUTOCOMMIT")
        reset_conn.exec_driver_sql(_RESET_SCOPE_SQL)
    except Exception:  # pragma: no cover - ensure connection close proceeds
        logger.warning("Failed to reset app.account_scopes on connection", exc_info=True)


@contextmanager
def db_session(request: Request) -> Iterator[Session]:
    """Provide a database session scoped to the request's authorized accounts.

    The returned context manager yields a SQLAlchemy ``Session`` object. It will
    automatically commit when the block exits successfully or roll back if an
    exception is raised.  The Postgres ``app.account_scopes`` session variable is
    set prior to yielding and reset when the connection is returned to the pool.
    """

    scopes = _normalize_account_scopes(getattr(request.state, "account_scopes", None))
    session_factory = _resolve_session_factory(request)
    session: Session | None = None
    connection: Connection | None = None
    applied_scopes = False

    try:
        session = session_factory()
        connection = session.connection()
        applied_scopes = _set_account_scopes(connection, scopes)

        yield session
        if _session_in_transaction(session):
            session.commit()
    except Exception:
        if session is not None and _session_in_transaction(session):
            session.rollback()
        raise
    finally:
        if connection is not None and applied_scopes:
            _reset_account_scopes(connection)
        if session is not None:
            session.close()


__all__ = ["db_session"]
