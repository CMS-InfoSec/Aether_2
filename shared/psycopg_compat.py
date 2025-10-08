"""Typed compatibility shims for the optional psycopg dependency."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType, TracebackType
from typing import Any, Mapping, Protocol, cast


class PsycopgCursor(Protocol):
    """Minimal cursor protocol implemented by psycopg cursors."""

    def execute(self, query: Any, params: Mapping[str, Any] | None = None) -> Any:
        ...

    def fetchone(self) -> Mapping[str, Any] | None:
        ...

    def __enter__(self) -> PsycopgCursor:
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        ...


class PsycopgConnection(Protocol):
    """Subset of the psycopg connection API exercised by the services."""

    def cursor(self, *args: Any, **kwargs: Any) -> PsycopgCursor:
        ...

    def __enter__(self) -> PsycopgConnection:
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        ...


class PsycopgModule(Protocol):
    """Runtime module contract for ``psycopg`` imports."""

    def connect(self, dsn: str, **kwargs: Any) -> PsycopgConnection:
        ...


class SqlModule(Protocol):
    """Protocol capturing the psycopg ``sql`` helper functions used in code."""

    def SQL(self, template: str) -> Any:
        ...

    def Identifier(self, name: str) -> Any:
        ...


class RowFactory(Protocol):
    """Callable signature for psycopg row factories such as ``dict_row``."""

    def __call__(self, cursor: Any) -> Any:
        ...


def load_psycopg() -> tuple[PsycopgModule | None, SqlModule | None]:
    """Attempt to import psycopg and return typed module handles if available."""

    try:
        psycopg_module = import_module("psycopg")
    except Exception:  # pragma: no cover - optional dependency during CI
        return None, None

    sql_module: ModuleType | None
    try:
        sql_module = import_module("psycopg.sql")
    except Exception:  # pragma: no cover - optional dependency during CI
        sql_module = None

    psycopg_typed = cast(PsycopgModule, psycopg_module)
    sql_typed = cast(SqlModule | None, sql_module)
    return psycopg_typed, sql_typed


def load_dict_row() -> RowFactory | None:
    """Return the psycopg ``dict_row`` factory when the dependency is available."""

    try:
        rows_module = import_module("psycopg.rows")
    except Exception:  # pragma: no cover - optional dependency during CI
        return None

    factory = getattr(rows_module, "dict_row", None)
    if factory is None:
        return None
    return cast(RowFactory, factory)


__all__ = [
    "PsycopgCursor",
    "PsycopgConnection",
    "PsycopgModule",
    "SqlModule",
    "RowFactory",
    "load_psycopg",
    "load_dict_row",
]
