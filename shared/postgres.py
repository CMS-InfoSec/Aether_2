"""Shared helpers for normalising PostgreSQL/Timescale connection strings."""

from __future__ import annotations

_SUPPORTED_POSTGRES_SCHEMES = {
    "postgres",
    "postgresql",
    "timescale",
    "postgresql+psycopg",
    "postgresql+psycopg2",
}

_SUPPORTED_SQLITE_SCHEMES = {
    "sqlite",
    "sqlite+pysqlite",
}


def normalize_postgres_dsn(raw_dsn: str, *, allow_sqlite: bool = True, label: str = "Timescale DSN") -> str:
    """Coerce supported connection schemes to variants psycopg understands.

    Parameters
    ----------
    raw_dsn:
        Raw URI sourced from configuration.
    allow_sqlite:
        Whether sqlite DSNs are permitted for local development/tests.
    label:
        Human friendly descriptor included in error messages.
    """

    stripped = raw_dsn.strip()
    if not stripped:
        raise RuntimeError(f"{label} cannot be empty once configured.")

    scheme, separator, remainder = stripped.partition("://")
    if not separator:
        raise RuntimeError(
            f"{label} must include a URI scheme such as postgresql:// or sqlite://."
        )

    normalized_scheme = scheme.lower()
    if normalized_scheme in _SUPPORTED_POSTGRES_SCHEMES:
        return f"postgresql://{remainder}"

    if allow_sqlite and normalized_scheme in _SUPPORTED_SQLITE_SCHEMES:
        return f"{normalized_scheme}://{remainder}"

    if allow_sqlite:
        raise RuntimeError(
            f"{label} must use a PostgreSQL/Timescale compatible scheme or sqlite:// for testing."
        )
    raise RuntimeError(
        f"{label} must use a PostgreSQL/Timescale compatible scheme."
    )


__all__ = ["normalize_postgres_dsn"]
