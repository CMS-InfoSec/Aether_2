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
    base_scheme, plus, driver = normalized_scheme.partition("+")
    if normalized_scheme in _SUPPORTED_POSTGRES_SCHEMES:
        return f"postgresql://{remainder}"

    if plus and driver and base_scheme in {"postgres", "postgresql", "timescale"}:
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


def normalize_sqlalchemy_dsn(
    raw_dsn: str,
    *,
    driver: str = "psycopg2",
    allow_sqlite: bool = True,
    label: str = "Timescale DSN",
) -> str:
    """Normalize a PostgreSQL/Timescale DSN for SQLAlchemy engines.

    ``normalize_postgres_dsn`` ensures the scheme is compatible with psycopg, but
    SQLAlchemy requires the driver name to be embedded in the URI.  This helper
    reuses the shared normaliser and, when targeting PostgreSQL, inserts the
    configured driver while preserving sqlite URLs for test environments.
    """

    normalized = normalize_postgres_dsn(
        raw_dsn, allow_sqlite=allow_sqlite, label=label
    )
    if normalized.startswith("postgresql://"):
        driver_name = driver.strip() or "psycopg2"
        return normalized.replace("postgresql://", f"postgresql+{driver_name}://", 1)
    return normalized


__all__ = ["normalize_postgres_dsn", "normalize_sqlalchemy_dsn"]
