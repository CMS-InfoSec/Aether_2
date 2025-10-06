"""Shared helpers for normalising PostgreSQL/Timescale connection strings."""

from __future__ import annotations

import re

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

_SCHEMA_INVALID_CHARS = re.compile(r"[^a-z0-9_]")


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


def normalize_postgres_schema(
    raw: str,
    *,
    label: str = "Postgres schema",
    prefix_if_missing: str | None = None,
    allow_leading_digit_prefix: bool = False,
) -> str:
    """Return a PostgreSQL-safe schema identifier."""

    candidate = raw.strip().lower().replace("-", "_")
    candidate = _SCHEMA_INVALID_CHARS.sub("", candidate)
    candidate = re.sub(r"_+", "_", candidate).strip("_")

    if not candidate:
        raise RuntimeError(f"{label} cannot be empty once configured")

    if candidate[0].isdigit():
        if prefix_if_missing and allow_leading_digit_prefix:
            candidate = f"{prefix_if_missing}{candidate}"
        else:
            raise RuntimeError(
                f"{label} must not start with a digit; adjust the configured value"
            )

    if prefix_if_missing and not candidate.startswith(prefix_if_missing):
        candidate = f"{prefix_if_missing}{candidate}"

    if len(candidate) > 63:
        raise RuntimeError(f"{label} must be 63 characters or fewer once normalised")

    return candidate


__all__ = [
    "normalize_postgres_dsn",
    "normalize_sqlalchemy_dsn",
    "normalize_postgres_schema",
]
