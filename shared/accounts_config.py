"""Configuration helpers for the accounts services."""

from __future__ import annotations

import os
from typing import Mapping

from shared.postgres import normalize_sqlalchemy_dsn

_SQLITE_FLAG = "ACCOUNTS_ALLOW_SQLITE_FOR_TESTS"
_DRIVER_ENV = "ACCOUNTS_SQLALCHEMY_DRIVER"
_DSN_ENV_KEYS = ("ACCOUNTS_DATABASE_URL", "TIMESCALE_DSN", "DATABASE_URL")


def _coerce_env(env: Mapping[str, str] | None) -> Mapping[str, str]:
    if env is None:
        return os.environ
    return env


def _extract_driver_from_dsn(raw_dsn: str) -> str | None:
    """Return the explicit SQLAlchemy driver embedded in a DSN, if present."""

    stripped = raw_dsn.strip()
    scheme, separator, _ = stripped.partition("://")
    if not separator:
        return None

    base_scheme, plus, driver = scheme.partition("+")
    if not plus:
        return None

    driver_name = driver.strip()
    if not driver_name:
        return None

    if base_scheme.lower() not in {"postgresql", "postgres", "timescale"}:
        return None

    return driver_name


def resolve_accounts_database_url(*, env: Mapping[str, str] | None = None) -> str:
    """Return a normalised SQLAlchemy DSN for the accounts persistence layer."""

    source = _coerce_env(env)
    allow_sqlite = source.get(_SQLITE_FLAG) == "1"
    configured_driver = (source.get(_DRIVER_ENV) or "").strip()

    for key in _DSN_ENV_KEYS:
        raw = source.get(key)
        if raw is None:
            continue
        value = str(raw).strip()
        if not value:
            raise RuntimeError(
                f"{key} is set but empty; configure a valid PostgreSQL/Timescale DSN."
            )
        label = "Accounts database DSN" if key == "ACCOUNTS_DATABASE_URL" else f"{key} DSN"
        driver_override = _extract_driver_from_dsn(value)
        effective_driver = configured_driver or driver_override or "psycopg2"

        return normalize_sqlalchemy_dsn(
            value,
            driver=effective_driver,
            allow_sqlite=allow_sqlite,
            label=label,
        )

    raise RuntimeError(
        "Accounts database DSN is not configured. Set ACCOUNTS_DATABASE_URL or provide "
        "TIMESCALE_DSN/DATABASE_URL."
    )


__all__ = ["resolve_accounts_database_url"]
