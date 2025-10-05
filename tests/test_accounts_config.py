from __future__ import annotations

import pytest

from shared.accounts_config import resolve_accounts_database_url


def test_accounts_database_url_requires_configuration() -> None:
    with pytest.raises(RuntimeError, match="Accounts database DSN is not configured"):
        resolve_accounts_database_url(env={})


def test_accounts_database_url_rejects_blank_values() -> None:
    with pytest.raises(RuntimeError, match="is set but empty"):
        resolve_accounts_database_url(env={"ACCOUNTS_DATABASE_URL": "   "})


def test_accounts_database_url_normalizes_postgres_scheme() -> None:
    url = resolve_accounts_database_url(env={"ACCOUNTS_DATABASE_URL": "postgres://user:pass@localhost/db"})
    assert url == "postgresql+psycopg2://user:pass@localhost/db"


def test_accounts_database_url_allows_sqlite_when_flag_enabled(tmp_path) -> None:
    db_path = tmp_path / "accounts.db"
    url = resolve_accounts_database_url(
        env={
            "ACCOUNTS_DATABASE_URL": f"sqlite:///{db_path}",
            "ACCOUNTS_ALLOW_SQLITE_FOR_TESTS": "1",
        }
    )
    assert url == f"sqlite:///{db_path}"


def test_accounts_database_url_honours_fallback_envs() -> None:
    url = resolve_accounts_database_url(env={"TIMESCALE_DSN": "postgresql://example.com/aether"})
    assert url == "postgresql+psycopg2://example.com/aether"


def test_accounts_database_url_uses_configured_driver() -> None:
    url = resolve_accounts_database_url(
        env={
            "ACCOUNTS_DATABASE_URL": "postgresql://example.com/aether",
            "ACCOUNTS_SQLALCHEMY_DRIVER": "psycopg",
        }
    )
    assert url == "postgresql+psycopg://example.com/aether"
