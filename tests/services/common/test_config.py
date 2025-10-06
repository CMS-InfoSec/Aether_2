import importlib.util
import importlib
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module spec for {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


for module_name, module_path in (
    ("services", ROOT / "services" / "__init__.py"),
    ("services.common", ROOT / "services" / "common" / "__init__.py"),
    ("services.common.config", ROOT / "services" / "common" / "config.py"),
):
    if module_name not in sys.modules:
        _load_module(module_name, module_path)


import services.common.config as config


@pytest.fixture(autouse=True)
def _clear_config_cache():
    config.get_timescale_session.cache_clear()
    config.get_redis_client.cache_clear()
    try:
        yield
    finally:
        config.get_timescale_session.cache_clear()
        config.get_redis_client.cache_clear()


def test_get_timescale_session_requires_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("AETHER_COMPANY_TIMESCALE_DSN", raising=False)

    with pytest.raises(RuntimeError, match="Timescale DSN is not configured"):
        config.get_timescale_session("company")


def test_get_redis_client_requires_explicit_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AETHER_COMPANY_REDIS_DSN", raising=False)

    with pytest.raises(RuntimeError, match="Redis DSN is not configured"):
        config.get_redis_client("company")


def test_get_redis_client_rejects_blank_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AETHER_COMPANY_REDIS_DSN", "   ")

    with pytest.raises(RuntimeError, match="is set but empty"):
        config.get_redis_client("company")


def test_get_redis_client_accepts_memory_scheme_for_tests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AETHER_COMPANY_REDIS_DSN", "memory://")

    client = config.get_redis_client("company")
    assert client.dsn == "memory://"


def test_get_timescale_session_uses_account_specific_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_DSN", "postgresql://global.example/aether")
    monkeypatch.setenv(
        "AETHER_DIRECTOR1_TIMESCALE_DSN",
        "sqlite:///tmp/director1.db",
    )

    session = config.get_timescale_session("director1")
    assert session.dsn == "sqlite:///tmp/director1.db"


def test_get_timescale_session_normalizes_postgres_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_DSN", "Postgres://user:pass@host:5432/db")

    session = config.get_timescale_session("company")
    assert session.dsn == "postgresql://user:pass@host:5432/db"


def test_get_timescale_session_allows_sqlite_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.setenv("AETHER_COMPANY_TIMESCALE_DSN", "SQLite+PySQLite:///:memory:")

    session = config.get_timescale_session("company")
    assert session.dsn == "sqlite+pysqlite:///:memory:"


def test_get_timescale_session_rejects_unsupported_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_DSN", "mysql://user:pass@host:3306/db")

    with pytest.raises(RuntimeError, match="Timescale DSN must use a PostgreSQL/Timescale"):
        config.get_timescale_session("company")


def test_get_timescale_session_sanitizes_default_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_DSN", "postgresql://user:pass@host:5432/db")

    session = config.get_timescale_session("Director-1")

    assert session.account_schema == "acct_director_1"


def test_get_timescale_session_sanitizes_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_DSN", "postgresql://user:pass@host:5432/db")
    monkeypatch.setenv("AETHER_COMPANY_TIMESCALE_SCHEMA", " Trading-Agg \n")

    session = config.get_timescale_session("company")

    assert session.account_schema == "trading_agg"


def test_get_timescale_session_rejects_invalid_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_DSN", "postgresql://user:pass@host:5432/db")
    monkeypatch.setenv("AETHER_COMPANY_TIMESCALE_SCHEMA", "123bad")

    with pytest.raises(RuntimeError, match="Timescale schema must not start with a digit"):
        config.get_timescale_session("company")


def test_get_timescale_session_rejects_blank_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_DSN", "postgresql://user:pass@host:5432/db")
    monkeypatch.setenv("AETHER_COMPANY_TIMESCALE_SCHEMA", "  \t  ")

    with pytest.raises(RuntimeError, match="is set but empty; configure a valid schema"):
        config.get_timescale_session("company")


def test_get_timescale_session_rejects_overlong_default_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_DSN", "postgresql://user:pass@host:5432/db")

    overlong_account = "director" + "x" * 70

    with pytest.raises(RuntimeError, match="63 characters or fewer"):
        config.get_timescale_session(overlong_account)


def test_get_timescale_session_rejects_overlong_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_DSN", "postgresql://user:pass@host:5432/db")
    monkeypatch.setenv("AETHER_COMPANY_TIMESCALE_SCHEMA", "schema" + "y" * 70)

    with pytest.raises(RuntimeError, match="63 characters or fewer"):
        config.get_timescale_session("company")
