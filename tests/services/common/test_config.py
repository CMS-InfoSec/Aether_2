import importlib.util
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
def _clear_timescale_cache():
    config.get_timescale_session.cache_clear()
    try:
        yield
    finally:
        config.get_timescale_session.cache_clear()


def test_get_timescale_session_requires_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("AETHER_COMPANY_TIMESCALE_DSN", raising=False)

    with pytest.raises(RuntimeError, match="Timescale DSN is not configured"):
        config.get_timescale_session("company")


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

