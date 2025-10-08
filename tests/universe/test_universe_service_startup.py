from __future__ import annotations

import importlib
import sys

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine.url import make_url


pytest.importorskip("sqlalchemy")

def _reload_universe_service():
    module_name = "services.universe.universe_service"
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


def test_universe_service_starts_with_default_psycopg2(monkeypatch):
    monkeypatch.delenv("TIMESCALE_DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv(
        "UNIVERSE_DATABASE_URL",
        "postgresql://timescale:password@localhost:5432/aether",
    )

    class _DummyEngine:
        def __init__(self, url: str) -> None:
            self.url = make_url(url)

    monkeypatch.setattr(
        "sqlalchemy.create_engine",
        lambda url, **kwargs: _DummyEngine(url),
        raising=False,
    )

    module = _reload_universe_service()
    monkeypatch.setattr(module, "run_migrations", lambda url=None: None)

    with TestClient(module.app):
        pass

    assert module.ENGINE is not None
    assert module.ENGINE.url.drivername == "postgresql+psycopg2"


def test_universe_service_normalises_postgresql_urls(monkeypatch):
    monkeypatch.delenv("TIMESCALE_DATABASE_URI", raising=False)
    monkeypatch.setenv(
        "UNIVERSE_DATABASE_URL",
        "postgresql+psycopg://timescale:password@localhost:5432/aether",
    )

    class _DummyEngine:
        def __init__(self, url: str) -> None:
            self.url = make_url(url)

    monkeypatch.setattr(
        "sqlalchemy.create_engine",
        lambda url, **kwargs: _DummyEngine(url),
        raising=False,
    )

    module = _reload_universe_service()
    monkeypatch.setattr(module, "run_migrations", lambda url=None: None)

    module.ensure_database()

    assert module.ENGINE is not None
    assert module.ENGINE.url.drivername == "postgresql+psycopg2"
