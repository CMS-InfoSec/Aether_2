from __future__ import annotations

import importlib
import sys

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine.url import make_url

from services.universe import universe_service


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

    universe_service = _reload_universe_service()
    monkeypatch.setattr(universe_service, "run_migrations", lambda url=None: None)

    with TestClient(universe_service.app):
        pass

    assert universe_service.ENGINE is not None
    assert universe_service.ENGINE.url.drivername == "postgresql+psycopg2"


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

    universe_service = _reload_universe_service()
    monkeypatch.setattr(universe_service, "run_migrations", lambda url=None: None)

    universe_service.ensure_database()

    assert universe_service.ENGINE is not None
    assert universe_service.ENGINE.url.drivername == "postgresql+psycopg2"
