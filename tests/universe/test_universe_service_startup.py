from __future__ import annotations

import importlib
import sys

import pytest
from fastapi.testclient import TestClient


pytest.importorskip("sqlalchemy")


def _reload_universe_service():
    module_name = "services.universe.universe_service"
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


def test_universe_service_starts_with_default_psycopg2(monkeypatch):
    monkeypatch.delenv("TIMESCALE_DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    universe_service = _reload_universe_service()

    with TestClient(universe_service.app):
        assert universe_service.ENGINE.url.drivername == "postgresql+psycopg2"


def test_universe_service_normalises_postgresql_urls(monkeypatch):
    monkeypatch.delenv("TIMESCALE_DATABASE_URI", raising=False)
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://timescale:password@localhost:5432/aether",
    )

    universe_service = _reload_universe_service()

    assert universe_service.ENGINE.url.drivername == "postgresql+psycopg2"
