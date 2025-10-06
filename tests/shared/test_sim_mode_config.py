import asyncio
import importlib
import sys

import pytest


@pytest.fixture
def sim_mode_module(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("AETHER_SIM_MODE_TEST_DSN", raising=False)
    monkeypatch.delenv("SIM_MODE_DATABASE_URL", raising=False)
    sys.modules.pop("shared.sim_mode", None)
    return importlib.import_module("shared.sim_mode")


def test_database_url_normalizes_timescale_scheme(monkeypatch, sim_mode_module):
    monkeypatch.setenv("SIM_MODE_DATABASE_URL", "timescale://user:secret@db.example/sim")
    normalized = sim_mode_module._database_url()
    assert normalized == "postgresql+psycopg2://user:secret@db.example/sim"
    scheme, _, remainder = normalized.partition("://")
    assert scheme.startswith("postgresql+")
    assert remainder == "user:secret@db.example/sim"


def test_dispatch_async_without_running_loop(sim_mode_module):
    invoked = False

    async def _stub():
        nonlocal invoked
        invoked = True

    sim_mode_module._dispatch_async(_stub(), context="test")
    assert invoked


@pytest.mark.asyncio
async def test_dispatch_async_with_running_loop(sim_mode_module):
    invoked = False

    async def _stub():
        nonlocal invoked
        invoked = True

    sim_mode_module._dispatch_async(_stub(), context="test")
    await asyncio.sleep(0)
    assert invoked
