"""Regression tests for the CoinGecko ingest script when SQLAlchemy is absent."""

from __future__ import annotations

import asyncio
import builtins
import importlib
import sys
from collections.abc import Callable


def _block_sqlalchemy_imports(monkeypatch) -> None:
    original_import: Callable[..., object] = builtins.__import__

    def _guarded_import(name: str, globals=None, locals=None, fromlist=(), level=0):  # type: ignore[override]
        if name.startswith("sqlalchemy") and name not in sys.modules:
            raise ModuleNotFoundError(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def test_coingecko_ingest_installs_sqlalchemy_stub(monkeypatch) -> None:
    """The ingest script should fall back to the in-repo SQLAlchemy shim."""

    for module_name in list(sys.modules):
        if module_name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.delitem(sys.modules, "services.coingecko_ingest", raising=False)
    _block_sqlalchemy_imports(monkeypatch)

    module = importlib.import_module("services.coingecko_ingest")

    assert module.insert.__module__ == "services.common.sqlalchemy_stub"

    async def _exercise() -> None:
        engine = module.create_async_engine("sqlite:///stub.db")
        async with engine.begin() as connection:
            await connection.execute(
                module.insert(module.BARS_DAILY).values(
                    {
                        "symbol": "btc",
                        "ts": module.datetime.now(module.UTC),
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "volume": 1.0,
                    }
                )
            )

    asyncio.run(_exercise())

    assert getattr(module.BARS_DAILY, "_records", [])
