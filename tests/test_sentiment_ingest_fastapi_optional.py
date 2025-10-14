import builtins
import importlib
import sys
from typing import Iterable

import pytest


def _purge_modules(prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name, None)


def test_sentiment_ingest_without_fastapi(monkeypatch: pytest.MonkeyPatch) -> None:
    """The sentiment ingestion module should fall back to the FastAPI shim."""

    _purge_modules([
        "fastapi",
        "sentiment_ingest",
        "services.common.security",
    ])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    module = importlib.import_module("sentiment_ingest")

    assert module.FastAPI.__module__ == "services.common.fastapi_stub"
    assert module.APIRouter.__module__ == "services.common.fastapi_stub"

    class _Repository:
        async def latest(self, symbol: str):  # pragma: no cover - exercised via shim route definitions
            return symbol, "positive", "stub", module.dt.datetime.now(module.dt.UTC)

    class _Service:
        async def ingest_many(self, symbols):  # pragma: no cover - exercised via shim route definitions
            self.symbols = list(symbols)

    app = module.create_app(service=_Service(), repository=_Repository())

    assert getattr(app, "title", None) == "Aether Sentiment Service"
    routes = {route.path for route in getattr(app, "routes", [])}
    assert "/sentiment/latest" in routes
    assert "/sentiment/refresh" in routes
