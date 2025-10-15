from __future__ import annotations

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


def test_chaos_monkey_fastapi_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    """Chaos monkey should provide lightweight FastAPI/JSON fallbacks."""

    _purge_modules(["fastapi", "ops.chaos.chaos_monkey"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    module = importlib.import_module("ops.chaos.chaos_monkey")

    config = module.ChaosConfig(actions=[module.ChaosAction("kill", weight=1.0)])
    app = module.build_app(config)

    assert "/chaos/status" in app.routes
    route = app.routes["/chaos/status"].endpoint

    response = route()
    assert response.status_code == 200
    assert response.content["config"]["min_interval_seconds"] == config.min_interval_seconds
    assert response.content["config"]["max_interval_seconds"] == config.max_interval_seconds
    assert response.content["config"]["actions"] == [
        {"name": "kill", "weight": 1.0, "args": {}}
    ]

