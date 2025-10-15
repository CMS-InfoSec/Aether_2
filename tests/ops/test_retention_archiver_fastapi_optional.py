"""Retention archiver should keep exposing its router without FastAPI."""

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


def test_retention_archiver_fastapi_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    """ops.logging.retention_archiver should fall back to lightweight stubs."""

    _purge_modules(["fastapi", "ops.logging.retention_archiver"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    module = importlib.import_module("ops.logging.retention_archiver")

    router = module.router
    assert router.prefix == "/logging/retention"
    assert router.tags == ["logging"]
    assert any(route.endpoint is module.retention_status for route in router.routes)

    with pytest.raises(module.HTTPException) as excinfo:
        module.retention_status()

    assert excinfo.value.status_code == 503
    assert excinfo.value.detail == "Retention archiver not configured"
