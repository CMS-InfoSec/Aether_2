import builtins
import importlib
import sys
from types import ModuleType

import pytest


def _purge_modules(prefixes: list[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name, None)


def test_collab_service_imports_without_fastapi(monkeypatch: pytest.MonkeyPatch) -> None:
    """The collaboration service should fall back to the FastAPI shim when missing."""

    _purge_modules(["fastapi", "collab_service"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object) -> ModuleType:
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    module = importlib.import_module("collab_service")

    for attr_name in ("FastAPI", "JSONResponse"):
        candidate = getattr(module, attr_name, None)
        assert callable(candidate), f"{attr_name} should be available on collab_service"
        assert getattr(candidate, "__module__", "").startswith(
            "services.common.fastapi_stub"
        ), f"{attr_name} should originate from the FastAPI shim"
