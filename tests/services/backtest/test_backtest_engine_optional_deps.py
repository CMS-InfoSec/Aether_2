"""Regression tests ensuring the backtest engine guards optional dependencies."""

from __future__ import annotations

import builtins
import importlib
import sys
from types import ModuleType
from typing import Optional

import pytest


def _reload_backtest_engine(monkeypatch: pytest.MonkeyPatch, missing: Optional[str] = None) -> ModuleType:
    """Reload ``backtest_engine`` while faking a missing dependency."""

    original_import = builtins.__import__

    def _fake_import(name: str, globals: Optional[dict] = None, locals: Optional[dict] = None, fromlist: tuple = (), level: int = 0):
        if missing and name.split(".")[0] == missing:
            raise ImportError(f"No module named '{name}'")
        return original_import(name, globals, locals, fromlist, level)

    if missing:
        for name in [key for key in list(sys.modules) if key == missing or key.startswith(f"{missing}.")]:
            sys.modules.pop(name, None)
        monkeypatch.setattr(builtins, "__import__", _fake_import)

    sys.modules.pop("backtest_engine", None)
    return importlib.import_module("backtest_engine")


def test_backtest_engine_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_backtest_engine(monkeypatch, missing="pandas")

    with pytest.raises(module.MissingDependencyError, match="pandas is required"):
        module.RollingFeeModel(module.FeeSchedule())


def test_backtest_engine_requires_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_backtest_engine(monkeypatch, missing="numpy")

    class _DummyPolicy:
        def generate(self, timestamp, market_state):  # pragma: no cover - interface stub
            return []

        def reset(self):  # pragma: no cover - interface stub
            return None

    backtester = module.Backtester([], [], _DummyPolicy())

    with pytest.raises(module.MissingDependencyError, match="numpy is required"):
        backtester.run()
