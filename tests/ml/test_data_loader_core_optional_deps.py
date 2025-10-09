from __future__ import annotations

import builtins
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable

import pytest


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "ml" / "data_loader.py"


def _load_data_loader(module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ModuleNotFoundError(module_name)

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def _dispose_module(module_name: str) -> None:
    sys.modules.pop(module_name, None)


def _install_dependency_stubs(
    monkeypatch: pytest.MonkeyPatch, *, missing: Iterable[str]
) -> None:
    missing_set = frozenset(missing)
    monkeypatch.delitem(sys.modules, "ml.data_loader", raising=False)

    original_import = builtins.__import__

    def _guarded_import(name: str, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        top_level = name.split(".", 1)[0]
        if top_level in missing_set:
            raise ModuleNotFoundError(top_level)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    for dependency in ("numpy", "pandas", "sqlalchemy"):
        if dependency in missing_set:
            continue
        try:
            __import__(dependency)
        except ModuleNotFoundError:
            monkeypatch.setitem(sys.modules, dependency, ModuleType(dependency))


def test_data_loader_requires_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "ml_data_loader_missing_numpy"
    _install_dependency_stubs(monkeypatch, missing={"numpy"})
    module = _load_data_loader(module_name)
    try:
        with pytest.raises(module.MissingDependencyError, match="numpy"):
            module.compute_fee_adjusted_labels([0.1], [0.2], 5)
    finally:
        _dispose_module(module_name)


def test_data_loader_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "ml_data_loader_missing_pandas"
    _install_dependency_stubs(monkeypatch, missing={"pandas"})
    module = _load_data_loader(module_name)
    try:
        engine = object()

        module._require_numpy = lambda: ModuleType("numpy")  # type: ignore[assignment]

        with pytest.raises(module.MissingDependencyError, match="pandas"):
            module._load_base_frame(engine, "select 1", "ts")
    finally:
        _dispose_module(module_name)


def test_data_loader_requires_sqlalchemy(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "ml_data_loader_missing_sqlalchemy"
    _install_dependency_stubs(monkeypatch, missing={"sqlalchemy"})
    module = _load_data_loader(module_name)
    try:
        with pytest.raises(module.MissingDependencyError, match="SQLAlchemy"):
            module._create_engine("postgresql://example")
    finally:
        _dispose_module(module_name)
