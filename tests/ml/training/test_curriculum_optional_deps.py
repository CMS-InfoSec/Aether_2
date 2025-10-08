from __future__ import annotations

import builtins
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable

import pytest


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "ml" / "training" / "curriculum.py"


def _load_curriculum_module(module_name: str) -> ModuleType:
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
    for name in ("ml.training.curriculum", "ml.training", "ml"):
        monkeypatch.delitem(sys.modules, name, raising=False)

    original_import = builtins.__import__

    def _guarded_import(name: str, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        top_level = name.split(".", 1)[0]
        if top_level in missing_set:
            raise ModuleNotFoundError(top_level)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    for dependency in ("numpy", "pandas", "torch"):
        if dependency in missing_set:
            continue
        try:
            __import__(dependency)
        except ModuleNotFoundError:
            monkeypatch.setitem(sys.modules, dependency, ModuleType(dependency))


def test_curriculum_requires_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "ml_training_curriculum_missing_numpy"
    _install_dependency_stubs(monkeypatch, missing={"numpy"})
    module = _load_curriculum_module(module_name)
    try:
        with pytest.raises(module.MissingDependencyError, match="numpy"):
            module._require_numpy()
    finally:
        _dispose_module(module_name)


def test_curriculum_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "ml_training_curriculum_missing_pandas"
    _install_dependency_stubs(monkeypatch, missing={"pandas"})
    module = _load_curriculum_module(module_name)
    try:
        with pytest.raises(module.MissingDependencyError, match="pandas"):
            module._require_pandas()
    finally:
        _dispose_module(module_name)


def test_curriculum_requires_torch(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "ml_training_curriculum_missing_torch"
    _install_dependency_stubs(monkeypatch, missing={"torch"})
    module = _load_curriculum_module(module_name)
    try:
        with pytest.raises(module.MissingDependencyError, match="torch"):
            module.build_model("lstm", input_size=4, hidden_size=8, num_layers=1, dropout=0.1, sequence_length=3)
    finally:
        _dispose_module(module_name)
