from __future__ import annotations

import builtins
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

MODULE_PATH = Path(__file__).resolve().parents[2] / "ml" / "models" / "supervised.py"


def _load_supervised_module(monkeypatch: pytest.MonkeyPatch, *, missing: frozenset[str]) -> ModuleType:
    """Reload the supervised module while simulating missing dependencies."""

    module_name = "tests.ml.supervised_optional"
    monkeypatch.delitem(sys.modules, module_name, raising=False)

    original_import = builtins.__import__

    def _guarded_import(name: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if name in missing:
            raise ModuleNotFoundError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

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
    finally:
        monkeypatch.setattr(builtins, "__import__", original_import)

    return module


def test_supervised_dataset_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_supervised_module(monkeypatch, missing=frozenset({"pandas"}))
    dataset = module.SupervisedDataset(features=object(), labels=object())

    with pytest.raises(module.MissingDependencyError, match="pandas is required"):
        dataset.to_numpy()

    monkeypatch.delitem(sys.modules, module.__name__, raising=False)


def test_supervised_dataset_requires_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    pandas_stub = ModuleType("pandas")

    class _Frame:  # noqa: D401 - minimal stub for tests
        def __init__(self, values):  # type: ignore[no-untyped-def]
            self._values = values

        def to_numpy(self):  # type: ignore[no-untyped-def]
            return self._values

    pandas_stub.DataFrame = _Frame  # type: ignore[attr-defined]
    pandas_stub.Series = _Frame  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pandas", pandas_stub)

    module = _load_supervised_module(monkeypatch, missing=frozenset({"numpy"}))

    dataset = module.SupervisedDataset(
        features=pandas_stub.DataFrame([[1.0]]),
        labels=pandas_stub.Series([0.0]),
    )

    with pytest.raises(module.MissingDependencyError, match="numpy is required"):
        dataset.to_numpy()

    monkeypatch.delitem(sys.modules, module.__name__, raising=False)
