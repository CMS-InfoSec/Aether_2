"""Optional dependency regression tests for auto feature discovery."""

from __future__ import annotations

import builtins
import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "auto_feature_discovery.py"


def _load_feature_module(module_name: str) -> ModuleType:
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


def _install_dependency_stubs(
    monkeypatch: pytest.MonkeyPatch, missing: frozenset[str]
) -> None:
    """Install lightweight dependency stubs for the discovery module."""

    for name in ("pandas", "numpy", "lightgbm", "psycopg", "psycopg.rows"):
        monkeypatch.delitem(sys.modules, name, raising=False)

    original_import = builtins.__import__

    def _guarded_import(name: str, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        if name in missing:
            raise ImportError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    if "numpy" not in missing:
        numpy_module = ModuleType("numpy")
        numpy_module.arange = lambda n: list(range(n))  # type: ignore[attr-defined]
        numpy_module.asarray = lambda data: data  # type: ignore[attr-defined]
        numpy_module.mean = lambda values: sum(values) / len(values) if values else 0.0  # type: ignore[attr-defined]
        numpy_module.std = lambda values: 0.0  # type: ignore[attr-defined]
        numpy_module.sign = lambda values: [1 if value >= 0 else -1 for value in values]  # type: ignore[attr-defined]
        numpy_module.unique = lambda values: list(dict.fromkeys(values))  # type: ignore[attr-defined]
        numpy_module.sqrt = __import__("math").sqrt  # type: ignore[attr-defined]
        numpy_module.inf = float("inf")  # type: ignore[attr-defined]
        numpy_module.ndarray = list  # type: ignore[attr-defined]
        numpy_module.float64 = float  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "numpy", numpy_module)

    if "pandas" not in missing:
        pandas_module = ModuleType("pandas")
        pandas_module.DataFrame = object  # type: ignore[attr-defined]
        pandas_module.Series = object  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "pandas", pandas_module)

    if "lightgbm" not in missing:
        lightgbm_module = ModuleType("lightgbm")

        class _Dataset:  # noqa: D401 - minimal stub for tests
            def __init__(self, *args: object, **kwargs: object) -> None:  # noqa: D401 - stub
                self.args = args
                self.kwargs = kwargs

        class _Model:  # noqa: D401 - minimal stub for tests
            def predict(self, data: object) -> list[float]:  # noqa: D401 - stub
                return [0.5] * len(data)  # type: ignore[arg-type]

        def _train(*args: object, **kwargs: object) -> _Model:  # noqa: D401 - stub
            return _Model()

        lightgbm_module.Dataset = _Dataset  # type: ignore[attr-defined]
        lightgbm_module.train = _train  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "lightgbm", lightgbm_module)

    if "psycopg" not in missing:
        psycopg_module = ModuleType("psycopg")

        class _Cursor:
            def __enter__(self) -> "_Cursor":
                return self

            def __exit__(self, *exc: object) -> None:
                return None

            def execute(self, *args: object, **kwargs: object) -> None:
                return None

            def executemany(self, *args: object, **kwargs: object) -> None:
                return None

        class _Connection:
            def __enter__(self) -> "_Connection":
                return self

            def __exit__(self, *exc: object) -> None:
                return None

            def cursor(self) -> _Cursor:
                return _Cursor()

            def commit(self) -> None:
                return None

        def _connect(*args: object, **kwargs: object) -> _Connection:
            return _Connection()

        psycopg_module.connect = _connect  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "psycopg", psycopg_module)

        rows_module = ModuleType("psycopg.rows")
        rows_module.dict_row = SimpleNamespace()  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "psycopg.rows", rows_module)


def _dispose_module(module_name: str) -> None:
    sys.modules.pop(module_name, None)


def test_feature_discovery_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "tests.ml.auto_feature_missing_pandas"
    _install_dependency_stubs(monkeypatch, missing=frozenset({"pandas"}))

    module = _load_feature_module(module_name)
    try:
        config = module.FeatureDiscoveryConfig(timescale_dsn="postgresql://user:pass@localhost/db")
        with pytest.raises(module.MissingDependencyError, match="pandas is required"):
            module.FeatureDiscoveryEngine(config)
    finally:
        _dispose_module(module_name)


def test_feature_discovery_requires_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "tests.ml.auto_feature_missing_numpy"
    _install_dependency_stubs(monkeypatch, missing=frozenset({"numpy"}))

    module = _load_feature_module(module_name)
    try:
        config = module.FeatureDiscoveryConfig(timescale_dsn="postgresql://user:pass@localhost/db")
        with pytest.raises(module.MissingDependencyError, match="numpy is required"):
            module.FeatureDiscoveryEngine(config)
    finally:
        _dispose_module(module_name)

