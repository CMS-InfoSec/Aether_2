"""Optional dependency regression tests for the feature build pipeline."""

from __future__ import annotations

import builtins
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "ml" / "features" / "build_features.py"


def _load_build_features_module(
    monkeypatch: pytest.MonkeyPatch,
    *,
    module_name: str,
    missing: frozenset[str] = frozenset(),
    provided: dict[str, ModuleType | object] | None = None,
) -> ModuleType:
    """Load the build_features module with specific dependencies removed."""

    for name in (
        "ml.features.build_features",
        "ml.features",
        "ml",
    ):
        monkeypatch.delitem(sys.modules, name, raising=False)

    if provided:
        for name, module in provided.items():
            monkeypatch.setitem(sys.modules, name, module)

    original_import = builtins.__import__

    def _guarded_import(name: str, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        root = name.split(".")[0]
        if root in missing:
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
    return module


def test_granularity_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_build_features_module(
        monkeypatch,
        module_name="build_features_missing_pandas",
        missing=frozenset({"pandas"}),
    )
    with pytest.raises(module.MissingDependencyError, match="pandas"):
        module._granularity_to_timedelta("5m")


def test_numpy_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_build_features_module(
        monkeypatch,
        module_name="build_features_missing_numpy",
        missing=frozenset({"numpy"}),
    )
    with pytest.raises(module.MissingDependencyError, match="numpy"):
        module._ensure_numpy()


def test_sqlalchemy_required_for_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_build_features_module(
        monkeypatch,
        module_name="build_features_missing_sqlalchemy",
        missing=frozenset({"sqlalchemy"}),
    )
    with pytest.raises(module.MissingDependencyError, match="SQLAlchemy"):
        module._resolve_engine()
