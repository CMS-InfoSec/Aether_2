"""Regression coverage for the distillation workflow optional dependencies."""

from __future__ import annotations

import builtins
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "distillation.py"


def _load_distillation_module(module_name: str) -> ModuleType:
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
    monkeypatch: pytest.MonkeyPatch, *, missing: frozenset[str]
) -> None:
    """Remove cached modules and guard imports for optional dependencies."""

    for name in (
        "distillation",
        "ml",
        "ml.experiment_tracking",
        "ml.experiment_tracking.model_registry",
        "shared",
        "shared.models",
        "shared.models.registry",
    ):
        monkeypatch.delitem(sys.modules, name, raising=False)

    original_import = builtins.__import__

    def _guarded_import(name: str, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        if name in missing:
            raise ModuleNotFoundError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def test_run_distillation_requires_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "distillation_missing_numpy"
    _install_dependency_stubs(monkeypatch, missing=frozenset({"numpy"}))
    module = _load_distillation_module(module_name)
    try:
        with pytest.raises(module.MissingDependencyError, match="numpy"):
            module.run_distillation(
                teacher_id="demo-teacher",
                student_size="tiny",
                num_samples=8,
                random_seed=3,
            )
    finally:
        _dispose_module(module_name)


def test_build_pyfunc_frame_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "distillation_missing_pandas"
    _install_dependency_stubs(monkeypatch, missing=frozenset({"pandas"}))
    module = _load_distillation_module(module_name)
    try:

        class _FeatureMatrix:
            def __init__(self, rows: list[list[float]]) -> None:
                self._rows = rows

            def __len__(self) -> int:  # pragma: no cover - trivial
                return len(self._rows)

            def __getitem__(self, item: Any):  # type: ignore[no-untyped-def]
                if isinstance(item, tuple):
                    row_selector, column_idx = item
                    if isinstance(row_selector, slice):
                        rows = self._rows[row_selector]
                    elif row_selector is Ellipsis:
                        rows = self._rows
                    else:
                        rows = [self._rows[row_selector]]
                    return [row[column_idx] for row in rows]
                return self._rows[item]

        dataset = module.DistillationDataset(
            features=_FeatureMatrix([[1.0, 2.0], [3.0, 4.0]]),
            feature_names=("feature_0", "feature_1"),
            book_snapshots=(
                {"spread_bps": 1.0, "imbalance": 0.1},
                {"spread_bps": 2.0, "imbalance": -0.2},
            ),
            states=(
                {"conviction": 0.6, "liquidity_score": 0.7},
                {"conviction": 0.4, "liquidity_score": 0.8},
            ),
            labels=[0.0, 0.0],
        )

        with pytest.raises(module.MissingDependencyError, match="pandas"):
            module._build_pyfunc_frame(dataset)
    finally:
        _dispose_module(module_name)
