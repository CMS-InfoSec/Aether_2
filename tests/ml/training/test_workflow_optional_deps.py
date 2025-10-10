import builtins
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "ml" / "training" / "workflow.py"


def _load_workflow_module(module_name: str) -> ModuleType:
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
    """Clear cached modules and guard imports for optional dependencies."""

    for name in (
        "ml.training.workflow",
        "ml.training",
        "ml",
    ):
        monkeypatch.delitem(sys.modules, name, raising=False)

    original_import = builtins.__import__

    def _guarded_import(name: str, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        top_level = name.split(".", 1)[0]
        if top_level in missing:
            raise ModuleNotFoundError(top_level)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    if "pandas" not in missing:
        try:
            __import__("pandas")
        except ModuleNotFoundError:
            monkeypatch.setitem(sys.modules, "pandas", ModuleType("pandas"))

    if "numpy" not in missing:
        try:
            __import__("numpy")
        except ModuleNotFoundError:
            monkeypatch.setitem(sys.modules, "numpy", ModuleType("numpy"))


def _make_minimal_config(module: ModuleType):
    return module.TrainingJobConfig(
        timescale=module.TimescaleSourceConfig(
            uri="postgresql://example",
            table="features",
            entity_column="entity",
            timestamp_column="ts",
            label_column="label",
            feature_columns=["feature"],
        )
    )


def test_run_training_job_requires_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "ml_training_workflow_missing_numpy"
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "0")
    _install_dependency_stubs(monkeypatch, missing=frozenset({"numpy"}))
    module = _load_workflow_module(module_name)
    try:
        config = _make_minimal_config(module)
        with pytest.raises(module.MissingDependencyError, match="numpy"):
            module.run_training_job(config)
    finally:
        _dispose_module(module_name)


def test_run_training_job_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "ml_training_workflow_missing_pandas"
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "0")
    _install_dependency_stubs(monkeypatch, missing=frozenset({"pandas"}))
    module = _load_workflow_module(module_name)
    try:
        config = _make_minimal_config(module)
        with pytest.raises(module.MissingDependencyError, match="pandas"):
            module.run_training_job(config)
    finally:
        _dispose_module(module_name)


def test_run_training_job_requires_torch(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "ml_training_workflow_missing_torch"
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "0")
    _install_dependency_stubs(monkeypatch, missing=frozenset({"torch"}))
    module = _load_workflow_module(module_name)
    try:
        config = _make_minimal_config(module)
        with pytest.raises(module.MissingDependencyError, match="torch"):
            module.run_training_job(config)
    finally:
        _dispose_module(module_name)
