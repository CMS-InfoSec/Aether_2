import builtins
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "auto_tuner.py"


def _install_dependency_guards(monkeypatch: pytest.MonkeyPatch, missing: frozenset[str]) -> None:
    """Patch imports so listed dependencies raise ``ImportError`` during load."""

    for name in (
        "numpy",
        "pandas",
        "torch",
        "torch.utils.data",
        "optuna",
        "mlflow",
        "mlflow.pytorch",
        "sqlalchemy",
    ):
        monkeypatch.delitem(sys.modules, name, raising=False)

    original_import = builtins.__import__

    def _guarded_import(name: str, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        if name in missing or any(name.startswith(f"{module}.") for module in missing):
            raise ImportError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def _load_auto_tuner(
    module_name: str, monkeypatch: pytest.MonkeyPatch, missing: frozenset[str]
) -> ModuleType:
    _install_dependency_guards(monkeypatch, missing)

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


@pytest.mark.parametrize(
    "missing, helper",
    [
        (frozenset({"numpy"}), "_require_numpy"),
        (frozenset({"pandas"}), "_require_pandas"),
        (frozenset({"torch"}), "_require_torch"),
        (frozenset({"optuna"}), "_require_optuna"),
        (frozenset({"mlflow"}), "_require_mlflow"),
        (frozenset({"sqlalchemy"}), "_require_sqlalchemy"),
    ],
)
def test_auto_tuner_missing_dependencies(
    monkeypatch: pytest.MonkeyPatch, missing: frozenset[str], helper: str
) -> None:
    module_name = f"tests.ml.auto_tuner_missing_{next(iter(missing)).replace('.', '_')}"
    module = _load_auto_tuner(module_name, monkeypatch, missing)
    try:
        require_helper = getattr(module, helper)
        with pytest.raises(module.MissingDependencyError, match="is required"):
            require_helper()
    finally:
        _dispose_module(module_name)
