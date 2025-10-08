from __future__ import annotations

import builtins
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

MODULE_PATH = Path(__file__).resolve().parents[2] / "ml" / "models" / "reinforcement.py"


def _load_reinforcement_module(
    monkeypatch: pytest.MonkeyPatch, *, missing: frozenset[str]
) -> ModuleType:
    """Reload the reinforcement module while simulating missing dependencies."""

    module_name = "ml.models.reinforcement_optional"
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


def test_reinforcement_evaluation_requires_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_reinforcement_module(monkeypatch, missing=frozenset({"numpy"}))

    class _StubModel:
        def predict(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
            return [[0.1, 0.2]], None

    monkeypatch.setattr(module.RLTrainer, "_initialise_model", lambda self: _StubModel())

    trainer = module.RLTrainer(module.RLTrainerConfig(algorithm="ppo", fee_bps=5.0), env=object())

    with pytest.raises(module.MissingDependencyError, match="numpy is required"):
        trainer.evaluate(observations=[[0.0]], returns=[0.1, 0.2], turnovers=[0.01, 0.02])

    monkeypatch.delitem(sys.modules, module.__name__, raising=False)
