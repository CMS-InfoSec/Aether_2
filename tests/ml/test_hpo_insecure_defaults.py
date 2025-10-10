from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def _reload_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("ML_STATE_DIR", str(tmp_path))
    sys.modules.pop("ml.hpo.optuna_runner", None)
    module = importlib.import_module("ml.hpo.optuna_runner")
    module.np = None  # type: ignore[attr-defined]
    module.pd = None  # type: ignore[attr-defined]
    module.optuna = None  # type: ignore[attr-defined]
    return module


def test_optuna_runner_local_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _reload_module(monkeypatch, tmp_path)

    dataset = SimpleNamespace(features=[], labels=SimpleNamespace(index=[]))
    runner = module.OptunaRunner(
        study_name="demo",
        storage="sqlite://",
        trainer_name="lightgbm",
        dataset=dataset,  # type: ignore[arg-type]
        objective_weights=module.ObjectiveWeights(),
        n_trials=1,
    )

    study = runner.run()
    assert hasattr(study, "best_trial")
    state_file = Path(tmp_path) / "optuna" / "demo.json"
    assert state_file.exists()
