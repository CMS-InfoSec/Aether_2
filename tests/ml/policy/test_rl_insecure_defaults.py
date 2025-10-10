from __future__ import annotations

import builtins
import importlib
import sys
from pathlib import Path

import pytest


class _DummySpace:
    def __init__(self, dim: int) -> None:
        self.shape = (dim,)


class _DeterministicEnv:
    action_space = _DummySpace(2)
    observation_space = _DummySpace(2)

    def reset(self):  # type: ignore[no-untyped-def]
        return [0.0, 0.0]

    def step(self, action):  # type: ignore[no-untyped-def]
        return [0.0, 0.0], 1.0, True, {"turnover": 0.0}


def _reload_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("ML_STATE_DIR", str(tmp_path))
    sys.modules.pop("ml.models.rl", None)
    module = importlib.import_module("ml.models.rl")

    original_import = builtins.__import__

    def _guarded_import(name: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if name in {"stable_baselines3", "torch"}:
            raise ModuleNotFoundError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)
    return module


def test_rl_trainer_uses_zero_policy_without_torch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _reload_module(monkeypatch, tmp_path)
    trainer = module.RLTrainer(algorithm="ppo")
    env = _DeterministicEnv()

    trainer.fit(env, total_timesteps=100)
    action = trainer.predict([0.0, 0.0])
    assert list(action) == [0.0, 0.0]

    output = tmp_path / "policy.json"
    trainer.save(str(output))
    assert output.exists()
    fallback_state = Path(tmp_path) / "rl" / "ppo_policy.json"
    assert fallback_state.exists()
