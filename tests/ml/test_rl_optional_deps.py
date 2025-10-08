from __future__ import annotations

import builtins
import importlib

import pytest


def test_policy_gradient_requires_torch(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.reload(importlib.import_module("ml.models.rl"))

    class _Space:
        shape = (1,)

    class _Env:
        observation_space = _Space()
        action_space = _Space()

        def reset(self):  # type: ignore[no-untyped-def]
            return [0.0]

        def step(self, action):  # type: ignore[no-untyped-def]
            return [0.0], 0.0, True, {"turnover": 0.0}

    original_import = builtins.__import__

    def _guarded_import(name: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if name == "stable_baselines3":
            raise ImportError(name)
        if name.startswith("torch"):
            raise ModuleNotFoundError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    trainer = module.RLTrainer(algorithm="ppo")

    with pytest.raises(module.MissingDependencyError, match="torch is required"):
        trainer.fit(env=_Env(), total_timesteps=1_000)

    monkeypatch.setattr(builtins, "__import__", original_import)
