"""Reinforcement learning trainers with fee-aware rewards."""
from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict

from .base import (
    MissingDependencyError,
    TrainingResult,
    UncertaintyGate,
    fee_aware_reward,
    require_numpy,
)

if TYPE_CHECKING:  # pragma: no cover - typing only.
    import numpy as np


@dataclass
class RLTrainerConfig:
    algorithm: str
    fee_bps: float
    uncertainty_threshold: float = 0.2
    total_timesteps: int = 10_000
    model_params: Dict[str, Any] = field(default_factory=dict)


class RLTrainer:
    """Wrapper around Stable-Baselines3 algorithms with fee-aware rewards."""

    def __init__(self, config: RLTrainerConfig, env: Any) -> None:
        self.config = config
        self.env = env
        self.uncertainty_gate = UncertaintyGate(config.uncertainty_threshold)
        self.model = self._initialise_model()

    def _initialise_model(self) -> Any:
        algorithm = self.config.algorithm.lower()
        try:
            sb3 = importlib.import_module("stable_baselines3")
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on optional dep.
            raise MissingDependencyError(
                "stable-baselines3 is required for reinforcement learning trainers"
            ) from exc
        if algorithm == "ppo":
            model_cls = getattr(sb3, "PPO")
        elif algorithm == "sac":
            model_cls = getattr(sb3, "SAC")
        elif algorithm == "td3":
            model_cls = getattr(sb3, "TD3")
        else:
            raise ValueError(f"Unsupported RL algorithm: {self.config.algorithm}")
        return model_cls("MlpPolicy", self.env, **self.config.model_params)

    def train(self) -> TrainingResult:
        self.model.learn(total_timesteps=self.config.total_timesteps)
        return TrainingResult(metrics={"timesteps": float(self.config.total_timesteps)}, model=self.model)

    def evaluate(
        self,
        observations: "np.ndarray",
        returns: Any,
        turnovers: Any,
    ) -> Dict[str, float]:
        np = require_numpy()
        actions, _ = self.model.predict(observations, deterministic=False)
        actions = np.asarray(actions)
        uncertainties = np.std(actions, axis=-1) if actions.ndim > 1 else np.abs(actions)
        gated_actions = self.uncertainty_gate.apply(actions, uncertainties)
        rewards = fee_aware_reward(returns, turnovers, self.config.fee_bps)
        expected_reward = float(np.mean(gated_actions * rewards))
        return {"expected_fee_adjusted_reward": expected_reward}


__all__ = ["RLTrainer", "RLTrainerConfig", "MissingDependencyError"]
