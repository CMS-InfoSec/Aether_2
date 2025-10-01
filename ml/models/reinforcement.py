"""Reinforcement learning trainers with fee-aware rewards."""
from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Dict

import numpy as np

from .base import TrainingResult, UncertaintyGate, fee_aware_reward


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
        sb3 = importlib.import_module("stable_baselines3")
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

    def evaluate(self, observations: np.ndarray, returns: np.ndarray, turnovers: np.ndarray) -> Dict[str, float]:
        actions, _ = self.model.predict(observations, deterministic=False)
        actions = np.asarray(actions)
        uncertainties = np.std(actions, axis=-1) if actions.ndim > 1 else np.abs(actions)
        gated_actions = self.uncertainty_gate.apply(actions, uncertainties)
        rewards = fee_aware_reward(returns, turnovers, self.config.fee_bps)
        expected_reward = float(np.mean(gated_actions * rewards))
        return {"expected_fee_adjusted_reward": expected_reward}


__all__ = ["RLTrainer", "RLTrainerConfig"]
