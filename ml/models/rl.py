"""Reinforcement learning trainers with fee-aware rewards."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

import numpy as np

from ml.experiment_tracking.mlflow_utils import MLFlowExperiment

LOGGER = logging.getLogger(__name__)


class Environment(Protocol):
    """Protocol defining the minimal API for trading environments."""

    action_space: Any
    observation_space: Any

    def reset(self) -> np.ndarray:
        ...

    def step(self, action: np.ndarray) -> tuple[np.ndarray, float, bool, Dict[str, Any]]:
        ...


@dataclass
class FeeAwareReward:
    """Utility for adjusting rewards for transaction costs."""

    taker_fee_bps: float = 0.0
    maker_fee_bps: float = 0.0
    borrow_cost_bps: float = 0.0

    def apply(self, gross_return: float, turnover: float) -> float:
        fees = turnover * (self.taker_fee_bps + self.maker_fee_bps) / 10_000
        financing = turnover * self.borrow_cost_bps / 10_000
        adjusted = gross_return - fees - financing
        LOGGER.debug(
            "Fee adjustment: gross=%s turnover=%s adjusted=%s", gross_return, turnover, adjusted
        )
        return adjusted


@dataclass
class RLTrainer:
    """Generic reinforcement learning trainer."""

    algorithm: str
    fee_reward: FeeAwareReward = field(default_factory=FeeAwareReward)
    experiment: Optional[MLFlowExperiment] = None
    _model: Any = None

    def fit(
        self,
        env: Environment,
        total_timesteps: int,
        eval_env: Optional[Environment] = None,
        eval_freq: int = 10_000,
        **kwargs: Any,
    ) -> Any:
        """Train the configured RL agent.

        The trainer defers to ``stable_baselines3`` implementations for
        algorithms like PPO, A2C, and SAC when available. A simple fallback
        policy gradient implementation is provided for environments where the
        dependency is not installed.
        """

        try:
            from stable_baselines3 import A2C, PPO, SAC
        except ImportError:  # pragma: no cover - optional dependency.
            LOGGER.warning(
                "stable-baselines3 is not installed. Falling back to a naive policy gradient."
            )
            return self._policy_gradient(env, total_timesteps, **kwargs)

        algo_cls = {
            "ppo": PPO,
            "a2c": A2C,
            "sac": SAC,
        }.get(self.algorithm.lower())
        if algo_cls is None:
            raise ValueError(f"Unsupported algorithm '{self.algorithm}'")

        model = algo_cls("MlpPolicy", env, **kwargs)
        self._model = model
        self._log_params(kwargs)
        model.learn(total_timesteps=total_timesteps, eval_env=eval_env, eval_freq=eval_freq)
        LOGGER.info("Trained %s agent for %d timesteps", self.algorithm.upper(), total_timesteps)
        return model

    def _policy_gradient(self, env: Environment, total_timesteps: int, **kwargs: Any) -> Any:
        """Fallback REINFORCE-style policy gradient."""

        import torch
        import torch.nn as nn
        import torch.optim as optim

        obs_dim = env.observation_space.shape[0]
        act_dim = env.action_space.shape[0]

        class Policy(nn.Module):
            def __init__(self, obs_dim: int, act_dim: int) -> None:
                super().__init__()
                self.net = nn.Sequential(
                    nn.Linear(obs_dim, 64),
                    nn.ReLU(),
                    nn.Linear(64, 64),
                    nn.ReLU(),
                    nn.Linear(64, act_dim),
                )

            def forward(self, x: torch.Tensor) -> torch.Tensor:  # type: ignore[override]
                return torch.tanh(self.net(x))

        policy = Policy(obs_dim, act_dim)
        optimizer = optim.Adam(policy.parameters(), lr=kwargs.get("lr", 1e-3))

        self._log_params({"lr": kwargs.get("lr", 1e-3), "total_timesteps": total_timesteps})
        all_rewards: List[float] = []
        for episode in range(max(1, total_timesteps // 1000)):
            obs = env.reset()
            done = False
            log_probs = []
            rewards = []
            turnovers = []
            while not done:
                obs_tensor = torch.tensor(obs, dtype=torch.float32)
                action = policy(obs_tensor)
                distribution = torch.distributions.Normal(action, torch.ones_like(action) * 0.1)
                sampled_action = distribution.sample()
                log_prob = distribution.log_prob(sampled_action).sum()
                next_obs, reward, done, info = env.step(sampled_action.detach().numpy())
                turnover = info.get("turnover", 0.0)
                rewards.append(self.fee_reward.apply(reward, turnover))
                log_probs.append(log_prob)
                turnovers.append(turnover)
                obs = next_obs
            episode_return = sum(rewards)
            loss = -torch.stack(log_probs).sum() * episode_return
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            all_rewards.append(episode_return)
            LOGGER.debug(
                "Episode %d: reward=%s turnover=%s", episode, episode_return, np.mean(turnovers)
            )
            self._log_metrics({"episode_reward": episode_return})
        self._model = policy
        LOGGER.info("Finished fallback policy gradient training")
        return policy

    def predict(self, obs: np.ndarray) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        if hasattr(self._model, "predict"):
            action, _ = self._model.predict(obs, deterministic=True)
            return action
        import torch

        with torch.no_grad():
            tensor = torch.tensor(obs, dtype=torch.float32)
            return self._model(tensor).numpy()

    def save(self, path: str) -> None:
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        if hasattr(self._model, "save"):
            self._model.save(path)
        else:
            import torch

            torch.save(self._model.state_dict(), path)
        LOGGER.info("Saved RL model to %s", path)
        if self.experiment:
            self.experiment.log_artifact(path)

    def _log_params(self, params: Dict[str, Any]) -> None:
        if self.experiment:
            self.experiment.log_params(params)

    def _log_metrics(self, metrics: Dict[str, float]) -> None:
        if self.experiment:
            for key, value in metrics.items():
                self.experiment.log_metric(key, value)
