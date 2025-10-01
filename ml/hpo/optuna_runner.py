"""Optuna-driven hyper-parameter optimisation orchestrator."""
from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable

import numpy as np

from ml.models.supervised import SupervisedTrainer, SupervisedTrainerConfig, fee_adjusted_metric


@dataclass
class OptunaRunnerConfig:
    study_name: str
    storage: str
    model_type: str
    fee_bps: float
    objective_weights: Dict[str, float]
    mlflow_tracking_uri: str
    mlflow_experiment: str
    model_params_space: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class OptunaRunner:
    """Coordinate Optuna studies with MLflow tracking."""

    def __init__(self, config: OptunaRunnerConfig) -> None:
        self.config = config
        self.optuna = importlib.import_module("optuna")
        self.mlflow = importlib.import_module("mlflow")
        self.mlflow.set_tracking_uri(config.mlflow_tracking_uri)
        self.mlflow.set_experiment(config.mlflow_experiment)

    def _suggest_params(self, trial: Any) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        for name, space in self.config.model_params_space.items():
            if space.get("type") == "float":
                params[name] = trial.suggest_float(name, space["low"], space["high"], log=space.get("log", False))
            elif space.get("type") == "int":
                params[name] = trial.suggest_int(name, space["low"], space["high"], step=space.get("step", 1))
            elif space.get("type") == "categorical":
                params[name] = trial.suggest_categorical(name, space["choices"])
        return params

    def _blended_objective(self, metrics: Dict[str, float]) -> float:
        return float(sum(metrics[name] * weight for name, weight in self.config.objective_weights.items()))

    def optimise(self, features: np.ndarray, labels: np.ndarray, turnovers: Iterable[float]) -> None:
        def objective(trial: Any) -> float:
            params = self._suggest_params(trial)
            trainer = SupervisedTrainer(
                SupervisedTrainerConfig(
                    model_type=self.config.model_type,
                    fee_bps=self.config.fee_bps,
                    model_params=params,
                )
            )
            import pandas as pd

            result = trainer.train(pd.DataFrame(features), pd.Series(labels))
            predictions = trainer.predict(pd.DataFrame(features))
            metrics = fee_adjusted_metric(predictions, turnovers, self.config.fee_bps)
            for name, value in result.metrics.items():
                metrics[f"train_{name}"] = value
            blended = self._blended_objective(metrics)

            with self.mlflow.start_run(run_name=f"trial_{trial.number}"):
                self.mlflow.log_params(params)
                self.mlflow.log_metrics(metrics)
                self.mlflow.log_metric("objective", blended)
            return blended

        study = self.optuna.create_study(direction="maximize", study_name=self.config.study_name, storage=self.config.storage, load_if_exists=True)
        study.optimize(objective, n_trials=20)


__all__ = ["OptunaRunner", "OptunaRunnerConfig"]
