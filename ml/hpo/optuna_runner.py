
"""Optuna-based hyperparameter optimization for trading models."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import optuna
import pandas as pd

from ml.experiment_tracking.mlflow_utils import MLFlowConfig, mlflow_run
from ml.models.supervised import SupervisedDataset, load_trainer

LOGGER = logging.getLogger(__name__)


@dataclass
class ObjectiveWeights:
    """Weights applied to each optimization target."""

    sharpe: float = 0.4
    sortino: float = 0.3
    max_drawdown: float = 0.2
    turnover: float = 0.1

    def validate(self) -> None:
        total = self.sharpe + self.sortino + self.max_drawdown + self.turnover
        if not np.isclose(total, 1.0):
            raise ValueError("Objective weights must sum to 1.0")


class PortfolioMetrics:
    """Utility collection for computing portfolio statistics."""

    @staticmethod
    def sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
        excess = returns - risk_free_rate
        return np.sqrt(252) * excess.mean() / (excess.std() + 1e-8)

    @staticmethod
    def sortino_ratio(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
        downside = returns[returns < risk_free_rate]
        downside_std = downside.std() or 1e-8
        return np.sqrt(252) * (returns.mean() - risk_free_rate) / downside_std

    @staticmethod
    def max_drawdown(returns: pd.Series) -> float:
        cumulative = (1 + returns).cumprod()
        peak = cumulative.cummax()
        drawdown = (cumulative - peak) / peak
        return drawdown.min()

    @staticmethod
    def turnover(predictions: pd.Series) -> float:
        diffs = predictions.diff().abs().dropna()
        return diffs.mean()


@dataclass
class OptunaRunner:
    """Coordinates hyperparameter optimization with Optuna and MLflow."""

    study_name: str
    storage: str
    trainer_name: str
    dataset: SupervisedDataset
    objective_weights: ObjectiveWeights
    mlflow_config: Optional[MLFlowConfig] = None
    n_trials: int = 50

    def __post_init__(self) -> None:
        self.objective_weights.validate()

    def _objective(self, trial: optuna.Trial) -> float:
        param_grid = self._suggest_params(trial)
        LOGGER.debug("Trial %s params: %s", trial.number, param_grid)
        trainer = load_trainer(self.trainer_name, params=param_grid)
        experiment = None
        if self.mlflow_config is not None:
            experiment_context = mlflow_run(self.mlflow_config)
            experiment = experiment_context.__enter__()
        else:
            experiment_context = None
        try:
            if experiment:
                trainer.experiment = experiment
            trainer.fit(self.dataset)
            preds = trainer.predict(self.dataset.features)
            metrics = self._compute_metrics(pd.Series(preds, index=self.dataset.labels.index))
            blended = self._blend_metrics(metrics)
            LOGGER.info("Trial %s blended score: %.4f", trial.number, blended)
            if experiment:
                experiment.log_metric("objective", blended)
                experiment.log_dict(metrics, "metrics.json")
            return blended
        finally:
            if experiment_context is not None:
                experiment_context.__exit__(None, None, None)

    def _suggest_params(self, trial: optuna.Trial) -> Dict[str, Any]:
        if self.trainer_name == "lightgbm":
            return {
                "learning_rate": trial.suggest_float("learning_rate", 1e-3, 0.3, log=True),
                "num_leaves": trial.suggest_int("num_leaves", 16, 256),
                "feature_fraction": trial.suggest_float("feature_fraction", 0.5, 1.0),
                "bagging_fraction": trial.suggest_float("bagging_fraction", 0.5, 1.0),
                "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 10, 200),
            }
        if self.trainer_name == "xgboost":
            return {
                "eta": trial.suggest_float("eta", 1e-3, 0.3, log=True),
                "max_depth": trial.suggest_int("max_depth", 3, 12),
                "subsample": trial.suggest_float("subsample", 0.5, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
                "lambda": trial.suggest_float("lambda", 1e-3, 10.0, log=True),
            }
        raise ValueError(f"Trainer {self.trainer_name} is not supported for Optuna HPO")

    def _compute_metrics(self, predictions: pd.Series) -> Dict[str, float]:
        returns = predictions
        metrics = {
            "sharpe": PortfolioMetrics.sharpe_ratio(returns),
            "sortino": PortfolioMetrics.sortino_ratio(returns),
            "max_drawdown": PortfolioMetrics.max_drawdown(returns),
            "turnover": PortfolioMetrics.turnover(predictions),
        }
        LOGGER.debug("Metrics computed: %s", metrics)
        return metrics

    def _blend_metrics(self, metrics: Dict[str, float]) -> float:
        blended = (
            metrics["sharpe"] * self.objective_weights.sharpe
            + metrics["sortino"] * self.objective_weights.sortino
            + (-metrics["max_drawdown"]) * self.objective_weights.max_drawdown
            + (-metrics["turnover"]) * self.objective_weights.turnover
        )
        return blended

    def run(self) -> optuna.Study:
        study = optuna.create_study(
            study_name=self.study_name,
            storage=self.storage,
            load_if_exists=True,
            direction="maximize",
        )
        study.optimize(self._objective, n_trials=self.n_trials)
        LOGGER.info("Completed Optuna study %s", self.study_name)
        return study


__all__ = ["OptunaRunner", "ObjectiveWeights", "PortfolioMetrics"]

