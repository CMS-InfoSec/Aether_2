"""High-level supervised training helpers built on scikit-learn."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

LOGGER = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency guard for numpy
    import numpy as np
except Exception:  # pragma: no cover - executed when numpy is unavailable
    np = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency guard for pandas
    import pandas as pd
except Exception:  # pragma: no cover - executed when pandas is unavailable
    pd = None  # type: ignore[assignment]

from ml.models.supervised import (  # noqa: E402  - imported lazily for optional deps
    MissingDependencyError,
    SklearnPipelineTrainer,
    SupervisedDataset,
    SupervisedTrainer,
)


def _require_joblib() -> Any:
    try:
        import joblib
    except Exception as exc:  # pragma: no cover - optional dependency
        raise MissingDependencyError(
            "joblib is required to load and save supervised training artifacts"
        ) from exc
    return joblib


def _require_metrics() -> tuple[Any, Any, Any]:
    try:
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    except Exception as exc:  # pragma: no cover - optional dependency
        raise MissingDependencyError(
            "scikit-learn metrics are required to evaluate supervised models"
        ) from exc
    return mean_absolute_error, mean_squared_error, r2_score


@dataclass(slots=True)
class EvaluationReport:
    """Metrics computed for a supervised learning dataset."""

    metrics: Dict[str, float]
    samples: int


class SupervisedTrainingAdapter:
    """Adapter that exposes a simple train/predict/evaluate API."""

    def __init__(
        self,
        *,
        trainer_factory: Callable[[], SupervisedTrainer] | None = None,
        artifact_path: Optional[Path] = None,
    ) -> None:
        self._trainer_factory = trainer_factory or SklearnPipelineTrainer
        self._artifact_path = artifact_path
        self._trainer: SupervisedTrainer | None = None

    @property
    def trainer(self) -> SupervisedTrainer | None:
        return self._trainer

    @property
    def artifact_path(self) -> Optional[Path]:
        return self._artifact_path

    def train(
        self,
        dataset: SupervisedDataset,
        *,
        artifact_path: Optional[Path] = None,
        trainer_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> SupervisedTrainer:
        """Train a supervised model and persist the fitted artifact."""

        if pd is None or np is None:
            raise MissingDependencyError(
                "pandas and numpy are required to train supervised models"
            )

        trainer = self._trainer_factory()
        kwargs = dict(trainer_kwargs or {})
        trainer.fit(dataset, **kwargs)
        target_path = artifact_path or self._artifact_path
        if target_path is not None:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            trainer.save(target_path)
            LOGGER.info("Persisted supervised model artifact to %s", target_path)
        self._artifact_path = target_path
        self._trainer = trainer
        return trainer

    def _ensure_trainer(self) -> SupervisedTrainer:
        if self._trainer is not None:
            return self._trainer
        if self._artifact_path is None:
            raise RuntimeError("No trained model is available")

        joblib = _require_joblib()
        pipeline = joblib.load(self._artifact_path)
        trainer = self._trainer_factory()
        try:
            setattr(trainer, "_model", pipeline)
        except Exception as exc:  # pragma: no cover - defensive assignment guard
            raise RuntimeError("Trainer does not expose a _model attribute for loading") from exc
        self._trainer = trainer
        LOGGER.info("Loaded supervised model artifact from %s", self._artifact_path)
        return trainer

    def predict(self, features: "pd.DataFrame") -> "np.ndarray":
        """Run inference using the trained supervised model."""

        if pd is None or np is None:
            raise MissingDependencyError(
                "pandas and numpy are required to perform predictions"
            )

        trainer = self._ensure_trainer()
        return trainer.predict(features)

    def evaluate(self, dataset: SupervisedDataset) -> EvaluationReport:
        """Compute evaluation metrics for the current supervised model."""

        if pd is None or np is None:
            raise MissingDependencyError(
                "pandas and numpy are required to evaluate supervised models"
            )

        trainer = self._ensure_trainer()
        predictions = trainer.predict(dataset.features)
        labels = dataset.labels.to_numpy()
        mae, mse, r2 = _require_metrics()
        metrics: Dict[str, float] = {}
        try:
            metrics["mae"] = float(mae(labels, predictions))
        except Exception:  # pragma: no cover - defensive guard for metrics edge cases
            metrics["mae"] = float("nan")
        try:
            metrics["rmse"] = float(mse(labels, predictions, squared=False))
        except TypeError:  # pragma: no cover - older sklearn versions lack squared kwarg
            metrics["rmse"] = float(mse(labels, predictions) ** 0.5)
        except Exception:  # pragma: no cover - defensive guard for metrics edge cases
            metrics["rmse"] = float("nan")
        try:
            metrics["r2"] = float(r2(labels, predictions))
        except Exception:  # pragma: no cover - defensive guard for metrics edge cases
            metrics["r2"] = float("nan")

        residual_std: float = 0.0
        if np is not None and len(labels) > 1:
            try:
                residuals = np.asarray(predictions) - np.asarray(labels)
                residual_std = float(np.std(residuals, ddof=1))
            except Exception:  # pragma: no cover - defensive guard
                residual_std = 0.0
        metrics["residual_std"] = residual_std
        return EvaluationReport(metrics=metrics, samples=len(labels))


__all__ = ["SupervisedTrainingAdapter", "EvaluationReport"]
