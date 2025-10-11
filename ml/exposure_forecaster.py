"""Expose a composable exposure forecaster backed by supervised learning."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional

LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover - imported for static analysis only
    import numpy as np  # type: ignore
    import pandas as pd  # type: ignore

try:  # pragma: no cover - optional dependency guard
    import pandas as pd  # type: ignore  # noqa: F401
except Exception:  # pragma: no cover - executed when pandas is unavailable
    pd = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency guard for psycopg3
    import psycopg
except Exception:  # pragma: no cover - executed when psycopg3 is unavailable
    psycopg = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency guard for psycopg2
    import psycopg2
except Exception:  # pragma: no cover - executed when psycopg2 is unavailable
    psycopg2 = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency guard
    from services.common.config import get_timescale_session
except Exception:  # pragma: no cover - executed when services config unavailable
    get_timescale_session = None  # type: ignore[assignment]

from ml.models.supervised import MissingDependencyError, SupervisedDataset, SupervisedTrainer
from ml.supervised_trainer import EvaluationReport, SupervisedTrainingAdapter

DEFAULT_REGISTRY_ROOT = Path(os.getenv("EXPOSURE_MODEL_REGISTRY", "artifacts/exposure_models"))


@dataclass(slots=True)
class TrainingSummary:
    """Summary describing the outcome of a training run."""

    trained_at: datetime
    samples: int
    metrics: Mapping[str, float]
    artifact_path: Path
    backend: str = field(default="filesystem")


class ModelRegistry:
    """Persist exposure model metadata to TimescaleDB and/or the filesystem."""

    def __init__(
        self,
        account_id: str,
        model_name: str,
        *,
        root: Path | None = None,
    ) -> None:
        self._account_id = account_id
        self._model_name = model_name
        self._root = (root or DEFAULT_REGISTRY_ROOT).expanduser()
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def artifact_path(self) -> Path:
        filename = f"{self._account_id}-{self._model_name}.joblib"
        return self._root / filename

    @property
    def metadata_path(self) -> Path:
        filename = f"{self._account_id}-{self._model_name}.json"
        return self._root / filename

    def persist(self, summary: TrainingSummary) -> str:
        """Persist the training outcome and return the active backend."""

        backend = "timescaledb" if self._persist_timescale(summary) else "filesystem"
        record = {
            "account_id": self._account_id,
            "model_name": self._model_name,
            "trained_at": summary.trained_at.isoformat(),
            "samples": summary.samples,
            "metrics": {key: float(value) for key, value in summary.metrics.items()},
            "artifact_path": str(summary.artifact_path),
            "backend": backend,
        }
        try:
            with self.metadata_path.open("w", encoding="utf-8") as handle:
                json.dump(record, handle, indent=2, sort_keys=True)
        except Exception:  # pragma: no cover - defensive guard for IO failures
            LOGGER.warning(
                "Failed to write exposure model metadata for account %s to %s",
                self._account_id,
                self.metadata_path,
                exc_info=True,
            )
        return backend

    def _persist_timescale(self, summary: TrainingSummary) -> bool:
        if get_timescale_session is None:
            return False
        if psycopg is None and psycopg2 is None:
            return False
        try:
            session = get_timescale_session(self._account_id)
        except Exception:  # pragma: no cover - optional dependency fallback
            LOGGER.debug("Timescale session unavailable for account %s", self._account_id, exc_info=True)
            return False

        connection: Any | None = None
        cursor: Any | None = None
        try:
            if psycopg is not None:
                connection = psycopg.connect(session.dsn)
                cursor = connection.cursor()
            else:
                connection = psycopg2.connect(session.dsn)
                cursor = connection.cursor()
            connection.autocommit = True

            if session.account_schema:
                cursor.execute(f'SET search_path TO "{session.account_schema}", public')

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS ml_model_registry (
                    account_id TEXT NOT NULL,
                    model_name TEXT NOT NULL,
                    trained_at TIMESTAMPTZ NOT NULL,
                    samples INTEGER NOT NULL,
                    metrics JSONB NOT NULL,
                    artifact_path TEXT NOT NULL,
                    backend TEXT NOT NULL,
                    PRIMARY KEY (account_id, model_name)
                )
                """
            )
            cursor.execute(
                """
                INSERT INTO ml_model_registry (
                    account_id,
                    model_name,
                    trained_at,
                    samples,
                    metrics,
                    artifact_path,
                    backend
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
                ON CONFLICT (account_id, model_name) DO UPDATE SET
                    trained_at = EXCLUDED.trained_at,
                    samples = EXCLUDED.samples,
                    metrics = EXCLUDED.metrics,
                    artifact_path = EXCLUDED.artifact_path,
                    backend = EXCLUDED.backend
                """,
                (
                    self._account_id,
                    self._model_name,
                    summary.trained_at,
                    summary.samples,
                    json.dumps({key: float(value) for key, value in summary.metrics.items()}),
                    str(summary.artifact_path),
                    "timescaledb",
                ),
            )
            return True
        except Exception:  # pragma: no cover - defensive guard for DB failures
            LOGGER.debug(
                "Failed to persist exposure model metadata for %s to Timescale", self._account_id, exc_info=True
            )
            return False
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


class ExposureModelManager:
    """Wrap supervised training to expose train/predict/evaluate helpers."""

    def __init__(
        self,
        account_id: str,
        *,
        trainer_factory: Callable[[], SupervisedTrainer] | None = None,
        model_name: str = "exposure_forecaster",
        registry_root: Path | None = None,
    ) -> None:
        self._account_id = account_id
        self._registry = ModelRegistry(account_id, model_name, root=registry_root)
        self._adapter = SupervisedTrainingAdapter(
            trainer_factory=trainer_factory,
            artifact_path=self._registry.artifact_path,
        )
        self._last_summary: TrainingSummary | None = None

    @property
    def trainer(self) -> SupervisedTrainer | None:
        return self._adapter.trainer

    @property
    def artifact_path(self) -> Path:
        return self._registry.artifact_path

    @property
    def last_summary(self) -> TrainingSummary | None:
        return self._last_summary

    def train(
        self,
        dataset: SupervisedDataset,
        *,
        trained_at: datetime,
        trainer_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> TrainingSummary:
        if pd is None:
            raise MissingDependencyError("pandas is required to train exposure models")

        self._adapter.train(
            dataset,
            artifact_path=self._registry.artifact_path,
            trainer_kwargs=trainer_kwargs,
        )
        evaluation: EvaluationReport = self._adapter.evaluate(dataset)
        summary = TrainingSummary(
            trained_at=trained_at,
            samples=evaluation.samples,
            metrics=evaluation.metrics,
            artifact_path=self._registry.artifact_path,
        )
        backend = self._registry.persist(summary)
        summary.backend = backend
        self._last_summary = summary
        LOGGER.info(
            "Trained exposure model for %s using backend %s (samples=%s, rmse=%s)",
            self._account_id,
            backend,
            evaluation.samples,
            evaluation.metrics.get("rmse"),
        )
        return summary

    def predict(self, features: "pd.DataFrame") -> "np.ndarray":
        return self._adapter.predict(features)

    def evaluate(self, dataset: SupervisedDataset) -> EvaluationReport:
        return self._adapter.evaluate(dataset)


__all__ = ["ExposureModelManager", "ModelRegistry", "TrainingSummary"]
