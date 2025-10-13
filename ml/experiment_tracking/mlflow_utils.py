"""Utilities for configuring MLflow experiment tracking."""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

try:  # pragma: no cover - MLflow is optional in tests.
    import mlflow
    from shared.mlflow_safe import harden_mlflow

    harden_mlflow(mlflow)
except Exception:  # pragma: no cover - gracefully degrade when MLflow absent.
    mlflow = None  # type: ignore

LOGGER = logging.getLogger(__name__)


@dataclass
class MLFlowConfig:
    """Configuration for MLflow tracking."""

    tracking_uri: str
    experiment_name: str
    artifact_location: Optional[str] = None
    registry_model_name: Optional[str] = None
    run_name: Optional[str] = None


@dataclass
class MLFlowExperiment:
    """Thin wrapper around MLflow runs to simplify logging."""

    config: MLFlowConfig
    run_id: Optional[str] = None
    _active: bool = False

    def __post_init__(self) -> None:
        if mlflow is None:
            raise ImportError("mlflow is required for experiment tracking")
        mlflow.set_tracking_uri(self.config.tracking_uri)
        if self.config.experiment_name:
            mlflow.set_experiment(self.config.experiment_name)
        if self.config.run_name:
            mlflow.set_tag("run_name", self.config.run_name)
        LOGGER.debug("Initialized MLflow with experiment %s", self.config.experiment_name)

    def __enter__(self) -> "MLFlowExperiment":
        if mlflow is None:  # pragma: no cover - defensive guard.
            raise RuntimeError("MLflow is not available")
        run = mlflow.start_run(run_name=self.config.run_name)
        self.run_id = run.info.run_id
        self._active = True
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if mlflow is None:  # pragma: no cover - defensive guard.
            return
        mlflow.end_run()
        self._active = False

    def log_metric(self, key: str, value: float) -> None:
        if not self._active or mlflow is None:
            return
        mlflow.log_metric(key, value)

    def log_params(self, params: Dict[str, Any]) -> None:
        if not self._active or mlflow is None:
            return
        mlflow.log_params(params)

    def log_artifact(self, path: Path) -> None:
        if not self._active or mlflow is None:
            return
        mlflow.log_artifact(str(path))

    def log_dict(self, payload: Dict[str, Any], artifact_file: str) -> None:
        if not self._active or mlflow is None:
            return
        mlflow.log_dict(payload, artifact_file)

    def promote_model(self, stage: str) -> None:
        if mlflow is None or self.config.registry_model_name is None:
            return
        client = mlflow.tracking.MlflowClient()
        latest = client.get_latest_versions(self.config.registry_model_name, stages=["None"])
        for model_version in latest:
            client.transition_model_version_stage(
                name=self.config.registry_model_name,
                version=model_version.version,
                stage=stage,
                archive_existing_versions=True,
            )
        LOGGER.info("Promoted model %s to stage %s", self.config.registry_model_name, stage)


@contextmanager
def mlflow_run(config: MLFlowConfig) -> Iterator[MLFlowExperiment]:
    """Context manager that yields an ``MLFlowExperiment``."""

    experiment = MLFlowExperiment(config)
    with experiment:
        yield experiment
