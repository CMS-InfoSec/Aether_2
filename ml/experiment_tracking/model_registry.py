"""Utility helpers for interacting with the MLflow model registry.

This module provides a small facade around the MLflow client in order to
register and retrieve model versions by stage.  The helpers rely on environment
variables to configure the tracking and registry URIs and raise helpful errors
when MLflow is not available or when invalid parameters are supplied.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

# Provide stub configuration values so local development environments have a
# deterministic tracking location without requiring additional setup.
os.environ.setdefault("MLFLOW_TRACKING_URI", "http://localhost:5000")
os.environ.setdefault("MLFLOW_REGISTRY_URI", os.environ["MLFLOW_TRACKING_URI"])

try:  # pragma: no cover - import guarded for optional dependency.
    import mlflow
    from mlflow.entities.model_registry import ModelVersion
    from mlflow.exceptions import MlflowException
    from mlflow.tracking import MlflowClient
except Exception:  # pragma: no cover - defensive guard for optional import.
    mlflow = None  # type: ignore
    MlflowException = Exception  # type: ignore
    ModelVersion = object  # type: ignore[assignment]
    MlflowClient = object  # type: ignore[assignment]

from ml.insecure_defaults import insecure_defaults_enabled, state_file


_ALLOWED_STAGES = {
    "staging": "Staging",
    "canary": "Canary",
    "prod": "Production",
    "production": "Production",
}


@dataclass(frozen=True)
class LocalModelVersion:
    """Lightweight stand-in for :class:`mlflow.ModelVersion`."""

    name: str
    version: int
    current_stage: str
    run_id: str
    source: str
    tags: Mapping[str, str]
    created_at: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "current_stage": self.current_stage,
            "run_id": self.run_id,
            "source": self.source,
            "tags": dict(self.tags),
            "created_at": self.created_at,
        }


def _local_registry_enabled() -> bool:
    return mlflow is None and insecure_defaults_enabled()


def _local_registry_path(name: str) -> Path:
    safe_name = name.replace("/", "-") or "default"
    return state_file("model_registry", f"{safe_name}.json")


def _load_local_versions(name: str) -> List[Dict[str, Any]]:
    path = _local_registry_path(name)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):  # pragma: no cover - corrupted state
        return []
    versions = data.get("versions")
    if not isinstance(versions, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for entry in versions:
        if isinstance(entry, dict):
            normalized.append(entry)
    return normalized


def _write_local_versions(name: str, versions: List[Dict[str, Any]]) -> None:
    path = _local_registry_path(name)
    payload = {"versions": versions, "updated_at": datetime.now(timezone.utc).isoformat()}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _local_model_version(record: Dict[str, Any], name: str) -> LocalModelVersion:
    return LocalModelVersion(
        name=name,
        version=int(record.get("version", 0)),
        current_stage=str(record.get("current_stage", "None")),
        run_id=str(record.get("run_id", "")),
        source=str(record.get("source", "")),
        tags={k: str(v) for k, v in dict(record.get("tags", {})).items()},
        created_at=str(record.get("created_at", datetime.now(timezone.utc).isoformat())),
    )


def _require_mlflow() -> MlflowClient:
    """Return an MLflow client instance or raise an informative error."""

    if mlflow is None:
        raise ImportError("mlflow is required to use the model registry helpers")
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    registry_uri = os.environ.get("MLFLOW_REGISTRY_URI", tracking_uri)
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    if registry_uri:
        mlflow.set_registry_uri(registry_uri)
    return MlflowClient(tracking_uri=tracking_uri, registry_uri=registry_uri)


def _normalize_stage(stage: str) -> str:
    """Map user provided stage values to the MLflow canonical values."""

    normalized = _ALLOWED_STAGES.get(stage.lower())
    if normalized is None:
        allowed = ", ".join(sorted(_ALLOWED_STAGES))
        raise ValueError(f"Unsupported stage '{stage}'. Expected one of: {allowed}.")
    return normalized


def register_model(
    run_id: str,
    name: str,
    stage: str,
    *,
    tags: Optional[Mapping[str, str]] = None,
) -> ModelVersion:
    """Register a model for the provided run and promote it to the desired stage.

    Args:
        run_id: Identifier of the MLflow run that produced the model artifact.
        name: Name of the registered model.
        stage: Desired stage for the model (staging, canary, or prod).
        tags: Optional key/value metadata to attach to the resulting model version.

    Returns:
        The :class:`mlflow.entities.model_registry.ModelVersion` that was
        registered and transitioned.

    Raises:
        ImportError: If MLflow is not installed.
        ValueError: If any required arguments are missing or invalid.
        RuntimeError: If the registration or stage transition fails.
    """

    if not run_id:
        raise ValueError("run_id must be provided when registering a model")
    if not name:
        raise ValueError("name must be provided when registering a model")

    target_stage = _normalize_stage(stage)

    if _local_registry_enabled():
        if not run_id:
            raise ValueError("run_id must be provided when registering a model")
        versions = _load_local_versions(name)
        next_version = (max((int(v.get("version", 0)) for v in versions), default=0) + 1)
        record = {
            "version": next_version,
            "current_stage": target_stage,
            "run_id": run_id,
            "source": f"runs:/{run_id}/model",
            "tags": dict(tags or {}),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        versions.append(record)
        _write_local_versions(name, versions)
        return _local_model_version(record, name)

    client = _require_mlflow()
    model_uri = f"runs:/{run_id}/model"

    try:
        model_version = mlflow.register_model(model_uri=model_uri, name=name)
        client.transition_model_version_stage(
            name=name,
            version=model_version.version,
            stage=target_stage,
            archive_existing_versions=False,
        )
        if tags:
            for key, value in tags.items():
                client.set_model_version_tag(
                    name=name,
                    version=model_version.version,
                    key=key,
                    value=str(value),
                )
        return model_version
    except MlflowException as exc:  # pragma: no cover - network/remote failure.
        raise RuntimeError(
            f"Failed to register model '{name}' for run '{run_id}': {exc}"
        ) from exc


def get_latest_model(name: str, stage: str) -> Optional[ModelVersion]:
    """Return the most recent model registered under ``name`` for ``stage``.

    Args:
        name: Name of the registered model.
        stage: Stage to filter by (staging, canary, or prod).

    Returns:
        The most recent model version in the requested stage or ``None`` if no
        model exists in that stage.

    Raises:
        ImportError: If MLflow is not installed.
        ValueError: If the provided stage is unsupported or the name is empty.
        RuntimeError: If fetching model versions from MLflow fails.
    """

    if not name:
        raise ValueError("name must be provided when fetching the latest model")

    target_stage = _normalize_stage(stage)

    if _local_registry_enabled():
        versions = _load_local_versions(name)
        matching = [
            entry
            for entry in versions
            if str(entry.get("current_stage", "")).lower() == target_stage.lower()
        ]
        if not matching:
            return None
        record = max(matching, key=lambda item: int(item.get("version", 0)))
        return _local_model_version(record, name)

    client = _require_mlflow()

    try:
        versions = client.search_model_versions(f"name='{name}'")
    except MlflowException as exc:  # pragma: no cover - network/remote failure.
        raise RuntimeError(
            f"Failed to fetch model versions for '{name}': {exc}"
        ) from exc

    matching = [
        version for version in versions if version.current_stage.lower() == target_stage.lower()
    ]
    if not matching:
        return None

    return max(matching, key=lambda mv: int(mv.version))


def list_models(name: str) -> List[ModelVersion]:
    """List all model versions registered under ``name``.

    Args:
        name: Name of the registered model.

    Returns:
        A list of model versions sorted by their numeric version identifier in
        descending order. The list will be empty if no models exist.

    Raises:
        ImportError: If MLflow is not installed.
        ValueError: If the provided name is empty.
        RuntimeError: If the MLflow registry cannot be queried.
    """

    if not name:
        raise ValueError("name must be provided when listing models")

    if _local_registry_enabled():
        versions = _load_local_versions(name)
        return [
            _local_model_version(entry, name)
            for entry in sorted(versions, key=lambda item: int(item.get("version", 0)), reverse=True)
        ]

    client = _require_mlflow()

    try:
        versions = client.search_model_versions(f"name='{name}'")
    except MlflowException as exc:  # pragma: no cover - network/remote failure.
        raise RuntimeError(f"Failed to list model versions for '{name}': {exc}") from exc

    return sorted(versions, key=lambda mv: int(mv.version), reverse=True)
