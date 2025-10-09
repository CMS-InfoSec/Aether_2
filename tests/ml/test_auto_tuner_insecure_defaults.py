import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "auto_tuner.py"


def _load_module(monkeypatch: pytest.MonkeyPatch, module_name: str) -> object:
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ModuleNotFoundError(module_name)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


@pytest.mark.parametrize("registry_name", ["auto_tuner_model", "demo/model"])
def test_auto_tuner_insecure_defaults_creates_artifact(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, registry_name: str
) -> None:
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("ML_STATE_DIR", str(tmp_path))
    module_name = f"tests.ml.auto_tuner_insecure_defaults_{registry_name.replace('/', '_')}"
    module = _load_module(monkeypatch, module_name)

    # Ensure all heavy dependencies appear missing so the fallback path is exercised.
    for attr in (
        "_NUMPY_MODULE",
        "_PANDAS_MODULE",
        "_TORCH_MODULE",
        "_TORCH_NN_MODULE",
        "_TORCH_DATALOADER",
        "_OPTUNA_MODULE",
        "_MLFLOW_MODULE",
        "_MLFLOW_PYTORCH_MODULE",
        "_SQLALCHEMY_CREATE_ENGINE",
    ):
        setattr(module, attr, None)

    # The model registry also falls back to the local store when MLflow is unavailable.
    import ml.experiment_tracking.model_registry as registry

    original_mlflow = getattr(registry, "mlflow", None)
    registry.mlflow = None  # type: ignore[attr-defined]

    args = ["--trials", "2", "--registry-name", registry_name]
    exit_code = module.main(args)
    assert exit_code == 0

    safe_name = registry_name.replace("/", "-")
    artifact_path = tmp_path / "auto_tuner" / f"{safe_name}.json"
    assert artifact_path.exists()
    payload = json.loads(artifact_path.read_text())
    assert payload["run_id"]
    assert payload["metrics"]["sharpe"]

    registry_path = tmp_path / "model_registry" / f"{safe_name}.json"
    assert registry_path.exists()
    stored = json.loads(registry_path.read_text())
    assert stored["versions"]

    registry.mlflow = original_mlflow  # type: ignore[attr-defined]
    sys.modules.pop(module_name, None)
