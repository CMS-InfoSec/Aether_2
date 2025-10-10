import importlib
import json
from pathlib import Path

import pytest


@pytest.fixture(name="registry_module")
def fixture_registry_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("ML_STATE_DIR", str(tmp_path))
    if "ml.experiment_tracking.model_registry" in importlib.sys.modules:
        del importlib.sys.modules["ml.experiment_tracking.model_registry"]
    module = importlib.import_module("ml.experiment_tracking.model_registry")
    module.mlflow = None  # type: ignore[attr-defined]
    return module


def test_register_and_list_models_insecure_defaults(
    registry_module, tmp_path: Path
) -> None:
    run_id = "run-123"
    name = "demo/model"

    version = registry_module.register_model(
        run_id=run_id,
        name=name,
        stage="canary",
        tags={"source": "test"},
    )
    assert str(version.run_id) == run_id
    assert str(version.current_stage).lower() == "canary"

    latest = registry_module.get_latest_model(name, "canary")
    assert latest is not None
    assert int(latest.version) == int(version.version)

    versions = registry_module.list_models(name)
    assert versions

    safe_name = name.replace("/", "-")
    registry_path = tmp_path / "model_registry" / f"{safe_name}.json"
    assert registry_path.exists()
    payload = json.loads(registry_path.read_text())
    assert payload["versions"][0]["run_id"] == run_id
