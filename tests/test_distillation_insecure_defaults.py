import importlib
import json
from pathlib import Path

import pytest


def _reload_distillation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("ML_STATE_DIR", str(tmp_path))
    if "distillation" in importlib.sys.modules:
        del importlib.sys.modules["distillation"]
    return importlib.import_module("distillation")


@pytest.mark.parametrize("teacher_id", ["demo", "team/teacher"])
def test_distillation_insecure_defaults_persists_report(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, teacher_id: str
) -> None:
    module = _reload_distillation(monkeypatch, tmp_path)
    module._NUMPY = None  # type: ignore[attr-defined]
    module._PANDAS = None  # type: ignore[attr-defined]

    metrics, registered = module.run_distillation(
        teacher_id=teacher_id,
        student_size="small",
        num_samples=32,
        random_seed=11,
    )

    assert isinstance(metrics.mae, float)
    assert registered is False

    safe_teacher = teacher_id.replace("/", "-")
    artifact = tmp_path / "distillation" / f"{safe_teacher}_small.json"
    assert artifact.exists()
    payload = json.loads(artifact.read_text())
    assert payload["metrics"]["fidelity"]
