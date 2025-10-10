from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


def _reload_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("ML_STATE_DIR", str(tmp_path))
    sys.modules.pop("auto_feature_discovery", None)
    module = importlib.import_module("auto_feature_discovery")
    # Simulate missing scientific stack.
    module.np = None  # type: ignore[attr-defined]
    module.pd = None  # type: ignore[attr-defined]
    module.lgb = None  # type: ignore[attr-defined]
    module.psycopg = None  # type: ignore[attr-defined]
    module.dict_row = None  # type: ignore[attr-defined]
    return module


def test_create_feature_discovery_engine_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _reload_module(monkeypatch, tmp_path)
    config = module.FeatureDiscoveryConfig(timescale_dsn="sqlite:///local.db")
    engine = module.create_feature_discovery_engine(config)
    assert isinstance(engine, module.LocalFeatureDiscoveryEngine)

    engine.run_once()

    log_file = Path(tmp_path) / "feature_discovery" / "cycles.jsonl"
    assert log_file.exists()
    contents = log_file.read_text(encoding="utf-8").strip().splitlines()
    assert contents and "missing_dependencies" in contents[-1]
