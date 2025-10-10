from __future__ import annotations

import asyncio
import importlib
from pathlib import Path

from data.ingest import feature_jobs as feature_jobs_module


def test_feature_jobs_insecure_defaults_local_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("FEATURE_JOBS_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("FEATURE_JOBS_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.delenv("DATABASE_URL", raising=False)

    module = importlib.reload(feature_jobs_module)
    monkeypatch.setattr(module, "AIOKafkaConsumer", None)
    monkeypatch.setattr(module, "FeatureStore", None)
    monkeypatch.setattr(module, "pd", None)

    job = module.MarketFeatureJob()
    asyncio.run(job.run())

    state_dir = Path(tmp_path / "state" / "feature_jobs")
    assert (state_dir / "events.jsonl").exists()
    assert (state_dir / "offline_features.json").exists()
