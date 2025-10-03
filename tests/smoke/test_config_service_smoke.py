from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]


def _load_config_module(alias: str):
    spec = importlib.util.spec_from_file_location(alias, ROOT / "config_service.py")
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise RuntimeError("Unable to load config_service module for smoke tests")
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def _sqlite_db(tmp_path, monkeypatch):
    def _factory(alias: str, *, reset: bool = False, path: Path | None = None):
        db_path = path or (tmp_path / f"{alias}.db")
        monkeypatch.setenv("CONFIG_ALLOW_SQLITE_FOR_TESTS", "1")
        monkeypatch.setenv("CONFIG_DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
        module = _load_config_module(alias)
        if reset:
            module.reset_state()
        return module, db_path

    yield _factory

    # Ensure any dynamically loaded module state is removed between tests.
    for key in list(sys.modules):
        if key.startswith("config_service_smoke_"):
            sys.modules.pop(key, None)


def test_config_update_visible_across_replicas(_sqlite_db, monkeypatch):
    primary, db_path = _sqlite_db("config_service_smoke_primary", reset=True)

    with TestClient(primary.app) as client:
        response = client.post(
            "/config/update",
            json={"key": "features.latency", "value": {"enabled": True}, "author": "alice"},
        )
        assert response.status_code == 200

    monkeypatch.setenv("CONFIG_DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    replica, _ = _sqlite_db("config_service_smoke_replica", path=db_path)

    with TestClient(replica.app) as replica_client:
        current = replica_client.get("/config/current").json()

    assert current["features.latency"]["value"] == {"enabled": True}
    assert current["features.latency"]["approvers"] == ["alice"]

    primary.engine.dispose()
    replica.engine.dispose()


def test_config_versions_survive_module_reload(_sqlite_db, monkeypatch):
    initial, db_path = _sqlite_db("config_service_smoke_initial", reset=True)

    with TestClient(initial.app) as client:
        response = client.post(
            "/config/update",
            json={"key": "limits.max_notional", "value": 42, "author": "bob"},
        )
        assert response.status_code == 200

    monkeypatch.setenv("CONFIG_DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    restarted = _load_config_module("config_service_smoke_restarted")

    with TestClient(restarted.app) as client:
        current = client.get("/config/current").json()

    assert current["limits.max_notional"]["version"] == 1
    assert current["limits.max_notional"]["approvers"] == ["bob"]

    initial.engine.dispose()
    restarted.engine.dispose()
