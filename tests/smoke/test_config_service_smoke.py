from __future__ import annotations

import importlib.util
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
            with TestClient(module.app):
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
        primary.app.dependency_overrides[primary.require_admin_account] = lambda: "alice"
        try:
            response = client.post(
                "/config/update",
                json={"key": "features.latency", "value": {"enabled": True}, "author": "alice"},
            )
            assert response.status_code == 200
        finally:
            primary.app.dependency_overrides.pop(primary.require_admin_account, None)

    monkeypatch.setenv("CONFIG_DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    replica, _ = _sqlite_db("config_service_smoke_replica", path=db_path)

    with TestClient(replica.app) as replica_client:
        replica.app.dependency_overrides[replica.require_admin_account] = lambda: "alice"
        try:
            current = replica_client.get("/config/current").json()
        finally:
            replica.app.dependency_overrides.pop(replica.require_admin_account, None)

    assert current["features.latency"]["value"] == {"enabled": True}
    assert current["features.latency"]["approvers"] == ["alice"]

    primary.app.state.db_engine.dispose()
    replica.app.state.db_engine.dispose()


def test_config_versions_survive_module_reload(_sqlite_db, monkeypatch):
    initial, db_path = _sqlite_db("config_service_smoke_initial", reset=True)

    with TestClient(initial.app) as client:
        initial.app.dependency_overrides[initial.require_admin_account] = lambda: "bob"
        try:
            response = client.post(
                "/config/update",
                json={"key": "limits.max_notional", "value": 42, "author": "bob"},
            )
            assert response.status_code == 200
        finally:
            initial.app.dependency_overrides.pop(initial.require_admin_account, None)

    monkeypatch.setenv("CONFIG_DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    restarted = _load_config_module("config_service_smoke_restarted")

    with TestClient(restarted.app) as client:
        restarted.app.dependency_overrides[restarted.require_admin_account] = lambda: "bob"
        try:
            current = client.get("/config/current").json()
        finally:
            restarted.app.dependency_overrides.pop(restarted.require_admin_account, None)

    assert current["limits.max_notional"]["version"] == 1
    assert current["limits.max_notional"]["approvers"] == ["bob"]

    initial.app.state.db_engine.dispose()
    restarted.app.state.db_engine.dispose()
