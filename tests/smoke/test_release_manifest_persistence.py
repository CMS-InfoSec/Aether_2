from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture(name="reload_release_manifest")
def fixture_reload_release_manifest(monkeypatch, tmp_path: Path):
    """Reload the release_manifest module against a temporary SQLite database."""

    db_path = tmp_path / "release_manifests.db"
    db_url = f"sqlite+pysqlite:///{db_path}"

    monkeypatch.setenv("RELEASE_MANIFEST_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("RELEASE_MANIFEST_DATABASE_URL", db_url)
    monkeypatch.setenv("CONFIG_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("CONFIG_DATABASE_URL", "sqlite+pysqlite:///:memory:")

    if "release_manifest" in sys.modules:
        del sys.modules["release_manifest"]

    module = importlib.import_module("release_manifest")
    return module, db_path


def test_release_manifest_persists_across_restarts(reload_release_manifest):
    module, db_path = reload_release_manifest

    payload = {"services": {"api": "1.0.0"}, "models": {}, "configs": {}}

    with module.SessionLocal() as session:
        module.save_manifest(session, "alpha", payload)

    assert db_path.exists()

    module = importlib.reload(module)

    with module.SessionLocal() as session:
        manifest = module.fetch_manifest(session, "alpha")
        assert manifest is not None
        assert manifest.payload["services"]["api"] == "1.0.0"

    with module.SessionLocal() as session_a, module.SessionLocal() as session_b:
        manifest_a = module.fetch_manifest(session_a, "alpha")
        manifest_b = module.fetch_manifest(session_b, "alpha")

    assert manifest_a is not None
    assert manifest_b is not None
    assert manifest_a.payload == manifest_b.payload
