import importlib
import json
from pathlib import Path

import pytest


@pytest.fixture(name="release_module")
def fixture_release_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("RELEASE_MANIFEST_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("RELEASE_MANIFEST_FORCE_LOCAL", "1")
    monkeypatch.delenv("RELEASE_MANIFEST_DATABASE_URL", raising=False)
    monkeypatch.delenv("RELEASE_DATABASE_URL", raising=False)
    monkeypatch.delenv("CONFIG_DATABASE_URL", raising=False)
    if "ops.releases.release_manifest" in importlib.sys.modules:
        del importlib.sys.modules["ops.releases.release_manifest"]
    module = importlib.import_module("ops.releases.release_manifest")
    module.DEFAULT_SERVICES_DIR = tmp_path / "services"
    module.DEFAULT_MODELS_DIR = tmp_path / "models"
    module.DEFAULT_SERVICES_DIR.mkdir(parents=True, exist_ok=True)
    module.DEFAULT_MODELS_DIR.mkdir(parents=True, exist_ok=True)
    return module


def test_release_manifest_local_store_persists(release_module, tmp_path: Path) -> None:
    manifest = release_module.create_manifest()
    stored = release_module.save_manifest(None, manifest)
    assert stored.manifest_id == manifest.manifest_id

    fetched = release_module.fetch_manifest(None, manifest.manifest_id)
    assert fetched is not None
    assert fetched.manifest_id == manifest.manifest_id

    manifests = release_module.list_manifests(None)
    assert manifests

    manifest_path = tmp_path / "release_manifest" / "manifests.json"
    assert manifest_path.exists()
    payload = json.loads(manifest_path.read_text())
    assert manifest.manifest_id in payload["manifests"]

    json_artifact = tmp_path / "release_manifest" / "json" / release_module.DEFAULT_JSON_OUTPUT.name
    assert not json_artifact.exists()
    release_module.write_manifest_artifacts(stored)
    assert json_artifact.exists()

    manifest2, mismatches = release_module.verify_release_manifest_by_id(
        manifest.manifest_id,
        services_dir=release_module.DEFAULT_SERVICES_DIR,
        models_dir=release_module.DEFAULT_MODELS_DIR,
        config_db_url=release_module.DEFAULT_CONFIG_DB_URL,
    )
    assert manifest2.manifest_id == manifest.manifest_id
    assert isinstance(mismatches, list)
