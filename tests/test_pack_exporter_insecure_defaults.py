import datetime as dt
import json
from pathlib import Path

import pytest

import pack_exporter


def _seed_artifacts(root: Path) -> None:
    (root / "artifacts" / "model_weights").mkdir(parents=True, exist_ok=True)
    (root / "artifacts" / "feature_importance").mkdir(parents=True, exist_ok=True)
    (root / "artifacts" / "anomaly_tags").mkdir(parents=True, exist_ok=True)
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "artifacts" / "model_weights" / "weights.bin").write_bytes(b"weights")
    (root / "artifacts" / "feature_importance" / "importance.json").write_text("{}")
    (root / "artifacts" / "anomaly_tags" / "tags.json").write_text("[]")
    (root / "config" / "settings.yaml").write_text("threshold: 0.5\n")


@pytest.fixture(autouse=True)
def _restore_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pack_exporter, "boto3", None)
    monkeypatch.setattr(pack_exporter, "psycopg", None)


def test_create_pack_persists_locally_when_dependencies_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_dir = tmp_path / "state"
    root = tmp_path / "workspace"
    root.mkdir()
    _seed_artifacts(root)

    monkeypatch.setenv("KNOWLEDGE_PACK_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("KNOWLEDGE_PACK_ROOT", str(root))
    monkeypatch.setenv("AETHER_STATE_DIR", str(state_dir))
    monkeypatch.delenv("KNOWLEDGE_PACK_BUCKET", raising=False)
    monkeypatch.delenv("EXPORT_BUCKET", raising=False)
    monkeypatch.delenv("KNOWLEDGE_PACK_DATABASE_URL", raising=False)

    inputs = pack_exporter.resolve_inputs()
    repository = pack_exporter.KnowledgePackRepository(state_dir=state_dir)
    output = tmp_path / "pack.tar.gz"

    record = pack_exporter.create_pack(
        output=output,
        inputs=inputs,
        storage_config=None,
        repository=repository,
    )

    assert output.exists()
    assert record.object_key.startswith("local/")
    local_path = repository.local_artifact_path(record)
    assert local_path.exists()

    latest = repository.latest_pack()
    assert latest is not None
    assert latest.sha256 == record.sha256

    metadata_path = state_dir / "knowledge_pack" / "metadata.json"
    payload = json.loads(metadata_path.read_text())
    assert payload[-1]["object_key"] == record.object_key
    assert payload[-1]["size"] == record.size


def test_latest_pack_endpoint_returns_local_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_dir = tmp_path / "state"
    repository = pack_exporter.KnowledgePackRepository(
        dsn=None, state_dir=state_dir
    )
    now = dt.datetime.now(dt.timezone.utc)
    record = pack_exporter.PackRecord(
        object_key="local/sample.tar.gz",
        sha256="abc123",
        created_at=now,
        size=123,
    )
    repository.ensure_table()
    repository.record_pack(record)

    monkeypatch.setenv("KNOWLEDGE_PACK_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(state_dir))
    monkeypatch.setattr(pack_exporter, "KnowledgePackRepository", lambda: repository)

    url = pack_exporter.latest_pack(_actor="company")["download_url"]
    assert url.startswith("file:")
