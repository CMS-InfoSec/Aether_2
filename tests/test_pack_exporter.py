import datetime as dt
import hashlib
import shutil
import tarfile
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi import FastAPI
from fastapi.testclient import TestClient

import pack_exporter
from services.common.security import require_admin_account


class StubRepository:
    def __init__(self) -> None:
        self.records = []
        self.table_ensured = False

    def ensure_table(self) -> None:
        self.table_ensured = True

    def record_pack(self, record: pack_exporter.PackRecord) -> None:
        self.records.append(record)

    def latest_pack(self) -> pack_exporter.PackRecord | None:  # pragma: no cover - helper
        return self.records[-1] if self.records else None


class FakeS3Client:
    def __init__(self) -> None:
        self.uploads = []
        self.presigned_calls = []

    def upload_fileobj(self, fileobj, bucket: str, key: str, ExtraArgs: dict) -> None:
        payload = fileobj.read()
        self.uploads.append((bucket, key, payload, ExtraArgs))

    def generate_presigned_url(self, operation: str, *, Params: dict, ExpiresIn: int) -> str:
        call = {"operation": operation, "params": Params, "expires": ExpiresIn}
        self.presigned_calls.append(call)
        return f"https://example.com/{Params['Key']}?ttl={ExpiresIn}"


def test_object_storage_config_normalises_prefix() -> None:
    config = pack_exporter.ObjectStorageConfig(bucket="bucket", prefix=" packs\\exports /2024 ")

    assert config.prefix == "packs/exports/2024"


def test_object_storage_config_rejects_traversal_prefix() -> None:
    with pytest.raises(ValueError, match="path traversal"):
        pack_exporter.ObjectStorageConfig(bucket="bucket", prefix="../secrets")


def test_object_storage_config_rejects_control_characters() -> None:
    with pytest.raises(ValueError, match="control characters"):
        pack_exporter.ObjectStorageConfig(bucket="bucket", prefix="invalid\nprefix")


@pytest.fixture()
def fake_s3(monkeypatch: pytest.MonkeyPatch) -> FakeS3Client:
    client = FakeS3Client()
    monkeypatch.setattr(pack_exporter, "_s3_client", lambda config: client)
    return client


def test_create_pack_bundles_artifacts_and_records_metadata(tmp_path: Path, fake_s3: FakeS3Client) -> None:
    weights = tmp_path / "weights"
    weights.mkdir()
    (weights / "model.bin").write_bytes(b"weights-blob")

    importance = tmp_path / "importance"
    importance.mkdir()
    (importance / "importance.json").write_text("{\"feature\": \"delta\"}")

    tags = tmp_path / "tags"
    tags.mkdir()
    (tags / "tags.csv").write_text("id,label\n1,spike\n")

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text("threshold: 0.5\n")

    inputs = pack_exporter.PackInputs(
        model_weights=weights,
        feature_importance=importance,
        anomaly_tags=tags,
        config=config_dir,
    )
    storage = pack_exporter.ObjectStorageConfig(bucket="bucket", prefix="packs")
    repository = StubRepository()
    output = tmp_path / "pack.tar.gz"

    record = pack_exporter.create_pack(
        output=output,
        inputs=inputs,
        storage_config=storage,
        repository=repository,
    )

    assert output.exists()
    assert repository.table_ensured is True
    assert repository.records[0] == record
    assert record.object_key == "packs/pack.tar.gz"
    assert record.size == output.stat().st_size

    assert len(fake_s3.uploads) == 1
    bucket, key, payload, extra = fake_s3.uploads[0]
    assert bucket == "bucket"
    assert key == "packs/pack.tar.gz"
    assert extra["Metadata"]["sha256"] == record.sha256
    assert hashlib.sha256(payload).hexdigest() == record.sha256

    with tarfile.open(output, "r:gz") as tar:
        names = set(tar.getnames())
    assert "model_weights/model.bin" in names
    assert "feature_importance/importance.json" in names
    assert "anomaly_tags/tags.csv" in names
    assert "config/settings.yaml" in names


def test_latest_pack_endpoint_returns_signed_url(monkeypatch: pytest.MonkeyPatch) -> None:
    created_at = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    sample = pack_exporter.PackRecord(
        object_key="packs/pack.tar.gz",
        sha256="abc123",
        created_at=created_at,
        size=2048,
    )

    class Repo:
        def __init__(self) -> None:
            self.latest_requested = False

        def latest_pack(self) -> pack_exporter.PackRecord | None:
            self.latest_requested = True
            return sample

    repo = Repo()
    monkeypatch.setattr(pack_exporter, "KnowledgePackRepository", lambda *_, **__: repo)
    monkeypatch.setattr(pack_exporter, "_require_psycopg", lambda: None)
    monkeypatch.setattr(pack_exporter, "_require_boto3", lambda: None)

    client = FakeS3Client()
    monkeypatch.setattr(pack_exporter, "_s3_client", lambda config: client)
    monkeypatch.setattr(
        pack_exporter,
        "_storage_config_from_env",
        lambda: pack_exporter.ObjectStorageConfig(bucket="bucket", prefix="packs"),
    )
    monkeypatch.setenv("KNOWLEDGE_PACK_URL_TTL", "600")

    app = FastAPI()
    app.include_router(pack_exporter.router)
    app.dependency_overrides[require_admin_account] = lambda: "company"
    http = TestClient(app)

    response = http.get("/knowledge/export/latest")
    assert response.status_code == 200
    payload = response.json()

    assert payload["id"] == sample.object_key
    assert payload["hash"] == sample.sha256
    assert payload["size"] == sample.size
    assert payload["ts"] == sample.created_at.isoformat()
    assert payload["download_url"] == "https://example.com/packs/pack.tar.gz?ttl=600"
    assert client.presigned_calls[0]["expires"] == 600


def test_latest_pack_endpoint_rejects_unauthorized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pack_exporter, "_require_psycopg", lambda: None)
    monkeypatch.setattr(pack_exporter, "_require_boto3", lambda: None)

    class Repo:
        def latest_pack(self) -> pack_exporter.PackRecord | None:
            return None

    monkeypatch.setattr(pack_exporter, "KnowledgePackRepository", lambda *_, **__: Repo())
    monkeypatch.setattr(
        pack_exporter,
        "_storage_config_from_env",
        lambda: pack_exporter.ObjectStorageConfig(bucket="bucket", prefix="packs"),
    )
    monkeypatch.setattr(pack_exporter, "_s3_client", lambda config: FakeS3Client())

    app = FastAPI()
    app.include_router(pack_exporter.router)
    http = TestClient(app)

    response = http.get("/knowledge/export/latest")

    assert response.status_code in {401, 403}


def _prepare_default_artifacts(root: Path) -> None:
    (root / "artifacts" / "model_weights").mkdir(parents=True, exist_ok=True)
    (root / "artifacts" / "feature_importance").mkdir(parents=True, exist_ok=True)
    (root / "artifacts" / "anomaly_tags").mkdir(parents=True, exist_ok=True)
    (root / "config").mkdir(parents=True, exist_ok=True)


def test_resolve_inputs_rejects_paths_outside_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    _prepare_default_artifacts(root)

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "weights.bin").write_text("secret")

    monkeypatch.setenv("KNOWLEDGE_PACK_ROOT", str(root))
    monkeypatch.delenv("KNOWLEDGE_PACK_WEIGHTS", raising=False)
    monkeypatch.setenv("KNOWLEDGE_PACK_WEIGHTS", str(outside / "weights.bin"))

    with pytest.raises(pack_exporter.MissingArtifactError) as excinfo:
        pack_exporter.resolve_inputs()

    assert "must reside within" in str(excinfo.value)


def test_resolve_inputs_rejects_symlinks_within_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    _prepare_default_artifacts(root)

    outside = tmp_path / "outside"
    outside.mkdir()
    secret = outside / "secrets.json"
    secret.write_text("{}")

    link = root / "artifacts" / "model_weights" / "leak.json"
    link.symlink_to(secret)

    monkeypatch.setenv("KNOWLEDGE_PACK_ROOT", str(root))

    with pytest.raises(pack_exporter.MissingArtifactError) as excinfo:
        pack_exporter.resolve_inputs()

    assert "must not contain symlinks" in str(excinfo.value)


def test_resolve_inputs_rejects_symlink_targets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    _prepare_default_artifacts(root)

    target = root / "artifacts" / "model_weights_real"
    target.mkdir(parents=True)
    link = root / "artifacts" / "model_weights"
    if link.exists():
        if link.is_symlink():
            link.unlink()
        else:
            shutil.rmtree(link)
    link.symlink_to(target, target_is_directory=True)

    monkeypatch.setenv("KNOWLEDGE_PACK_ROOT", str(root))
    monkeypatch.delenv("KNOWLEDGE_PACK_WEIGHTS", raising=False)
    monkeypatch.setenv("KNOWLEDGE_PACK_WEIGHTS", str(link))

    with pytest.raises(pack_exporter.MissingArtifactError) as excinfo:
        pack_exporter.resolve_inputs()

    assert "must not reference symlinks" in str(excinfo.value)
