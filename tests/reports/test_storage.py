from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pytest

import reports.storage as storage_module
from reports.storage import ArtifactStorage


class StubSession:
    def __init__(self) -> None:
        self.executions: list[tuple[str, Mapping[str, Any]]] = []
        self.commit_count = 0

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> None:
        self.executions.append((query.strip(), params or {}))

    def commit(self) -> None:
        self.commit_count += 1


def test_store_artifact_writes_expected_audit_log(tmp_path: Path) -> None:
    storage = ArtifactStorage(tmp_path)
    session = StubSession()
    metadata = {"format": "csv"}

    artifact = storage.store_artifact(
        session,
        account_id="acct-123",
        object_key="reports/output.csv",
        data=b"payload",
        content_type="text/csv",
        metadata=metadata,
    )

    assert (tmp_path / artifact.object_key).read_bytes() == b"payload"
    checksum_path = tmp_path / artifact.checksum_object_key
    assert checksum_path.read_text() == f"{artifact.checksum}  {artifact.object_key}\n"

    assert session.commit_count == 1
    assert len(session.executions) == 1

    query, params = session.executions[0]
    assert "INSERT INTO audit_logs" in query
    expected_columns = """
                actor,
                action,
                target,
                created_at,
                payload
    """.strip()
    assert expected_columns in query
    assert set(params) == {
        "actor",
        "action",
        "target",
        "created_at",
        "payload",
    }

    assert params["action"] == "report.artifact.stored"
    assert params["actor"] == "acct-123"
    assert params["target"] == artifact.object_key

    created_at = params["created_at"]
    assert isinstance(created_at, datetime)
    assert created_at.tzinfo is timezone.utc
    assert created_at == artifact.created_at

    payload = json.loads(params["payload"])
    assert payload["entity"] == {"type": "report_artifact", "id": artifact.object_key}
    event_id = payload["event_id"]
    assert isinstance(event_id, str)
    assert len(event_id) == 64
    assert payload["event_time"] == created_at.isoformat()
    metadata_payload = payload["metadata"]
    assert metadata_payload["checksum"] == artifact.checksum
    assert metadata_payload["checksum_object_key"] == artifact.checksum_object_key
    assert metadata_payload["content_type"] == "text/csv"
    assert metadata_payload["format"] == metadata["format"]
    assert metadata_payload["size_bytes"] == len(b"payload")
    assert metadata_payload["object_key"] == artifact.object_key
    assert metadata_payload["storage_backend"] == "filesystem"


class StubS3Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def put_object(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


def test_s3_storage_rejects_traversal_prefix(tmp_path: Path) -> None:
    client = StubS3Client()

    with pytest.raises(ValueError) as excinfo:
        ArtifactStorage(
            tmp_path,
            s3_bucket="aether-bucket",
            s3_prefix="../reports",
            s3_client=client,
        )

    assert "prefix" in str(excinfo.value)


def test_s3_storage_normalizes_prefix(tmp_path: Path) -> None:
    client = StubS3Client()

    storage = ArtifactStorage(
        tmp_path,
        s3_bucket="aether-bucket",
        s3_prefix=" /reports/quarterly/ ",
        s3_client=client,
    )

    assert storage._s3_prefix == "reports/quarterly"


def test_store_artifact_logs_target_descriptor_on_audit_failure_s3(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = ArtifactStorage(
        tmp_path,
        s3_bucket="aether-bucket",
        s3_prefix="reports",
        s3_client=StubS3Client(),
    )
    session = StubSession()

    def boom(
        self,
        *,
        actor: str,
        entity_id: str,
        metadata: Mapping[str, Any],
        event_time: datetime,
    ) -> None:
        raise RuntimeError("audit failure")

    monkeypatch.setattr(storage_module.AuditLogWriter, "artifact_stored", boom)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError):
            storage.store_artifact(
                session,
                account_id="acct-123",
                object_key="reports/output.csv",
                data=b"payload",
                content_type="text/csv",
            )

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "Failed to write audit log entry for s3://aether-bucket/reports/acct-123/reports/output.csv"
        in message
        for message in messages
    )


def test_store_artifact_rejects_object_key_traversal(tmp_path: Path) -> None:
    storage = ArtifactStorage(tmp_path)
    session = StubSession()

    with pytest.raises(ValueError) as excinfo:
        storage.store_artifact(
            session,
            account_id="acct-123",
            object_key="../secrets.txt",
            data=b"payload",
            content_type="text/plain",
        )

    assert "path traversal" in str(excinfo.value)
    # Ensure nothing was written to disk and no audit record attempted.
    assert not any(tmp_path.iterdir())
    assert not session.executions


def test_store_artifact_rejects_account_traversal(tmp_path: Path) -> None:
    storage = ArtifactStorage(tmp_path)
    session = StubSession()

    with pytest.raises(ValueError) as excinfo:
        storage.store_artifact(
            session,
            account_id="../../acct-123",
            object_key="reports/output.csv",
            data=b"payload",
            content_type="text/csv",
        )

    assert "path traversal" in str(excinfo.value)
    assert not any(tmp_path.iterdir())
    assert not session.executions


def test_store_artifact_rejects_symlink_directory(tmp_path: Path) -> None:
    if not hasattr(os, "symlink"):
        pytest.skip("platform does not support symlinks")

    storage = ArtifactStorage(tmp_path)
    session = StubSession()

    outside = tmp_path.parent / "outside"
    outside.mkdir()

    account_root = tmp_path / "acct-123"
    account_root.mkdir()
    (account_root / "reports").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError) as excinfo:
        storage.store_artifact(
            session,
            account_id="acct-123",
            object_key="reports/output.csv",
            data=b"payload",
            content_type="text/csv",
        )

    assert "symlink" in str(excinfo.value)
    assert not list(outside.iterdir())
    assert not session.executions


def test_store_artifact_rejects_symlink_target(tmp_path: Path) -> None:
    if not hasattr(os, "symlink"):
        pytest.skip("platform does not support symlinks")

    storage = ArtifactStorage(tmp_path)
    session = StubSession()

    object_dir = tmp_path / "acct-123" / "reports"
    object_dir.mkdir(parents=True)

    outside_file = tmp_path.parent / "outside.csv"
    outside_file.write_text("secret")
    (object_dir / "output.csv").symlink_to(outside_file)

    with pytest.raises(ValueError) as excinfo:
        storage.store_artifact(
            session,
            account_id="acct-123",
            object_key="reports/output.csv",
            data=b"payload",
            content_type="text/csv",
        )

    assert "symlink" in str(excinfo.value)
    assert outside_file.read_text() == "secret"
    assert not session.executions


def test_filesystem_storage_rejects_symlink_base(tmp_path: Path) -> None:
    if not hasattr(os, "symlink"):
        pytest.skip("platform does not support symlinks")

    real_base = tmp_path / "real-base"
    real_base.mkdir()

    symlink_base = tmp_path / "symlink-base"
    symlink_base.symlink_to(real_base, target_is_directory=True)

    with pytest.raises(ValueError) as excinfo:
        ArtifactStorage(symlink_base)

    assert "symlink" in str(excinfo.value)
