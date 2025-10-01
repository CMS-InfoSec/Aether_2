from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

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
                event_id,
                entity_type,
                entity_id,
                actor,
                action,
                event_time,
                payload
    """.strip()
    assert expected_columns in query
    assert set(params) == {
        "event_id",
        "entity_type",
        "entity_id",
        "actor",
        "action",
        "event_time",
        "payload",
    }

    assert params["action"] == "report.artifact.stored"
    assert params["entity_type"] == "report_artifact"
    assert params["entity_id"] == artifact.object_key
    assert params["actor"] == "acct-123"

    created_at = params["event_time"]
    assert isinstance(created_at, datetime)
    assert created_at.tzinfo is timezone.utc
    assert created_at == artifact.created_at

    event_id = params["event_id"]
    assert isinstance(event_id, str)
    assert len(event_id) == 64

    payload = json.loads(params["payload"])
    assert payload["entity"] == {"type": "report_artifact", "id": artifact.object_key}
    metadata_payload = payload["metadata"]
    assert metadata_payload["checksum"] == artifact.checksum
    assert metadata_payload["checksum_object_key"] == artifact.checksum_object_key
    assert metadata_payload["content_type"] == "text/csv"
    assert metadata_payload["format"] == metadata["format"]
    assert metadata_payload["size_bytes"] == len(b"payload")
    assert metadata_payload["object_key"] == artifact.object_key
    assert metadata_payload["storage_backend"] == "filesystem"
