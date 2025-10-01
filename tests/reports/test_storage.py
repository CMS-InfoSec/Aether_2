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
    assert session.commit_count == 1
    assert len(session.executions) == 1

    query, params = session.executions[0]
    assert "INSERT INTO audit_logs" in query
    assert params["actor"] == "acct-123"
    assert params["action"] == "report_artifact_stored"
    assert params["target"] == artifact.object_key

    payload = json.loads(params["payload"])
    assert payload["checksum"] == artifact.checksum
    assert payload["content_type"] == "text/csv"
    assert payload["metadata"] == metadata
    assert payload["size_bytes"] == len(b"payload")
    assert payload["object_key"] == artifact.object_key

    created_at = params["created_at"]
    assert isinstance(created_at, datetime)
    assert created_at.tzinfo is timezone.utc
    assert created_at == artifact.created_at
