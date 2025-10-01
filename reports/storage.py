"""Object storage integration for report artifacts.

This module provides a thin abstraction that writes generated report
artifacts into a filesystem-backed bucket and records immutable audit
entries in TimescaleDB's ``audit_log`` hypertable.  The storage backend is
kept intentionally simple so it can be swapped out for a real object store
(S3, GCS, MinIO, …) without touching the reporting jobs themselves.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol

LOGGER = logging.getLogger(__name__)


class TimescaleSession(Protocol):
    """Protocol describing the subset of a DB API session we rely on."""

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> Any:
        ...

    def commit(self) -> None:  # pragma: no cover - optional behaviour
        ...


class AuditLogWriter:
    """Helper responsible for emitting audit log events."""

    def __init__(self, session: TimescaleSession) -> None:
        self._session = session

    def artifact_stored(
        self,
        *,
        actor: str,
        entity_id: str,
        metadata: Mapping[str, Any],
        event_time: datetime,
    ) -> None:
        """Record an audit event describing a stored artifact."""

        event_id = uuid.uuid4()
        params = {
            "event_id": event_id,
            "entity_type": "report_artifact",
            "entity_id": entity_id,
            "actor": actor,
            "event_time": event_time,
            "metadata": json.dumps(metadata),
        }

        self._session.execute(
            """
            INSERT INTO audit_log (
                event_id,
                entity_type,
                entity_id,
                actor,
                event_time,
                metadata
            )
            VALUES (
                %(event_id)s,
                %(entity_type)s,
                %(entity_id)s,
                %(actor)s,
                %(event_time)s,
                %(metadata)s::jsonb
            )
            """,
            params,
        )


@dataclass
class StoredArtifact:
    """Metadata describing a stored artifact."""

    account_id: str
    object_key: str
    checksum: str
    size: int
    content_type: str
    created_at: datetime


class ArtifactStorage:
    """Filesystem-backed object store with audit logging."""

    def __init__(self, base_path: str | Path) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        LOGGER.debug("Initialized ArtifactStorage at %s", self.base_path)

    def store_artifact(
        self,
        session: TimescaleSession,
        *,
        account_id: str,
        object_key: str,
        data: bytes,
        content_type: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> StoredArtifact:
        """Persist *data* and emit an ``audit_log`` entry.

        Parameters
        ----------
        session:
            Database session capable of executing SQL statements.
        account_id:
            Logical account namespace for the artifact.
        object_key:
            Path component under the account namespace where the artifact will
            be written.
        data:
            The raw bytes that should be written to storage.
        content_type:
            MIME type describing the payload.
        metadata:
            Optional structured metadata that will be JSON-encoded and stored in
            the audit log entry.
        """

        account_root = self.base_path / account_id
        account_root.mkdir(parents=True, exist_ok=True)
        target_path = account_root / object_key
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(data)
        checksum = hashlib.sha256(data).hexdigest()
        created_at = datetime.now(timezone.utc)
        relative_path = str(target_path.relative_to(self.base_path))
        LOGGER.info(
            "Stored artifact for account %s at %s (checksum=%s)",
            account_id,
            target_path,
            checksum,
        )

        audit_metadata: dict[str, Any] = {
            "account_id": account_id,
            "object_key": relative_path,
            "checksum": checksum,
            "size_bytes": len(data),
            "content_type": content_type,
            "created_at": created_at.isoformat(),
        }
        if metadata:
            for key, value in metadata.items():
                audit_metadata.setdefault(key, value)

        try:
            audit_log_writer = AuditLogWriter(session)
            audit_log_writer.artifact_stored(
                actor=account_id,
                entity_id=relative_path,
                metadata=audit_metadata,
                event_time=created_at,
            )
            if hasattr(session, "commit"):
                try:
                    session.commit()
                except Exception:  # pragma: no cover - defensive logging
                    LOGGER.debug("Session commit failed; continuing", exc_info=True)
        except Exception:  # pragma: no cover - logging for production visibility
            LOGGER.exception("Failed to write audit log entry for %s", target_path)
            raise

        return StoredArtifact(
            account_id=account_id,
            object_key=str(target_path.relative_to(self.base_path)),
            checksum=checksum,
            size=len(data),
            content_type=content_type,
            created_at=created_at,
        )


def build_storage_from_env(env: Mapping[str, str]) -> ArtifactStorage:
    """Factory that instantiates :class:`ArtifactStorage` from environment variables."""

    base_path = env.get("REPORT_STORAGE_PATH", "/tmp/aether-reports")
    return ArtifactStorage(base_path)


__all__ = ["ArtifactStorage", "StoredArtifact", "build_storage_from_env", "TimescaleSession"]
