"""Object storage integration for report artifacts.

This module provides a thin abstraction that writes generated report

artifacts into a filesystem-backed bucket and records immutable audit
entries in TimescaleDB's ``audit_logs`` hypertable.  The storage backend is
kept intentionally simple so it can be swapped out for a real object store
(S3, GCS, MinIO, …) without touching the reporting jobs themselves.

"""
from __future__ import annotations

import hashlib
import json
import logging
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

        event_id = hashlib.sha256(
            f"{entity_id}:{event_time.isoformat()}".encode("utf-8")
        ).hexdigest()
        payload = json.dumps(
            {
                "entity": {
                    "type": "report_artifact",
                    "id": entity_id,
                },
                "metadata": metadata,
                "event_id": event_id,
                "event_time": event_time.isoformat(),
            }
        )
        params = {
            "actor": actor,
            "action": "report.artifact.stored",
            "target": entity_id,
            "created_at": event_time,
            "payload": payload,
        }

        insert_audit_log_sql = """
            INSERT INTO audit_logs (
                actor,
                action,
                target,
                created_at,
                payload
            )
            VALUES (
                %(actor)s,
                %(action)s,
                %(target)s,
                %(created_at)s,
                %(payload)s::jsonb
            )
        """

        self._session.execute(insert_audit_log_sql, params)


@dataclass
class StoredArtifact:
    """Metadata describing a stored artifact."""

    account_id: str
    object_key: str
    checksum: str
    checksum_object_key: str
    size: int
    content_type: str
    created_at: datetime


class ArtifactStorage:
    """Filesystem or S3-backed object store with audit logging."""

    def __init__(
        self,
        base_path: str | Path | None = None,
        *,
        s3_bucket: str | None = None,
        s3_prefix: str = "",
        s3_client: Any | None = None,
    ) -> None:
        self.base_path = Path(base_path or "/tmp/aether-reports")
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix.strip("/")
        self._s3_client = s3_client

        if self._s3_bucket and self._s3_client is None:
            try:  # pragma: no cover - optional dependency
                import boto3  # type: ignore
            except Exception as exc:  # pragma: no cover - optional dependency
                raise RuntimeError("boto3 is required for S3 artifact storage") from exc
            self._s3_client = boto3.client("s3")  # type: ignore[assignment]

        if not self._s3_bucket:
            self.base_path.mkdir(parents=True, exist_ok=True)
            LOGGER.debug(
                "Initialized filesystem ArtifactStorage at %s", self.base_path
            )
        else:
            LOGGER.debug(
                "Initialized S3 ArtifactStorage bucket=%s prefix=%s base_path=%s",
                self._s3_bucket,
                self._s3_prefix,
                self.base_path,
            )

    # ------------------------------------------------------------------
    # Internal helpers

    def _canonical_key(self, account_id: str, object_key: str) -> str:
        parts = [part.strip("/") for part in (account_id, object_key) if part]
        return "/".join(parts)

    def _s3_key(self, canonical_key: str) -> str:
        if not self._s3_prefix:
            return canonical_key
        return f"{self._s3_prefix}/{canonical_key}"

    @staticmethod
    def _checksum_payload(canonical_key: str, checksum: str) -> bytes:
        return f"{checksum}  {canonical_key}\n".encode("utf-8")

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
        """Persist *data* and emit an ``audit_logs`` entry.

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

            Optional structured metadata merged with the automatically populated
            audit attributes and JSON-encoded before being persisted in the
            audit log entry.

        """

        canonical_key = self._canonical_key(account_id, object_key)
        checksum = hashlib.sha256(data).hexdigest()
        checksum_object_key = f"{canonical_key}.sha256"
        created_at = datetime.now(timezone.utc)

        if not self._s3_bucket:
            target_path = self.base_path / canonical_key
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(data)
            checksum_path = target_path.with_name(target_path.name + ".sha256")
            checksum_path.write_bytes(
                self._checksum_payload(canonical_key, checksum)
            )
            LOGGER.info(
                "Stored artifact for account %s at %s (checksum=%s)",
                account_id,
                target_path,
                checksum,
            )
            storage_backend = "filesystem"
        else:
            assert self._s3_client is not None  # for type checkers
            s3_key = self._s3_key(canonical_key)
            checksum_key = self._s3_key(checksum_object_key)
            extra_args = {
                "Bucket": self._s3_bucket,
                "Key": s3_key,
                "Body": data,
                "ContentType": content_type,
                "Metadata": {"sha256": checksum},
            }
            self._s3_client.put_object(**extra_args)
            self._s3_client.put_object(
                Bucket=self._s3_bucket,
                Key=checksum_key,
                Body=self._checksum_payload(canonical_key, checksum),
                ContentType="text/plain",
            )
            LOGGER.info(
                "Stored artifact for account %s in bucket %s at %s (checksum=%s)",
                account_id,
                self._s3_bucket,
                s3_key,
                checksum,
            )
            storage_backend = "s3"

        audit_metadata: dict[str, Any] = {
            "account_id": account_id,
            "object_key": canonical_key,
            "checksum": checksum,
            "checksum_object_key": checksum_object_key,
            "size_bytes": len(data),
            "content_type": content_type,
            "created_at": created_at.isoformat(),
            "storage_backend": storage_backend,
        }

        if self._s3_bucket:
            audit_metadata["bucket"] = self._s3_bucket
            if self._s3_prefix:
                audit_metadata["bucket_prefix"] = self._s3_prefix

        if metadata:
            for key, value in metadata.items():
                audit_metadata.setdefault(key, value)

        try:
            audit_log_writer = AuditLogWriter(session)
            audit_log_writer.artifact_stored(
                actor=account_id,
                entity_id=canonical_key,
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
            object_key=canonical_key,
            checksum=checksum,
            checksum_object_key=checksum_object_key,
            size=len(data),
            content_type=content_type,
            created_at=created_at,
        )


def build_storage_from_env(env: Mapping[str, str]) -> ArtifactStorage:
    """Factory that instantiates :class:`ArtifactStorage` from environment variables."""

    base_path = env.get("REPORT_STORAGE_PATH", "/tmp/aether-reports")
    backend = env.get("REPORT_STORAGE_BACKEND", "filesystem").lower()

    if backend == "s3":
        bucket = env.get("REPORT_STORAGE_S3_BUCKET")
        if not bucket:
            raise ValueError(
                "REPORT_STORAGE_S3_BUCKET must be configured when using the S3 backend"
            )
        prefix = env.get("REPORT_STORAGE_S3_PREFIX", "")
        client_kwargs: dict[str, Any] = {}
        endpoint = env.get("REPORT_STORAGE_S3_ENDPOINT")
        if endpoint:
            client_kwargs["endpoint_url"] = endpoint
        try:  # pragma: no cover - optional dependency
            import boto3  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("boto3 is required for S3 artifact storage") from exc
        s3_client = boto3.client("s3", **client_kwargs)  # type: ignore[assignment]
        return ArtifactStorage(
            base_path,
            s3_bucket=bucket,
            s3_prefix=prefix,
            s3_client=s3_client,
        )

    return ArtifactStorage(base_path)


__all__ = ["ArtifactStorage", "StoredArtifact", "build_storage_from_env", "TimescaleSession"]
