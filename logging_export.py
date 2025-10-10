"""Utilities for exporting audit and regulatory logs to external storage."""

from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:  # pragma: no cover - boto3 is optional during unit tests.
    import boto3
except Exception:  # pragma: no cover
    boto3 = None  # type: ignore

try:  # pragma: no cover - psycopg might not be present in some test environments.
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore


DEFAULT_EXPORT_PREFIX = "log-exports"
_INSECURE_DEFAULTS_FLAG = "LOG_EXPORT_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "AETHER_STATE_DIR"
_LOCAL_STATE_SUBDIR = "log_export"


LOGGER = logging.getLogger(__name__)


def _normalise_s3_prefix(prefix: str) -> str:
    """Return a sanitised S3 prefix free from traversal or control chars."""

    if not prefix:
        return ""

    segments: list[str] = []
    for raw_segment in prefix.replace("\\", "/").split("/"):
        segment = raw_segment.strip()
        if not segment:
            continue
        if segment in {".", ".."}:
            raise ValueError("Export prefix must not contain path traversal sequences")
        if any(ord(char) < 32 for char in segment):
            raise ValueError("Export prefix must not contain control characters")
        segments.append(segment)

    return "/".join(segments)


class MissingDependencyError(RuntimeError):
    """Raised when an optional dependency required at runtime is missing."""


def _insecure_defaults_enabled() -> bool:
    return (
        os.getenv(_INSECURE_DEFAULTS_FLAG) == "1"
        or bool(os.getenv("PYTEST_CURRENT_TEST"))
    )


def _state_root() -> Path:
    root = Path(os.getenv(_STATE_DIR_ENV, ".aether_state")) / _LOCAL_STATE_SUBDIR
    root.mkdir(parents=True, exist_ok=True)
    return root


def _artifacts_root() -> Path:
    root = _state_root() / "artifacts"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _snapshots_root() -> Path:
    root = _state_root() / "snapshots"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _metadata_path() -> Path:
    return _state_root() / "metadata.json"


@dataclass(frozen=True)
class ExportConfig:
    """Configuration required to ship the generated archive to object storage."""

    bucket: str
    prefix: str = DEFAULT_EXPORT_PREFIX
    endpoint_url: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "prefix", _normalise_s3_prefix(self.prefix))


@dataclass(frozen=True)
class ExportResult:
    """Metadata captured once an export is successfully persisted."""

    export_date: dt.date
    exported_at: dt.datetime
    s3_bucket: str
    s3_key: str
    sha256: str


def _require_psycopg() -> None:
    if psycopg is None and not _insecure_defaults_enabled():
        raise MissingDependencyError("psycopg is required for log export functionality")


def _database_dsn(allow_missing: bool = False) -> Optional[str]:
    dsn = (
        os.getenv("LOG_EXPORT_DATABASE_URL")
        or os.getenv("AUDIT_DATABASE_URL")
        or os.getenv("DATABASE_URL")
    )
    if not dsn and not allow_missing:
        raise RuntimeError(
            "LOG_EXPORT_DATABASE_URL, AUDIT_DATABASE_URL, or DATABASE_URL must be set",
        )
    return dsn


def ensure_export_table(conn: "psycopg.Connection[Any]") -> None:
    """Ensure the metadata table for exports exists."""

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS log_export_status (
                id BIGSERIAL PRIMARY KEY,
                export_date DATE NOT NULL,
                exported_at TIMESTAMPTZ NOT NULL,
                s3_bucket TEXT NOT NULL,
                s3_key TEXT NOT NULL,
                sha256 TEXT NOT NULL UNIQUE
            )
            """.strip()
        )
    conn.commit()


def _normalise_json(raw: str) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return raw


def _fetch_audit_logs(
    conn: "psycopg.Connection[Any]",
    start: dt.datetime,
    end: dt.datetime,
) -> List[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:  # type: ignore[arg-type]
        cur.execute(
            """
            SELECT id, actor, action, entity, before, after, ts, ip_hash, hash, prev_hash
            FROM audit_log
            WHERE ts >= %s AND ts < %s
            ORDER BY ts ASC
            """.strip(),
            (start, end),
        )
        rows: Iterable[dict[str, Any]] = cur.fetchall()
    records: List[dict[str, Any]] = []
    for row in rows:
        row = dict(row)
        row["before"] = _normalise_json(row.get("before", "{}"))
        row["after"] = _normalise_json(row.get("after", "{}"))
        if isinstance(row.get("ts"), dt.datetime):
            row["ts"] = row["ts"].astimezone(dt.timezone.utc).isoformat()
        records.append(row)
    return records


def _fetch_reg_logs(
    conn: "psycopg.Connection[Any]",
    start: dt.datetime,
    end: dt.datetime,
) -> List[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:  # type: ignore[arg-type]
        cur.execute(
            """
            SELECT id, ts, actor, action, payload, hash, prev_hash
            FROM reg_log
            WHERE ts >= %s AND ts < %s
            ORDER BY ts ASC
            """.strip(),
            (start, end),
        )
        rows: Iterable[dict[str, Any]] = cur.fetchall()
    records: List[dict[str, Any]] = []
    for row in rows:
        row = dict(row)
        row["payload"] = _normalise_json(row.get("payload", "{}"))
        if isinstance(row.get("ts"), dt.datetime):
            row["ts"] = row["ts"].astimezone(dt.timezone.utc).isoformat()
        records.append(row)
    return records


def _gzip_bytes(payload: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(mode="wb", fileobj=buffer) as gz:
        gz.write(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    return buffer.getvalue()


def _compute_sha256(blob: bytes) -> str:
    import hashlib

    digest = hashlib.sha256()
    digest.update(blob)
    return digest.hexdigest()


def _s3_client(endpoint_url: str | None = None):
    if boto3 is None:  # pragma: no cover - guard against optional dependency missing in tests.
        raise MissingDependencyError("boto3 is required for log export uploads")
    client_kwargs: dict[str, Any] = {}
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    return boto3.client("s3", **client_kwargs)  # type: ignore[return-value]


def _build_s3_key(prefix: str, export_date: dt.date) -> str:
    safe_prefix = _normalise_s3_prefix(prefix)
    suffix = f"audit-reg-log-{export_date.isoformat()}.json.gz"
    return f"{safe_prefix}/{suffix}" if safe_prefix else suffix


def _load_local_snapshot(kind: str, export_date: dt.date) -> List[dict[str, Any]]:
    snapshot_path = _snapshots_root() / f"{kind}-{export_date.isoformat()}.json"
    if not snapshot_path.exists():
        return []
    try:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - fallback path is best effort
        LOGGER.warning("Failed to load %s snapshot for %s: %s", kind, export_date, exc)
        return []
    if isinstance(payload, list):
        return [dict(record) for record in payload if isinstance(record, dict)]
    LOGGER.warning(
        "Snapshot %s did not contain a list; ignoring invalid payload", snapshot_path
    )
    return []


def _persist_local_artifact(key: str, blob: bytes) -> Path:
    target = _artifacts_root() / key
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(blob)
    return target


def _record_local_metadata(entry: Dict[str, Any]) -> None:
    path = _metadata_path()
    existing: Dict[str, Dict[str, Any]] = {}
    if path.exists():
        try:
            raw_entries = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover - best effort logging
            LOGGER.warning("Failed to read log export metadata: %s", exc)
            raw_entries = []
        for item in raw_entries:
            if isinstance(item, dict) and "sha256" in item:
                existing[str(item["sha256"])] = item
    existing[str(entry["sha256"])] = entry
    ordered = sorted(
        existing.values(),
        key=lambda item: item.get("exported_at", ""),
        reverse=True,
    )
    path.write_text(json.dumps(ordered, indent=2, sort_keys=True), encoding="utf-8")


def _latest_local_metadata() -> Optional[ExportResult]:
    path = _metadata_path()
    if not path.exists():
        return None
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - best effort logging
        LOGGER.warning("Failed to parse log export metadata: %s", exc)
        return None
    if not isinstance(entries, list):
        return None
    for item in entries:
        if not isinstance(item, dict):
            continue
        try:
            export_date = dt.date.fromisoformat(str(item["export_date"]))
            exported_at = dt.datetime.fromisoformat(str(item["exported_at"]))
            if exported_at.tzinfo is None:
                exported_at = exported_at.replace(tzinfo=dt.timezone.utc)
            return ExportResult(
                export_date=export_date,
                exported_at=exported_at,
                s3_bucket=str(item["s3_bucket"]),
                s3_key=str(item["s3_key"]),
                sha256=str(item["sha256"]),
            )
        except Exception:  # pragma: no cover - skip malformed metadata entries
            continue
    return None


def run_export(
    *,
    for_date: dt.date,
    config: ExportConfig,
    dsn: str | None = None,
) -> ExportResult:
    """Export audit and regulatory logs for *for_date* and upload to object storage."""

    resolved_dsn = dsn or _database_dsn(allow_missing=_insecure_defaults_enabled())
    now = dt.datetime.now(dt.timezone.utc)
    start = dt.datetime.combine(for_date, dt.time.min, tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=1)
    use_database = bool(psycopg is not None and dict_row is not None and resolved_dsn)
    conn = None
    audit_records: List[dict[str, Any]]
    reg_records: List[dict[str, Any]]
    local_artifact: Optional[Path] = None

    try:
        if use_database:
            conn = psycopg.connect(resolved_dsn)  # type: ignore[arg-type]
            ensure_export_table(conn)
            audit_records = _fetch_audit_logs(conn, start, end)
            reg_records = _fetch_reg_logs(conn, start, end)
        else:
            _require_psycopg()
            audit_records = _load_local_snapshot("audit", for_date)
            reg_records = _load_local_snapshot("reg", for_date)
            if not audit_records and not reg_records:
                LOGGER.info(
                    "No local audit/reg snapshots found for %s; generating empty export",
                    for_date,
                )

        export_payload = {
            "export_date": for_date.isoformat(),
            "generated_at": now.isoformat(),
            "audit_log": audit_records,
            "reg_log": reg_records,
        }

        gz_blob = _gzip_bytes(export_payload)
        digest = _compute_sha256(gz_blob)
        key = _build_s3_key(config.prefix, for_date)

        if boto3 is not None:
            client = _s3_client(config.endpoint_url)
            client.put_object(Bucket=config.bucket, Key=key, Body=gz_blob)
        else:
            if not _insecure_defaults_enabled():
                raise MissingDependencyError("boto3 is required for log export uploads")
            local_artifact = _persist_local_artifact(key, gz_blob)
            LOGGER.info("Stored log export locally at %s", local_artifact)

        if conn is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO log_export_status (export_date, exported_at, s3_bucket, s3_key, sha256)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (sha256) DO NOTHING
                    """.strip(),
                    (for_date, now, config.bucket, key, digest),
                )
            conn.commit()
        else:
            metadata_entry: Dict[str, Any] = {
                "export_date": for_date.isoformat(),
                "exported_at": now.isoformat(),
                "s3_bucket": config.bucket,
                "s3_key": key,
                "sha256": digest,
            }
            if local_artifact is not None:
                metadata_entry["local_path"] = str(local_artifact)
            _record_local_metadata(metadata_entry)

        return ExportResult(
            export_date=for_date,
            exported_at=now,
            s3_bucket=config.bucket,
            s3_key=key,
            sha256=digest,
        )
    finally:
        if conn is not None:
            conn.close()


def latest_export(dsn: str | None = None) -> Optional[ExportResult]:
    """Return metadata for the most recent export if available."""

    resolved_dsn = dsn or _database_dsn(allow_missing=_insecure_defaults_enabled())
    if psycopg is not None and dict_row is not None and resolved_dsn:
        with psycopg.connect(resolved_dsn) as conn:  # type: ignore[arg-type]
            ensure_export_table(conn)
            with conn.cursor(row_factory=dict_row) as cur:  # type: ignore[arg-type]
                cur.execute(
                    """
                    SELECT export_date, exported_at, s3_bucket, s3_key, sha256
                    FROM log_export_status
                    ORDER BY exported_at DESC
                    LIMIT 1
                    """.strip()
                )
                row = cur.fetchone()
        if not row:
            return None
        exported_at = row["exported_at"]
        if isinstance(exported_at, dt.datetime) and exported_at.tzinfo is None:
            exported_at = exported_at.replace(tzinfo=dt.timezone.utc)
        return ExportResult(
            export_date=row["export_date"],
            exported_at=exported_at,
            s3_bucket=row["s3_bucket"],
            s3_key=row["s3_key"],
            sha256=row["sha256"],
        )

    if _insecure_defaults_enabled():
        return _latest_local_metadata()

    _require_psycopg()
    return None


__all__ = [
    "ExportConfig",
    "ExportResult",
    "MissingDependencyError",
    "latest_export",
    "run_export",
]

