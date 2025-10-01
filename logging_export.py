"""Utilities for exporting audit and regulatory logs to external storage."""

from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import os
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional

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


class MissingDependencyError(RuntimeError):
    """Raised when an optional dependency required at runtime is missing."""


@dataclass(frozen=True)
class ExportConfig:
    """Configuration required to ship the generated archive to object storage."""

    bucket: str
    prefix: str = DEFAULT_EXPORT_PREFIX
    endpoint_url: str | None = None


@dataclass(frozen=True)
class ExportResult:
    """Metadata captured once an export is successfully persisted."""

    export_date: dt.date
    exported_at: dt.datetime
    s3_bucket: str
    s3_key: str
    sha256: str


def _require_psycopg() -> None:
    if psycopg is None:  # pragma: no cover - sanity guard for environments without psycopg.
        raise MissingDependencyError("psycopg is required for log export functionality")


def _database_dsn() -> str:
    dsn = (
        os.getenv("LOG_EXPORT_DATABASE_URL")
        or os.getenv("AUDIT_DATABASE_URL")
        or os.getenv("DATABASE_URL")
    )
    if not dsn:
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
            SELECT id, actor, action, entity, before_json, after_json, ts, ip_hash
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
        row["before_json"] = _normalise_json(row.get("before_json", "{}"))
        row["after_json"] = _normalise_json(row.get("after_json", "{}"))
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
    return f"{prefix.rstrip('/')}/audit-reg-log-{export_date.isoformat()}.json.gz"


def run_export(
    *,
    for_date: dt.date,
    config: ExportConfig,
    dsn: str | None = None,
) -> ExportResult:
    """Export audit and regulatory logs for *for_date* and upload to object storage."""

    _require_psycopg()
    resolved_dsn = dsn or _database_dsn()
    now = dt.datetime.now(dt.timezone.utc)
    start = dt.datetime.combine(for_date, dt.time.min, tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=1)

    with psycopg.connect(resolved_dsn) as conn:  # type: ignore[arg-type]
        ensure_export_table(conn)
        audit_records = _fetch_audit_logs(conn, start, end)
        reg_records = _fetch_reg_logs(conn, start, end)

        export_payload = {
            "export_date": for_date.isoformat(),
            "generated_at": now.isoformat(),
            "audit_log": audit_records,
            "reg_log": reg_records,
        }

        gz_blob = _gzip_bytes(export_payload)
        digest = _compute_sha256(gz_blob)
        key = _build_s3_key(config.prefix, for_date)

        client = _s3_client(config.endpoint_url)
        client.put_object(Bucket=config.bucket, Key=key, Body=gz_blob)

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

    return ExportResult(
        export_date=for_date,
        exported_at=now,
        s3_bucket=config.bucket,
        s3_key=key,
        sha256=digest,
    )


def latest_export(dsn: str | None = None) -> Optional[ExportResult]:
    """Return metadata for the most recent export if available."""

    _require_psycopg()
    resolved_dsn = dsn or _database_dsn()
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


__all__ = [
    "ExportConfig",
    "ExportResult",
    "MissingDependencyError",
    "latest_export",
    "run_export",
]

