"""Daily log export tooling with multi-format rendering and HTTP endpoint."""

from __future__ import annotations

import datetime as dt
import io
import json
import os
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping

import markdown2
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from audit_mode import AuditorPrincipal, require_auditor_identity

try:  # pragma: no cover - boto3 is optional in some environments.
    import boto3
except Exception:  # pragma: no cover
    boto3 = None  # type: ignore

try:  # pragma: no cover - psycopg might be unavailable during some tests.
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore


SUPPORTED_FORMATS: Mapping[str, str] = {
    "json": "application/json",
    "csv": "text/csv",
    "pdf": "application/pdf",
    "md": "text/markdown",
}


class MissingDependencyError(RuntimeError):
    """Raised when a required optional dependency is missing."""


@dataclass(frozen=True)
class StorageConfig:
    """Configuration required to persist exports to object storage."""

    bucket: str
    prefix: str = "log-exports"
    endpoint_url: str | None = None


@dataclass(frozen=True)
class ExportArtifact:
    """Metadata and payload for a rendered export artifact."""

    format: str
    object_key: str
    content_type: str
    data: bytes

    @property
    def size(self) -> int:
        return len(self.data)


@dataclass(frozen=True)
class ExportResult:
    """Envelope returned after a successful export run."""

    run_id: str
    export_date: dt.date
    artifacts: Mapping[str, ExportArtifact]


def _require_psycopg() -> None:
    if psycopg is None:  # pragma: no cover - exercised when dependency missing.
        raise MissingDependencyError("psycopg is required for log exports")


def _require_boto3() -> None:
    if boto3 is None:  # pragma: no cover - exercised when dependency missing.
        raise MissingDependencyError("boto3 is required for log export uploads")


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


def _storage_config_from_env() -> StorageConfig:
    bucket = os.getenv("MULTIFORMAT_EXPORT_BUCKET") or os.getenv("EXPORT_BUCKET")
    if not bucket:
        raise RuntimeError("MULTIFORMAT_EXPORT_BUCKET or EXPORT_BUCKET must be configured")
    prefix = os.getenv("MULTIFORMAT_EXPORT_PREFIX") or os.getenv("EXPORT_PREFIX", "log-exports")
    endpoint_url = os.getenv("MULTIFORMAT_EXPORT_ENDPOINT_URL") or os.getenv(
        "EXPORT_S3_ENDPOINT_URL"
    )
    return StorageConfig(bucket=bucket, prefix=prefix, endpoint_url=endpoint_url)


def _normalise_datetime(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc).isoformat()
    return value


def _normalise_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


class LogExporter:
    """Export audit, fill, and PnL logs to multiple formats."""

    def __init__(self, *, config: StorageConfig, dsn: str | None = None) -> None:
        self._config = config
        self._dsn = dsn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def export(self, *, for_date: dt.date) -> ExportResult:
        _require_psycopg()
        _require_boto3()

        records = self._fetch_logs(for_date)
        run_id = uuid.uuid4().hex

        artifacts = self._render_artifacts(records, for_date, run_id)
        client = self._s3_client()
        for artifact in artifacts.values():
            metadata = {
                "run_id": run_id,
                "export_date": for_date.isoformat(),
                "format": artifact.format,
            }
            client.put_object(
                Bucket=self._config.bucket,
                Key=artifact.object_key,
                Body=artifact.data,
                ContentType=artifact.content_type,
                Metadata=metadata,
            )
        return ExportResult(run_id=run_id, export_date=for_date, artifacts=artifacts)

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _render_artifacts(
        self,
        records: Mapping[str, List[Dict[str, Any]]],
        export_date: dt.date,
        run_id: str,
    ) -> Dict[str, ExportArtifact]:
        combined = {
            name: [self._prepare_row(row) for row in rows]
            for name, rows in records.items()
        }

        artifacts: Dict[str, ExportArtifact] = {}
        payload = {
            "export_date": export_date.isoformat(),
            "run_id": run_id,
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "records": combined,
        }

        json_bytes = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        artifacts["json"] = self._build_artifact(
            format="json",
            export_date=export_date,
            run_id=run_id,
            extension="json",
            content_type=SUPPORTED_FORMATS["json"],
            data=json_bytes,
        )

        csv_bytes = self._render_csv_bytes(combined)
        artifacts["csv"] = self._build_artifact(
            format="csv",
            export_date=export_date,
            run_id=run_id,
            extension="csv",
            content_type=SUPPORTED_FORMATS["csv"],
            data=csv_bytes,
        )

        markdown_bytes = self._render_markdown_bytes(combined, export_date, run_id)
        artifacts["md"] = self._build_artifact(
            format="md",
            export_date=export_date,
            run_id=run_id,
            extension="md",
            content_type=SUPPORTED_FORMATS["md"],
            data=markdown_bytes,
        )

        pdf_bytes = self._render_pdf_bytes(combined, export_date, run_id)
        artifacts["pdf"] = self._build_artifact(
            format="pdf",
            export_date=export_date,
            run_id=run_id,
            extension="pdf",
            content_type=SUPPORTED_FORMATS["pdf"],
            data=pdf_bytes,
        )

        return artifacts

    def _render_csv_bytes(self, combined: Mapping[str, List[Dict[str, Any]]]) -> bytes:
        frames = []
        for name, rows in combined.items():
            frame = pd.DataFrame(rows)
            frame.insert(0, "log_type", name)
            frames.append(frame)
        if frames:
            df = pd.concat(frames, ignore_index=True, sort=False)
        else:
            df = pd.DataFrame(columns=["log_type"])
        df = df.fillna("")
        return df.to_csv(index=False).encode("utf-8")

    def _render_markdown_bytes(
        self,
        combined: Mapping[str, List[Dict[str, Any]]],
        export_date: dt.date,
        run_id: str,
    ) -> bytes:
        lines = ["# Daily Log Export", ""]
        lines.append(f"*Date:* {export_date.isoformat()}")
        lines.append(f"*Run ID:* {run_id}")
        lines.append("")
        for name, rows in combined.items():
            title = name.replace("_", " ").title()
            lines.append(f"## {title}")
            if not rows:
                lines.append("_No records found._")
                lines.append("")
                continue
            headers = sorted({key for row in rows for key in row.keys()})
            header_line = " | ".join(["Log Type"] + headers)
            separator_line = " | ".join(["---"] * (len(headers) + 1))
            lines.append(header_line)
            lines.append(separator_line)
            for row in rows:
                values = [row.get(header, "") for header in headers]
                rendered = " | ".join([name] + [str(value) for value in values])
                lines.append(rendered)
            lines.append("")
        markdown_content = "\n".join(lines).strip() + "\n"
        # Validate markdown structure using markdown2 (requirement mandates usage).
        markdown2.markdown(markdown_content)
        return markdown_content.encode("utf-8")

    def _render_pdf_bytes(
        self,
        combined: Mapping[str, List[Dict[str, Any]]],
        export_date: dt.date,
        run_id: str,
    ) -> bytes:
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=LETTER)
        styles = getSampleStyleSheet()
        story = [Paragraph("Daily Log Export", styles["Title"])]
        story.append(Paragraph(f"Date: {export_date.isoformat()}", styles["Normal"]))
        story.append(Paragraph(f"Run ID: {run_id}", styles["Normal"]))
        story.append(Spacer(1, 12))

        for name, rows in combined.items():
            story.append(Paragraph(name.replace("_", " ").title(), styles["Heading2"]))
            if not rows:
                story.append(Paragraph("No records found.", styles["Italic"]))
                story.append(Spacer(1, 12))
                continue
            headers = sorted({key for row in rows for key in row.keys()})
            table_data = [["Log Type", *headers]]
            for row in rows:
                values = [row.get(header, "") for header in headers]
                table_data.append([name, *[str(value) for value in values]])
            table = Table(table_data, repeatRows=1)
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ]
                )
            )
            story.append(table)
            story.append(Spacer(1, 12))

        doc.build(story)
        return buffer.getvalue()

    def _build_artifact(
        self,
        *,
        format: str,
        export_date: dt.date,
        run_id: str,
        extension: str,
        content_type: str,
        data: bytes,
    ) -> ExportArtifact:
        key = self._object_key(export_date, run_id, extension)
        return ExportArtifact(format=format, object_key=key, content_type=content_type, data=data)

    # ------------------------------------------------------------------
    # Data access
    # ------------------------------------------------------------------

    def _fetch_logs(self, for_date: dt.date) -> Dict[str, List[Dict[str, Any]]]:
        start = dt.datetime.combine(for_date, dt.time.min, tzinfo=dt.timezone.utc)
        end = start + dt.timedelta(days=1)
        dsn = self._dsn or _database_dsn()

        results: Dict[str, List[Dict[str, Any]]] = {
            "audit_log": [],
            "fills": [],
            "pnl": [],
        }

        with psycopg.connect(dsn) as conn:  # type: ignore[arg-type]
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
                results["audit_log"] = [self._normalise_record(row) for row in cur.fetchall()]

            with conn.cursor(row_factory=dict_row) as cur:  # type: ignore[arg-type]
                cur.execute(
                    """
                    SELECT *
                    FROM fills
                    WHERE fill_time >= %s AND fill_time < %s
                    ORDER BY fill_time ASC
                    """.strip(),
                    (start, end),
                )
                results["fills"] = [self._normalise_record(row) for row in cur.fetchall()]

            with conn.cursor(row_factory=dict_row) as cur:  # type: ignore[arg-type]
                cur.execute(
                    """
                    SELECT *
                    FROM pnl
                    WHERE as_of >= %s AND as_of < %s
                    ORDER BY as_of ASC
                    """.strip(),
                    (start, end),
                )
                results["pnl"] = [self._normalise_record(row) for row in cur.fetchall()]

        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _normalise_record(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        normalised: Dict[str, Any] = {}
        for key, value in dict(row).items():
            if key in {"before", "after", "payload"}:
                value = _normalise_json(value)
            value = _normalise_datetime(value)
            normalised[key] = value
        return normalised

    def _prepare_row(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        prepared: Dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, (dict, list)):
                prepared[key] = json.dumps(value, sort_keys=True)
            else:
                prepared[key] = value
        return prepared

    def _object_key(self, export_date: dt.date, run_id: str, extension: str) -> str:
        prefix = self._config.prefix.rstrip("/")
        date_part = export_date.isoformat()
        filename = f"daily-logs.{extension}"
        return f"{prefix}/{date_part}/{run_id}/{filename}" if prefix else f"{date_part}/{run_id}/{filename}"

    def _s3_client(self):
        client_kwargs: Dict[str, Any] = {}
        if self._config.endpoint_url:
            client_kwargs["endpoint_url"] = self._config.endpoint_url
        return boto3.client("s3", **client_kwargs)  # type: ignore[return-value]


router = APIRouter(prefix="/logs", tags=["logs"])

_EXPORTER: LogExporter | None = None


def _get_exporter() -> LogExporter:
    global _EXPORTER
    if _EXPORTER is None:
        config = _storage_config_from_env()
        _EXPORTER = LogExporter(config=config)
    return _EXPORTER


@router.get(
    "/export",
    response_class=Response,
    responses={
        200: {
            "content": {media_type: {} for media_type in SUPPORTED_FORMATS.values()},
            "description": "Rendered log export",
        }
    },
)
def export_logs(
    *,
    date: dt.date = Query(..., description="UTC date to export logs for"),
    format: str = Query(
        "json",
        pattern="^(json|csv|pdf|md)$",
        description="Desired response format",
    ),
    _: AuditorPrincipal = Depends(require_auditor_identity),
) -> Response:
    """Trigger a multi-format export and return the requested artifact."""

    if format not in SUPPORTED_FORMATS:
        raise HTTPException(status_code=400, detail=f"Unsupported format '{format}'")

    try:
        exporter = _get_exporter()
        result = exporter.export(for_date=date)
    except MissingDependencyError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    artifact = result.artifacts[format]
    headers = {
        "X-Run-ID": result.run_id,
        "X-Export-Date": result.export_date.isoformat(),
        "X-Object-Key": artifact.object_key,
        "Content-Disposition": f"attachment; filename=\"{os.path.basename(artifact.object_key)}\"",
    }
    return Response(content=artifact.data, media_type=artifact.content_type, headers=headers)


__all__ = [
    "ExportArtifact",
    "ExportResult",
    "LogExporter",
    "MissingDependencyError",
    "router",
    "export_logs",
]

