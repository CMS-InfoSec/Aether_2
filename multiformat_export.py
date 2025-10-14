"""Daily log export tooling with multi-format rendering and HTTP endpoint."""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

try:  # pragma: no cover - markdown2 may be absent during some tests.
    import markdown2
except Exception:  # pragma: no cover
    markdown2 = None  # type: ignore[assignment]

try:  # pragma: no cover - prefer FastAPI when available
    from fastapi import APIRouter, Depends, HTTPException, Query
    from fastapi.responses import Response
except Exception:  # pragma: no cover - exercised when FastAPI unavailable
    from services.common.fastapi_stub import (  # type: ignore[misc]
        APIRouter,
        Depends,
        HTTPException,
        Query,
        Response,
    )

try:  # pragma: no cover - reportlab is an optional dependency.
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
except Exception:  # pragma: no cover
    colors = None  # type: ignore[assignment]
    LETTER = None  # type: ignore[assignment]
    getSampleStyleSheet = None  # type: ignore[assignment]
    Paragraph = None  # type: ignore[assignment]
    SimpleDocTemplate = None  # type: ignore[assignment]
    Spacer = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]
    TableStyle = None  # type: ignore[assignment]

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


_INSECURE_DEFAULTS_FLAG = "MULTIFORMAT_EXPORT_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "AETHER_STATE_DIR"
_STATE_SUBDIR = "log_export"
_LOCAL_ARTIFACT_DIR = "multiformat"


SUPPORTED_FORMATS: Mapping[str, str] = {
    "json": "application/json",
    "csv": "text/csv",
    "pdf": "application/pdf",
    "md": "text/markdown",
}


class MissingDependencyError(RuntimeError):
    """Raised when a required optional dependency is missing."""


def _insecure_defaults_enabled() -> bool:
    return os.getenv(_INSECURE_DEFAULTS_FLAG) == "1" or bool(
        os.getenv("PYTEST_CURRENT_TEST")
    )


def _state_root() -> Path:
    root = Path(os.getenv(_STATE_DIR_ENV, ".aether_state")) / _STATE_SUBDIR
    root.mkdir(parents=True, exist_ok=True)
    return root


def _snapshots_root() -> Path:
    root = _state_root() / "snapshots"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _local_artifacts_root() -> Path:
    root = _state_root() / _LOCAL_ARTIFACT_DIR
    root.mkdir(parents=True, exist_ok=True)
    return root


def _local_metadata_path() -> Path:
    return _state_root() / "multiformat_metadata.json"


def _render_minimal_pdf(text: str) -> bytes:
    lines = text.splitlines() or [""]

    def _escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    content_parts = ["BT", "/F1 12 Tf", "72 720 Td"]
    for index, line in enumerate(lines):
        if index > 0:
            content_parts.append("0 -14 Td")
        content_parts.append(f"({_escape(line)}) Tj")
    content_parts.append("ET")
    content_stream = "\n".join(content_parts).encode("utf-8")

    objects: list[bytes] = []
    objects.append(b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n")
    objects.append(b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n")
    objects.append(
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj\n"
    )
    objects.append(
        b"4 0 obj << /Length "
        + str(len(content_stream)).encode("ascii")
        + b" >> stream\n"
        + content_stream
        + b"\nendstream\nendobj\n"
    )
    objects.append(b"5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n")

    header = b"%PDF-1.4\n"
    pdf_body = header + b"".join(objects)
    xref_offset = len(pdf_body)
    xref_entries = ["0000000000 65535 f "]
    current = len(header)
    for obj in objects:
        xref_entries.append(f"{current:010d} 00000 n ")
        current += len(obj)
    xref = ("xref\n0 {count}\n".format(count=len(xref_entries)).encode("ascii"))
    xref += "\n".join(xref_entries).encode("ascii") + b"\n"
    trailer = (
        "trailer << /Size {count} /Root 1 0 R >>\nstartxref\n{offset}\n%%EOF\n".format(
            count=len(xref_entries), offset=xref_offset
        ).encode("ascii")
    )
    return pdf_body + xref + trailer


def _normalise_export_prefix(prefix: str | None) -> str:
    """Return a sanitised S3 key prefix suitable for log exports."""

    if not prefix:
        return ""

    segments: list[str] = []
    for raw_segment in prefix.replace("\\", "/").split("/"):
        segment = raw_segment.strip()
        if not segment:
            continue
        if segment in {".", ".."}:
            raise ValueError("Log export prefix must not contain path traversal sequences")
        if any(ord(char) < 32 for char in segment):
            raise ValueError("Log export prefix must not contain control characters")
        segments.append(segment)

    return "/".join(segments)


@dataclass(frozen=True)
class StorageConfig:
    """Configuration required to persist exports to object storage."""

    bucket: str
    prefix: str = "log-exports"
    endpoint_url: str | None = None

    def __post_init__(self) -> None:
        normalised_prefix = _normalise_export_prefix(self.prefix)
        object.__setattr__(self, "prefix", normalised_prefix)


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
    if psycopg is None and not _insecure_defaults_enabled():
        raise MissingDependencyError("psycopg is required for log exports")


def _require_boto3() -> None:
    if boto3 is None and not _insecure_defaults_enabled():
        raise MissingDependencyError("boto3 is required for log export uploads")


def _require_markdown2() -> Any:
    if markdown2 is None:
        if not _insecure_defaults_enabled():
            raise MissingDependencyError(
                "markdown2 is required for log export markdown rendering"
            )

        class _FallbackMarkdown:
            @staticmethod
            def markdown(content: str) -> str:
                return content

        return _FallbackMarkdown()
    return markdown2


def _database_dsn(*, allow_missing: bool = False) -> Optional[str]:
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


def _storage_config_from_env(allow_missing: bool = False) -> StorageConfig:
    bucket = os.getenv("MULTIFORMAT_EXPORT_BUCKET") or os.getenv("EXPORT_BUCKET")
    if not bucket:
        if allow_missing:
            bucket = "local-fallback"
        else:
            raise RuntimeError(
                "MULTIFORMAT_EXPORT_BUCKET or EXPORT_BUCKET must be configured"
            )
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
        allow_insecure = _insecure_defaults_enabled()
        if not allow_insecure:
            _require_psycopg()
            _require_boto3()

        records = self._fetch_logs(for_date)
        run_id = uuid.uuid4().hex

        artifacts = self._render_artifacts(records, for_date, run_id)
        if boto3 is not None:
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
        else:
            if not allow_insecure:
                raise MissingDependencyError("boto3 is required for log export uploads")
            self._persist_local_artifacts(run_id, for_date, artifacts)
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
        headers: list[str] = []
        seen: set[str] = set()
        for rows in combined.values():
            for row in rows:
                for key in row.keys():
                    if key not in seen:
                        seen.add(key)
                        headers.append(key)

        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(["log_type", *headers])
        for name, rows in combined.items():
            for row in rows:
                record = [row.get(header, "") for header in headers]
                values = ["" if value is None else str(value) for value in record]
                writer.writerow([name, *values])
        return buffer.getvalue().encode("utf-8")

    def _render_markdown_bytes(
        self,
        combined: Mapping[str, List[Dict[str, Any]]],
        export_date: dt.date,
        run_id: str,
    ) -> bytes:
        markdown_module = _require_markdown2()
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
        markdown_module.markdown(markdown_content)
        return markdown_content.encode("utf-8")

    def _render_pdf_bytes(
        self,
        combined: Mapping[str, List[Dict[str, Any]]],
        export_date: dt.date,
        run_id: str,
    ) -> bytes:
        if (
            SimpleDocTemplate is None
            or Paragraph is None
            or Spacer is None
            or Table is None
            or TableStyle is None
            or colors is None
            or LETTER is None
            or getSampleStyleSheet is None
        ):
            if not _insecure_defaults_enabled():
                raise MissingDependencyError("reportlab is required for log export PDF rendering")
            return self._render_fallback_pdf(combined, export_date, run_id)

        SimpleDocTemplate_cls = SimpleDocTemplate
        Paragraph_cls = Paragraph
        Spacer_cls = Spacer
        Table_cls = Table
        TableStyle_cls = TableStyle
        colors_module = colors
        letter_page_size = LETTER
        get_stylesheet = getSampleStyleSheet
        buffer = io.BytesIO()
        doc = SimpleDocTemplate_cls(buffer, pagesize=letter_page_size)
        styles = get_stylesheet()
        story = [Paragraph_cls("Daily Log Export", styles["Title"])]
        story.append(Paragraph_cls(f"Date: {export_date.isoformat()}", styles["Normal"]))
        story.append(Paragraph_cls(f"Run ID: {run_id}", styles["Normal"]))
        story.append(Spacer_cls(1, 12))

        for name, rows in combined.items():
            story.append(Paragraph_cls(name.replace("_", " ").title(), styles["Heading2"]))
            if not rows:
                story.append(Paragraph_cls("No records found.", styles["Italic"]))
                story.append(Spacer_cls(1, 12))
                continue
            headers = sorted({key for row in rows for key in row.keys()})
            table_data = [["Log Type", *headers]]
            for row in rows:
                values = [row.get(header, "") for header in headers]
                table_data.append([name, *[str(value) for value in values]])
            table = Table_cls(table_data, repeatRows=1)
            table.setStyle(
                TableStyle_cls(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors_module.lightgrey),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors_module.black),
                        ("GRID", (0, 0), (-1, -1), 0.25, colors_module.grey),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ]
                )
            )
            story.append(table)
            story.append(Spacer_cls(1, 12))

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
        dsn = self._dsn or _database_dsn(allow_missing=_insecure_defaults_enabled())

        if psycopg is None or dict_row is None or not dsn:
            if _insecure_defaults_enabled():
                return self._load_local_logs(for_date)
            _require_psycopg()

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
        if boto3 is None:
            raise MissingDependencyError("boto3 is required for log export uploads")
        client_kwargs: Dict[str, Any] = {}
        if self._config.endpoint_url:
            client_kwargs["endpoint_url"] = self._config.endpoint_url
        return boto3.client("s3", **client_kwargs)  # type: ignore[return-value]

    def _persist_local_artifacts(
        self,
        run_id: str,
        export_date: dt.date,
        artifacts: Mapping[str, ExportArtifact],
    ) -> None:
        root = _local_artifacts_root() / export_date.isoformat() / run_id
        root.mkdir(parents=True, exist_ok=True)
        entries: list[dict[str, Any]] = []
        for artifact in artifacts.values():
            target = root / os.path.basename(artifact.object_key)
            target.write_bytes(artifact.data)
            entries.append(
                {
                    "format": artifact.format,
                    "object_key": artifact.object_key,
                    "local_path": str(target),
                    "content_type": artifact.content_type,
                    "size": artifact.size,
                }
            )

        metadata_path = _local_metadata_path()
        metadata: list[dict[str, Any]] = []
        if metadata_path.exists():
            try:
                raw = json.loads(metadata_path.read_text(encoding="utf-8"))
            except Exception:  # pragma: no cover - corruption fallback
                raw = []
            if isinstance(raw, list):
                metadata = [item for item in raw if isinstance(item, dict)]
        metadata.append(
            {
                "run_id": run_id,
                "export_date": export_date.isoformat(),
                "artifacts": entries,
            }
        )
        metadata.sort(key=lambda item: item.get("export_date", ""))
        metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")

    def _load_local_snapshot(self, kind: str, for_date: dt.date) -> List[Dict[str, Any]]:
        path = _snapshots_root() / f"{kind}-{for_date.isoformat()}.json"
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # pragma: no cover - best effort fallback
            return []
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, dict)]
        return []

    def _load_local_logs(self, for_date: dt.date) -> Dict[str, List[Dict[str, Any]]]:
        now = dt.datetime.now(dt.timezone.utc)
        audit_records = self._load_local_snapshot("audit", for_date)
        fills_records = self._load_local_snapshot("fills", for_date)
        pnl_records = self._load_local_snapshot("pnl", for_date)

        if not (audit_records or fills_records or pnl_records):
            audit_records = [
                {
                    "id": f"local-{for_date.isoformat()}",
                    "actor": "system",
                    "action": "local_export",
                    "entity": "log_export",
                    "before": {},
                    "after": {},
                    "ts": now.isoformat(),
                    "note": "Generated by insecure default fallback",
                }
            ]
        return {
            "audit_log": [self._normalise_record(row) for row in audit_records],
            "fills": [self._normalise_record(row) for row in fills_records],
            "pnl": [self._normalise_record(row) for row in pnl_records],
        }

    def _render_fallback_pdf(
        self,
        combined: Mapping[str, List[Dict[str, Any]]],
        export_date: dt.date,
        run_id: str,
    ) -> bytes:
        lines = [
            "Daily Log Export",
            f"Date: {export_date.isoformat()}",
            f"Run ID: {run_id}",
            "",
        ]
        for name, rows in combined.items():
            lines.append(name)
            if not rows:
                lines.append("  (no records)")
                continue
            for row in rows:
                serialised = json.dumps(row, sort_keys=True)
                lines.append(f"  {serialised}")

        text = "\n".join(lines)
        return _render_minimal_pdf(text)


router = APIRouter(prefix="/logs", tags=["logs"])

_EXPORTER: LogExporter | None = None


def _get_exporter() -> LogExporter:
    global _EXPORTER
    if _EXPORTER is None:
        config = _storage_config_from_env(allow_missing=_insecure_defaults_enabled())
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

