"""Retention archiver for operational logging and metrics stores."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

try:  # pragma: no cover - optional dependency in some environments.
    import boto3
except Exception:  # pragma: no cover - boto3 may not be installed during static checks.
    boto3 = None  # type: ignore

try:  # pragma: no cover - optional dependency
    import requests
except Exception:  # pragma: no cover - requests may not be installed during static checks.
    requests = None  # type: ignore

from fastapi import APIRouter, HTTPException


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


DEFAULT_PREFIX = "logging-retention"
DEFAULT_PROMETHEUS_SNAPSHOT_DIR = Path("/var/lib/prometheus/snapshots")
DEFAULT_AUDIT_LOG_DIR = Path("/var/log/aether/audit_log")
DEFAULT_OMS_LOG_DIR = Path("/var/log/aether/oms_log")
DEFAULT_TCA_REPORT_DIR = Path("/var/log/aether/tca_reports")


def _ensure_safe_directory(path: Path, env_var: str) -> Path:
    """Validate that *path* is an absolute, traversal-free directory reference.

    Paths supplied via environment variables participate directly in filesystem
    archival routines.  Guard against directory escape by requiring absolute
    paths, prohibiting ".." segments, and rejecting symlink-backed ancestors.
    """

    if not path.is_absolute():
        raise ValueError(f"{env_var} must be an absolute path")
    if any(part == ".." for part in path.parts):
        raise ValueError(f"{env_var} must not contain parent directory references")

    for ancestor in (path,) + tuple(path.parents):
        # ``Path.is_symlink`` safely handles non-existent elements and raises no
        # exceptions when intermediate directories are missing.
        if ancestor.is_symlink():
            raise ValueError(f"{env_var} must not reference symlinked directories")

    return path


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
            raise ValueError("RETENTION_PREFIX must not contain path traversal sequences")
        if any(ord(char) < 32 for char in segment):
            raise ValueError("RETENTION_PREFIX must not contain control characters")
        segments.append(segment)

    return "/".join(segments)


@dataclass(frozen=True)
class ArchiveRecord:
    """Metadata recorded for each log archive exported to object storage."""

    category: str
    s3_key: str
    sha256: str
    file_count: int
    size_bytes: int

    def to_dict(self) -> Dict[str, object]:
        return {
            "category": self.category,
            "s3_key": self.s3_key,
            "sha256": self.sha256,
            "file_count": self.file_count,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class MetricsSnapshot:
    """Metadata about the metrics downsampling/snapshot activity."""

    snapshot_name: str
    snapshot_path: str
    requested_at: str
    cutoff: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "snapshot_name": self.snapshot_name,
            "snapshot_path": self.snapshot_path,
            "requested_at": self.requested_at,
            "cutoff": self.cutoff,
        }


@dataclass(frozen=True)
class RetentionConfig:
    """Configuration for the retention archiver job."""

    prometheus_url: str
    bucket: str
    metrics_retention_days: int
    log_retention_days: int
    prefix: str = DEFAULT_PREFIX
    endpoint_url: Optional[str] = None
    storage_class: Optional[str] = None
    prometheus_snapshot_dir: Path = DEFAULT_PROMETHEUS_SNAPSHOT_DIR
    audit_log_dir: Path = DEFAULT_AUDIT_LOG_DIR
    oms_log_dir: Path = DEFAULT_OMS_LOG_DIR
    tca_report_dir: Path = DEFAULT_TCA_REPORT_DIR

    @classmethod
    def from_env(cls) -> "RetentionConfig":
        """Instantiate configuration from environment variables."""

        prometheus_url = os.getenv("PROMETHEUS_URL")
        bucket = os.getenv("RETENTION_BUCKET")
        metrics_days = os.getenv("METRICS_RETENTION_DAYS")
        logs_days = os.getenv("LOG_RETENTION_DAYS")

        if not prometheus_url:
            raise RuntimeError("PROMETHEUS_URL environment variable is required")
        if not bucket:
            raise RuntimeError("RETENTION_BUCKET environment variable is required")
        if not metrics_days:
            raise RuntimeError("METRICS_RETENTION_DAYS environment variable is required")
        if not logs_days:
            raise RuntimeError("LOG_RETENTION_DAYS environment variable is required")

        prefix = _normalise_s3_prefix(os.getenv("RETENTION_PREFIX", DEFAULT_PREFIX))
        endpoint_url = os.getenv("RETENTION_ENDPOINT")
        storage_class = os.getenv("RETENTION_STORAGE_CLASS")

        snapshot_dir = _ensure_safe_directory(
            Path(os.getenv("PROMETHEUS_SNAPSHOT_DIR", str(DEFAULT_PROMETHEUS_SNAPSHOT_DIR))),
            "PROMETHEUS_SNAPSHOT_DIR",
        )
        audit_dir = _ensure_safe_directory(
            Path(os.getenv("AUDIT_LOG_DIR", str(DEFAULT_AUDIT_LOG_DIR))),
            "AUDIT_LOG_DIR",
        )
        oms_dir = _ensure_safe_directory(
            Path(os.getenv("OMS_LOG_DIR", str(DEFAULT_OMS_LOG_DIR))),
            "OMS_LOG_DIR",
        )
        tca_dir = _ensure_safe_directory(
            Path(os.getenv("TCA_REPORT_DIR", str(DEFAULT_TCA_REPORT_DIR))),
            "TCA_REPORT_DIR",
        )

        return cls(
            prometheus_url=prometheus_url,
            bucket=bucket,
            metrics_retention_days=int(metrics_days),
            log_retention_days=int(logs_days),
            prefix=prefix,
            endpoint_url=endpoint_url,
            storage_class=storage_class,
            prometheus_snapshot_dir=snapshot_dir,
            audit_log_dir=audit_dir,
            oms_log_dir=oms_dir,
            tca_report_dir=tca_dir,
        )


@dataclass
class RetentionStatus:
    """Tracks the most recent execution status for the API surface."""

    last_run: Optional[dt.datetime] = None
    archived_counts: Dict[str, int] = field(default_factory=dict)
    storage_path: Optional[str] = None

    def to_response(self) -> Dict[str, object]:
        return {
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "archived_counts": dict(self.archived_counts),
            "storage_path": self.storage_path,
        }


class RetentionArchiver:
    """Nightly job that enforces logging and metrics retention policies."""

    def __init__(self, config: RetentionConfig) -> None:
        self.config = config
        self._s3_client = self._build_s3_client()
        self.status = RetentionStatus()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self, *, when: Optional[dt.datetime] = None) -> RetentionStatus:
        """Execute the retention workflow and return the updated status."""

        now = (when or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
        metrics_cutoff = now - dt.timedelta(days=self.config.metrics_retention_days)
        logs_cutoff = now - dt.timedelta(days=self.config.log_retention_days)

        LOGGER.info(
            "Starting retention archiver run at %s (metrics cutoff: %s, logs cutoff: %s)",
            now.isoformat(),
            metrics_cutoff.isoformat(),
            logs_cutoff.isoformat(),
        )

        snapshot = self._downsample_metrics(metrics_cutoff, now)
        archives: List[ArchiveRecord] = []
        for category, directory in self._log_directories().items():
            archives.extend(self._archive_category(category, directory, logs_cutoff, now))

        manifest_key = self._write_manifest(now, metrics_cutoff, logs_cutoff, snapshot, archives)
        archived_counts = self._summarise_counts(archives)
        self.status = RetentionStatus(
            last_run=now,
            archived_counts=archived_counts,
            storage_path=f"s3://{self.config.bucket}/{manifest_key}",
        )
        LOGGER.info("Retention archiver completed. Manifest uploaded to %s", self.status.storage_path)
        return self.status

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _build_s3_client(self):
        if boto3 is None:  # pragma: no cover - guard against optional dependency missing.
            raise RuntimeError("boto3 is required to upload retention archives")
        client_kwargs: Dict[str, object] = {}
        if self.config.endpoint_url:
            client_kwargs["endpoint_url"] = self.config.endpoint_url
        return boto3.client("s3", **client_kwargs)  # type: ignore[return-value]

    def _log_directories(self) -> Dict[str, Path]:
        return {
            "audit_log": self.config.audit_log_dir,
            "oms_log": self.config.oms_log_dir,
            "tca_reports": self.config.tca_report_dir,
        }

    def _downsample_metrics(
        self, cutoff: dt.datetime, now: dt.datetime
    ) -> Optional[MetricsSnapshot]:
        """Request Prometheus to snapshot or downsample metrics older than *cutoff*."""

        if requests is None:  # pragma: no cover - requests optional for static analysis.
            LOGGER.warning("requests module unavailable; skipping metrics downsampling")
            return None

        endpoint = f"{self.config.prometheus_url.rstrip('/')}/api/v1/admin/tsdb/snapshot"
        payload = {
            "skip_head": True,
            "with_exemplars": True,
            "max_block_duration": f"{self.config.metrics_retention_days}d",
            "retention_cutoff": cutoff.isoformat(),
        }
        try:
            response = requests.post(endpoint, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json().get("data", {})
            snapshot_name = data.get("name") or data.get("id") or now.strftime("snapshot-%Y%m%dT%H%M%SZ")
            snapshot_path = str(self.config.prometheus_snapshot_dir / snapshot_name)
            LOGGER.info("Prometheus snapshot requested: name=%s path=%s", snapshot_name, snapshot_path)
            return MetricsSnapshot(
                snapshot_name=snapshot_name,
                snapshot_path=snapshot_path,
                requested_at=now.isoformat(),
                cutoff=cutoff.isoformat(),
            )
        except Exception as exc:  # pragma: no cover - network heavy path in prod only.
            LOGGER.exception("Failed to request Prometheus snapshot: %s", exc)
            raise RuntimeError("Prometheus snapshot failed") from exc

    def _archive_category(
        self, category: str, directory: Path, cutoff: dt.datetime, now: dt.datetime
    ) -> List[ArchiveRecord]:
        """Archive files under *directory* older than *cutoff*."""

        if not directory.exists():
            LOGGER.info("Directory %s missing for category %s; skipping", directory, category)
            return []

        files = list(self._select_files(directory, cutoff))
        if not files:
            LOGGER.info("No files eligible for archive in %s", directory)
            return []

        tar_name = f"{category}-{now.strftime('%Y%m%dT%H%M%SZ')}.tar.gz"
        fd, tmp_name = tempfile.mkstemp(prefix="retention-", suffix=".tar.gz")
        os.close(fd)
        temp_path = Path(tmp_name)
        try:
            size_bytes = self._create_archive(temp_path, directory, files)
            sha256 = self._compute_sha256(temp_path)
            s3_key = self._build_s3_key(now, tar_name)
            self._upload_file(temp_path, s3_key)
            LOGGER.info(
                "Archived %s files from %s to s3://%s/%s", len(files), directory, self.config.bucket, s3_key
            )
            return [
                ArchiveRecord(
                    category=category,
                    s3_key=s3_key,
                    sha256=sha256,
                    file_count=len(files),
                    size_bytes=size_bytes,
                )
            ]
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:  # pragma: no cover - cleanup best effort
                LOGGER.debug("Failed to remove temporary archive %s", temp_path, exc_info=True)

    def _select_files(self, directory: Path, cutoff: dt.datetime) -> Iterable[Path]:
        for path in directory.rglob("*"):
            if not path.is_file():
                continue
            modified = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
            if modified <= cutoff:
                yield path

    def _create_archive(self, destination: Path, root: Path, files: Iterable[Path]) -> int:
        size_bytes = 0
        with destination.open("wb") as fh:
            with tarfile.open(mode="w:gz", fileobj=fh) as archive:
                for path in files:
                    arcname = path.relative_to(root)
                    archive.add(path, arcname=str(arcname))
                    size_bytes += path.stat().st_size
        return size_bytes

    def _compute_sha256(self, file_path: Path) -> str:
        digest = hashlib.sha256()
        with file_path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1_048_576), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _build_s3_key(self, now: dt.datetime, filename: str) -> str:
        parts = []
        prefix = self.config.prefix.strip("/")
        if prefix:
            parts.append(prefix)
        parts.append(f"{now:%Y/%m/%d}")
        parts.append(filename)
        return "/".join(parts)

    def _upload_file(self, path: Path, key: str) -> None:
        extra_args: Dict[str, object] = {}
        if self.config.storage_class:
            extra_args["StorageClass"] = self.config.storage_class
        self._s3_client.upload_file(str(path), self.config.bucket, key, ExtraArgs=extra_args)

    def _write_manifest(
        self,
        now: dt.datetime,
        metrics_cutoff: dt.datetime,
        logs_cutoff: dt.datetime,
        snapshot: Optional[MetricsSnapshot],
        archives: List[ArchiveRecord],
    ) -> str:
        payload = {
            "manifest_version": 1,
            "generated_at": now.isoformat(),
            "retention_policy": {
                "metrics_retention_days": self.config.metrics_retention_days,
                "log_retention_days": self.config.log_retention_days,
            },
            "metrics_cutoff": metrics_cutoff.isoformat(),
            "logs_cutoff": logs_cutoff.isoformat(),
            "metrics_snapshot": snapshot.to_dict() if snapshot else None,
            "archives": [record.to_dict() for record in archives],
        }
        payload_blob = json.dumps(payload, sort_keys=True, indent=2).encode("utf-8")
        payload_sha = hashlib.sha256(payload_blob).hexdigest()
        manifest = dict(payload)
        manifest["payload_sha256"] = payload_sha
        manifest_blob = json.dumps(manifest, sort_keys=True, indent=2).encode("utf-8")

        manifest_key = self._build_s3_key(now, f"retention-manifest-{now:%Y%m%dT%H%M%SZ}.json")
        self._s3_client.put_object(
            Bucket=self.config.bucket,
            Key=manifest_key,
            Body=manifest_blob,
            ContentType="application/json",
            Metadata={"sha256": payload_sha},
        )
        return manifest_key

    def _summarise_counts(self, archives: List[ArchiveRecord]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for record in archives:
            counts[record.category] = counts.get(record.category, 0) + record.file_count
        if archives:
            counts.setdefault("total", 0)
            counts["total"] += sum(record.file_count for record in archives)
        return counts


# ----------------------------------------------------------------------
# FastAPI router exposing the last retention run status
# ----------------------------------------------------------------------
router = APIRouter(prefix="/logging/retention", tags=["logging"])
_archiver_instance: Optional[RetentionArchiver] = None


def configure_retention_archiver(config: RetentionConfig) -> RetentionArchiver:
    """Configure the module-level archiver instance for API access."""

    global _archiver_instance
    _archiver_instance = RetentionArchiver(config)
    return _archiver_instance


@router.get("/status")
def retention_status() -> Dict[str, object]:
    if _archiver_instance is None:
        raise HTTPException(status_code=503, detail="Retention archiver not configured")
    return _archiver_instance.status.to_response()


__all__ = [
    "RetentionArchiver",
    "RetentionConfig",
    "RetentionStatus",
    "ArchiveRecord",
    "MetricsSnapshot",
    "configure_retention_archiver",
    "router",
]

