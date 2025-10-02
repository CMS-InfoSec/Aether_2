"""Knowledge pack exporter for bundling and serving model artefacts."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib
import importlib.util
import os
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional

from fastapi import APIRouter, Depends, HTTPException

from services.common.security import require_admin_account


def _optional_import(module_name: str) -> object | None:
    """Attempt to import *module_name* without raising on failure."""

    spec = importlib.util.find_spec(module_name)
    if spec is None:
        return None
    return importlib.import_module(module_name)


boto3 = _optional_import("boto3")
psycopg = _optional_import("psycopg")


router = APIRouter(prefix="/knowledge", tags=["knowledge"])


class MissingDependencyError(RuntimeError):
    """Raised when an optional dependency required at runtime is missing."""


class MissingArtifactError(RuntimeError):
    """Raised when an expected artefact directory or file is absent."""


@dataclass(frozen=True)
class ObjectStorageConfig:
    """Configuration for interacting with object storage."""

    bucket: str
    prefix: str = "knowledge-packs"
    endpoint_url: str | None = None


@dataclass(frozen=True)
class PackInputs:
    """Resolved filesystem locations to bundle into a knowledge pack."""

    model_weights: Path
    feature_importance: Path
    anomaly_tags: Path
    config: Path

    def as_mapping(self) -> Mapping[str, Path]:
        return {
            "model_weights": self.model_weights,
            "feature_importance": self.feature_importance,
            "anomaly_tags": self.anomaly_tags,
            "config": self.config,
        }


@dataclass(frozen=True)
class PackRecord:
    """Metadata describing a persisted knowledge pack."""

    object_key: str
    sha256: str
    created_at: dt.datetime
    size: int


def _require_psycopg() -> None:
    if psycopg is None:  # pragma: no cover - sanity guard when dependency missing.
        raise MissingDependencyError("psycopg is required for knowledge pack exports")


def _require_boto3() -> None:
    if boto3 is None:  # pragma: no cover - sanity guard when dependency missing.
        raise MissingDependencyError("boto3 is required for knowledge pack exports")


def _database_dsn() -> str:
    dsn = os.getenv("KNOWLEDGE_PACK_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("KNOWLEDGE_PACK_DATABASE_URL or DATABASE_URL must be set")
    return dsn


def _storage_config_from_env() -> ObjectStorageConfig:
    bucket = os.getenv("KNOWLEDGE_PACK_BUCKET") or os.getenv("EXPORT_BUCKET")
    if not bucket:
        raise RuntimeError("KNOWLEDGE_PACK_BUCKET or EXPORT_BUCKET must be configured")
    prefix = os.getenv("KNOWLEDGE_PACK_PREFIX") or os.getenv(
        "EXPORT_PREFIX", "knowledge-packs"
    )
    endpoint_url = os.getenv("KNOWLEDGE_PACK_ENDPOINT_URL") or os.getenv(
        "EXPORT_S3_ENDPOINT_URL"
    )
    return ObjectStorageConfig(bucket=bucket, prefix=prefix, endpoint_url=endpoint_url)


def _resolve_path(env_name: str, *, default: Path | None = None) -> Path:
    raw = os.getenv(env_name)
    if raw:
        path = Path(raw).expanduser()
    elif default is not None:
        path = default
    else:
        raise MissingArtifactError(
            f"{env_name} must be set to locate knowledge pack artefacts"
        )
    resolved = path.resolve()
    if not resolved.exists():
        raise MissingArtifactError(
            f"Expected artefact at {resolved} (from {env_name!s}) does not exist"
        )
    return resolved


def resolve_inputs() -> PackInputs:
    """Resolve artefact locations from environment variables."""

    base = Path(os.getenv("KNOWLEDGE_PACK_ROOT", ".")).resolve()
    defaults = {
        "model_weights": base / "artifacts" / "model_weights",
        "feature_importance": base / "artifacts" / "feature_importance",
        "anomaly_tags": base / "artifacts" / "anomaly_tags",
        "config": Path("config").resolve(),
    }
    return PackInputs(
        model_weights=_resolve_path("KNOWLEDGE_PACK_WEIGHTS", default=defaults["model_weights"]),
        feature_importance=_resolve_path(
            "KNOWLEDGE_PACK_FEATURE_IMPORTANCE", default=defaults["feature_importance"]
        ),
        anomaly_tags=_resolve_path(
            "KNOWLEDGE_PACK_ANOMALY_TAGS", default=defaults["anomaly_tags"]
        ),
        config=_resolve_path("KNOWLEDGE_PACK_CONFIG", default=defaults["config"]),
    )


def _normalise_prefix(prefix: str) -> str:
    return prefix.strip("/")


def _tarball_contents(inputs: PackInputs, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output_path, "w:gz") as tar:
        for label, path in inputs.as_mapping().items():
            arcname = label
            if path.is_file():
                arcname = f"{label}/{path.name}"
            tar.add(path, arcname=arcname)
    return output_path


def _compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _s3_client(config: ObjectStorageConfig):  # pragma: no cover - trivial wrapper
    _require_boto3()
    return boto3.client("s3", endpoint_url=config.endpoint_url)


class KnowledgePackRepository:
    """Persistence helper for knowledge pack metadata."""

    def __init__(self, *, dsn: str | None = None) -> None:
        _require_psycopg()
        self._dsn = dsn or _database_dsn()

    def _connect(self):  # pragma: no cover - minimal wrapper
        return psycopg.connect(self._dsn)

    def ensure_table(self) -> None:
        create_sql = """
            CREATE TABLE IF NOT EXISTS knowledge_packs (
                id TEXT PRIMARY KEY,
                hash TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL,
                size BIGINT NOT NULL
            )
        """.strip()
        with self._connect() as conn:  # type: ignore[call-arg]
            with conn.cursor() as cur:
                cur.execute(create_sql)
            conn.commit()

    def record_pack(self, record: PackRecord) -> None:
        insert_sql = """
            INSERT INTO knowledge_packs (id, hash, ts, size)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                hash = EXCLUDED.hash,
                ts = EXCLUDED.ts,
                size = EXCLUDED.size
        """.strip()
        with self._connect() as conn:  # type: ignore[call-arg]
            with conn.cursor() as cur:
                cur.execute(
                    insert_sql,
                    (record.object_key, record.sha256, record.created_at, record.size),
                )
            conn.commit()

    def latest_pack(self) -> PackRecord | None:
        query_sql = """
            SELECT id, hash, ts, size
            FROM knowledge_packs
            ORDER BY ts DESC
            LIMIT 1
        """.strip()
        with self._connect() as conn:  # type: ignore[call-arg]
            with conn.cursor() as cur:
                cur.execute(query_sql)
                row = cur.fetchone()
        if not row:
            return None
        object_key, sha256_hash, created_at, size = row
        return PackRecord(
            object_key=str(object_key),
            sha256=str(sha256_hash),
            created_at=created_at,
            size=int(size),
        )


def _object_key(config: ObjectStorageConfig, output_file: Path) -> str:
    prefix = _normalise_prefix(config.prefix)
    filename = output_file.name
    if prefix:
        return f"{prefix}/{filename}"
    return filename


def _upload_tarball(
    *,
    file_path: Path,
    sha256_hash: str,
    storage_config: ObjectStorageConfig,
    metadata: Optional[Mapping[str, str]] = None,
) -> str:
    client = _s3_client(storage_config)
    object_key = _object_key(storage_config, file_path)
    headers = {"sha256": sha256_hash}
    if metadata:
        headers.update(metadata)
    with file_path.open("rb") as handle:
        client.upload_fileobj(
            handle,
            storage_config.bucket,
            object_key,
            ExtraArgs={"Metadata": headers},
        )
    return object_key


def _url_ttl_seconds() -> int:
    raw = os.getenv("KNOWLEDGE_PACK_URL_TTL", "3600")
    try:
        value = int(raw)
    except ValueError as exc:  # pragma: no cover - defensive coding
        raise RuntimeError("KNOWLEDGE_PACK_URL_TTL must be an integer") from exc
    return max(value, 60)


def create_pack(
    *,
    output: Path,
    inputs: PackInputs,
    storage_config: ObjectStorageConfig,
    repository: KnowledgePackRepository,
) -> PackRecord:
    tarball = _tarball_contents(inputs, output)
    sha256_hash = _compute_sha256(tarball)
    size = tarball.stat().st_size
    created_at = dt.datetime.now(dt.timezone.utc)
    object_key = _upload_tarball(
        file_path=tarball,
        sha256_hash=sha256_hash,
        storage_config=storage_config,
        metadata={"created_at": created_at.isoformat()},
    )
    record = PackRecord(
        object_key=object_key,
        sha256=sha256_hash,
        created_at=created_at,
        size=size,
    )
    repository.ensure_table()
    repository.record_pack(record)
    return record


@router.get("/export/latest")
def latest_pack(_actor: str = Depends(require_admin_account)) -> Dict[str, object]:
    """Return the download URL for the most recent knowledge pack."""

    _require_psycopg()
    _require_boto3()
    repo = KnowledgePackRepository()
    record = repo.latest_pack()
    if record is None:
        raise HTTPException(status_code=404, detail="No knowledge pack available")
    config = _storage_config_from_env()
    client = _s3_client(config)
    url = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": config.bucket, "Key": record.object_key},
        ExpiresIn=_url_ttl_seconds(),
    )
    return {
        "id": record.object_key,
        "hash": record.sha256,
        "size": record.size,
        "ts": record.created_at.isoformat(),
        "download_url": url,
    }


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Knowledge pack exporter")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Create and upload a pack")
    create_parser.add_argument("--out", required=True, help="Output tarball path")

    return parser.parse_args(argv)


def _cmd_create(out: str) -> PackRecord:
    _require_psycopg()
    _require_boto3()
    inputs = resolve_inputs()
    storage_config = _storage_config_from_env()
    repository = KnowledgePackRepository()
    output = Path(out).expanduser().resolve()
    return create_pack(
        output=output,
        inputs=inputs,
        storage_config=storage_config,
        repository=repository,
    )


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = _parse_args(argv)
    if args.command == "create":
        record = _cmd_create(args.out)
        print(
            f"Knowledge pack uploaded as {record.object_key} "
            f"(sha256={record.sha256}, size={record.size})"
        )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()


__all__ = [
    "KnowledgePackRepository",
    "ObjectStorageConfig",
    "PackInputs",
    "PackRecord",
    "create_pack",
    "latest_pack",
    "main",
    "resolve_inputs",
    "router",
]
