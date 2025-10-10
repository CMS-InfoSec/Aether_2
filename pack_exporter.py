"""Knowledge pack exporter for bundling and serving model artefacts."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib
import importlib.util
import json
import os
import shutil
import tarfile
import threading
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


_INSECURE_DEFAULTS_FLAG = "KNOWLEDGE_PACK_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "AETHER_STATE_DIR"
_STATE_SUBDIR = "knowledge_pack"


boto3 = _optional_import("boto3")
psycopg = _optional_import("psycopg")


router = APIRouter(prefix="/knowledge", tags=["knowledge"])


class MissingDependencyError(RuntimeError):
    """Raised when an optional dependency required at runtime is missing."""


def _insecure_defaults_enabled() -> bool:
    """Return ``True`` when local fallbacks are explicitly allowed."""

    return os.getenv(_INSECURE_DEFAULTS_FLAG) == "1" or bool(
        os.getenv("PYTEST_CURRENT_TEST")
    )


def _default_state_base() -> Path:
    return Path(os.getenv(_STATE_DIR_ENV, ".aether_state"))


def _state_root(explicit: Path | None = None) -> Path:
    """Return the final directory for local knowledge-pack state."""

    root = explicit or (_default_state_base() / _STATE_SUBDIR)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _state_dir_from_base(base: Path | None = None) -> Path:
    """Normalise *base* into the knowledge-pack state directory."""

    if base is None:
        return _state_root()
    candidate = Path(base)
    if candidate.name == _STATE_SUBDIR:
        return _state_root(candidate)
    return _state_root(candidate / _STATE_SUBDIR)


def _artifacts_root(base: Path | None = None) -> Path:
    """Directory that stores persisted tarballs under insecure defaults."""

    root_dir = _state_dir_from_base(base)
    root = root_dir / "artifacts"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _metadata_path(base: Path | None = None) -> Path:
    """Path to the JSON metadata file for local pack history."""

    return _state_dir_from_base(base) / "metadata.json"


class MissingArtifactError(RuntimeError):
    """Raised when an expected artefact directory or file is absent."""


def _normalise_storage_prefix(prefix: str | None) -> str:
    """Return a sanitised S3 prefix for knowledge pack uploads."""

    if not prefix:
        return ""

    segments: list[str] = []
    for raw_segment in prefix.replace("\\", "/").split("/"):
        segment = raw_segment.strip()
        if not segment:
            continue
        if segment in {".", ".."}:
            raise ValueError(
                "Knowledge pack prefix must not contain path traversal sequences"
            )
        if any(ord(char) < 32 or ord(char) == 127 for char in segment):
            raise ValueError(
                "Knowledge pack prefix must not contain control characters"
            )
        segments.append(segment)

    return "/".join(segments)


@dataclass(frozen=True)
class ObjectStorageConfig:
    """Configuration for interacting with object storage."""

    bucket: str
    prefix: str = "knowledge-packs"
    endpoint_url: str | None = None

    def __post_init__(self) -> None:
        normalised_prefix = _normalise_storage_prefix(self.prefix)
        object.__setattr__(self, "prefix", normalised_prefix)


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
    if psycopg is None and not _insecure_defaults_enabled():  # pragma: no cover
        raise MissingDependencyError("psycopg is required for knowledge pack exports")


def _require_boto3() -> None:
    if boto3 is None and not _insecure_defaults_enabled():  # pragma: no cover
        raise MissingDependencyError("boto3 is required for knowledge pack exports")


def _database_dsn(*, allow_missing: bool = False) -> str | None:
    dsn = os.getenv("KNOWLEDGE_PACK_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn and not allow_missing:
        raise RuntimeError("KNOWLEDGE_PACK_DATABASE_URL or DATABASE_URL must be set")
    return dsn


def _storage_config_from_env(
    *, allow_missing: bool = False
) -> ObjectStorageConfig | None:
    bucket = os.getenv("KNOWLEDGE_PACK_BUCKET") or os.getenv("EXPORT_BUCKET")
    if not bucket:
        if allow_missing:
            return None
        raise RuntimeError("KNOWLEDGE_PACK_BUCKET or EXPORT_BUCKET must be configured")
    prefix = os.getenv("KNOWLEDGE_PACK_PREFIX") or os.getenv(
        "EXPORT_PREFIX", "knowledge-packs"
    )
    endpoint_url = os.getenv("KNOWLEDGE_PACK_ENDPOINT_URL") or os.getenv(
        "EXPORT_S3_ENDPOINT_URL"
    )
    return ObjectStorageConfig(bucket=bucket, prefix=prefix, endpoint_url=endpoint_url)


def _ensure_no_symlink_ancestors(path: Path, *, context: str) -> None:
    """Raise ``MissingArtifactError`` if *path* or an ancestor is a symlink."""

    for ancestor in (path,) + tuple(path.parents):
        if ancestor.exists() and ancestor.is_symlink():
            raise MissingArtifactError(f"{context} must not reference symlinks")


def _ensure_regular_path(path: Path, *, context: str) -> None:
    """Ensure *path* points to a regular file or directory."""

    if path.is_dir() or path.is_file():
        return
    raise MissingArtifactError(f"{context} must reference a file or directory")


def _ensure_within_base(base: Path, target: Path, *, context: str) -> None:
    """Ensure *target* resides within *base*."""

    try:
        target.relative_to(base)
    except ValueError as exc:
        raise MissingArtifactError(
            f"{context} must reside within {base}"
        ) from exc


def _ensure_safe_tree(path: Path, *, base: Path, context: str) -> None:
    """Ensure that *path* and its descendants are safe to package."""

    stack = [path]
    while stack:
        current = stack.pop()
        if current.is_symlink():
            raise MissingArtifactError(
                f"{context} must not contain symlinks (found {current})"
            )

        resolved = current.resolve(strict=True)
        _ensure_within_base(base, resolved, context=context)

        if resolved.is_dir():
            stack.extend(resolved.iterdir())
            continue

        if not resolved.is_file():
            raise MissingArtifactError(
                f"{context} must contain only regular files and directories"
            )


def _resolve_base_path() -> Path:
    raw_base = Path(os.getenv("KNOWLEDGE_PACK_ROOT", ".")).expanduser()
    try:
        resolved = raw_base.resolve(strict=True)
    except FileNotFoundError as exc:
        raise MissingArtifactError(
            "KNOWLEDGE_PACK_ROOT must reference an existing directory"
        ) from exc
    if not resolved.is_dir():
        raise MissingArtifactError("KNOWLEDGE_PACK_ROOT must be a directory")
    _ensure_no_symlink_ancestors(resolved, context="KNOWLEDGE_PACK_ROOT")
    return resolved


def _resolve_path(env_name: str, *, default: Path | None = None, base: Path) -> Path:
    raw = os.getenv(env_name)
    if raw:
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = base / candidate
    elif default is not None:
        candidate = default
    else:
        raise MissingArtifactError(
            f"{env_name} must be set to locate knowledge pack artefacts"
        )

    context = f"{env_name}"
    if not candidate.exists():
        raise MissingArtifactError(
            f"Expected artefact at {candidate} (from {env_name!s}) does not exist"
        )
    _ensure_no_symlink_ancestors(candidate, context=context)
    _ensure_regular_path(candidate, context=context)

    resolved = candidate.resolve(strict=True)
    _ensure_within_base(base, resolved, context=context)
    _ensure_safe_tree(resolved, base=base, context=context)
    return resolved


def resolve_inputs() -> PackInputs:
    """Resolve artefact locations from environment variables."""

    base = _resolve_base_path()
    defaults = {
        "model_weights": base / "artifacts" / "model_weights",
        "feature_importance": base / "artifacts" / "feature_importance",
        "anomaly_tags": base / "artifacts" / "anomaly_tags",
        "config": base / "config",
    }
    return PackInputs(
        model_weights=_resolve_path(
            "KNOWLEDGE_PACK_WEIGHTS", default=defaults["model_weights"], base=base
        ),
        feature_importance=_resolve_path(
            "KNOWLEDGE_PACK_FEATURE_IMPORTANCE",
            default=defaults["feature_importance"],
            base=base,
        ),
        anomaly_tags=_resolve_path(
            "KNOWLEDGE_PACK_ANOMALY_TAGS", default=defaults["anomaly_tags"], base=base
        ),
        config=_resolve_path("KNOWLEDGE_PACK_CONFIG", default=defaults["config"], base=base),
    )


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

    def __init__(
        self,
        *,
        dsn: str | None = None,
        state_dir: Path | None = None,
    ) -> None:
        self._state_dir = state_dir
        self._metadata_lock = threading.Lock()
        resolved_dsn = dsn or _database_dsn(allow_missing=_insecure_defaults_enabled())
        self._use_local_metadata = psycopg is None or not resolved_dsn
        if self._use_local_metadata:
            if not _insecure_defaults_enabled():
                _require_psycopg()
                raise RuntimeError(
                    "Knowledge pack metadata requires psycopg or insecure defaults",
                )
            self._state_dir = _state_dir_from_base(self._state_dir)
            self._dsn = None
        else:
            self._dsn = resolved_dsn

    # Database-backed helpers -------------------------------------------------
    def _connect(self):  # pragma: no cover - minimal wrapper
        if self._dsn is None or psycopg is None:
            raise RuntimeError("Local metadata store in use")
        return psycopg.connect(self._dsn)

    # Local metadata helpers --------------------------------------------------
    def _local_metadata_path(self) -> Path:
        return _metadata_path(self._state_dir)

    def _write_local_metadata(self, record: PackRecord) -> None:
        path = self._local_metadata_path()
        payload: list[dict[str, object]] = []
        if path.exists():
            try:
                payload_raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception:  # pragma: no cover - metadata corruption fallback
                payload_raw = []
            if isinstance(payload_raw, list):
                payload = [item for item in payload_raw if isinstance(item, dict)]

        entry = {
            "object_key": record.object_key,
            "sha256": record.sha256,
            "created_at": record.created_at.isoformat(),
            "size": record.size,
        }
        payload = [item for item in payload if item.get("object_key") != record.object_key]
        payload.append(entry)
        payload.sort(key=lambda item: item.get("created_at", ""))
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _load_latest_local_record(self) -> PackRecord | None:
        path = self._local_metadata_path()
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # pragma: no cover - metadata corruption fallback
            return None
        if not isinstance(payload, list):
            return None
        latest: Optional[dict[str, object]] = None
        for item in payload:
            if not isinstance(item, dict):
                continue
            if latest is None or str(item.get("created_at", "")) > str(
                latest.get("created_at", "")
            ):
                latest = item
        if not latest:
            return None
        created_at = dt.datetime.fromisoformat(str(latest["created_at"]))
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=dt.timezone.utc)
        return PackRecord(
            object_key=str(latest["object_key"]),
            sha256=str(latest["sha256"]),
            created_at=created_at,
            size=int(latest["size"]),
        )

    # Public API --------------------------------------------------------------
    def ensure_table(self) -> None:
        if self._use_local_metadata:
            path = self._local_metadata_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                path.write_text("[]", encoding="utf-8")
            return

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
        if self._use_local_metadata:
            with self._metadata_lock:
                self._write_local_metadata(record)
            return

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
        if self._use_local_metadata:
            return self._load_latest_local_record()

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

    @property
    def uses_local_store(self) -> bool:
        return self._use_local_metadata

    def local_artifact_path(self, record: PackRecord) -> Path:
        if not self._use_local_metadata:
            raise RuntimeError("Local store is not active")
        filename = Path(record.object_key).name
        return _artifacts_root(self._state_dir) / filename


def _object_key(config: ObjectStorageConfig, output_file: Path) -> str:
    filename = output_file.name
    if config.prefix:
        return f"{config.prefix}/{filename}"
    return filename


def _persist_local_tarball(file_path: Path, *, state_dir: Path | None = None) -> Path:
    target_dir = _artifacts_root(state_dir)
    target = target_dir / file_path.name
    if file_path.resolve() != target.resolve():
        shutil.copy2(file_path, target)
    return target


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
    storage_config: ObjectStorageConfig | None,
    repository: KnowledgePackRepository,
) -> PackRecord:
    tarball = _tarball_contents(inputs, output)
    sha256_hash = _compute_sha256(tarball)
    size = tarball.stat().st_size
    created_at = dt.datetime.now(dt.timezone.utc)
    client = None
    if storage_config is not None:
        try:
            client = _s3_client(storage_config)
        except MissingDependencyError:
            client = None
    if client is not None:
        object_key = _object_key(storage_config, tarball)
        headers = {"created_at": created_at.isoformat(), "sha256": sha256_hash}
        with tarball.open("rb") as handle:
            client.upload_fileobj(
                handle,
                storage_config.bucket,
                object_key,
                ExtraArgs={"Metadata": headers},
            )
    else:
        if not _insecure_defaults_enabled():
            _require_boto3()
            raise RuntimeError("Unable to upload knowledge pack without object storage")
        local_path = _persist_local_tarball(
            tarball, state_dir=getattr(repository, "_state_dir", None)
        )
        object_key = f"local/{local_path.name}"
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

    repo = KnowledgePackRepository()
    record = repo.latest_pack()
    if record is None:
        raise HTTPException(status_code=404, detail="No knowledge pack available")
    uses_local = getattr(repo, "uses_local_store", False)
    if uses_local:
        if hasattr(repo, "local_artifact_path"):
            path = repo.local_artifact_path(record)
        else:
            path = _artifacts_root().joinpath(Path(record.object_key).name)
        url = path.resolve().as_uri()
    else:
        _require_psycopg()
        _require_boto3()
        config = _storage_config_from_env()
        if config is None:
            raise RuntimeError("Object storage configuration is required")
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
    inputs = resolve_inputs()
    storage_config = _storage_config_from_env(
        allow_missing=_insecure_defaults_enabled()
    )
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
