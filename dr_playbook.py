"""Disaster recovery playbook for TimescaleDB, Redis, and MLflow artifacts.

This module exposes ``snapshot_cluster`` and ``restore_cluster`` helpers that
collect crash-consistent snapshots of TimescaleDB, Redis, and MLflow artifacts
before storing them in object storage.  Snapshots are bundled into a tarball and
uploaded to S3-compatible object storage.  A minimal CLI allows operators to run
``python dr_playbook.py snapshot`` or ``python dr_playbook.py restore
<snapshot_id>`` during incident response.
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from getpass import getuser
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

LOGGER = logging.getLogger("dr_playbook")

from common.utils.tar import safe_extract_tar


# Cache of log tables that were already created in this process to avoid
# issuing ``CREATE TABLE IF NOT EXISTS`` on every write.
_LOG_TABLE_BOOTSTRAPPED: set[str] = set()


try:  # pragma: no cover - optional dependency exercised in integration tests
    import psycopg
    from psycopg import sql
except Exception as exc:  # pragma: no cover - captured for runtime errors
    psycopg = None  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]
    _PSYCOPG_IMPORT_ERROR = exc
else:  # pragma: no cover - optional dependency
    _PSYCOPG_IMPORT_ERROR = None


try:  # pragma: no cover - optional dependency exercised in integration tests
    import redis
except Exception as exc:  # pragma: no cover - captured for runtime errors
    redis = None  # type: ignore[assignment]
    _REDIS_IMPORT_ERROR = exc
else:  # pragma: no cover - optional dependency
    _REDIS_IMPORT_ERROR = None


@dataclass
class DisasterRecoveryConfig:
    """Configuration bundle used by the disaster recovery helpers."""

    timescale_dsn: str
    redis_url: str
    mlflow_artifact_uri: str
    object_store_bucket: str
    object_store_prefix: str = "disaster-recovery"
    object_store_endpoint_url: Optional[str] = None
    object_store_region: Optional[str] = None
    work_dir: Path = Path(tempfile.gettempdir()) / "aether-dr"
    pg_dump_bin: str = os.environ.get("PG_DUMP_BIN", "pg_dump")
    pg_restore_bin: str = os.environ.get("PG_RESTORE_BIN", "pg_restore")
    mlflow_restore_path: Optional[Path] = None
    actor: str = getuser()
    log_table: str = "dr_log"

    @classmethod
    def from_env(cls) -> "DisasterRecoveryConfig":
        """Instantiate the configuration from environment variables."""

        timescale_dsn = os.environ.get("DR_TIMESCALE_DSN")
        redis_url = os.environ.get("DR_REDIS_URL")
        mlflow_artifact_uri = os.environ.get("DR_MLFLOW_ARTIFACT_URI")
        object_store_bucket = os.environ.get("DR_OBJECT_STORE_BUCKET")

        missing = [
            name
            for name, value in {
                "DR_TIMESCALE_DSN": timescale_dsn,
                "DR_REDIS_URL": redis_url,
                "DR_MLFLOW_ARTIFACT_URI": mlflow_artifact_uri,
                "DR_OBJECT_STORE_BUCKET": object_store_bucket,
            }.items()
            if not value
        ]
        if missing:
            raise EnvironmentError(
                "Missing required environment variables: " + ", ".join(missing)
            )

        work_dir = _resolve_work_dir(os.environ.get("DR_WORK_DIR"))

        config = cls(
            timescale_dsn=timescale_dsn,
            redis_url=redis_url,
            mlflow_artifact_uri=mlflow_artifact_uri,
            object_store_bucket=object_store_bucket,
            object_store_prefix=os.environ.get(
                "DR_OBJECT_STORE_PREFIX", "disaster-recovery"
            ),
            object_store_endpoint_url=os.environ.get(
                "DR_OBJECT_STORE_ENDPOINT_URL"
            ),
            object_store_region=os.environ.get("DR_OBJECT_STORE_REGION"),
            work_dir=work_dir,
            mlflow_restore_path=(
                Path(os.environ["DR_MLFLOW_RESTORE_PATH"])
                if "DR_MLFLOW_RESTORE_PATH" in os.environ
                else None
            ),
            actor=os.environ.get("DR_ACTOR", getuser()),
            log_table=os.environ.get("DR_LOG_TABLE", "dr_log"),
        )
        config.work_dir.mkdir(parents=True, exist_ok=True)
        return config

    @property
    def redis_connection_kwargs(self) -> Dict[str, Any]:
        return _parse_redis_url(self.redis_url)


def _timestamp(prefix: str) -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{now}"


def _parse_redis_url(url: str) -> Dict[str, Any]:
    parsed = urlparse(url)
    if parsed.scheme not in {"redis", "rediss"}:
        raise ValueError(f"Unsupported Redis URL scheme: {parsed.scheme}")

    db = 0
    if parsed.path and parsed.path != "/":
        try:
            db = int(parsed.path.lstrip("/"))
        except ValueError as exc:
            raise ValueError(f"Invalid Redis database in URL: {parsed.path}") from exc

    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 6379,
        "db": db,
        "password": parsed.password,
        "ssl": parsed.scheme == "rediss",
    }


def _resolve_work_dir(raw: Optional[str]) -> Path:
    """Return a safe working directory for DR artifacts.

    The directory is derived from ``raw`` (or ``tempfile.gettempdir()`` when unset)
    and must be absolute, free from symlinked ancestors, and point to a directory
    when it already exists.
    """

    base = Path(raw).expanduser() if raw else Path(tempfile.gettempdir())
    if not base.is_absolute():
        base = Path.cwd() / base

    _ensure_no_symlink_ancestors(base, context="DR_WORK_DIR")

    resolved = base.resolve()
    if resolved.exists() and not resolved.is_dir():
        raise ValueError("DR_WORK_DIR must reference a directory")

    work_dir = resolved / "aether-dr"
    _ensure_no_symlink_ancestors(work_dir, context="DR_WORK_DIR")
    return work_dir


def _build_object_store_client(config: DisasterRecoveryConfig):
    """Construct a boto3 S3 client using the configuration values."""

    try:  # pragma: no cover - boto3 is optional in unit tests
        import boto3  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("boto3 is required for the disaster recovery playbook") from exc

    session = boto3.session.Session(region_name=config.object_store_region)
    return session.client("s3", endpoint_url=config.object_store_endpoint_url)


def snapshot_timescaledb(config: DisasterRecoveryConfig) -> Path:
    """Run ``pg_dump`` against the configured TimescaleDB instance."""

    snapshot_path = config.work_dir / f"{_timestamp('timescale')}.dump"
    command = [
        config.pg_dump_bin,
        f"--dbname={config.timescale_dsn}",
        "--format=custom",
        f"--file={snapshot_path}",
    ]
    LOGGER.info("Running TimescaleDB snapshot: %s", " ".join(command))
    try:
        subprocess.run(command, check=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        LOGGER.error(
            "pg_dump failed with exit code %s: %s",
            exc.returncode,
            exc.stderr.decode("utf-8", errors="ignore"),
        )
        raise RuntimeError("TimescaleDB snapshot failed") from exc

    LOGGER.info("TimescaleDB snapshot written to %s", snapshot_path)
    return snapshot_path


def snapshot_redis_state(config: DisasterRecoveryConfig) -> Path:
    """Capture Redis state using the ``DUMP`` command for every key."""

    if redis is None:  # pragma: no cover - dependency enforced at runtime
        raise RuntimeError(
            "redis-py is required for Redis snapshots"
        ) from _REDIS_IMPORT_ERROR

    client = redis.Redis(**config.redis_connection_kwargs)
    snapshot_path = config.work_dir / f"{_timestamp('redis')}.json"
    LOGGER.info("Capturing Redis snapshot to %s", snapshot_path)

    state = []
    key_count = 0
    for key in client.scan_iter(count=1000):
        key_count += 1
        value = client.dump(key)
        if value is None:
            continue
        ttl = client.pttl(key)
        key_bytes = key if isinstance(key, bytes) else str(key).encode("utf-8")
        state.append(
            {
                "key": base64.b64encode(key_bytes).decode("ascii"),
                "value": base64.b64encode(value).decode("ascii"),
                "ttl": ttl,
            }
        )

    snapshot_path.write_text(json.dumps({"keys": state, "count": key_count}))
    LOGGER.info("Redis snapshot captured with %s keys", key_count)
    return snapshot_path


def _resolve_mlflow_artifact_source(uri: str) -> Tuple[Path, Optional[Path]]:
    path_candidate = Path(uri.replace("file://", ""))
    if path_candidate.exists():
        return path_candidate, None

    try:  # pragma: no cover - mlflow optional in unit tests
        from mlflow.artifacts import download_artifacts
        from shared.mlflow_safe import harden_mlflow

        harden_mlflow()
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "mlflow is required to download artifact snapshots from non-local URIs"
        ) from exc

    local_path = download_artifacts(artifact_uri=uri)
    return Path(local_path), Path(local_path)


def snapshot_mlflow_artifacts(config: DisasterRecoveryConfig) -> Path:
    """Archive MLflow artifacts into a compressed tarball."""

    source, cleanup = _resolve_mlflow_artifact_source(config.mlflow_artifact_uri)
    snapshot_path = config.work_dir / f"{_timestamp('mlflow')}.tar.gz"

    LOGGER.info("Archiving MLflow artifacts from %s", source)
    with tarfile.open(snapshot_path, "w:gz") as tar:
        if source.is_file():
            tar.add(source, arcname=source.name)
        else:
            for item in source.rglob("*"):
                tar.add(item, arcname=item.relative_to(source))

    LOGGER.info("MLflow artifact archive written to %s", snapshot_path)
    if cleanup is not None and cleanup.exists():
        shutil.rmtree(cleanup, ignore_errors=True)
    return snapshot_path


def create_snapshot_bundle(config: DisasterRecoveryConfig) -> Path:
    """Bundle the database dump, Redis snapshot, and MLflow archive into a tarball."""

    timescale_dump = snapshot_timescaledb(config)
    redis_snapshot = snapshot_redis_state(config)
    mlflow_archive = snapshot_mlflow_artifacts(config)

    bundle_name = f"{_timestamp('aether-dr')}.tar.gz"
    bundle_path = config.work_dir / bundle_name
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "timescale_dump": timescale_dump.name,
        "redis_snapshot": redis_snapshot.name,
        "mlflow_archive": mlflow_archive.name,
    }
    manifest_path = config.work_dir / "snapshot-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    LOGGER.info("Creating DR bundle %s", bundle_path)
    with tarfile.open(bundle_path, "w:gz") as tar:
        tar.add(timescale_dump, arcname=timescale_dump.name)
        tar.add(redis_snapshot, arcname=redis_snapshot.name)
        tar.add(mlflow_archive, arcname=mlflow_archive.name)
        tar.add(manifest_path, arcname="manifest.json")

    LOGGER.info("Snapshot bundle ready: %s", bundle_path)

    timescale_dump.unlink(missing_ok=True)
    redis_snapshot.unlink(missing_ok=True)
    mlflow_archive.unlink(missing_ok=True)
    manifest_path.unlink(missing_ok=True)

    return bundle_path


def push_snapshot(
    bundle_path: Path,
    config: DisasterRecoveryConfig,
    *,
    delete_after_upload: bool = True,
) -> str:
    """Upload the snapshot bundle to the configured object storage bucket."""

    client = _build_object_store_client(config)
    key = f"{config.object_store_prefix.strip('/')}/{bundle_path.name}".strip("/")
    LOGGER.info(
        "Uploading snapshot %s to s3://%s/%s",
        bundle_path,
        config.object_store_bucket,
        key,
    )
    client.upload_file(str(bundle_path), config.object_store_bucket, key)
    LOGGER.info("Upload complete")
    if delete_after_upload:
        bundle_path.unlink(missing_ok=True)
    return key


def _normalize_snapshot_key(snapshot_id: str, config: DisasterRecoveryConfig) -> str:
    snapshot_id = snapshot_id.strip()
    if snapshot_id.startswith("s3://"):
        parsed = urlparse(snapshot_id)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if bucket and bucket != config.object_store_bucket:
            raise ValueError(
                f"Snapshot bucket {bucket} does not match configured bucket {config.object_store_bucket}"
            )
        return key

    key = snapshot_id.lstrip("/")
    prefix = config.object_store_prefix.strip("/")
    if prefix and not key.startswith(f"{prefix}/"):
        key = f"{prefix}/{key}" if key else prefix
    return key


def _restore_timescaledb(
    config: DisasterRecoveryConfig, dump_path: Path, *, drop_existing: bool = True
) -> None:
    restore_cmd = [config.pg_restore_bin, f"--dbname={config.timescale_dsn}"]
    if drop_existing:
        restore_cmd.extend(["--clean", "--create"])
    restore_cmd.append(str(dump_path))
    LOGGER.info("Restoring TimescaleDB: %s", " ".join(restore_cmd))
    try:
        subprocess.run(restore_cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        LOGGER.error(
            "pg_restore failed with exit code %s: %s",
            exc.returncode,
            exc.stderr.decode("utf-8", errors="ignore"),
        )
        raise RuntimeError("TimescaleDB restore failed") from exc


def _restore_redis_state(config: DisasterRecoveryConfig, snapshot: Path) -> None:
    if redis is None:  # pragma: no cover - dependency enforced at runtime
        raise RuntimeError(
            "redis-py is required for Redis restore"
        ) from _REDIS_IMPORT_ERROR

    payload = json.loads(snapshot.read_text())
    keys = payload.get("keys", [])
    LOGGER.info("Restoring Redis dataset containing %s keys", len(keys))

    client = redis.Redis(**config.redis_connection_kwargs)
    client.flushdb()

    pipeline = client.pipeline()
    for entry in keys:
        key_bytes = base64.b64decode(entry["key"])
        value_bytes = base64.b64decode(entry["value"])
        ttl = entry.get("ttl", 0)
        ttl = ttl if isinstance(ttl, int) and ttl > 0 else 0
        pipeline.restore(key_bytes, ttl, value_bytes, replace=True)
    if keys:
        pipeline.execute()
    LOGGER.info("Redis restore complete")


def _ensure_no_symlink_ancestors(path: Path, *, context: str) -> None:
    """Raise ``ValueError`` if ``path`` or any existing ancestor is a symlink."""

    for ancestor in (path,) + tuple(path.parents):
        if ancestor.exists() and ancestor.is_symlink():
            raise ValueError(f"{context} must not reference symlinked directories")


def _restore_mlflow_artifacts(config: DisasterRecoveryConfig, archive: Path) -> None:
    if config.mlflow_restore_path is None:
        raise RuntimeError(
            "MLflow restore path is not configured (set DR_MLFLOW_RESTORE_PATH)."
        )

    target_path = config.mlflow_restore_path.expanduser()
    if not target_path.is_absolute():
        raise ValueError("MLflow restore path must be absolute")

    _ensure_no_symlink_ancestors(target_path, context="MLflow restore path")

    LOGGER.info("Restoring MLflow artifacts to %s", target_path)
    if target_path.exists():
        if target_path.is_symlink():
            raise ValueError("MLflow restore path must not be a symlink")
        if not target_path.is_dir():
            raise ValueError("MLflow restore path must be a directory")
        shutil.rmtree(target_path)

    target_path.mkdir(parents=True, exist_ok=True)

    with tarfile.open(archive, "r:gz") as tar:
        safe_extract_tar(tar, target_path)


def _log_dr_action(config: DisasterRecoveryConfig, action: str) -> None:
    if psycopg is None or sql is None:  # pragma: no cover - dependency enforced at runtime
        raise RuntimeError(
            "psycopg is required to log disaster recovery actions"
        ) from _PSYCOPG_IMPORT_ERROR

    LOGGER.debug("Recording DR action '%s' for actor '%s'", action, config.actor)
    insert_query = sql.SQL(
        "INSERT INTO {table} (actor, action, ts) VALUES (%s, %s, NOW())"
    ).format(table=sql.Identifier(config.log_table))
    with psycopg.connect(config.timescale_dsn, autocommit=True) as conn:  # type: ignore[arg-type]
        with conn.cursor() as cur:
            _ensure_log_table_exists(cur, config.log_table)
            cur.execute(insert_query, (config.actor, action))


def _ensure_log_table_exists(cur: Any, table_name: str) -> None:
    """Ensure the disaster recovery log table exists."""

    if table_name in _LOG_TABLE_BOOTSTRAPPED:
        return

    create_query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGSERIAL PRIMARY KEY,
            actor TEXT NOT NULL,
            action TEXT NOT NULL,
            ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    ).format(table=sql.Identifier(table_name))

    cur.execute(create_query)

    _LOG_TABLE_BOOTSTRAPPED.add(table_name)


def restore_cluster(
    snapshot_id: str,
    config: Optional[DisasterRecoveryConfig] = None,
    *,
    drop_existing: bool = True,
) -> None:
    """Download the specified snapshot from object storage and restore it."""

    config = config or DisasterRecoveryConfig.from_env()
    key = _normalize_snapshot_key(snapshot_id, config)
    _log_dr_action(config, f"restore:start:{key}")

    try:
        client = _build_object_store_client(config)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bundle_path = tmp_path / Path(key).name
            LOGGER.info(
                "Downloading snapshot s3://%s/%s to %s",
                config.object_store_bucket,
                key,
                bundle_path,
            )
            client.download_file(config.object_store_bucket, key, str(bundle_path))

            with tarfile.open(bundle_path, "r:gz") as tar:
                safe_extract_tar(tar, tmp_path)

            manifest_path = tmp_path / "manifest.json"
            if not manifest_path.exists():
                raise RuntimeError("Snapshot manifest missing from bundle")
            manifest = json.loads(manifest_path.read_text())

            dump_path = tmp_path / manifest["timescale_dump"]
            redis_snapshot = tmp_path / manifest["redis_snapshot"]
            mlflow_archive = tmp_path / manifest["mlflow_archive"]

            _restore_timescaledb(config, dump_path, drop_existing=drop_existing)
            _restore_redis_state(config, redis_snapshot)
            _restore_mlflow_artifacts(config, mlflow_archive)
    except Exception:
        _log_dr_action(config, f"restore:error:{key}")
        raise

    LOGGER.info("Cluster restore completed successfully")
    _log_dr_action(config, f"restore:complete:{key}")


def snapshot_cluster(config: Optional[DisasterRecoveryConfig] = None) -> str:
    """Create a full cluster snapshot and upload it to object storage."""

    config = config or DisasterRecoveryConfig.from_env()
    _log_dr_action(config, "snapshot:start")

    try:
        bundle = create_snapshot_bundle(config)
        key = push_snapshot(bundle, config)
    except Exception:
        _log_dr_action(config, "snapshot:error")
        raise

    LOGGER.info("Snapshot uploaded to s3://%s/%s", config.object_store_bucket, key)
    _log_dr_action(config, f"snapshot:complete:{key}")
    return key


def _configure_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity (use -vv for debug)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("snapshot", parents=[parent], help="Create a new cluster snapshot")

    restore_parser = subparsers.add_parser(
        "restore",
        parents=[parent],
        help="Restore cluster from a snapshot",
    )
    restore_parser.add_argument("snapshot_id", help="Snapshot identifier or S3 key")
    restore_parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Skip dropping existing objects during restore (pg_restore --clean)",
    )

    return parser


def main() -> None:
    parser = _build_cli()
    args = parser.parse_args()
    _configure_logging(args.verbose)
    config = DisasterRecoveryConfig.from_env()

    if args.command == "snapshot":
        key = snapshot_cluster(config)
        print(f"Snapshot uploaded to s3://{config.object_store_bucket}/{key}")
    elif args.command == "restore":
        restore_cluster(args.snapshot_id, config, drop_existing=not args.no_clean)
        print("Cluster restore completed")


if __name__ == "__main__":
    main()
