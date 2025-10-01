"""Disaster recovery playbook for TimescaleDB and MLflow artifacts.

This module provides helpers to capture crash-consistent snapshots of the
TimescaleDB instance backing Aether as well as MLflow experiment artifacts.
Snapshots are bundled into a tarball and pushed to remote object storage
(currently S3-compatible).  The ``restore_cluster`` entry point downloads the
latest snapshot and rebuilds the data plane.  A small CLI is provided so the
runbook can be executed manually during incident response.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

LOGGER = logging.getLogger("dr_playbook")


@dataclass
class DisasterRecoveryConfig:
    """Configuration bundle used by the disaster recovery helpers."""

    timescale_dsn: str
    mlflow_artifact_uri: str
    object_store_bucket: str
    object_store_prefix: str = "disaster-recovery"
    object_store_endpoint_url: Optional[str] = None
    object_store_region: Optional[str] = None
    work_dir: Path = Path(tempfile.gettempdir()) / "aether-dr"
    pg_dump_bin: str = os.environ.get("PG_DUMP_BIN", "pg_dump")
    pg_restore_bin: str = os.environ.get("PG_RESTORE_BIN", "pg_restore")
    mlflow_restore_path: Optional[Path] = None

    @classmethod
    def from_env(cls) -> "DisasterRecoveryConfig":
        """Instantiate the configuration from environment variables."""

        timescale_dsn = os.environ.get("DR_TIMESCALE_DSN")
        mlflow_artifact_uri = os.environ.get("DR_MLFLOW_ARTIFACT_URI")
        object_store_bucket = os.environ.get("DR_OBJECT_STORE_BUCKET")

        missing = [
            name
            for name, value in {
                "DR_TIMESCALE_DSN": timescale_dsn,
                "DR_MLFLOW_ARTIFACT_URI": mlflow_artifact_uri,
                "DR_OBJECT_STORE_BUCKET": object_store_bucket,
            }.items()
            if not value
        ]
        if missing:
            raise EnvironmentError(
                "Missing required environment variables: " + ", ".join(missing)
            )

        config = cls(
            timescale_dsn=timescale_dsn,
            mlflow_artifact_uri=mlflow_artifact_uri,
            object_store_bucket=object_store_bucket,
            object_store_prefix=os.environ.get(
                "DR_OBJECT_STORE_PREFIX", "disaster-recovery"
            ),
            object_store_endpoint_url=os.environ.get(
                "DR_OBJECT_STORE_ENDPOINT_URL"
            ),
            object_store_region=os.environ.get("DR_OBJECT_STORE_REGION"),
            work_dir=Path(os.environ.get("DR_WORK_DIR", tempfile.gettempdir()))
            / "aether-dr",
            mlflow_restore_path=(
                Path(os.environ["DR_MLFLOW_RESTORE_PATH"])
                if "DR_MLFLOW_RESTORE_PATH" in os.environ
                else None
            ),
        )
        config.work_dir.mkdir(parents=True, exist_ok=True)
        return config


def _timestamp(prefix: str) -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{now}"


def _build_object_store_client(config: DisasterRecoveryConfig):
    """Construct a boto3 S3 client using the configuration values."""

    try:  # pragma: no cover - boto3 is optional in tests
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


def _resolve_mlflow_artifact_source(uri: str) -> Tuple[Path, Optional[Path]]:
    path_candidate = Path(uri.replace("file://", ""))
    if path_candidate.exists():
        return path_candidate, None

    try:  # pragma: no cover - mlflow optional in tests
        from mlflow.artifacts import download_artifacts
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


def create_snapshot_bundle(
    config: DisasterRecoveryConfig,
) -> Path:
    """Bundle the database dump and MLflow archive into a single tarball."""

    timescale_dump = snapshot_timescaledb(config)
    mlflow_archive = snapshot_mlflow_artifacts(config)

    bundle_name = f"{_timestamp('aether-dr')}.tar.gz"
    bundle_path = config.work_dir / bundle_name
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "timescale_dump": timescale_dump.name,
        "mlflow_archive": mlflow_archive.name,
    }
    manifest_path = config.work_dir / "snapshot-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    LOGGER.info("Creating DR bundle %s", bundle_path)
    with tarfile.open(bundle_path, "w:gz") as tar:
        tar.add(timescale_dump, arcname=timescale_dump.name)
        tar.add(mlflow_archive, arcname=mlflow_archive.name)
        tar.add(manifest_path, arcname="manifest.json")

    LOGGER.info("Snapshot bundle ready: %s", bundle_path)

    # Clean up intermediate files after bundling
    timescale_dump.unlink(missing_ok=True)
    mlflow_archive.unlink(missing_ok=True)
    manifest_path.unlink(missing_ok=True)

    return bundle_path


def push_snapshot(
    bundle_path: Path, config: DisasterRecoveryConfig, *, delete_after_upload: bool = True
) -> str:
    """Upload the snapshot bundle to the configured object storage bucket."""

    client = _build_object_store_client(config)
    key = f"{config.object_store_prefix.strip('/')}/{bundle_path.name}".strip("/")
    LOGGER.info(
        "Uploading snapshot %s to s3://%s/%s", bundle_path, config.object_store_bucket, key
    )
    client.upload_file(str(bundle_path), config.object_store_bucket, key)
    LOGGER.info("Upload complete")
    if delete_after_upload:
        bundle_path.unlink(missing_ok=True)
    return key


def restore_cluster(config: DisasterRecoveryConfig, *, drop_existing: bool = True) -> None:
    """Download the latest snapshot from object storage and restore it."""

    client = _build_object_store_client(config)
    prefix = config.object_store_prefix.strip("/")
    paginator = client.get_paginator("list_objects_v2")
    latest_obj = None
    for page in paginator.paginate(
        Bucket=config.object_store_bucket,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            if latest_obj is None or obj["LastModified"] > latest_obj["LastModified"]:
                latest_obj = obj

    if latest_obj is None:
        raise RuntimeError("No disaster recovery snapshots found in object storage")

    key = latest_obj["Key"]
    LOGGER.info("Restoring from snapshot s3://%s/%s", config.object_store_bucket, key)

    with tempfile.TemporaryDirectory() as tmpdir:
        bundle_path = Path(tmpdir) / Path(key).name
        client.download_file(config.object_store_bucket, key, str(bundle_path))

        with tarfile.open(bundle_path, "r:gz") as tar:
            tar.extractall(path=tmpdir)

        manifest_path = Path(tmpdir) / "manifest.json"
        if not manifest_path.exists():
            raise RuntimeError("Snapshot manifest missing from bundle")
        manifest = json.loads(manifest_path.read_text())

        dump_path = Path(tmpdir) / manifest["timescale_dump"]
        mlflow_archive = Path(tmpdir) / manifest["mlflow_archive"]

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

        if config.mlflow_restore_path is None:
            raise RuntimeError(
                "MLflow restore path is not configured (set DR_MLFLOW_RESTORE_PATH)."
            )

        target_path = config.mlflow_restore_path
        LOGGER.info("Restoring MLflow artifacts to %s", target_path)
        if target_path.exists():
            shutil.rmtree(target_path)
        target_path.mkdir(parents=True, exist_ok=True)

        with tarfile.open(mlflow_archive, "r:gz") as tar:
            tar.extractall(path=target_path)

        LOGGER.info("Cluster restore completed successfully")


def trigger_manual_failover(config: DisasterRecoveryConfig) -> None:
    """Manual failover entry point that wraps ``restore_cluster`` with logging."""

    LOGGER.warning("Manual failover initiated. Proceeding to restore latest snapshot.")
    restore_cluster(config, drop_existing=True)
    LOGGER.warning("Manual failover complete. Promote restored cluster as primary.")


def _configure_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["snapshot", "restore", "failover"], help="Action to perform")
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity (use -vv for debug)",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep local snapshot artifacts after upload",
    )
    parser.add_argument(
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
        bundle = create_snapshot_bundle(config)
        key = push_snapshot(bundle, config, delete_after_upload=not args.keep_local)
        if args.keep_local:
            LOGGER.info("Snapshot retained locally at %s", bundle)
        print(f"Snapshot uploaded to s3://{config.object_store_bucket}/{key}")
    elif args.command == "restore":
        restore_cluster(config, drop_existing=not args.no_clean)
        print("Cluster restore completed")
    elif args.command == "failover":
        trigger_manual_failover(config)
        print("Manual failover finished")


if __name__ == "__main__":
    main()
