"""Backup and restore orchestration for TimescaleDB and MLflow artifacts.

This module provides a nightly backup job that snapshots the primary TimescaleDB
instance and the MLflow artifact store, encrypts the artifacts locally with
AES-GCM, uploads the resulting archives to Linode Object Storage, and records a
manifest that can be used to restore the system. Restores are performed from the
manifest to ensure integrity by validating recorded hashes before any data is
put back in place.

Environment variables
---------------------
PG_DSN                libpq connection string used by ``pg_dump`` / ``pg_restore``
PG_DATABASE           Optional database name for pg_restore (fallback derived from DSN)
MLFLOW_ARTIFACT_DIR   Directory that contains the MLflow artifact repository
LINODE_BUCKET         S3 bucket name hosted on Linode Object Storage
LINODE_REGION         Region for the Linode bucket (optional)
LINODE_PREFIX         Optional key prefix within the bucket for all backups
LINODE_ENDPOINT       Custom S3 endpoint for Linode Object Storage (optional)
LINODE_ACCESS_KEY     Access key for Linode Object Storage
LINODE_SECRET_KEY     Secret key for Linode Object Storage
LINODE_SSE_ALGO       Server-side encryption algorithm (default ``AES256``)
LINODE_SSE_KMS_KEY    Optional KMS key id if using kms encryption
BACKUP_RETENTION_DAYS Optional retention policy (not yet enforced, but stored)
BACKUP_ENCRYPTION_KEY Base64 encoded 256-bit key used for AES-GCM encryption

The script exposes a CLI with two commands:

``python backup_job.py backup``
    Executes a backup immediately using the provided configuration.

``python backup_job.py restore <manifest_id>``
    Restores the TimescaleDB database and MLflow artifacts referenced by the
    manifest identifier.

The code is written to be operational in production but leans on subprocesses
for PostgreSQL utilities so that TimescaleDB-specific features (hypertables,
compression) are preserved.
"""
from __future__ import annotations

import argparse
import datetime as dt
import base64
import hashlib
import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Dict, Iterable, List, Optional

try:  # pragma: no cover - optional dependency for operational runtime
    import boto3
except Exception:  # pragma: no cover - boto3 not required for static analysis
    boto3 = None  # type: ignore

try:  # pragma: no cover - optional dependency
    import psycopg2
except Exception:  # pragma: no cover - psycopg2 not required for static analysis
    psycopg2 = None  # type: ignore

try:  # pragma: no cover - optional dependency
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except Exception:  # pragma: no cover - cryptography optional for static analysis
    AESGCM = None  # type: ignore


LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


DEFAULT_BUCKET_PREFIX = "backups"
DEFAULT_NONCE_SIZE = 12
MANIFEST_NAME = "manifest.json"


@dataclass
class BackupConfig:
    """Configuration for the backup job."""

    pg_dsn: str
    mlflow_artifact_dir: Path
    bucket_name: str
    bucket_prefix: str = DEFAULT_BUCKET_PREFIX
    region_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    sse_algorithm: str = "AES256"
    kms_key_id: Optional[str] = None
    pg_database: Optional[str] = None
    retention_days: Optional[int] = None
    encryption_key: bytes = b""

    @classmethod
    def from_env(cls) -> "BackupConfig":
        """Initialise configuration from environment variables."""

        pg_dsn = os.environ.get("PG_DSN")
        artifact_dir = os.environ.get("MLFLOW_ARTIFACT_DIR")
        bucket = os.environ.get("LINODE_BUCKET")

        if not pg_dsn:
            raise RuntimeError("PG_DSN environment variable is required")
        if not artifact_dir:
            raise RuntimeError("MLFLOW_ARTIFACT_DIR environment variable is required")
        if not bucket:
            raise RuntimeError("LINODE_BUCKET environment variable is required")

        encryption_key_b64 = os.environ.get("BACKUP_ENCRYPTION_KEY")
        if not encryption_key_b64:
            raise RuntimeError("BACKUP_ENCRYPTION_KEY environment variable is required")

        try:
            encryption_key = base64.b64decode(encryption_key_b64)
        except Exception as exc:  # pragma: no cover - validation logic
            raise RuntimeError("Invalid BACKUP_ENCRYPTION_KEY encoding") from exc

        if len(encryption_key) not in {16, 24, 32}:
            raise RuntimeError("BACKUP_ENCRYPTION_KEY must be 128/192/256 bits")

        return cls(
            pg_dsn=pg_dsn,
            mlflow_artifact_dir=Path(artifact_dir),
            bucket_name=bucket,
            bucket_prefix=os.environ.get("LINODE_PREFIX", DEFAULT_BUCKET_PREFIX),
            region_name=os.environ.get("LINODE_REGION"),
            endpoint_url=os.environ.get("LINODE_ENDPOINT"),
            access_key=os.environ.get("LINODE_ACCESS_KEY"),
            secret_key=os.environ.get("LINODE_SECRET_KEY"),
            sse_algorithm=os.environ.get("LINODE_SSE_ALGO", "AES256"),
            kms_key_id=os.environ.get("LINODE_SSE_KMS_KEY"),
            pg_database=os.environ.get("PG_DATABASE"),
            retention_days=(
                int(os.environ["BACKUP_RETENTION_DAYS"])
                if os.environ.get("BACKUP_RETENTION_DAYS")
                else None
            ),
            encryption_key=encryption_key,
        )


@dataclass
class BackupArtifact:
    """Represents a single file recorded in the backup manifest."""

    name: str
    path: Path
    s3_key: str
    sha256: str
    size_bytes: int
    nonce_b64: str
    type: str
    encryption: str = "AESGCM"

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "s3_key": self.s3_key,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "type": self.type,
            "nonce": self.nonce_b64,
            "encryption": self.encryption,
        }


def _safe_extract_tar(archive: tarfile.TarFile, destination: Path) -> None:
    """Safely extract ``archive`` into ``destination``.

    Each archive member is validated to ensure it is a relative path located
    within ``destination``.  Absolute paths, traversal sequences, and entries
    that would resolve outside of the destination directory raise
    ``ValueError`` and prevent any extraction.
    """

    destination_resolved = destination.resolve()
    members = archive.getmembers()

    for member in members:
        member_path = PurePosixPath(member.name)
        if member_path.is_absolute():
            raise ValueError("Tar archive entries must be relative paths")
        if any(part == ".." for part in member_path.parts):
            raise ValueError("Tar archive entries must not contain traversal sequences")

        candidate = destination_resolved.joinpath(Path(*member_path.parts))
        resolved_candidate = candidate.resolve(strict=False)
        try:
            resolved_candidate.relative_to(destination_resolved)
        except ValueError as exc:
            raise ValueError("Tar archive entry escapes extraction directory") from exc

    archive.extractall(path=destination_resolved, filter="data")


class BackupJob:
    """Orchestrates backup creation and restoration."""

    def __init__(self, config: BackupConfig) -> None:
        self.config = config
        self._s3_client = self._build_s3_client()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_backup(self) -> str:
        """Execute the backup workflow.

        Returns
        -------
        str
            Manifest identifier for the completed backup.
        """

        manifest_id = str(uuid.uuid4())
        timestamp = dt.datetime.utcnow().replace(microsecond=0).isoformat()
        backup_prefix = self._build_backup_prefix(timestamp, manifest_id)

        LOGGER.info("Starting backup %s", manifest_id)
        artifacts: List[BackupArtifact] = []

        try:
            with tempfile.TemporaryDirectory(prefix="backup_job_") as tmp_dir:
                temp_path = Path(tmp_dir)
                db_dump = temp_path / "timescaledb.dump"
                mlflow_archive = temp_path / "mlflow_artifacts.tar.gz"
                encrypted_dir = temp_path / "encrypted"
                encrypted_dir.mkdir(parents=True, exist_ok=True)
                manifest_path = temp_path / MANIFEST_NAME

                self._dump_database(db_dump)
                encrypted_db_path = encrypted_dir / "timescaledb.dump.enc"
                artifacts.append(
                    self._stage_artifact(
                        name="timescaledb",
                        source_path=db_dump,
                        encrypted_path=encrypted_db_path,
                        s3_key=f"{backup_prefix}/timescaledb.dump.enc",
                        artifact_type="database",
                    )
                )

                self._archive_mlflow(mlflow_archive)
                encrypted_mlflow_path = encrypted_dir / "mlflow_artifacts.tar.gz.enc"
                artifacts.append(
                    self._stage_artifact(
                        name="mlflow_artifacts",
                        source_path=mlflow_archive,
                        encrypted_path=encrypted_mlflow_path,
                        s3_key=f"{backup_prefix}/mlflow_artifacts.tar.gz.enc",
                        artifact_type="mlflow",
                    )
                )

                manifest = self._write_manifest(
                    manifest_path=manifest_path,
                    manifest_id=manifest_id,
                    timestamp=timestamp,
                    artifacts=artifacts,
                )

                # Upload artifacts and manifest to object storage
                for artifact in artifacts:
                    self._upload_file(
                        artifact.path,
                        artifact.s3_key,
                    )

                manifest_key = f"{backup_prefix}/{MANIFEST_NAME}"
                self._upload_file(manifest_path, manifest_key)

            self._log_backup(manifest_id, timestamp, "SUCCESS", manifest)
            LOGGER.info("Backup %s completed", manifest_id)
            return manifest_id

        except Exception as exc:  # noqa: BLE001 - log & rethrow
            LOGGER.exception("Backup %s failed", manifest_id)
            failure_manifest = {
                "manifest_id": manifest_id,
                "timestamp": timestamp,
                "error": str(exc),
            }
            self._log_backup(manifest_id, timestamp, "FAILED", failure_manifest)
            raise exc

    def restore_backup(self, manifest_id: str) -> None:
        """Restore the system to the state captured in ``manifest_id``."""

        LOGGER.info("Restoring backup %s", manifest_id)
        manifest = self._download_manifest(manifest_id)
        self._verify_manifest(manifest)

        with tempfile.TemporaryDirectory(prefix="restore_job_") as tmp_dir:
            temp_path = Path(tmp_dir)
            download_dir = temp_path / "downloads"
            download_dir.mkdir(parents=True, exist_ok=True)

            for entry in manifest["artifacts"]:
                artifact_path = download_dir / Path(entry["s3_key"]).name
                LOGGER.info("Downloading %s", entry["s3_key"])
                self._download_file(entry["s3_key"], artifact_path)
                decrypted_path = download_dir / f"{Path(entry['s3_key']).stem}.decrypted"
                self._decrypt_file(
                    encrypted_path=artifact_path,
                    destination=decrypted_path,
                    nonce_b64=entry.get("nonce", ""),
                )
                computed_hash = self._hash_file(decrypted_path)
                if computed_hash != entry["sha256"]:
                    raise RuntimeError(
                        "Hash mismatch for %s: expected %s got %s"
                        % (entry["name"], entry["sha256"], computed_hash)
                    )

                if entry["type"] == "database":
                    self._restore_database(decrypted_path)
                elif entry["type"] == "mlflow":
                    self._restore_mlflow_artifacts(decrypted_path)
                else:
                    LOGGER.warning("Unknown artifact type %s", entry["type"])

        self._log_backup(manifest_id, manifest["timestamp"], "RESTORED", manifest)
        LOGGER.info("Backup %s restored", manifest_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _build_s3_client(self):  # type: ignore[override]
        if boto3 is None:
            raise RuntimeError(
                "boto3 is required for backup operations but is not installed"
            )

        session = boto3.session.Session(
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            region_name=self.config.region_name,
        )
        return session.client("s3", endpoint_url=self.config.endpoint_url)

    def _dump_database(self, destination: Path) -> None:
        LOGGER.info("Dumping TimescaleDB to %s", destination)
        cmd = [
            "pg_dump",
            "--format=custom",
            "--file",
            str(destination),
            self.config.pg_dsn,
        ]
        self._run_subprocess(cmd, "pg_dump")

    def _archive_mlflow(self, destination: Path) -> None:
        LOGGER.info("Archiving MLflow artifacts from %s", self.config.mlflow_artifact_dir)
        if not self.config.mlflow_artifact_dir.exists():
            raise FileNotFoundError(
                f"MLflow artifact directory {self.config.mlflow_artifact_dir} does not exist"
            )

        with tarfile.open(destination, "w:gz") as archive:
            archive.add(
                self.config.mlflow_artifact_dir,
                arcname=self.config.mlflow_artifact_dir.name,
            )

    def _stage_artifact(
        self,
        name: str,
        source_path: Path,
        encrypted_path: Path,
        s3_key: str,
        artifact_type: str,
    ) -> BackupArtifact:
        plaintext_hash = self._hash_file(source_path)
        nonce_b64 = self._encrypt_file(source_path, encrypted_path)
        return BackupArtifact(
            name=name,
            path=encrypted_path,
            s3_key=s3_key,
            sha256=plaintext_hash,
            size_bytes=source_path.stat().st_size,
            type=artifact_type,
            nonce_b64=nonce_b64,
        )

    def _write_manifest(
        self,
        manifest_path: Path,
        manifest_id: str,
        timestamp: str,
        artifacts: Iterable[BackupArtifact],
    ) -> Dict[str, object]:
        manifest = {
            "manifest_id": manifest_id,
            "timestamp": timestamp,
            "artifacts": [artifact.to_dict() for artifact in artifacts],
            "config": {
                "bucket": self.config.bucket_name,
                "bucket_prefix": self.config.bucket_prefix,
                "retention_days": self.config.retention_days,
            },
        }
        manifest["hash"] = self._hash_manifest(manifest["artifacts"])

        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
        return manifest

    def _hash_manifest(self, artifacts: Iterable[Dict[str, object]]) -> str:
        serialized = json.dumps(list(artifacts), sort_keys=True).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest()

    def _upload_file(self, path: Path, key: str) -> None:
        LOGGER.info("Uploading %s to s3://%s/%s", path, self.config.bucket_name, key)
        extra_args = {"ServerSideEncryption": self.config.sse_algorithm}
        if self.config.kms_key_id:
            extra_args["SSEKMSKeyId"] = self.config.kms_key_id

        with path.open("rb") as body:
            self._s3_client.upload_fileobj(
                Fileobj=body,
                Bucket=self.config.bucket_name,
                Key=key,
                ExtraArgs=extra_args,
            )

    def _build_backup_prefix(self, timestamp: str, manifest_id: str) -> str:
        prefix = (self.config.bucket_prefix or "").strip("/")
        slug = f"{timestamp}_{manifest_id}"
        return f"{prefix}/{slug}" if prefix else slug

    def _download_manifest(self, manifest_id: str) -> Dict[str, object]:
        LOGGER.info("Fetching manifest for %s", manifest_id)
        paginator = self._s3_client.get_paginator("list_objects_v2")
        prefix_root = (self.config.bucket_prefix or "").strip("/")
        prefix = f"{prefix_root}/" if prefix_root else ""
        for page in paginator.paginate(Bucket=self.config.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(f"{manifest_id}/{MANIFEST_NAME}"):
                    return self._fetch_manifest(obj["Key"])
                if obj["Key"].endswith(f"{manifest_id}_{MANIFEST_NAME}"):
                    return self._fetch_manifest(obj["Key"])
                if manifest_id in obj["Key"] and obj["Key"].endswith(MANIFEST_NAME):
                    return self._fetch_manifest(obj["Key"])
        raise FileNotFoundError(f"Manifest {manifest_id} not found in bucket")

    def _fetch_manifest(self, key: str) -> Dict[str, object]:
        LOGGER.info("Downloading manifest %s", key)
        response = self._s3_client.get_object(Bucket=self.config.bucket_name, Key=key)
        data = json.loads(response["Body"].read())
        return data

    def _verify_manifest(self, manifest: Dict[str, object]) -> None:
        artifacts = manifest.get("artifacts", [])
        expected_hash = manifest.get("hash")
        computed_hash = self._hash_manifest(artifacts)
        if expected_hash != computed_hash:
            raise RuntimeError(
                f"Manifest hash mismatch: expected {expected_hash} got {computed_hash}"
            )

    def _download_file(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("wb") as handle:
            self._s3_client.download_fileobj(
                Bucket=self.config.bucket_name,
                Key=key,
                Fileobj=handle,
            )

    def _encrypt_file(self, source_path: Path, destination: Path) -> str:
        if AESGCM is None:
            raise RuntimeError(
                "cryptography is required for AES-GCM encryption but is not installed"
            )

        aesgcm = AESGCM(self.config.encryption_key)
        nonce = os.urandom(DEFAULT_NONCE_SIZE)
        with source_path.open("rb") as handle:
            plaintext = handle.read()
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        with destination.open("wb") as handle:
            handle.write(ciphertext)
        return base64.b64encode(nonce).decode("ascii")

    def _decrypt_file(self, encrypted_path: Path, destination: Path, nonce_b64: str) -> None:
        if AESGCM is None:
            raise RuntimeError(
                "cryptography is required for AES-GCM decryption but is not installed"
            )
        if not nonce_b64:
            raise RuntimeError("Missing nonce for encrypted artifact")

        aesgcm = AESGCM(self.config.encryption_key)
        nonce = base64.b64decode(nonce_b64)
        if len(nonce) != DEFAULT_NONCE_SIZE:
            raise RuntimeError(
                f"Unexpected nonce length {len(nonce)} for AES-GCM payload"
            )
        with encrypted_path.open("rb") as handle:
            ciphertext = handle.read()
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        with destination.open("wb") as handle:
            handle.write(plaintext)

    def _restore_database(self, dump_path: Path) -> None:
        database = self.config.pg_database or self._extract_db_name(self.config.pg_dsn)
        LOGGER.info("Restoring TimescaleDB dump into database %s", database)
        cmd = [
            "pg_restore",
            "--clean",
            "--if-exists",
            "--dbname",
            database,
            str(dump_path),
        ]
        env = os.environ.copy()
        env["PGCONNECT_TIMEOUT"] = env.get("PGCONNECT_TIMEOUT", "10")
        self._run_subprocess(cmd, "pg_restore", env=env)

    def _restore_mlflow_artifacts(self, archive_path: Path) -> None:
        target_dir = self.config.mlflow_artifact_dir
        if not target_dir.is_absolute():
            raise ValueError("MLflow artifact directory must be absolute")

        for ancestor in (target_dir,) + tuple(target_dir.parents):
            if ancestor.exists() and ancestor.is_symlink():
                raise ValueError("MLflow artifact directory must not reference symlinks")

        LOGGER.info("Restoring MLflow artifacts to %s", target_dir)
        backup_dir = target_dir.with_suffix(".bak")
        if target_dir.exists():
            if target_dir.is_symlink():
                raise ValueError("MLflow artifact directory must not be a symlink")
            if backup_dir.exists():
                if backup_dir.is_symlink():
                    raise ValueError("MLflow artifact backup path must not be a symlink")
                shutil.rmtree(backup_dir)
            target_dir.rename(backup_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        parent_dir = target_dir.parent
        if parent_dir.exists() and parent_dir.is_symlink():
            raise ValueError("MLflow artifact parent directory must not be a symlink")
        with tarfile.open(archive_path, "r:gz") as archive:
            _safe_extract_tar(archive, parent_dir)
        # tarball contains original directory name; move contents to target
        extracted_root = target_dir.parent / target_dir.name
        if extracted_root.exists() and extracted_root != target_dir:
            if target_dir.exists():
                shutil.rmtree(target_dir)
            extracted_root.rename(target_dir)

    def _log_backup(
        self,
        manifest_id: str,
        timestamp: str,
        status: str,
        manifest: Optional[Dict[str, object]] = None,
    ) -> None:
        LOGGER.info("Recording backup status %s for %s", status, manifest_id)
        if psycopg2 is None:
            LOGGER.warning(
                "psycopg2 not installed; skipping backup_log database write for %s",
                manifest_id,
            )
            return
        try:
            with psycopg2.connect(self.config.pg_dsn) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS backup_log (
                            manifest_id TEXT PRIMARY KEY,
                            ts TIMESTAMPTZ,
                            status TEXT,
                            manifest JSONB
                        )
                        """
                    )
                    cursor.execute(
                        """
                        INSERT INTO backup_log (manifest_id, ts, status, manifest)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (manifest_id)
                        DO UPDATE SET status = EXCLUDED.status, manifest = EXCLUDED.manifest, ts = EXCLUDED.ts
                        """,
                        (
                            manifest_id,
                            dt.datetime.fromisoformat(timestamp),
                            status,
                            json.dumps(manifest) if manifest else None,
                        ),
                    )
        except Exception as exc:  # pragma: no cover - runtime logging
            LOGGER.exception("Failed to write to backup_log: %s", exc)

    def _run_subprocess(self, command: List[str], name: str, env: Optional[Dict[str, str]] = None) -> None:
        LOGGER.debug("Running %s command: %s", name, " ".join(command))
        try:
            subprocess.run(command, check=True, env=env)
        except subprocess.CalledProcessError as exc:  # pragma: no cover - runtime error handling
            raise RuntimeError(f"{name} command failed: {exc}") from exc

    def _hash_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _extract_db_name(self, dsn: str) -> str:
        for part in dsn.split():
            if part.startswith("dbname="):
                return part.split("=", 1)[1]
        return dsn


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TimescaleDB + MLflow backup tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("backup", help="Run backup now")

    restore_parser = subparsers.add_parser("restore", help="Restore from manifest")
    restore_parser.add_argument("manifest_id", help="Manifest identifier to restore")

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    config = BackupConfig.from_env()
    job = BackupJob(config)

    if args.command == "backup":
        manifest_id = job.run_backup()
        LOGGER.info("Backup completed with manifest %s", manifest_id)
    elif args.command == "restore":
        job.restore_backup(args.manifest_id)
    else:  # pragma: no cover - argparse enforces commands
        raise RuntimeError(f"Unsupported command {args.command}")


if __name__ == "__main__":  # pragma: no cover - CLI execution
    main()
