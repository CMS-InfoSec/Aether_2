"""Feast feature store backup workflow.

This module provides a CLI that captures the Feast registry metadata and the
Redis online store snapshot, encrypts the archives locally using AES-GCM, and
uploads the artifacts to Linode Object Storage. The manifest mirrors the
Timescale/MLflow backup format so the restore drill can enumerate Feast backups
alongside the existing data dumps.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import logging
import os
import subprocess
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .backup_job import (
    DEFAULT_BUCKET_PREFIX,
    DEFAULT_NONCE_SIZE,
    MANIFEST_NAME,
    _normalise_bucket_prefix,
)

LOGGER = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency for runtime execution
    import boto3
except Exception:  # pragma: no cover - allow static analysis without boto3
    boto3 = None  # type: ignore

try:  # pragma: no cover - optional dependency for runtime execution
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except Exception:  # pragma: no cover - allow static analysis without cryptography
    AESGCM = None  # type: ignore


@dataclass
class FeastBackupConfig:
    """Configuration values for the Feast backup workflow."""

    feature_store_path: Path
    repo_path: Path
    redis_host: str
    redis_port: int
    redis_password: Optional[str]
    bucket_name: str
    bucket_prefix: str
    region_name: Optional[str]
    endpoint_url: Optional[str]
    access_key: Optional[str]
    secret_key: Optional[str]
    sse_algorithm: str
    kms_key_id: Optional[str]
    retention_days: Optional[int]
    encryption_key: bytes

    @classmethod
    def from_env(cls) -> "FeastBackupConfig":
        """Initialise configuration from environment variables."""

        feature_store_path = Path(
            os.environ.get("FEAST_FEATURE_STORE_PATH", "/etc/feast/feature_store.yaml")
        )
        repo_path = Path(os.environ.get("FEAST_REPO_PATH", feature_store_path.parent))
        redis_host = os.environ.get("FEAST_REDIS_HOST", "redis")
        redis_port = int(os.environ.get("FEAST_REDIS_PORT", "6379"))
        redis_password = os.environ.get("FEAST_REDIS_PASSWORD")

        bucket = os.environ.get("LINODE_BUCKET")
        if not bucket:
            raise RuntimeError("LINODE_BUCKET environment variable is required")

        encryption_key_b64 = os.environ.get("BACKUP_ENCRYPTION_KEY")
        if not encryption_key_b64:
            raise RuntimeError("BACKUP_ENCRYPTION_KEY environment variable is required")

        try:
            encryption_key = base64.b64decode(encryption_key_b64)
        except Exception as exc:  # pragma: no cover - validation only
            raise RuntimeError("Invalid BACKUP_ENCRYPTION_KEY encoding") from exc

        if len(encryption_key) not in {16, 24, 32}:
            raise RuntimeError("BACKUP_ENCRYPTION_KEY must be 128/192/256 bits")

        return cls(
            feature_store_path=feature_store_path,
            repo_path=repo_path,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_password=redis_password,
            bucket_name=bucket,
            bucket_prefix=_normalise_bucket_prefix(
                os.environ.get("LINODE_PREFIX", DEFAULT_BUCKET_PREFIX)
            ),
            region_name=os.environ.get("LINODE_REGION"),
            endpoint_url=os.environ.get("LINODE_ENDPOINT"),
            access_key=os.environ.get("LINODE_ACCESS_KEY"),
            secret_key=os.environ.get("LINODE_SECRET_KEY"),
            sse_algorithm=os.environ.get("LINODE_SSE_ALGO", "AES256"),
            kms_key_id=os.environ.get("LINODE_SSE_KMS_KEY"),
            retention_days=(
                int(os.environ["BACKUP_RETENTION_DAYS"])
                if os.environ.get("BACKUP_RETENTION_DAYS")
                else None
            ),
            encryption_key=encryption_key,
        )


@dataclass
class BackupArtifact:
    """Representation of a single encrypted artifact."""

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


class FeastBackupJob:
    """Orchestrates Feast registry and Redis backups."""

    def __init__(self, config: FeastBackupConfig) -> None:
        self.config = config
        self._s3_client = self._build_s3_client()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_backup(self) -> str:
        manifest_id = str(uuid.uuid4())
        timestamp = dt.datetime.utcnow().replace(microsecond=0).isoformat()
        backup_prefix = self._build_backup_prefix(timestamp, manifest_id)

        LOGGER.info("Starting Feast backup %s", manifest_id)
        artifacts: List[BackupArtifact] = []

        with tempfile.TemporaryDirectory(prefix="feast_backup_") as tmp_dir:
            temp_path = Path(tmp_dir)
            registry_dump = temp_path / "feast_registry.json"
            redis_snapshot = temp_path / "feast_redis.rdb"
            encrypted_dir = temp_path / "encrypted"
            encrypted_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = temp_path / MANIFEST_NAME

            self._dump_registry(registry_dump)
            artifacts.append(
                self._stage_artifact(
                    name="feast_registry",
                    source_path=registry_dump,
                    encrypted_path=encrypted_dir / "feast_registry.json.enc",
                    s3_key=f"{backup_prefix}/feast_registry.json.enc",
                    artifact_type="feast_registry",
                )
            )

            self._dump_redis(redis_snapshot)
            artifacts.append(
                self._stage_artifact(
                    name="feast_redis",
                    source_path=redis_snapshot,
                    encrypted_path=encrypted_dir / "feast_redis.rdb.enc",
                    s3_key=f"{backup_prefix}/feast_redis.rdb.enc",
                    artifact_type="redis_rdb",
                )
            )

            manifest = self._write_manifest(
                manifest_path=manifest_path,
                manifest_id=manifest_id,
                timestamp=timestamp,
                artifacts=artifacts,
            )

            for artifact in artifacts:
                self._upload_file(artifact.path, artifact.s3_key)

            manifest_key = f"{backup_prefix}/{MANIFEST_NAME}"
            self._upload_file(manifest_path, manifest_key)

        LOGGER.info("Feast backup %s completed", manifest_id)
        return manifest_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _build_s3_client(self):  # type: ignore[override]
        if boto3 is None:
            raise RuntimeError(
                "boto3 is required for Feast backups but is not installed"
            )

        session = boto3.session.Session(
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            region_name=self.config.region_name,
        )
        return session.client("s3", endpoint_url=self.config.endpoint_url)

    def _dump_registry(self, destination: Path) -> None:
        LOGGER.info("Dumping Feast registry to %s", destination)
        if not self.config.feature_store_path.exists():
            raise FileNotFoundError(
                f"Feature store config {self.config.feature_store_path} not found"
            )
        command = [
            "feast",
            "--chdir",
            str(self.config.repo_path),
            "--feature-store-yaml",
            str(self.config.feature_store_path),
            "registry-dump",
        ]
        result = self._run_subprocess(command, "feast registry-dump")
        destination.write_text(result.stdout, encoding="utf-8")

    def _dump_redis(self, destination: Path) -> None:
        LOGGER.info("Capturing Redis snapshot to %s", destination)
        command = [
            "redis-cli",
            "-h",
            self.config.redis_host,
            "-p",
            str(self.config.redis_port),
        ]
        if self.config.redis_password:
            command.extend(["-a", self.config.redis_password])
        command.extend(["--rdb", str(destination)])
        self._run_subprocess(command, "redis-cli --rdb")
        if not destination.exists():
            raise RuntimeError("redis-cli --rdb did not produce a snapshot")

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
                "feast_repo": str(self.config.repo_path),
                "redis_host": self.config.redis_host,
                "redis_port": self.config.redis_port,
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
        LOGGER.info(
            "Uploading %s to s3://%s/%s", path, self.config.bucket_name, key
        )
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

    def _encrypt_file(self, source_path: Path, destination: Path) -> str:
        if AESGCM is None:
            raise RuntimeError(
                "cryptography is required for Feast backup encryption but is not installed"
            )
        aesgcm = AESGCM(self.config.encryption_key)
        nonce = os.urandom(DEFAULT_NONCE_SIZE)
        with source_path.open("rb") as handle:
            plaintext = handle.read()
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        with destination.open("wb") as handle:
            handle.write(ciphertext)
        return base64.b64encode(nonce).decode("ascii")

    def _hash_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(8192), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _build_backup_prefix(self, timestamp: str, manifest_id: str) -> str:
        prefix = (self.config.bucket_prefix or "").strip("/")
        slug = f"{timestamp}_{manifest_id}"
        return f"{prefix}/{slug}" if prefix else slug

    def _run_subprocess(self, command: List[str], label: str):
        LOGGER.debug("Running %s", " ".join(command))
        try:
            completed = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
            )
            if completed.stderr:
                LOGGER.debug("%s stderr: %s", label, completed.stderr.strip())
            return completed
        except subprocess.CalledProcessError as exc:  # pragma: no cover - pass-through
            LOGGER.error("%s failed: %s", label, exc.stderr)
            raise


def run_cli() -> int:
    parser = argparse.ArgumentParser(description="Feast registry/Redis backup")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("backup", help="Execute the Feast backup workflow")

    args = parser.parse_args()
    if args.command != "backup":
        parser.print_help()
        return 1

    logging.basicConfig(
        level=os.environ.get("BACKUP_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    job = FeastBackupJob(FeastBackupConfig.from_env())
    job.run_backup()
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(run_cli())
