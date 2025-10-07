"""Regression tests for the backup job security hardening."""

from __future__ import annotations

import io
import os
import tarfile
from pathlib import Path

import pytest

from ops.backup.backup_job import (
    BackupConfig,
    BackupJob,
    _safe_extract_tar,
)


def _build_archive(path: Path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for name, payload in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


class _StubBackupJob(BackupJob):
    """Test double that bypasses boto3 initialisation."""

    def _build_s3_client(self):  # type: ignore[override]
        class _DummyClient:
            pass

        return _DummyClient()


def _make_config(artifact_dir: Path) -> BackupConfig:
    return BackupConfig(
        pg_dsn="postgresql://localhost/postgres",
        mlflow_artifact_dir=artifact_dir,
        bucket_name="dummy",
        encryption_key=b"\x00" * 16,
    )


def test_safe_extract_tar_rejects_traversal(tmp_path: Path) -> None:
    archive_path = tmp_path / "malicious.tar.gz"
    _build_archive(archive_path, {"../evil.txt": b"malicious"})
    destination = tmp_path / "dest"
    destination.mkdir()

    with tarfile.open(archive_path, "r:gz") as archive:
        with pytest.raises(ValueError, match="traversal"):
            _safe_extract_tar(archive, destination)

    assert not any(destination.rglob("evil.txt"))


def test_safe_extract_tar_rejects_symlinks(tmp_path: Path) -> None:
    archive_path = tmp_path / "symlink.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        info = tarfile.TarInfo(name="mlruns/link")
        info.type = tarfile.SYMTYPE
        info.linkname = "../evil.txt"
        archive.addfile(info)

    destination = tmp_path / "dest"
    destination.mkdir()

    with tarfile.open(archive_path, "r:gz") as archive:
        with pytest.raises(ValueError, match="regular files or directories"):
            _safe_extract_tar(archive, destination)

    assert not any(destination.rglob("link"))


def test_safe_extract_tar_writes_expected_payload(tmp_path: Path) -> None:
    archive_path = tmp_path / "safe.tar.gz"
    _build_archive(archive_path, {"mlruns/metrics.json": b"{}"})
    destination = tmp_path / "dest"
    destination.mkdir()

    with tarfile.open(archive_path, "r:gz") as archive:
        _safe_extract_tar(archive, destination)

    extracted = destination / "mlruns" / "metrics.json"
    assert extracted.read_bytes() == b"{}"


def test_restore_mlflow_artifacts_rejects_symlink_destination(tmp_path: Path) -> None:
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    link = tmp_path / "link"
    os.symlink(real_dir, link)

    archive_path = tmp_path / "safe.tar.gz"
    _build_archive(archive_path, {"link/file.txt": b"data"})

    config = _make_config(link)
    job = _StubBackupJob(config)

    with pytest.raises(ValueError, match="symlink"):
        job._restore_mlflow_artifacts(archive_path)


def test_restore_mlflow_artifacts_blocks_traversal(tmp_path: Path) -> None:
    archive_path = tmp_path / "malicious.tar.gz"
    _build_archive(archive_path, {"../evil.txt": b"malicious"})

    target_dir = tmp_path / "mlruns"
    config = _make_config(target_dir)
    job = _StubBackupJob(config)

    with pytest.raises(ValueError):
        job._restore_mlflow_artifacts(archive_path)

    assert not any(tmp_path.rglob("evil.txt"))


def test_restore_mlflow_artifacts_extracts_payload(tmp_path: Path) -> None:
    archive_path = tmp_path / "safe.tar.gz"
    _build_archive(archive_path, {"mlruns/metrics.json": b"{}"})

    target_dir = tmp_path / "mlruns"
    config = _make_config(target_dir)
    job = _StubBackupJob(config)

    job._restore_mlflow_artifacts(archive_path)

    extracted = target_dir / "metrics.json"
    assert extracted.read_bytes() == b"{}"
