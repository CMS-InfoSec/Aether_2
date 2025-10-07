"""Regression coverage for the DR playbook restore hardening."""
from __future__ import annotations

import io
import os
import tarfile
from pathlib import Path

import pytest

from dr_playbook import DisasterRecoveryConfig, _restore_mlflow_artifacts


def _config_for(path: Path) -> DisasterRecoveryConfig:
    return DisasterRecoveryConfig(
        timescale_dsn="postgresql://localhost/postgres",
        redis_url="redis://localhost:6379/0",
        mlflow_artifact_uri="s3://dummy/artifacts",
        object_store_bucket="dummy",
        mlflow_restore_path=path,
    )


def _write_archive(path: Path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for name, payload in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


def test_restore_mlflow_artifacts_rejects_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "malicious.tar.gz"
    _write_archive(archive, {"../escape.txt": b"bad"})

    config = _config_for(tmp_path / "restore")

    with pytest.raises(ValueError, match="traversal"):
        _restore_mlflow_artifacts(config, archive)

    assert not any(tmp_path.rglob("escape.txt"))


def test_restore_mlflow_artifacts_rejects_symlink_base(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    os.symlink(target, link)

    archive = tmp_path / "safe.tar.gz"
    _write_archive(archive, {"artifact/file.txt": b"data"})

    config = _config_for(link)

    with pytest.raises(ValueError, match="symlink"):
        _restore_mlflow_artifacts(config, archive)

    assert not any(target.rglob("file.txt"))
