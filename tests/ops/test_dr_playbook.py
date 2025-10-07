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


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DR_TIMESCALE_DSN", "postgresql://localhost/postgres")
    monkeypatch.setenv("DR_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DR_MLFLOW_ARTIFACT_URI", "s3://dummy/artifacts")
    monkeypatch.setenv("DR_OBJECT_STORE_BUCKET", "dummy")


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


def test_from_env_resolves_relative_work_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DR_WORK_DIR", "relative/work")

    config = DisasterRecoveryConfig.from_env()

    expected = (tmp_path / "relative" / "work" / "aether-dr").resolve()
    assert config.work_dir == expected
    assert config.work_dir.is_dir()


def test_from_env_rejects_symlink_work_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_required_env(monkeypatch)
    base = tmp_path / "base"
    base.mkdir()
    link = tmp_path / "link"
    os.symlink(base, link)
    monkeypatch.setenv("DR_WORK_DIR", str(link))

    with pytest.raises(ValueError, match="DR_WORK_DIR must not reference symlinked directories"):
        DisasterRecoveryConfig.from_env()


def test_from_env_rejects_file_work_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_required_env(monkeypatch)
    file_path = tmp_path / "work-file"
    file_path.write_text("not a directory")
    monkeypatch.setenv("DR_WORK_DIR", str(file_path))

    with pytest.raises(ValueError, match="DR_WORK_DIR must reference a directory"):
        DisasterRecoveryConfig.from_env()
