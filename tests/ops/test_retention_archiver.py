"""Tests for the logging retention archiver configuration guards."""

from __future__ import annotations

from pathlib import Path

import pytest

from ops.logging.retention_archiver import RetentionConfig


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROMETHEUS_URL", "https://prometheus.internal")
    monkeypatch.setenv("RETENTION_BUCKET", "ops-retention")
    monkeypatch.setenv("METRICS_RETENTION_DAYS", "30")
    monkeypatch.setenv("LOG_RETENTION_DAYS", "14")


def test_from_env_accepts_absolute_directories(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_required_env(monkeypatch)
    snapshot_dir = tmp_path / "snapshots"
    audit_dir = tmp_path / "audit"
    oms_dir = tmp_path / "oms"
    tca_dir = tmp_path / "tca"

    for directory in (snapshot_dir, audit_dir, oms_dir, tca_dir):
        directory.mkdir()

    monkeypatch.setenv("PROMETHEUS_SNAPSHOT_DIR", str(snapshot_dir))
    monkeypatch.setenv("AUDIT_LOG_DIR", str(audit_dir))
    monkeypatch.setenv("OMS_LOG_DIR", str(oms_dir))
    monkeypatch.setenv("TCA_REPORT_DIR", str(tca_dir))

    config = RetentionConfig.from_env()

    assert config.prometheus_snapshot_dir == snapshot_dir
    assert config.audit_log_dir == audit_dir
    assert config.oms_log_dir == oms_dir
    assert config.tca_report_dir == tca_dir


def test_from_env_rejects_relative_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("AUDIT_LOG_DIR", "relative/path")

    with pytest.raises(ValueError, match="absolute path"):
        RetentionConfig.from_env()


def test_from_env_rejects_parent_directory_reference(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_required_env(monkeypatch)
    escape_path = tmp_path / ".." / "escape"
    monkeypatch.setenv("OMS_LOG_DIR", str(escape_path))

    with pytest.raises(ValueError, match="parent directory references"):
        RetentionConfig.from_env()


def test_from_env_rejects_symlink_ancestor(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_required_env(monkeypatch)
    target_dir = tmp_path / "real"
    target_dir.mkdir()
    symlink_dir = tmp_path / "link"
    symlink_dir.symlink_to(target_dir)
    monkeypatch.setenv("TCA_REPORT_DIR", str(symlink_dir / "reports"))

    with pytest.raises(ValueError, match="symlinked directories"):
        RetentionConfig.from_env()
