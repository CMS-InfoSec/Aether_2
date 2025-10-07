"""Tests for the logging retention archiver configuration guards."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from ops.logging.retention_archiver import RetentionArchiver, RetentionConfig


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


def _prime_directory_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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


def test_from_env_normalises_prefix(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_required_env(monkeypatch)
    _prime_directory_env(monkeypatch, tmp_path)
    monkeypatch.setenv("RETENTION_PREFIX", " logs//2024 / ")

    config = RetentionConfig.from_env()

    assert config.prefix == "logs/2024"


def test_from_env_rejects_prefix_traversal(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_required_env(monkeypatch)
    _prime_directory_env(monkeypatch, tmp_path)
    monkeypatch.setenv("RETENTION_PREFIX", "../escape")

    with pytest.raises(ValueError, match="path traversal"):
        RetentionConfig.from_env()


def test_from_env_rejects_prefix_control_chars(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_required_env(monkeypatch)
    _prime_directory_env(monkeypatch, tmp_path)
    monkeypatch.setenv("RETENTION_PREFIX", "bad\x01prefix")

    with pytest.raises(ValueError, match="control characters"):
        RetentionConfig.from_env()


def _make_archiver() -> RetentionArchiver:
    archiver = RetentionArchiver.__new__(RetentionArchiver)
    archiver.config = None  # type: ignore[assignment]
    archiver.status = None  # type: ignore[assignment]
    return archiver


def test_select_files_skips_symlink_files(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    archiver = _make_archiver()
    root = tmp_path / "logs"
    root.mkdir()
    allowed = root / "inside.log"
    allowed.write_text("ok")

    outside = tmp_path / "outside.log"
    outside.write_text("secret")
    symlink = root / "link.log"
    symlink.symlink_to(outside)

    cutoff = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)

    caplog.set_level("WARNING")

    files = list(archiver._select_files(root, cutoff))

    assert allowed in files
    assert symlink not in files
    assert any("symlinked file" in record.message for record in caplog.records)


def test_select_files_ignores_symlink_directories(tmp_path: Path) -> None:
    archiver = _make_archiver()
    root = tmp_path / "logs"
    root.mkdir()
    allowed = root / "inside.log"
    allowed.write_text("ok")

    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    (outside_dir / "secret.log").write_text("secret")
    (root / "linkdir").symlink_to(outside_dir, target_is_directory=True)

    cutoff = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)

    files = list(archiver._select_files(root, cutoff))

    assert allowed in files
    assert all(path.parent.name != "linkdir" for path in files)
