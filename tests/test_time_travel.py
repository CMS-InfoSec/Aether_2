from datetime import datetime, timezone

import pytest

import time_travel


def test_resolve_artifact_dirs_normalises_relative_paths(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "logs"
    target.mkdir()

    monkeypatch.setenv("AETHER_EVENT_LOG_DIRS", "logs")

    result = time_travel._resolve_artifact_dirs("AETHER_EVENT_LOG_DIRS", ())
    assert result == [target.absolute()]


def test_resolve_artifact_dirs_rejects_traversal(monkeypatch):
    monkeypatch.setenv("AETHER_EVENT_LOG_DIRS", "../etc")
    with pytest.raises(ValueError):
        time_travel._resolve_artifact_dirs("AETHER_EVENT_LOG_DIRS", ())


def test_resolve_artifact_dirs_rejects_symlink(tmp_path, monkeypatch):
    real_dir = tmp_path / "data"
    real_dir.mkdir()
    symlink = tmp_path / "link"
    symlink.symlink_to(real_dir)

    monkeypatch.setenv("AETHER_EVENT_LOG_DIRS", str(symlink))

    with pytest.raises(ValueError):
        time_travel._resolve_artifact_dirs("AETHER_EVENT_LOG_DIRS", ())


def test_resolve_artifact_dirs_rejects_files(tmp_path, monkeypatch):
    file_path = tmp_path / "file.txt"
    file_path.write_text("hello")

    monkeypatch.setenv("AETHER_EVENT_LOG_DIRS", str(file_path))

    with pytest.raises(ValueError):
        time_travel._resolve_artifact_dirs("AETHER_EVENT_LOG_DIRS", ())


def test_reconstruct_state_raises_for_invalid_env(monkeypatch):
    monkeypatch.setenv("AETHER_EVENT_LOG_DIRS", "../etc")

    with pytest.raises(ValueError):
        time_travel.reconstruct_state(datetime.now(timezone.utc))


def test_time_travel_endpoint_returns_400_for_invalid_dirs(monkeypatch):
    monkeypatch.setenv("AETHER_EVENT_LOG_DIRS", "../etc")

    with pytest.raises(time_travel.HTTPException) as exc:
        time_travel.time_travel_endpoint(ts="2025-01-01T00:00:00Z")

    assert exc.value.status_code == 400
    assert "AETHER_EVENT_LOG_DIRS" in exc.value.detail


def test_sanitise_directory_rejects_control_chars():
    with pytest.raises(ValueError):
        time_travel._sanitise_directory("logs\x00", "AETHER_EVENT_LOG_DIRS")
