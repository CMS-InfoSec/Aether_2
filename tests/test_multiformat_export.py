"""Tests for the multi-format log export service configuration guards."""

from __future__ import annotations

import pytest

pytest.importorskip("pandas", reason="pandas is required for multiformat export module tests")
pytest.importorskip("markdown2", reason="markdown2 is required for multiformat export module tests")
pytest.importorskip("reportlab", reason="reportlab is required for multiformat export module tests")

from multiformat_export import StorageConfig, _storage_config_from_env


def _clear_prefix_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MULTIFORMAT_EXPORT_PREFIX", raising=False)
    monkeypatch.delenv("EXPORT_PREFIX", raising=False)


def test_storage_config_normalises_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_prefix_env(monkeypatch)
    monkeypatch.setenv("MULTIFORMAT_EXPORT_BUCKET", "aether-logs")
    monkeypatch.setenv("MULTIFORMAT_EXPORT_PREFIX", " logs//exports ")

    config = _storage_config_from_env()

    assert config.prefix == "logs/exports"


def test_storage_config_rejects_traversal_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_prefix_env(monkeypatch)
    monkeypatch.setenv("MULTIFORMAT_EXPORT_BUCKET", "aether-logs")
    monkeypatch.setenv("MULTIFORMAT_EXPORT_PREFIX", "../secrets")

    with pytest.raises(ValueError, match="must not contain path traversal"):
        _storage_config_from_env()


def test_storage_config_rejects_control_characters(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_prefix_env(monkeypatch)
    monkeypatch.setenv("MULTIFORMAT_EXPORT_BUCKET", "aether-logs")
    # Embed a newline which should be rejected by the sanitiser.
    monkeypatch.setenv("MULTIFORMAT_EXPORT_PREFIX", "exports\nlogs")

    with pytest.raises(ValueError, match="control characters"):
        _storage_config_from_env()


def test_storage_config_direct_instantiation_sanitises_prefix() -> None:
    config = StorageConfig(bucket="bucket", prefix=" nested /prefix ")

    assert config.prefix == "nested/prefix"
