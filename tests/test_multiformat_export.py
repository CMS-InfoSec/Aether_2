"""Tests for the multi-format log export service configuration guards."""

from __future__ import annotations

import datetime as dt
import io
from types import SimpleNamespace

import pytest

from multiformat_export import (
    LogExporter,
    MissingDependencyError,
    StorageConfig,
    _storage_config_from_env,
)


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


def test_markdown_rendering_requires_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    exporter = LogExporter(config=StorageConfig(bucket="bucket"))
    monkeypatch.setattr("multiformat_export.markdown2", None, raising=False)

    with pytest.raises(MissingDependencyError, match="markdown2 is required"):
        exporter._render_markdown_bytes({}, dt.date(2024, 1, 1), "run")


def test_pdf_rendering_uses_reportlab_components(monkeypatch: pytest.MonkeyPatch) -> None:
    exporter = LogExporter(config=StorageConfig(bucket="bucket"))

    class DummyTemplate:
        def __init__(self, buffer: io.BytesIO, pagesize: object) -> None:
            self.buffer = buffer

        def build(self, story: list[object]) -> None:
            self.buffer.write(b"done")

    def dummy_paragraph(*args: object, **kwargs: object) -> str:
        return "paragraph"

    def dummy_spacer(*args: object, **kwargs: object) -> str:
        return "spacer"

    def dummy_table(data: list[list[object]], repeatRows: int = 0) -> SimpleNamespace:
        return SimpleNamespace(setStyle=lambda _: None)

    dummy_colors = SimpleNamespace(lightgrey="lightgrey", black="black", grey="grey")

    def dummy_stylesheet() -> dict[str, str]:
        return {"Title": "title", "Normal": "normal", "Heading2": "heading", "Italic": "italic"}

    monkeypatch.setattr("multiformat_export._require_reportlab", lambda: (
        DummyTemplate,
        dummy_paragraph,
        dummy_spacer,
        dummy_table,
        lambda _: None,
        dummy_colors,
        (0, 0),
        dummy_stylesheet,
    ))

    result = exporter._render_pdf_bytes({"logs": [{"field": "value"}]}, dt.date(2024, 1, 1), "run")

    assert result == b"done"
