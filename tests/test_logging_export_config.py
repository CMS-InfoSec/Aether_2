"""Tests for log export configuration sanitisation."""

from __future__ import annotations

import datetime as dt

import pytest

from logging_export import ExportConfig, _build_s3_key


def test_export_config_sanitises_prefix() -> None:
    config = ExportConfig(bucket="a-bucket", prefix=" /logs//daily/ ")
    assert config.prefix == "logs/daily"


def test_export_config_allows_empty_prefix() -> None:
    config = ExportConfig(bucket="a-bucket", prefix="   ")
    key = _build_s3_key(config.prefix, dt.date(2024, 1, 1))
    assert config.prefix == ""
    assert key == "audit-reg-log-2024-01-01.json.gz"


@pytest.mark.parametrize("prefix", ["../escape", "logs/..", "..", ".", "logs/./nested"])
def test_export_config_rejects_traversal(prefix: str) -> None:
    with pytest.raises(ValueError, match="path traversal"):
        ExportConfig(bucket="bucket", prefix=prefix)


def test_export_config_rejects_control_characters() -> None:
    with pytest.raises(ValueError, match="control characters"):
        ExportConfig(bucket="bucket", prefix="logs/\x00bad")


def test_build_s3_key_with_prefix() -> None:
    key = _build_s3_key("logs/daily", dt.date(2024, 2, 29))
    assert key == "logs/daily/audit-reg-log-2024-02-29.json.gz"
