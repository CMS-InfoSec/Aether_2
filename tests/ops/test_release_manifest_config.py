"""Configuration guards for :mod:`ops.releases.release_manifest`."""

from __future__ import annotations

import importlib
import sys

import pytest


MODULE_NAME = "ops.releases.release_manifest"


@pytest.fixture(name="release_manifest_module")
def fixture_release_manifest_module(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("RELEASE_MANIFEST_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("RELEASE_MANIFEST_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("CONFIG_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("CONFIG_DATABASE_URL", "sqlite+pysqlite:///:memory:")

    module = importlib.import_module(MODULE_NAME)
    monkeypatch.setitem(sys.modules, MODULE_NAME, module)
    return module


def test_release_manifest_requires_explicit_database_url(
    release_manifest_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key in ("RELEASE_MANIFEST_DATABASE_URL", "RELEASE_DATABASE_URL"):
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(RuntimeError, match="RELEASE_MANIFEST_DATABASE_URL"):
        release_manifest_module._resolve_release_db_url()


def test_release_manifest_normalises_timescale_urls(
    release_manifest_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "RELEASE_MANIFEST_DATABASE_URL",
        "timescale://admin:secret@localhost:5432/releases",
    )

    resolved = release_manifest_module._resolve_release_db_url()
    assert resolved.startswith("postgresql+psycopg2://")


def test_release_manifest_rejects_sqlite_without_flag(
    release_manifest_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RELEASE_MANIFEST_ALLOW_SQLITE_FOR_TESTS", raising=False)
    monkeypatch.setenv("RELEASE_MANIFEST_DATABASE_URL", "sqlite:///tmp/releases.db")

    with pytest.raises(RuntimeError, match="PostgreSQL/Timescale"):
        release_manifest_module._resolve_release_db_url()


def test_release_manifest_allows_sqlite_when_flag_enabled(
    release_manifest_module, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RELEASE_MANIFEST_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("RELEASE_MANIFEST_DATABASE_URL", "sqlite:///tmp/releases.db")

    resolved = release_manifest_module._resolve_release_db_url()
    assert resolved.startswith("sqlite://")
