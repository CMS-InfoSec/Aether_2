from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

import sentiment_ingest


def _without_pytest() -> dict[str, object | None]:
    removed = {}
    if "pytest" in sys.modules:
        removed["pytest"] = sys.modules.pop("pytest")
    return removed


def _restore_modules(state: dict[str, object | None]) -> None:
    for name, module in state.items():
        if module is not None:
            sys.modules[name] = module  # pragma: no cover - restoration guard


@pytest.fixture(autouse=True)
def _clear_sentiment_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SENTIMENT_DATABASE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DATABASE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SENTIMENT_ALLOW_SQLITE", raising=False)
    importlib.reload(sentiment_ingest)


def test_resolve_database_url_requires_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    removed = _without_pytest()
    try:
        with pytest.raises(RuntimeError, match="Sentiment repository database URL is not configured"):
            sentiment_ingest._resolve_database_url(None)
    finally:
        _restore_modules(removed)


def test_resolve_database_url_normalizes_timescale(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "SENTIMENT_DATABASE_URL",
        "timescale://user:pass@example.com:5432/sentiment",
    )
    removed = _without_pytest()
    try:
        url = sentiment_ingest._resolve_database_url(None)
    finally:
        _restore_modules(removed)

    assert url.startswith("postgresql+")
    assert "timescale" not in url


def test_resolve_database_url_rejects_sqlite_without_flag(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(
        "SENTIMENT_DATABASE_URL", f"sqlite:///{tmp_path / 'sentiment.db'}"
    )
    removed = _without_pytest()
    try:
        with pytest.raises(RuntimeError, match="Sentiment repository database URL"):
            sentiment_ingest._resolve_database_url(None)
    finally:
        _restore_modules(removed)


def test_resolve_database_url_allows_sqlite_with_flag(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / "nested" / "sentiment.db"
    monkeypatch.setenv("SENTIMENT_DATABASE_URL", f"sqlite:///{target}")
    monkeypatch.setenv("SENTIMENT_ALLOW_SQLITE", "1")

    removed = _without_pytest()
    try:
        url = sentiment_ingest._resolve_database_url(None)
    finally:
        _restore_modules(removed)

    assert url.startswith("sqlite")
    assert target.parent.exists()
