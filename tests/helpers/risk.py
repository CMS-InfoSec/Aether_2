"""Test helpers for working with the risk service module."""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Iterator

import sqlalchemy


MANAGED_RISK_DSN = "postgresql://risk:integration@timescaledb:5432/risk"


def _configure_test_engine(monkeypatch, db_path: Path) -> None:
    """Patch SQLAlchemy's ``create_engine`` to back the risk service with SQLite."""

    real_create_engine = sqlalchemy.create_engine

    def _create_engine(url: str, **kwargs):  # type: ignore[override]
        if url.startswith("postgresql"):
            sqlite_kwargs = dict(kwargs)
            connect_args = dict(sqlite_kwargs.pop("connect_args", {}) or {})
            connect_args.pop("sslmode", None)
            connect_args.setdefault("check_same_thread", False)
            sqlite_kwargs["connect_args"] = connect_args
            sqlite_kwargs.pop("pool_size", None)
            sqlite_kwargs.pop("max_overflow", None)
            sqlite_kwargs.pop("pool_timeout", None)
            sqlite_kwargs.pop("pool_recycle", None)
            return real_create_engine(f"sqlite:///{db_path}", **sqlite_kwargs)
        return real_create_engine(url, **kwargs)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(sqlalchemy, "create_engine", _create_engine)
    monkeypatch.setenv("RISK_DATABASE_URL", MANAGED_RISK_DSN)


@contextmanager
def risk_service_instance(
    tmp_path: Path,
    monkeypatch,
    *,
    db_filename: str = "risk.db",
) -> Iterator[ModuleType]:
    """Yield a risk service module backed by an isolated SQLite database."""

    db_path = tmp_path / db_filename
    _configure_test_engine(monkeypatch, db_path)
    sys.modules.pop("risk_service", None)
    module = importlib.import_module("risk_service")
    try:
        yield module
    finally:
        try:
            module.ENGINE.dispose()
        except Exception:  # pragma: no cover - defensive cleanup
            pass
        sys.modules.pop("risk_service", None)


def reload_risk_service(tmp_path: Path, monkeypatch, *, db_filename: str = "risk.db") -> ModuleType:
    """Return a freshly imported risk service module using the shared SQLite store."""

    db_path = tmp_path / db_filename
    _configure_test_engine(monkeypatch, db_path)
    sys.modules.pop("risk_service", None)
    return importlib.import_module("risk_service")
