"""Test helpers for working with the risk service module."""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Callable, Iterator

import os
import sqlalchemy


MANAGED_RISK_DSN = "postgresql://risk:integration@timescaledb:5432/risk"


def _sqlite_engine_proxy(db_path: Path, real_create_engine):
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
            sqlite_kwargs.pop("pool_pre_ping", None)
            return real_create_engine(f"sqlite:///{db_path}", **sqlite_kwargs)
        return real_create_engine(url, **kwargs)

    return _create_engine


def patch_sqlalchemy_for_risk(db_path: Path) -> Callable[[], None]:
    """Patch SQLAlchemy globally so risk services use a SQLite backing store."""

    db_path.parent.mkdir(parents=True, exist_ok=True)
    real_create_engine = sqlalchemy.create_engine
    proxy = _sqlite_engine_proxy(db_path, real_create_engine)
    sqlalchemy.create_engine = proxy
    os.environ.setdefault("RISK_DATABASE_URL", MANAGED_RISK_DSN)
    os.environ.setdefault("ESG_DATABASE_URL", MANAGED_RISK_DSN)

    def restore() -> None:
        sqlalchemy.create_engine = real_create_engine

    return restore


def _configure_test_engine(monkeypatch, db_path: Path) -> None:
    """Patch SQLAlchemy's ``create_engine`` to back the risk service with SQLite."""

    db_path.parent.mkdir(parents=True, exist_ok=True)
    proxy = _sqlite_engine_proxy(db_path, sqlalchemy.create_engine)
    monkeypatch.setattr(sqlalchemy, "create_engine", proxy)
    monkeypatch.setenv("RISK_DATABASE_URL", MANAGED_RISK_DSN)
    monkeypatch.setenv("ESG_DATABASE_URL", MANAGED_RISK_DSN)


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
    monkeypatch.setattr(
        "shared.graceful_shutdown.install_sigterm_handler",
        lambda manager: None,
        raising=False,
    )
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


@contextmanager
def esg_filter_instance(
    tmp_path: Path,
    monkeypatch,
    *,
    db_filename: str = "risk.db",
) -> Iterator[ModuleType]:
    """Yield an ESG filter module backed by an isolated SQLite database."""

    db_path = tmp_path / db_filename
    _configure_test_engine(monkeypatch, db_path)
    sys.modules.pop("esg_filter", None)
    module = importlib.import_module("esg_filter")
    try:
        yield module
    finally:
        try:
            module.ENGINE.dispose()
        except Exception:  # pragma: no cover - defensive cleanup
            pass
        sys.modules.pop("esg_filter", None)


def reload_esg_filter(tmp_path: Path, monkeypatch, *, db_filename: str = "risk.db") -> ModuleType:
    """Return a freshly imported ESG filter module using the shared SQLite store."""

    db_path = tmp_path / db_filename
    _configure_test_engine(monkeypatch, db_path)
    sys.modules.pop("esg_filter", None)
    return importlib.import_module("esg_filter")
