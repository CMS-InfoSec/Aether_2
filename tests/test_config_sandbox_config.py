"""Regression tests for the config sandbox database configuration guards."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MODULE_PATH = ROOT / "config_sandbox.py"


def _load_module(module_name: str) -> object:
    """Import ``config_sandbox`` under *module_name* and return the module."""

    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ModuleNotFoundError(module_name)

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def _dispose_module(module_name: str) -> None:
    module = sys.modules.pop(module_name, None)
    engine = getattr(module, "ENGINE", None)
    if engine is not None and hasattr(engine, "dispose"):
        engine.dispose()


def test_config_sandbox_requires_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONFIG_SANDBOX_DATABASE_URL", raising=False)

    module_name = "tests.config_sandbox_missing_dsn"
    with pytest.raises(RuntimeError, match="Config sandbox database URL must be provided"):
        _load_module(module_name)


def test_config_sandbox_normalizes_timescale_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONFIG_SANDBOX_DATABASE_URL", "timescale://user:pass@example.com/sandbox")

    captured: dict[str, object] = {}

    from sqlalchemy import create_engine as real_create_engine
    from sqlalchemy.pool import StaticPool

    def _fake_create_engine(url: str, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return real_create_engine(
            "sqlite:///:memory:",
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )

    monkeypatch.setattr("sqlalchemy.create_engine", _fake_create_engine)

    module_name = "tests.config_sandbox_timescale"
    module = _load_module(module_name)
    try:
        assert str(captured["url"]).startswith("postgresql+psycopg2://")
    finally:
        _dispose_module(module_name)


def test_config_sandbox_rejects_sqlite_outside_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONFIG_SANDBOX_DATABASE_URL", "sqlite:///./sandbox.db")
    monkeypatch.delenv("CONFIG_SANDBOX_ALLOW_SQLITE", raising=False)
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    module_name = "tests.config_sandbox_sqlite"
    with pytest.raises(RuntimeError, match="Timescale compatible scheme"):
        _load_module(module_name)


def test_config_sandbox_rejects_blank_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONFIG_SANDBOX_DATABASE_URL", "   ")

    module_name = "tests.config_sandbox_blank_dsn"
    with pytest.raises(RuntimeError, match="must be provided"):
        _load_module(module_name)

