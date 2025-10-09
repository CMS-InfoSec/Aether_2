"""Regression tests for optional dependency handling in postmortem module."""

from __future__ import annotations

import builtins
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, Mapping

import pytest

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "postmortem.py"


class _StubCursor:
    def __enter__(self) -> "_StubCursor":  # pragma: no cover - trivial
        return self

    def __exit__(self, *exc: object) -> None:  # pragma: no cover - trivial
        return None

    def execute(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - trivial
        return None

    def fetchone(self) -> dict[str, str]:  # pragma: no cover - trivial
        return {"account_id": "demo-account", "admin_slug": "demo"}

    def fetchall(self) -> list[dict[str, str]]:  # pragma: no cover - trivial
        return []


class _StubConnection:
    def cursor(self) -> _StubCursor:  # pragma: no cover - trivial
        return _StubCursor()

    def close(self) -> None:  # pragma: no cover - trivial
        return None


def _install_psycopg_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_psycopg = ModuleType("psycopg")

    def _connect(*args: Any, **kwargs: Any) -> _StubConnection:  # pragma: no cover - trivial
        return _StubConnection()

    stub_psycopg.connect = _connect  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg", stub_psycopg)

    rows_module = ModuleType("psycopg.rows")
    rows_module.dict_row = lambda *args, **kwargs: kwargs  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg.rows", rows_module)


def _load_postmortem_module(
    module_name: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    missing: Iterable[str],
    available: Mapping[str, ModuleType] | None = None,
) -> ModuleType:
    """Load the postmortem module under a controlled import environment."""

    for name in (module_name, "postmortem"):
        monkeypatch.delitem(sys.modules, name, raising=False)

    _install_psycopg_stub(monkeypatch)

    for name, module in (available or {}).items():
        monkeypatch.setitem(sys.modules, name, module)

    original_import = builtins.__import__
    missing_set = frozenset(missing)

    def _guarded_import(name: str, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        if name in missing_set:
            raise ModuleNotFoundError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ModuleNotFoundError(module_name)

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def _dispose_module(module_name: str) -> None:
    sys.modules.pop(module_name, None)


def test_postmortem_requires_pandas(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module_name = "postmortem_missing_pandas"
    module = _load_postmortem_module(module_name, monkeypatch, missing={"pandas"})
    analyzer = module.PostmortemAnalyzer(
        account_identifier="demo",
        hours=4,
        output_dir=tmp_path,
    )
    try:
        with pytest.raises(module.MissingDependencyError, match="pandas"):
            analyzer.run()
    finally:
        analyzer.close()
        _dispose_module(module_name)


def test_postmortem_requires_numpy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module_name = "postmortem_missing_numpy"
    pandas_stub = ModuleType("pandas")
    module = _load_postmortem_module(
        module_name,
        monkeypatch,
        missing={"numpy"},
        available={"pandas": pandas_stub},
    )
    analyzer = module.PostmortemAnalyzer(
        account_identifier="demo",
        hours=4,
        output_dir=tmp_path,
    )
    try:
        with pytest.raises(module.MissingDependencyError, match="numpy"):
            analyzer.run()
    finally:
        analyzer.close()
        _dispose_module(module_name)
