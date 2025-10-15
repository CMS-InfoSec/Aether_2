"""Static regression tests for the TCA service optional dependency guards."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from services.common import sqlalchemy_stub

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_account_scope_guard_defined_before_sqlalchemy_branch() -> None:
    """Ensure the account-scope guard is declared before the SQLAlchemy branch."""

    source = (PROJECT_ROOT / "tca_service.py").read_text().splitlines()

    assignment_line = next(
        idx for idx, line in enumerate(source) if "_ACCOUNT_SCOPE_AVAILABLE =" in line
    )
    conditional_line = next(
        idx for idx, line in enumerate(source) if line.strip().startswith("if _SQLALCHEMY_AVAILABLE:")
    )

    assert assignment_line < conditional_line


def test_sqlalchemy_stub_keeps_service_importable(monkeypatch: pytest.MonkeyPatch) -> None:
    """The SQLAlchemy shim should mark the service as available for fallback paths."""

    original_sqlalchemy = sys.modules.pop("sqlalchemy", None)
    original_account_scope = sys.modules.pop("shared.account_scope", None)
    original_module = sys.modules.pop("tca_service", None)

    try:
        stub = sqlalchemy_stub.install()
        monkeypatch.setitem(sys.modules, "sqlalchemy", stub)

        module = importlib.import_module("tca_service")

        assert module._SQLALCHEMY_AVAILABLE is True

        column = module.account_id_column(primary_key=True)
        assert getattr(column, "primary_key", False) is True
    finally:
        for name, value in (
            ("tca_service", original_module),
            ("shared.account_scope", original_account_scope),
            ("sqlalchemy", original_sqlalchemy),
        ):
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value
