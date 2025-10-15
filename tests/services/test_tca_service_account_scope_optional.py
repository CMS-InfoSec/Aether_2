"""Static regression tests for the TCA service optional dependency guards."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_account_scope_guard_defined_before_sqlalchemy_branch() -> None:
    """Ensure the account-scope guard is declared before the SQLAlchemy branch."""

    source = (PROJECT_ROOT / "tca_service.py").read_text().splitlines()

    assignment_line = next(
        idx for idx, line in enumerate(source) if "_ACCOUNT_SCOPE_AVAILABLE =" in line
    )
    conditional_line = next(
        idx
        for idx, line in enumerate(source)
        if "if _SQLALCHEMY_AVAILABLE and _ACCOUNT_SCOPE_AVAILABLE" in line
    )

    assert assignment_line < conditional_line
