"""Test helpers for configuring the advisor service."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType

import pytest


def bootstrap_advisor_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    reset: bool = False,
    db_filename: str = "advisor.db",
    dsn: str | None = None,
) -> ModuleType:
    """Import ``advisor_service`` backed by a persistent SQLite database for tests."""

    db_path = tmp_path / db_filename

    if reset and db_path.exists():
        db_path.unlink()

    database_url = dsn or f"sqlite:///{db_path}"
    monkeypatch.setenv("ADVISOR_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("ADVISOR_DATABASE_URL", database_url)
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://advisor-tests")

    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        monkeypatch.syspath_prepend(str(repo_root))

    for name in ("services.common.security", "services.common", "services"):
        sys.modules.pop(name, None)
    sys.modules.pop("advisor_service", None)
    module = importlib.import_module("advisor_service")

    module._init_database(module.app)  # type: ignore[attr-defined]

    if reset:
        module.Base.metadata.drop_all(bind=module.ENGINE)  # type: ignore[attr-defined]

    module.Base.metadata.create_all(bind=module.ENGINE)  # type: ignore[attr-defined]

    return module

