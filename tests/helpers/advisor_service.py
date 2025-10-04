"""Test helpers for configuring the advisor service."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType
from typing import Final

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_DEFAULT_TEST_DSN: Final[str] = "postgresql://advisor.test/advisor"


def bootstrap_advisor_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    reset: bool = False,
    db_filename: str = "advisor.db",
    dsn: str | None = None,
) -> ModuleType:
    """Import ``advisor_service`` backed by a persistent SQLite database for tests."""

    database_url = dsn or _DEFAULT_TEST_DSN
    monkeypatch.setenv("ADVISOR_DATABASE_URL", database_url)
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://advisor-tests")

    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        monkeypatch.syspath_prepend(str(repo_root))

    for name in ("services.common.security", "services.common", "services"):
        sys.modules.pop(name, None)
    sys.modules.pop("advisor_service", None)
    module = importlib.import_module("advisor_service")

    db_path = tmp_path / db_filename
    engine = create_engine(
        f"sqlite:///{db_path}",
        future=True,
        connect_args={"check_same_thread": False},
    )
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)

    module.ENGINE = engine  # type: ignore[attr-defined]
    module.SessionLocal = Session  # type: ignore[attr-defined]

    if reset:
        module.Base.metadata.drop_all(bind=engine)  # type: ignore[attr-defined]
    module.Base.metadata.create_all(bind=engine)  # type: ignore[attr-defined]

    return module

