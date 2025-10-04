"""Test helpers for the benchmark service."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType
from typing import Final

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_DEFAULT_TEST_DSN: Final[str] = "postgresql://benchmark.test/benchmark"


def bootstrap_benchmark_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    reset: bool = False,
    db_filename: str = "benchmark.db",
    dsn: str | None = None,
) -> ModuleType:
    """Import ``benchmark_service`` with a persistent SQLite database for tests."""

    database_url = dsn or _DEFAULT_TEST_DSN
    monkeypatch.setenv("BENCHMARK_DATABASE_URL", database_url)
    # Ensure optional fallbacks do not interfere with the configured DSN.
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    repo_root = Path(__file__).resolve().parents[2]
    repo_path = str(repo_root)
    if repo_path not in sys.path:
        sys.path.insert(0, repo_path)
    sys.modules.pop("services", None)
    sys.modules.pop("services.common", None)

    sys.modules.pop("benchmark_service", None)
    module = importlib.import_module("benchmark_service")

    db_path = tmp_path / db_filename
    engine = create_engine(
        f"sqlite:///{db_path}",
        future=True,
        connect_args={"check_same_thread": False},
    )
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    module.ENGINE = engine  # type: ignore[attr-defined]
    module.SessionLocal = Session  # type: ignore[attr-defined]
    module.app.state.db_sessionmaker = Session  # type: ignore[attr-defined]

    try:
        from fastapi import HTTPException, Request, status
    except Exception:  # pragma: no cover - FastAPI is optional in some environments
        pass
    else:
        allowed_admin = "company"

        def _test_require_admin_account(request: Request) -> str:
            account = request.headers.get("X-Account-ID", "").strip()
            if not account or account.lower() != allowed_admin:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Account is not authorized for administrative access.",
                )
            return allowed_admin

        _test_require_admin_account.__annotations__["request"] = Request
        module.app.dependency_overrides[module.require_admin_account] = _test_require_admin_account  # type: ignore[attr-defined]

    if reset:
        module.Base.metadata.drop_all(bind=engine)  # type: ignore[attr-defined]
    module.Base.metadata.create_all(bind=engine)  # type: ignore[attr-defined]

    return module

