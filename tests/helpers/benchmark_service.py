"""Test helpers for the benchmark service."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType
from typing import Final

import pytest

_ALLOW_SQLITE_FLAG: Final[str] = "BENCHMARK_ALLOW_SQLITE"
_DATABASE_ENV: Final[str] = "BENCHMARK_DATABASE_URL"
_DEFAULT_TEST_DSN: Final[str] = "sqlite:///benchmark.db"


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
    monkeypatch.setenv(_DATABASE_ENV, database_url)
    monkeypatch.setenv(_ALLOW_SQLITE_FLAG, "1")
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
    resolved_dsn = f"sqlite:///{db_path}" if database_url.startswith("sqlite") else database_url
    if resolved_dsn != module.DATABASE_URL:  # type: ignore[attr-defined]
        monkeypatch.setenv(_DATABASE_ENV, resolved_dsn)
        sys.modules.pop("benchmark_service", None)
        module = importlib.import_module("benchmark_service")

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
        module.Base.metadata.drop_all(bind=module.ENGINE)  # type: ignore[attr-defined]
    module.Base.metadata.create_all(bind=module.ENGINE)  # type: ignore[attr-defined]

    return module

