"""Smoke tests for the compliance filter's shared persistence."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def test_restricted_symbols_persist_and_replicate(
    tmp_path: Path,
) -> None:
    """Restricted instruments must persist across restarts and replicas."""

    project_root = Path(__file__).resolve().parents[2]
    filtered_path = [str(project_root)]
    filtered_path.extend(p for p in sys.path if "tests" not in p)
    sys.path = filtered_path
    importlib.invalidate_caches()
    sys.modules.pop("services", None)
    sys.modules.pop("services.common", None)
    sys.modules.pop("services.common.security", None)

    module = importlib.import_module("compliance_filter")
    module = importlib.reload(module)

    db_path = tmp_path / "compliance-smoke.db"
    dsn = f"sqlite:///{db_path}"

    engine = create_engine(dsn, future=True)
    module.run_compliance_migrations(engine)
    session_factory = sessionmaker(
        bind=engine, autoflush=False, expire_on_commit=False, future=True
    )

    primary = module.ComplianceFilter(session_factory)
    replica = module.ComplianceFilter(session_factory)

    entry = primary.update_asset("btc-usd", "restricted", "manual review")

    allowed, replica_entry = replica.evaluate("btc-usd")
    assert not allowed
    assert replica_entry is not None
    assert replica_entry.status == "restricted"
    assert replica_entry.reason == "manual review"

    engine.dispose()

    restarted_engine = create_engine(dsn, future=True)
    module.run_compliance_migrations(restarted_engine)
    restarted_factory = sessionmaker(
        bind=restarted_engine, autoflush=False, expire_on_commit=False, future=True
    )
    restarted_filter = module.ComplianceFilter(restarted_factory)

    allowed_after_restart, persisted_entry = restarted_filter.evaluate("btc-usd")
    assert not allowed_after_restart
    assert persisted_entry is not None
    assert persisted_entry.status == "restricted"
    assert persisted_entry.reason == "manual review"
    assert persisted_entry.updated_at >= entry.updated_at


@pytest.fixture(autouse=True)
def _configure_sqlite_compliance_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Configure a throwaway SQLite URL so the module can import in tests."""

    monkeypatch.setattr(sys, "path", list(sys.path), raising=False)
    bootstrap_dsn = f"sqlite:///{(tmp_path / 'bootstrap.db')}"
    monkeypatch.setenv("COMPLIANCE_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("COMPLIANCE_DATABASE_URL", bootstrap_dsn)
    yield
    monkeypatch.delenv("COMPLIANCE_DATABASE_URL", raising=False)
    monkeypatch.delenv("COMPLIANCE_ALLOW_SQLITE_FOR_TESTS", raising=False)
