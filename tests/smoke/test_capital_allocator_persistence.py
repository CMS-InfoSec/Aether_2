"""Smoke test ensuring capital allocation history persists across replicas."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine as _sa_create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import StaticPool

from tests.helpers.authentication import override_admin_auth

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("services.common.security")
pytest.importorskip("sqlalchemy")
pytest.importorskip("fastapi")

_REAL_CREATE_ENGINE = _sa_create_engine
_MODULE_NAME = "capital_allocator"


class _EngineProxy:
    """Proxy Postgres engine wrapper delegating to SQLite."""

    def __init__(self, inner: Engine) -> None:
        self._inner = inner
        self.url = make_url("postgresql+psycopg2://allocator:test@localhost/allocator")

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)

    def dispose(self) -> None:  # pragma: no cover - passthrough
        self._inner.dispose()


def _reload_allocator(monkeypatch: pytest.MonkeyPatch, sqlite_url: str) -> Any:
    monkeypatch.setenv("CAPITAL_ALLOCATOR_DB_URL", "postgresql://allocator:test@localhost/allocator")
    monkeypatch.setenv("ALLOCATOR_DRAWDOWN_THRESHOLD", "0.8")
    monkeypatch.setenv("ALLOCATOR_MIN_THROTTLE_PCT", "0.05")
    monkeypatch.delenv("CAPITAL_ALLOCATOR_SSLMODE", raising=False)

    def _patched_create_engine(url: str, **kwargs: Any) -> _EngineProxy:  # type: ignore[override]
        kwargs.pop("pool_size", None)
        kwargs.pop("max_overflow", None)
        kwargs.pop("pool_timeout", None)
        kwargs.pop("pool_recycle", None)
        connect_args = kwargs.pop("connect_args", {}) or {}
        connect_args.pop("sslmode", None)
        connect_args.pop("application_name", None)
        inner = _REAL_CREATE_ENGINE(
            sqlite_url,
            future=True,
            connect_args={**connect_args, "check_same_thread": False},
            poolclass=StaticPool,
        )
        return _EngineProxy(inner)

    monkeypatch.setattr("sqlalchemy.create_engine", _patched_create_engine, raising=False)

    if _MODULE_NAME in sys.modules:
        del sys.modules[_MODULE_NAME]

    module = importlib.import_module(_MODULE_NAME)
    monkeypatch.setattr(
        module,
        "run_allocator_migrations",
        lambda: module.Base.metadata.create_all(bind=module.ENGINE),
    )
    module.run_allocator_migrations()
    return module


def _seed_nav_curves(module: Any, now: str) -> None:
    with module.ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS pnl_curves (
                    account_id TEXT,
                    nav REAL,
                    drawdown REAL,
                    drawdown_limit REAL,
                    curve_ts TEXT
                )
                """
            )
        )
        conn.execute(text("DELETE FROM pnl_curves"))
        conn.execute(
            text(
                "INSERT INTO pnl_curves (account_id, nav, drawdown, drawdown_limit, curve_ts) "
                "VALUES (:account_id, :nav, :drawdown, :drawdown_limit, :curve_ts)"
            ),
            [
                {
                    "account_id": account,
                    "nav": nav,
                    "drawdown": drawdown,
                    "drawdown_limit": limit,
                    "curve_ts": now,
                }
                for account, nav, drawdown, limit in (
                    ("alpha", 500_000.0, 5_000.0, 20_000.0),
                    ("beta", 300_000.0, 2_500.0, 12_000.0),
                    ("gamma", 200_000.0, 1_000.0, 10_000.0),
                )
            ],
        )


@pytest.mark.smoke
def test_allocation_history_survives_restart_and_replica(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sqlite_path = tmp_path / "allocator-history.db"
    module = _reload_allocator(monkeypatch, f"sqlite:///{sqlite_path}")
    now = "2024-01-01T00:00:00+00:00"
    _seed_nav_curves(module, now)

    monkeypatch.setenv("CAPITAL_ALLOCATOR_ADMINS", "alpha,beta,gamma")

    with TestClient(module.app) as client:
        payload = {"allocations": {"alpha": 0.5, "beta": 0.3, "gamma": 0.2}}
        with override_admin_auth(client.app, module.require_admin_account, "alpha") as headers:
            response = client.post("/allocator/rebalance", json=payload, headers=headers)
        assert response.status_code == 200

    module.ENGINE.dispose()
    if _MODULE_NAME in sys.modules:
        del sys.modules[_MODULE_NAME]

    reloaded = _reload_allocator(monkeypatch, f"sqlite:///{sqlite_path}")
    with TestClient(reloaded.app) as client:
        with override_admin_auth(client.app, reloaded.require_admin_account, "alpha") as headers:
            status = client.get("/allocator/status", headers=headers)
        assert status.status_code == 200

    with reloaded.SessionLocal() as session:
        count = session.execute(text("SELECT COUNT(*) FROM capital_allocations"))
        assert count.scalar() == 3

    replica_engine = _REAL_CREATE_ENGINE(f"sqlite:///{sqlite_path}", future=True)
    try:
        with replica_engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT account_id, COUNT(*) FROM capital_allocations GROUP BY account_id ORDER BY account_id"
                )
            ).all()
        assert rows == [("alpha", 1), ("beta", 1), ("gamma", 1)]
    finally:
        replica_engine.dispose()

    reloaded.ENGINE.dispose()
