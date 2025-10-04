import importlib
import os
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

pytest.importorskip("fastapi")
pytest.importorskip("sqlalchemy")

_REAL_CREATE_ENGINE = _sa_create_engine
_POSTGRES_DSN = "postgresql://capital_flow:test@localhost/capital_flow"


class _EngineProxy:
    """Proxy Postgres engine wrapper delegating to SQLite."""

    def __init__(self, inner: Engine) -> None:
        self._inner = inner
        self.url = make_url("postgresql+psycopg://capital_flow:test@localhost/capital_flow")

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)

    def dispose(self) -> None:  # pragma: no cover - passthrough
        self._inner.dispose()


def _reload_capital_flow(monkeypatch: pytest.MonkeyPatch, sqlite_url: str):
    monkeypatch.syspath_prepend(str(ROOT))
    monkeypatch.setenv(
        "PYTHONPATH",
        str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""),
    )

    monkeypatch.setenv("CAPITAL_FLOW_DATABASE_URL", _POSTGRES_DSN)
    monkeypatch.delenv("CAPITAL_FLOW_DB_SSLMODE", raising=False)

    def _patched_create_engine(url: str, **kwargs: Any) -> _EngineProxy:  # type: ignore[override]
        kwargs.pop("pool_size", None)
        kwargs.pop("max_overflow", None)
        kwargs.pop("pool_timeout", None)
        kwargs.pop("pool_recycle", None)
        connect_args = dict(kwargs.pop("connect_args", {}) or {})
        connect_args.pop("sslmode", None)
        connect_args.pop("application_name", None)
        connect_args.pop("sslrootcert", None)
        connect_args.pop("sslcert", None)
        connect_args.pop("sslkey", None)
        inner = _REAL_CREATE_ENGINE(
            sqlite_url,
            future=True,
            connect_args={**connect_args, "check_same_thread": False},
            poolclass=StaticPool,
        )
        return _EngineProxy(inner)

    monkeypatch.setattr("sqlalchemy.create_engine", _patched_create_engine, raising=False)

    previous_modules = {
        "services.common.security": sys.modules.get("services.common.security"),
        "services.common": sys.modules.get("services.common"),
        "services": sys.modules.get("services"),
    }

    for name in list(previous_modules):
        sys.modules.pop(name, None)

    if "capital_flow" in sys.modules:
        del sys.modules["capital_flow"]

    module = importlib.import_module("capital_flow")

    for name, previous in previous_modules.items():
        if previous is not None:
            sys.modules[name] = previous

    return module


@pytest.mark.smoke
def test_capital_flow_history_survives_restart_and_replica(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sqlite_path = tmp_path / "capital-flow.db"
    sqlite_url = f"sqlite:///{sqlite_path}"

    module = _reload_capital_flow(monkeypatch, sqlite_url)

    with TestClient(module.app) as client:
        with override_admin_auth(client.app, module.require_admin_account, "company") as headers:
            deposit = client.post(
                "/finance/deposit",
                headers=headers,
                json={"account_id": "company", "amount": 125.0, "currency": "USD"},
            )
            assert deposit.status_code == 201
            withdraw = client.post(
                "/finance/withdraw",
                headers=headers,
                json={"account_id": "company", "amount": 25.0, "currency": "USD"},
            )
            assert withdraw.status_code == 201

    module.ENGINE.dispose()
    if "capital_flow" in sys.modules:
        del sys.modules["capital_flow"]

    reloaded = _reload_capital_flow(monkeypatch, sqlite_url)

    with TestClient(reloaded.app) as client:
        with override_admin_auth(client.app, reloaded.require_admin_account, "company") as headers:
            flows = client.get(
                "/finance/flows",
                headers=headers,
                params={"account_id": "company", "limit": 10},
            )
            assert flows.status_code == 200
            history = flows.json()["flows"]
            assert len(history) == 2
            amounts = sorted(entry["amount"] for entry in history)
            assert amounts == [25.0, 125.0]
            baselines = [entry["nav_baseline"] for entry in history]
            assert baselines[0] == pytest.approx(100.0)

    replica_engine = _REAL_CREATE_ENGINE(sqlite_url, future=True)
    try:
        with replica_engine.connect() as connection:
            count = connection.execute(text("SELECT COUNT(*) FROM capital_flows")).scalar()
        assert count == 2
    finally:
        replica_engine.dispose()

    reloaded.ENGINE.dispose()
