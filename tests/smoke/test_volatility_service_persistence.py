"""Smoke tests for the volatility analytics service."""
from __future__ import annotations

import importlib
from importlib import util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine as _sa_create_engine, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("sqlalchemy")

_REAL_CREATE_ENGINE = _sa_create_engine
_SERVICE_MODULE = "services.analytics.volatility_service"


class _EngineProxy:
    """Proxy that pretends to be a Postgres engine while using SQLite underneath."""

    def __init__(self, inner: Engine) -> None:
        self._inner = inner
        self.url = make_url("postgresql+psycopg2://analytics:test@localhost/analytics")

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)

    def dispose(self) -> None:  # pragma: no cover - passthrough
        self._inner.dispose()


def _reload_service(monkeypatch: pytest.MonkeyPatch, sqlite_url: str) -> Any:
    monkeypatch.setenv(
        "ANALYTICS_DATABASE_URL",
        "postgresql://analytics:test@localhost/analytics",
    )

    def _patched_create_engine(url: str, **kwargs: Any) -> _EngineProxy:  # type: ignore[override]
        assert url.startswith("postgresql")
        inner = _REAL_CREATE_ENGINE(sqlite_url, future=True)
        return _EngineProxy(inner)

    monkeypatch.setattr("sqlalchemy.create_engine", _patched_create_engine, raising=False)

    importlib.import_module("services")

    analytics_pkg = "services.analytics"
    if analytics_pkg not in sys.modules:
        pkg_spec = util.spec_from_file_location(
            analytics_pkg, ROOT / "services" / "analytics" / "__init__.py"
        )
        if pkg_spec and pkg_spec.loader:
            package = util.module_from_spec(pkg_spec)
            sys.modules[analytics_pkg] = package
            pkg_spec.loader.exec_module(package)

    if _SERVICE_MODULE in sys.modules:
        del sys.modules[_SERVICE_MODULE]

    spec = util.spec_from_file_location(
        _SERVICE_MODULE, ROOT / "services" / "analytics" / "volatility_service.py"
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load volatility service module for testing")

    module = util.module_from_spec(spec)
    sys.modules[_SERVICE_MODULE] = module
    spec.loader.exec_module(module)
    return module


def _install_migration_stub(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    def _stub() -> None:
        module.Base.metadata.create_all(bind=module.ENGINE)

    monkeypatch.setattr(module, "run_migrations", _stub)


def _seed_ohlcv(module: Any) -> None:
    with module.SessionLocal() as session:
        base_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        close = 30_000.0
        for offset in range(6):
            bucket_start = base_time + timedelta(minutes=offset)
            candle = module.OhlcvBar(
                market="BTC-USD",
                bucket_start=bucket_start,
                open=close - 25 + offset,
                high=close + 35 + offset,
                low=close - 45,
                close=close + offset * 50,
                volume=1_000 + offset * 10,
            )
            session.merge(candle)
        session.commit()


@pytest.mark.smoke
def test_volatility_metrics_survive_service_restart(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sqlite_path = tmp_path / "volatility-analytics.db"
    module = _reload_service(monkeypatch, f"sqlite:///{sqlite_path}")
    _install_migration_stub(module, monkeypatch)

    with TestClient(module.app) as client:
        _seed_ohlcv(module)
        response = client.get(
            "/volatility/realized",
            params={"symbol": "BTC-USD", "window": 5},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["symbol"] == "BTC-USD"
        assert payload["realized_vol"] >= 0.0

    with module.SessionLocal() as session:
        stored = session.execute(select(module.VolatilityMetric)).scalars().all()
        assert len(stored) == 1
        stored_symbol = stored[0].symbol
        stored_timestamp = stored[0].ts

    module.ENGINE.dispose()
    if _SERVICE_MODULE in sys.modules:
        del sys.modules[_SERVICE_MODULE]

    module_reloaded = _reload_service(monkeypatch, f"sqlite:///{sqlite_path}")
    _install_migration_stub(module_reloaded, monkeypatch)

    with module_reloaded.SessionLocal() as session:
        persisted = session.execute(select(module_reloaded.VolatilityMetric)).scalars().all()
        assert [(row.symbol, row.ts) for row in persisted] == [(stored_symbol, stored_timestamp)]

    other_engine = _REAL_CREATE_ENGINE(f"sqlite:///{sqlite_path}", future=True)
    try:
        with other_engine.connect() as connection:
            rows = connection.execute(text("SELECT symbol FROM vol_metrics"))
            symbols = [row[0] for row in rows]
        assert symbols == ["BTC-USD"]
    finally:
        other_engine.dispose()
