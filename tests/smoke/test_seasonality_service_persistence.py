"""Smoke tests ensuring the seasonality service persists cached metrics."""
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
from sqlalchemy.pool import StaticPool

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("sqlalchemy")

_REAL_CREATE_ENGINE = _sa_create_engine
_SERVICE_MODULE = "services.analytics.seasonality_service"


class _EngineProxy:
    """Proxy object that mimics a PostgreSQL engine while using SQLite underneath."""

    def __init__(self, inner: Engine) -> None:
        self._inner = inner
        self.url = make_url("postgresql+psycopg2://analytics:test@localhost/analytics")

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)

    def dispose(self) -> None:  # pragma: no cover - passthrough
        self._inner.dispose()


def _reload_service(monkeypatch: pytest.MonkeyPatch, sqlite_url: str) -> Any:
    monkeypatch.setenv("SEASONALITY_DATABASE_URI", "postgresql://analytics:test@localhost/analytics")

    def _patched_create_engine(url: str, **kwargs: Any) -> _EngineProxy:  # type: ignore[override]
        assert url.startswith("postgresql")
        inner = _REAL_CREATE_ENGINE(
            sqlite_url,
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
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
        _SERVICE_MODULE, ROOT / "services" / "analytics" / "seasonality_service.py"
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load seasonality service module for testing")

    module = util.module_from_spec(spec)
    sys.modules[_SERVICE_MODULE] = module
    spec.loader.exec_module(module)
    return module


def _seed_ohlcv(module: Any) -> None:
    with module.SessionLocal() as session:
        base_time = datetime.now(timezone.utc) - timedelta(hours=6)
        close = 30_000.0
        for offset in range(24):
            bucket_start = base_time + timedelta(hours=offset)
            candle = module.OhlcvBar(
                market="BTC-USD",
                bucket_start=bucket_start,
                open=close - 50 + offset,
                high=close + 75 + offset,
                low=close - 90,
                close=close + offset * 20,
                volume=1_000 + offset * 25,
            )
            session.merge(candle)
        session.commit()


@pytest.mark.smoke
def test_seasonality_metrics_survive_service_restart(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sqlite_path = tmp_path / "seasonality-analytics.db"
    module = _reload_service(monkeypatch, f"sqlite:///{sqlite_path}")
    module.OhlcvBase.metadata.create_all(bind=module.ENGINE)
    module.MetricsBase.metadata.create_all(bind=module.ENGINE)
    _seed_ohlcv(module)

    with TestClient(module.app) as client:
        response = client.get(
            "/seasonality/session_liquidity",
            params={"symbol": "BTC-USD"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["symbol"] == "BTC-USD"
        assert len(payload["metrics"]) == 3

    with module.SessionLocal() as session:
        stored = session.execute(select(module.SeasonalityMetric)).scalars().all()
        assert stored
        cached = {(row.symbol, row.period, round(row.avg_volume, 6)) for row in stored}

    module.ENGINE.dispose()
    if _SERVICE_MODULE in sys.modules:
        del sys.modules[_SERVICE_MODULE]

    module_reloaded = _reload_service(monkeypatch, f"sqlite:///{sqlite_path}")
    module_reloaded.OhlcvBase.metadata.create_all(bind=module_reloaded.ENGINE)
    module_reloaded.MetricsBase.metadata.create_all(bind=module_reloaded.ENGINE)

    with module_reloaded.SessionLocal() as session:
        persisted = session.execute(select(module_reloaded.SeasonalityMetric)).scalars().all()
        assert {(row.symbol, row.period, round(row.avg_volume, 6)) for row in persisted} == cached

    with TestClient(module_reloaded.app) as client:
        response = client.get("/seasonality/current_session")
        assert response.status_code == 200
        payload = response.json()
        assert payload["reference_volume"] >= 0
        assert payload["benchmark_volume"] >= 0

    other_engine = _REAL_CREATE_ENGINE(f"sqlite:///{sqlite_path}", future=True)
    try:
        with other_engine.connect() as connection:
            rows = connection.execute(text("SELECT symbol, period FROM seasonality_metrics"))
            persisted = [tuple(row) for row in rows]
        assert persisted
    finally:
        other_engine.dispose()
