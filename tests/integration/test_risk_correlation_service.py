from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import importlib
import sys

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import Column, DateTime, Float, MetaData, String, Table, insert, select


@pytest.fixture
def correlation_service_env(tmp_path, monkeypatch):
    marketdata_path = tmp_path / "marketdata.db"
    correlations_path = tmp_path / "correlations.db"

    pytest.importorskip(
        "pandas",
        reason="pandas (with numpy) is required to validate correlation computations",
        exc_type=ImportError,
    )
    pd = importlib.import_module("pandas")
    globals()["pd"] = pd
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root))
    import importlib.util

    services_init = root / "services" / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        "services",
        services_init,
        submodule_search_locations=[str(root / "services")],
    )
    services_pkg = importlib.util.module_from_spec(spec)
    sys.modules["services"] = services_pkg
    assert spec.loader is not None
    spec.loader.exec_module(services_pkg)
    monkeypatch.setenv("RISK_MARKETDATA_URL", f"sqlite:///{marketdata_path}")
    monkeypatch.setenv("RISK_CORRELATION_DATABASE_URL", f"sqlite:///{correlations_path}")
    monkeypatch.setenv("RISK_CORRELATION_SYMBOLS", "BTC-USD,ETH-USD")
    monkeypatch.setenv("RISK_MARKETDATA_LOOKBACK", "32")

    import services.risk.correlation_service as correlation_service

    module = importlib.reload(correlation_service)
    module._reset_marketdata_engine()

    engine = module._marketdata_engine()
    metadata = MetaData()
    ohlcv_bars = Table(
        "ohlcv_bars",
        metadata,
        Column("market", String, primary_key=True),
        Column("bucket_start", DateTime(timezone=True), primary_key=True),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float, nullable=False),
        Column("volume", Float),
    )
    metadata.create_all(engine)

    yield module, engine, ohlcv_bars

    module.price_store.clear()
    module._reset_marketdata_engine()


def _seed_ohlcv(
    engine,
    table: Table,
    *,
    market: str,
    closes: Iterable[float],
    start: datetime,
    interval: timedelta,
) -> None:
    rows = []
    for index, close in enumerate(closes):
        bucket_start = start + interval * index
        rows.append(
            {
                "market": market,
                "bucket_start": bucket_start,
                "open": float(close),
                "high": float(close),
                "low": float(close),
                "close": float(close),
                "volume": 100.0,
            }
        )
    with engine.begin() as conn:
        conn.execute(insert(table), rows)


def test_correlation_matrix_persists_real_data(correlation_service_env):
    module, engine, table = correlation_service_env

    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = now - timedelta(minutes=25)
    interval = timedelta(minutes=5)

    btc = [30150.2, 30152.4, 30155.8, 30153.1, 30158.9, 30156.7]
    eth = [2020.3, 2021.5, 2024.9, 2023.2, 2027.4, 2025.1]

    _seed_ohlcv(engine, table, market="BTC-USD", closes=btc, start=start, interval=interval)
    _seed_ohlcv(engine, table, market="ETH-USD", closes=eth, start=start, interval=interval)

    expected = (
        pd.DataFrame({"BTC-USD": btc, "ETH-USD": eth})
        .pct_change()
        .dropna(how="any")
        .corr()
        .loc["BTC-USD", "ETH-USD"]
    )

    with TestClient(module.app) as client:
        response = client.get("/correlations/matrix", params={"window": 5})

    assert response.status_code == 200
    payload = response.json()

    observed = payload["matrix"]["BTC-USD"]["ETH-USD"]
    assert observed == pytest.approx(expected, rel=1e-6)

    response_timestamp = datetime.fromisoformat(payload["timestamp"])

    with module.SessionLocal() as session:
        rows = session.execute(
            select(module.CorrelationRecord).where(
                module.CorrelationRecord.timestamp == response_timestamp
            )
        ).scalars().all()

    assert rows, "expected persisted correlation records"
    pair = next(
        record
        for record in rows
        if {record.symbol1, record.symbol2} == {"BTC-USD", "ETH-USD"}
    )
    assert pair.correlation == pytest.approx(expected, rel=1e-6)


def test_alert_triggered_when_history_is_stale(correlation_service_env, monkeypatch):
    module, engine, table = correlation_service_env

    monkeypatch.setenv("RISK_MARKETDATA_STALE_MINUTES", "5")
    module._reset_marketdata_engine()
    module.price_store.clear()
    engine = module._marketdata_engine()

    now = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=2)
    start = now - timedelta(minutes=25)
    interval = timedelta(minutes=5)

    btc = [30150.2, 30152.4, 30155.8, 30153.1]
    eth = [2020.3, 2021.5, 2024.9, 2023.2]

    _seed_ohlcv(engine, table, market="BTC-USD", closes=btc, start=start, interval=interval)
    _seed_ohlcv(engine, table, market="ETH-USD", closes=eth, start=start, interval=interval)

    recorded: list[object] = []

    def _capture(event):
        recorded.append(event)

    monkeypatch.setattr(module.alert_manager, "handle_risk_event", _capture)

    with TestClient(module.app) as client:
        response = client.get("/correlations/matrix", params={"window": 3})

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert recorded, "expected market data alert"
    assert recorded[0].event_type == "market_data_stale"

    with module.SessionLocal() as session:
        persisted = session.execute(select(module.CorrelationRecord)).scalars().all()
    assert not persisted, "stale data should not persist correlations"
