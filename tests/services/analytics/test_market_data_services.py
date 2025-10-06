from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import importlib.util
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_module(module_name: str, relative_path: str):
    module_path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module {module_name} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


os.environ.setdefault("SESSION_REDIS_URL", "memory://signal-service-tests")

_load_module("services", "services/__init__.py")
_load_module("services.common", "services/common/__init__.py")
_load_module("services.analytics", "services/analytics/__init__.py")
_load_module("services.analytics.market_data_store", "services/analytics/market_data_store.py")

crossasset_service = _load_module("services.analytics.crossasset_service", "services/analytics/crossasset_service.py")


from prometheus_client import REGISTRY


def _reload_signal_service():
    module_name = "services.analytics.signal_service"
    existing = sys.modules.get(module_name)
    if existing is not None:
        gauge = getattr(existing, "DATA_STALENESS_GAUGE", None)
        if gauge is not None:
            try:
                REGISTRY.unregister(gauge)
            except KeyError:
                pass
    sys.modules.pop(module_name, None)
    return _load_module(module_name, "services/analytics/signal_service.py")

from services.analytics.market_data_store import TimescaleMarketDataAdapter

FIXTURE_PATH = Path(__file__).resolve().parent.parent.parent / "fixtures" / "market_data" / "signal_service_fixture.json"

SIGNAL_ENDPOINTS = (
    ("/signals/orderflow/BTC-USD", {"window": 600}),
    (
        "/signals/crossasset",
        {"base_symbol": "BTC-USD", "alt_symbol": "ETH-USD", "window": 60, "max_lag": 5},
    ),
    ("/signals/volatility/BTC-USD", {"window": 60, "horizon": 5}),
    ("/signals/whales/BTC-USD", {"window": 900, "threshold_sigma": 2.0}),
    ("/signals/stress/BTC-USD", {"window": 120}),
)


@pytest.fixture(scope="session")
def fixture_payload() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


@pytest.fixture()
def market_data_dsn(tmp_path, fixture_payload: dict):
    database_path = tmp_path / "signal-timescale.db"
    dsn = f"sqlite:///{database_path}"
    engine = create_engine(dsn, future=True)
    metadata = MetaData()

    orders = Table(
        "orders",
        metadata,
        Column("order_id", String, primary_key=True),
        Column("market", String, nullable=False),
        Column("side", String, nullable=False),
        Column("submitted_at", DateTime(timezone=True), nullable=False),
    )
    fills = Table(
        "fills",
        metadata,
        Column("order_id", String, nullable=False),
        Column("fill_time", DateTime(timezone=True), nullable=False),
        Column("price", Float, nullable=False),
        Column("size", Float, nullable=False),
    )
    orderbook_snapshots = Table(
        "orderbook_snapshots",
        metadata,
        Column("symbol", String, nullable=False),
        Column("depth", Integer, nullable=False),
        Column("as_of", DateTime(timezone=True), nullable=False),
        Column("bids", JSON, nullable=False),
        Column("asks", JSON, nullable=False),
    )
    ohlcv_bars = Table(
        "ohlcv_bars",
        metadata,
        Column("market", String, nullable=False),
        Column("bucket_start", DateTime(timezone=True), nullable=False),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float, nullable=False),
        Column("volume", Float),
    )

    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(orders.insert(), [
            {
                "order_id": row["order_id"],
                "market": row["market"],
                "side": row["side"],
                "submitted_at": datetime.fromisoformat(row["submitted_at"]),
            }
            for row in fixture_payload["orders"]
        ])
        conn.execute(fills.insert(), [
            {
                "order_id": row["order_id"],
                "fill_time": datetime.fromisoformat(row["fill_time"]),
                "price": row["price"],
                "size": row["size"],
            }
            for row in fixture_payload["fills"]
        ])
        conn.execute(orderbook_snapshots.insert(), [
            {
                "symbol": row["symbol"],
                "depth": row["depth"],
                "as_of": datetime.fromisoformat(row["as_of"]),
                "bids": row["bids"],
                "asks": row["asks"],
            }
            for row in fixture_payload["orderbook_snapshots"]
        ])
        for market, rows in fixture_payload["bars"].items():
            conn.execute(
                ohlcv_bars.insert(),
                [
                    {
                        "market": market,
                        "bucket_start": datetime.fromisoformat(bar["bucket_start"]),
                        "open": bar["open"],
                        "high": bar["high"],
                        "low": bar["low"],
                        "close": bar["close"],
                        "volume": bar["volume"],
                    }
                    for bar in rows
                ],
            )

    yield dsn

    engine.dispose()


@pytest.fixture()
def timescale_adapter(market_data_dsn: str):
    adapter = TimescaleMarketDataAdapter(database_url=market_data_dsn)
    try:
        yield adapter
    finally:
        engine = getattr(adapter, "_engine", None)
        if engine is not None:
            engine.dispose()


@pytest.fixture()
def signal_service_module(monkeypatch: pytest.MonkeyPatch, market_data_dsn: str):
    monkeypatch.setenv("SIGNAL_DATABASE_URL", market_data_dsn)
    module = _reload_signal_service()
    globals()["signal_service"] = module
    yield module
    module.app.dependency_overrides.clear()
    module.app.state.market_data_adapter = None
    gauge = getattr(module, "DATA_STALENESS_GAUGE", None)
    if gauge is not None:
        try:
            REGISTRY.unregister(gauge)
        except KeyError:
            pass
    sys.modules.pop("services.analytics.signal_service", None)
    globals().pop("signal_service", None)


@pytest.fixture()
def signal_client(signal_service_module) -> TestClient:
    with TestClient(signal_service_module.app) as client:
        yield client


@pytest.fixture()
def signal_admin_headers() -> dict[str, str]:
    session = signal_service.SESSION_STORE.create(admin_id="company")
    return {"Authorization": f"Bearer {session.token}"}


@pytest.mark.parametrize("path,params", SIGNAL_ENDPOINTS)
def test_signal_routes_require_authentication(signal_client: TestClient, path: str, params: dict):
    response = signal_client.get(path, params=params)
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.parametrize("path,params", SIGNAL_ENDPOINTS)
def test_signal_routes_reject_non_admin(signal_client: TestClient, path: str, params: dict):
    session = signal_service.SESSION_STORE.create(admin_id="visitor")
    headers = {"Authorization": f"Bearer {session.token}"}
    response = signal_client.get(path, params=params, headers=headers)
    assert response.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.parametrize("path,params", SIGNAL_ENDPOINTS)
def test_signal_routes_allow_admin(signal_client: TestClient, signal_admin_headers: dict[str, str], path: str, params: dict):
    response = signal_client.get(path, params=params, headers=signal_admin_headers)
    assert response.status_code == status.HTTP_200_OK


@pytest.fixture()
def crossasset_client(fixture_payload: dict):
    engine = create_engine(
        "sqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    crossasset_service.ENGINE = engine
    crossasset_service.SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    crossasset_service.Base.metadata.create_all(engine)

    bars_table = crossasset_service.OhlcvBar.__table__
    with engine.begin() as conn:
        for market, rows in fixture_payload["bars"].items():
            conn.execute(
                insert(bars_table),
                [
                    {
                        "market": market,
                        "bucket_start": datetime.fromisoformat(bar["bucket_start"]),
                        "open": bar["open"],
                        "high": bar["high"],
                        "low": bar["low"],
                        "close": bar["close"],
                        "volume": bar["volume"],
                    }
                    for bar in rows
                ],
            )

    return TestClient(crossasset_service.app)


def test_timescale_adapter_recent_trades(timescale_adapter: TimescaleMarketDataAdapter):
    trades = timescale_adapter.recent_trades("BTC-USD", window=1800)
    assert len(trades) == 6
    assert {trade.side for trade in trades} == {"buy", "sell"}
    assert all(trade.price > 0 for trade in trades)


def test_timescale_adapter_order_book(timescale_adapter: TimescaleMarketDataAdapter):
    snapshot = timescale_adapter.order_book_snapshot("BTC-USD")
    assert "bids" in snapshot and "asks" in snapshot
    assert len(snapshot["bids"]) == 10
    assert len(snapshot["asks"]) == 10


def test_timescale_adapter_price_history(timescale_adapter: TimescaleMarketDataAdapter):
    prices = timescale_adapter.price_history("BTC-USD", length=50)
    assert len(prices) == 50
    assert prices[0] < prices[-1]
    latest = timescale_adapter.latest_price_timestamp("BTC-USD")
    assert latest is not None


def test_signal_order_flow_endpoint(signal_client: TestClient, signal_admin_headers: dict[str, str]):
    response = signal_client.get("/signals/orderflow/BTC-USD", params={"window": 600}, headers=signal_admin_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTC-USD"
    assert payload["buy_volume"] > 0
    assert payload["sell_volume"] > 0


def test_signal_volatility_endpoint(signal_client: TestClient, signal_admin_headers: dict[str, str]):
    response = signal_client.get(
        "/signals/volatility/BTC-USD",
        params={"window": 60, "horizon": 5},
        headers=signal_admin_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["variance"] > 0
    assert len(payload["forecasts"]) == 5


def test_signal_crossasset_endpoint(signal_client: TestClient, signal_admin_headers: dict[str, str]):
    response = signal_client.get(
        "/signals/crossasset",
        params={"base_symbol": "BTC-USD", "alt_symbol": "ETH-USD", "window": 60, "max_lag": 5},
        headers=signal_admin_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["correlation"] > 0


def test_signal_whales_endpoint(signal_client: TestClient, signal_admin_headers: dict[str, str]):
    response = signal_client.get(
        "/signals/whales/BTC-USD",
        params={"window": 900, "threshold_sigma": 2.0},
        headers=signal_admin_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] >= 0


def test_signal_stress_endpoint(signal_client: TestClient, signal_admin_headers: dict[str, str]):
    response = signal_client.get(
        "/signals/stress/BTC-USD",
        params={"window": 120},
        headers=signal_admin_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert "flash_crash" in payload
    assert "spread_widening" in payload


def test_signal_service_requires_dsn(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("SIGNAL_DATABASE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    module = _reload_signal_service()
    with pytest.raises(RuntimeError, match="SIGNAL_DATABASE_URL or TIMESCALE_DSN"):
        with TestClient(module.app):
            pass
    gauge = getattr(module, "DATA_STALENESS_GAUGE", None)
    if gauge is not None:
        try:
            REGISTRY.unregister(gauge)
        except KeyError:
            pass
    sys.modules.pop("services.analytics.signal_service", None)


def test_signal_service_requires_session_store(monkeypatch: pytest.MonkeyPatch):
    for env_var in ("SESSION_REDIS_URL", "SESSION_STORE_URL", "SESSION_BACKEND_DSN"):
        monkeypatch.delenv(env_var, raising=False)

    module = _reload_signal_service()
    with pytest.raises(RuntimeError, match="Session store misconfigured"):
        with TestClient(module.app):
            pass

    gauge = getattr(module, "DATA_STALENESS_GAUGE", None)
    if gauge is not None:
        try:
            REGISTRY.unregister(gauge)
        except KeyError:
            pass
    sys.modules.pop("services.analytics.signal_service", None)


def test_signal_service_rejects_memory_session_store_without_pytest(
    monkeypatch: pytest.MonkeyPatch,
):
    for env_var in ("SESSION_REDIS_URL", "SESSION_STORE_URL", "SESSION_BACKEND_DSN"):
        monkeypatch.delenv(env_var, raising=False)
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://ephemeral-signal-store")
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    module = _reload_signal_service()
    with pytest.raises(RuntimeError, match="memory:// DSNs are only supported"):
        with TestClient(module.app):
            pass

    gauge = getattr(module, "DATA_STALENESS_GAUGE", None)
    if gauge is not None:
        try:
            REGISTRY.unregister(gauge)
        except KeyError:
            pass
    sys.modules.pop("services.analytics.signal_service", None)


def test_signal_service_normalises_timescale_dsn(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SIGNAL_DATABASE_URL", "timescale://user:pass@localhost:5432/analytics")
    module = _reload_signal_service()
    resolved = module._resolve_market_data_dsn()
    assert resolved.startswith("postgresql+psycopg2://")

    gauge = getattr(module, "DATA_STALENESS_GAUGE", None)
    if gauge is not None:
        try:
            REGISTRY.unregister(gauge)
        except KeyError:
            pass
    sys.modules.pop("services.analytics.signal_service", None)


def test_crossasset_lead_lag(crossasset_client: TestClient):
    response = crossasset_client.get("/crossasset/leadlag", params={"base": "BTC-USD", "target": "ETH-USD"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["correlation"] > 0


def test_crossasset_beta(crossasset_client: TestClient):
    response = crossasset_client.get(
        "/crossasset/beta",
        params={"alt": "ETH-USD", "base": "BTC-USD", "window": 40},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["beta"] != 0


def test_crossasset_stablecoin(crossasset_client: TestClient):
    response = crossasset_client.get("/crossasset/stablecoin", params={"symbol": "USDT/USD"})
    assert response.status_code == 200
    payload = response.json()
    assert abs(payload["price"] - 1.0) < 0.1
