from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import JSON, Column, DateTime, Float, Integer, MetaData, String, Table, create_engine
from sqlalchemy.pool import StaticPool

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


_load_module("services", "services/__init__.py")
_load_module("services.analytics", "services/analytics/__init__.py")
_load_module("services.common", "services/common/__init__.py")
_load_module("services.common.config", "services/common/config.py")
market_data_store = _load_module(
    "services.analytics.market_data_store", "services/analytics/market_data_store.py"
)
orderflow_service = _load_module(
    "services.analytics.orderflow_service", "services/analytics/orderflow_service.py"
)

TimescaleMarketDataAdapter = market_data_store.TimescaleMarketDataAdapter
MarketDataProvider = orderflow_service.MarketDataProvider
OrderflowMetricsStore = orderflow_service.OrderflowMetricsStore
OrderflowService = orderflow_service.OrderflowService

FIXTURE_PATH = Path(__file__).resolve().parent.parent.parent / "fixtures" / "market_data" / "kraken_orderflow_fixture.json"


@pytest.fixture()
def kraken_replay_payload() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


@pytest.fixture()
def kraken_replay_adapter(kraken_replay_payload: dict) -> TimescaleMarketDataAdapter:
    engine = create_engine(
        "sqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
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

    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            orders.insert(),
            [
                {
                    "order_id": row["order_id"],
                    "market": row["market"],
                    "side": row["side"],
                    "submitted_at": datetime.fromisoformat(row["submitted_at"]),
                }
                for row in kraken_replay_payload["orders"]
            ],
        )
        conn.execute(
            fills.insert(),
            [
                {
                    "order_id": row["order_id"],
                    "fill_time": datetime.fromisoformat(row["fill_time"]),
                    "price": row["price"],
                    "size": row["size"],
                }
                for row in kraken_replay_payload["fills"]
            ],
        )
        conn.execute(
            orderbook_snapshots.insert(),
            [
                {
                    "symbol": row["symbol"],
                    "depth": row["depth"],
                    "as_of": datetime.fromisoformat(row["as_of"]),
                    "bids": row["bids"],
                    "asks": row["asks"],
                }
                for row in kraken_replay_payload["orderbook_snapshots"]
            ],
        )

    return TimescaleMarketDataAdapter(engine=engine)


@pytest.mark.asyncio
async def test_orderflow_snapshot_replay(kraken_replay_adapter: TimescaleMarketDataAdapter) -> None:
    provider = MarketDataProvider(adapter=kraken_replay_adapter, order_book_depth=10)
    store = OrderflowMetricsStore()
    service = OrderflowService(data_provider=provider, store=store)

    snapshot = await service.snapshot("BTC-USD", window=900)

    assert snapshot.buy_sell_imbalance == pytest.approx(0.25, rel=1e-3)
    assert snapshot.depth_imbalance == pytest.approx(0.11811, rel=1e-3)
    assert snapshot.bid_depth == pytest.approx(142.0, rel=1e-6)
    assert snapshot.ask_depth == pytest.approx(112.0, rel=1e-6)

    history = store.history("BTC-USD")
    assert history, "Expected order flow metrics to be persisted"
    latest = history[-1]
    assert latest["buy_sell_imbalance"] == pytest.approx(snapshot.buy_sell_imbalance, rel=1e-6)
    assert latest["depth_imbalance"] == pytest.approx(snapshot.depth_imbalance, rel=1e-6)
