"""Integration tests for ingestion and training pipelines using lightweight fakes."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest
import httpx

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from services import coingecko_ingest
from services import kraken_ws_ingest
from tests.fixtures.backends import FakeKafkaProducer


@pytest.mark.asyncio
async def test_coingecko_ingestion_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    payload = {
        "prices": [
            [start.timestamp() * 1000, 10.0],
            [(start + timedelta(hours=12)).timestamp() * 1000, 15.0],
            [(start + timedelta(days=1)).timestamp() * 1000, 12.0],
        ],
        "total_volumes": [
            [start.timestamp() * 1000, 1_000.0],
            [(start + timedelta(days=1)).timestamp() * 1000, 1_200.0],
        ],
    }

    async def handler(request):
        assert request.url.path.endswith("/coins/bitcoin/market_chart/range")
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(base_url=coingecko_ingest.API_BASE_URL, transport=transport) as client:
        data = await coingecko_ingest.fetch_market_chart(client, "bitcoin", "usd", start, end)

    rows = coingecko_ingest.aggregate_daily_rows("bitcoin", data, start, end)
    assert len(rows) == 2

    captured: List[Dict[str, Any]] = []

    async def fake_execute(connection, batch):
        captured.extend(batch)

    class FakeEngine:
        def begin(self):
            class _Ctx:
                async def __aenter__(self_inner):
                    return SimpleNamespace()

                async def __aexit__(self_inner, exc_type, exc, tb):
                    return False

            return _Ctx()

    monkeypatch.setattr(coingecko_ingest, "_execute_upsert", fake_execute)
    await coingecko_ingest.upsert_ohlcv_rows(FakeEngine(), rows, batch_size=1)
    assert len(captured) == len(rows)


@pytest.mark.asyncio
async def test_kraken_ingestor_normalises_messages(fake_kafka_producer: FakeKafkaProducer) -> None:
    config = kraken_ws_ingest.KrakenConfig(pairs=["BTC/USD"], kafka_bootstrap_servers="localhost:9092")
    ingestor = kraken_ws_ingest.KrakenIngestor(config)
    ingestor._producer = fake_kafka_producer

    trades = [["50000.0", "0.1", "1700000000", "b"]]
    await ingestor._handle_trade_message(trades, "BTC/USD")

    book_payload = {"a": [["50010.0", "0.2", "1700000001"]]}
    await ingestor._handle_book_message(book_payload, "BTC/USD")

    topics = {topic for topic, _ in fake_kafka_producer.messages}
    assert topics == {config.trade_topic, config.book_topic}


def test_materialization_refresh_invokes_store(monkeypatch: pytest.MonkeyPatch) -> None:
    from data.feast import materialize

    calls: List[str] = []

    class FakeStore:
        def materialize(self, start_date, end_date):
            calls.append("daily")

        def materialize_incremental(self, end_date):
            calls.append("incremental")

    materialize.refresh_online_store(FakeStore(), days=2)
    assert calls == ["daily", "incremental"]


def test_training_pipeline_generates_splits_and_trains(monkeypatch: pytest.MonkeyPatch) -> None:
    from ml.data_loader import PurgedWalkForwardDataLoader, TimescaleFeastConfig
    from ml.models import supervised

    timestamps = pd.date_range("2024-01-01", periods=10, freq="D", tz="UTC")
    base_frame = pd.DataFrame(
        {
            "event_ts": timestamps,
            "asset": ["BTC"] * 10,
            "label": np.linspace(0, 1, 10),
            "turnover": np.linspace(10, 20, 10),
        }
    )
    features_frame = pd.DataFrame(
        {
            "event_ts": timestamps,
            "asset": ["BTC"] * 10,
            "feature_a": np.linspace(1, 10, 10),
        }
    )

    monkeypatch.setattr("ml.data_loader._create_engine", lambda uri: object())
    monkeypatch.setattr("ml.data_loader._load_base_frame", lambda engine, query, time_column: base_frame)
    monkeypatch.setattr(
        "ml.data_loader._load_feast_features",
        lambda feature_view, entities, feature_service, feature_store_repo: features_frame,
    )

    config = TimescaleFeastConfig(
        timescale_uri="postgresql://",  # unused because of monkeypatch
        base_query="select * from data",
        time_column="event_ts",
        entity_column="asset",
        label_column="label",
        transaction_fee_bps=5.0,
        feature_view="features_view",
    )

    loader = PurgedWalkForwardDataLoader(
        config=config,
        turnover_column="turnover",
        train_window=timedelta(days=3),
        validation_window=timedelta(days=2),
        test_window=timedelta(days=2),
    )
    splits = loader.load()
    assert splits

    dataset = supervised.SupervisedDataset(features=splits[0].train.drop(columns=["label"]), labels=splits[0].train["label"])

    class DummyTrainer(supervised.SupervisedTrainer):
        name = "dummy"

        def __init__(self) -> None:
            super().__init__()
            self.fitted = False

        def fit(self, dataset, **kwargs):  # type: ignore[override]
            self.fitted = True
            self._dataset = dataset
            return "model"

        def predict(self, features):  # type: ignore[override]
            if not self.fitted:
                raise RuntimeError("model not fitted")
            return np.zeros(len(features))

        def save(self, path):  # type: ignore[override]
            return None

    monkeypatch.setitem(supervised.TRAINER_REGISTRY, "dummy", DummyTrainer)
    trainer = supervised.load_trainer("dummy")
    trainer.fit(dataset)
    preds = trainer.predict(dataset.features)
    assert len(preds) == len(dataset.features)
