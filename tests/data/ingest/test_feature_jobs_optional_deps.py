from __future__ import annotations

import datetime as dt
import importlib
import json
import sys

import pytest

from services.ingest.event_ordering import OrderedEvent


def _load_feature_jobs(monkeypatch: pytest.MonkeyPatch):
    module_name = "data.ingest.feature_jobs"
    monkeypatch.setenv("DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("FEATURE_JOBS_ALLOW_SQLITE_FOR_TESTS", "1")
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


def test_feature_writer_persists_to_memory_when_sqlalchemy_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_feature_jobs(monkeypatch)
    writer = module.FeatureWriter(
        engine=module._InMemoryEngine(url="memory://tests"),
        store=None,
        feature_view="features",
    )
    event_ts = dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc)
    payload = {
        "rolling_vwap": 101.5,
        "spread": 0.25,
        "order_book_imbalance": 0.1,
    }

    writer.persist(symbol="BTC-USD", event_ts=event_ts, feature_payload=payload)

    storage = writer.memory_storage
    assert storage is not None
    rows = storage.list_features()
    assert len(rows) == 1
    record = rows[0]
    assert record["symbol"] == "BTC-USD"
    assert record["event_timestamp"] == event_ts
    assert record["created_at"] >= event_ts
    for key, value in payload.items():
        assert record[key] == value


def test_record_late_event_stored_in_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_feature_jobs(monkeypatch)
    writer = module.FeatureWriter(
        engine=module._InMemoryEngine(url="memory://tests"),
        store=None,
        feature_view="features",
    )
    event_ts = dt.datetime(2024, 1, 3, 12, tzinfo=dt.timezone.utc)
    arrival_ts = event_ts + dt.timedelta(milliseconds=25)
    ordered = OrderedEvent(
        stream="md.trades",
        payload={"symbol": "ETH-USD"},
        event_ts=event_ts,
        arrival_ts=arrival_ts,
        lateness_ms=25,
        is_late=True,
    )

    writer.record_late_event("md.trades", ordered)

    storage = writer.memory_storage
    assert storage is not None
    late_records = storage.list_late_events()
    assert len(late_records) == 1
    late = late_records[0]
    assert late["stream"] == "md.trades"
    assert late["symbol"] == "ETH-USD"
    assert late["event_timestamp"] == event_ts
    assert late["arrival_timestamp"] == arrival_ts
    payload = json.loads(late["payload"])
    assert payload["symbol"] == "ETH-USD"
    assert payload["lateness_ms"] == 25
