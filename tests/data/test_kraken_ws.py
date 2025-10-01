"""Integration-style tests for Kraken WebSocket ingestion utilities."""
from __future__ import annotations

from datetime import timezone

import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy.engine import create_mock_engine

from data.ingest.kraken_ws import flatten_updates, persist_updates


def test_flatten_snapshot_records_align_with_orderbook_schema():
    payload = {
        "as": [["50000.1", "0.5", "1616666666.123456"]],
        "bs": [["49999.9", "1.25", "1616666666.123456"]],
        "sequence": 42,
    }

    events = flatten_updates("XBT/USD", payload)

    assert len(events) == 2
    for event in events:
        assert event.symbol == "XBT/USD"
        assert event.ts.tzinfo == timezone.utc
        assert event.side in {"ask", "bid"}
        assert event.action == "snapshot"
        assert event.sequence == 42
        assert event.meta["payload"] == payload
        assert event.meta["side"] == event.side
        assert event.meta["price"] == event.price
        assert event.meta["size"] == event.size


def test_persist_updates_upserts_against_primary_key():
    payload = {
        "a": [["50010.1", "0.0", "1616666667.654321"]],
        "sequence": 43,
    }
    events = flatten_updates("XBT/USD", payload)

    executed_statements = []

    def capture(sql, *multiparams, **params):
        executed_statements.append((sql, multiparams, params))

    engine = create_mock_engine("postgresql://", capture)
    persist_updates(engine, events)

    assert executed_statements, "persist_updates should issue an insert statement"
    sql_text, parameters, _ = executed_statements[0]
    compiled_sql = str(sql_text)
    assert "INSERT INTO orderbook_events" in compiled_sql
    assert 'ON CONFLICT ("symbol", "ts", "side", "price") DO UPDATE' in compiled_sql

    params = parameters[0]
    assert params["symbol"] == "XBT/USD"
    assert params["side"] == "ask"
    assert params["price"] == 50010.1
    assert params["action"] == "update"
    assert params["sequence"] == 43
