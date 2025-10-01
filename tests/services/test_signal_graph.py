from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List

import pytest

pytest.importorskip("fastapi", reason="FastAPI is required for signal graph tests")

from fastapi.testclient import TestClient

import signal_graph


class StubRepository:
    def __init__(self, records: Iterable[signal_graph.SignalGraphRecord]):
        self._records = list(records)

    def load(self) -> List[signal_graph.SignalGraphRecord]:
        return list(self._records)


class StubStorage:
    def __init__(self) -> None:
        self.snapshot_key = "snapshots/mock.json"
        self.calls: list[str] = []

    def store_snapshot(self, graph, generated_at):  # type: ignore[no-untyped-def]
        self.calls.append(generated_at.isoformat())
        return self.snapshot_key


def _build_records() -> List[signal_graph.SignalGraphRecord]:
    return [
        signal_graph.SignalGraphRecord(
            trade_id="trade-1",
            model="alpha_v1",
            pnl=50.0,
            confidence=0.8,
            outcome="profit",
            executed_at=datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
            features=[
                signal_graph.FeatureContribution(name="rsi", importance=0.4),
                signal_graph.FeatureContribution(name="volume_trend", importance=-0.2),
            ],
        ),
        signal_graph.SignalGraphRecord(
            trade_id="trade-2",
            model="alpha_v1",
            pnl=-10.0,
            confidence=0.6,
            features=[
                signal_graph.FeatureContribution(name="rsi", importance=-0.6),
                signal_graph.FeatureContribution(name="momentum", importance=0.3),
            ],
        ),
    ]


def _client_with_dataset():
    records = _build_records()
    repository = StubRepository(records)
    storage = StubStorage()
    service = signal_graph.SignalGraphService(
        repository, storage, signal_graph.SignalGraphBuilder()
    )
    signal_graph.GRAPH_SERVICE = service
    client = TestClient(signal_graph.app)
    return client, service


def test_trade_subgraph_includes_feature_and_outcome():
    client, service = _client_with_dataset()
    with client:
        response = client.get("/signals/graph/trade", params={"trade_id": "trade-1"})
    assert response.status_code == 200

    payload = response.json()
    assert payload["trade_id"] == "trade-1"
    assert payload["snapshot_key"] == service.snapshot_key

    node_ids = {node["id"] for node in payload["nodes"]}
    assert "trade::trade-1" in node_ids
    assert "model::alpha_v1" in node_ids
    assert "feature::rsi" in node_ids
    assert "feature::volume_trend" in node_ids
    assert any(node["type"] == "outcome" for node in payload["nodes"])

    trade_node = next(node for node in payload["nodes"] if node["id"] == "trade::trade-1")
    assert trade_node["attributes"]["pnl"] == 50.0
    assert trade_node["attributes"]["confidence"] == 0.8
    assert trade_node["attributes"]["executed_at"].startswith("2024-06-01T12:00:00")


def test_feature_subgraph_returns_linked_trades_and_pnl_impact():
    client, service = _client_with_dataset()
    with client:
        response = client.get("/signals/graph/feature", params={"name": "rsi"})
    assert response.status_code == 200

    payload = response.json()
    assert payload["feature_name"] == "rsi"
    assert len(payload["linked_trades"]) == 2
    trade_ids = {item["trade_id"] for item in payload["linked_trades"]}
    assert trade_ids == {"trade-1", "trade-2"}

    # pnl impact should incorporate both trades: 50 * 0.4 + (-10 * -0.6) = 26.0
    assert payload["pnl_impact"] == 26.0

    feature_node = next(node for node in payload["nodes"] if node["id"] == "feature::rsi")
    assert feature_node["attributes"]["importance_score"] == 0.5


def test_trade_not_found_returns_404():
    client, _ = _client_with_dataset()
    with client:
        response = client.get("/signals/graph/trade", params={"trade_id": "missing"})
    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown trade"

