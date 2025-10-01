"""FastAPI service for exploring signal attribution graphs.

The service builds a directed graph describing how engineered features feed
into models, which then generate trades that realise specific outcomes.  The
graph can be queried to retrieve the neighbourhood around a trade or to
inspect how a feature influences downstream trades and their profit impact.

Snapshots of the constructed graph are written to a filesystem or S3 compatible
object store so the structure can be inspected offline.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import networkx as nx
from fastapi import FastAPI, HTTPException, Query, status
from networkx.readwrite import json_graph
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------


class FeatureContribution(BaseModel):
    """Contribution of an engineered feature to a trade decision."""

    name: str = Field(..., description="Name of the engineered feature")
    importance: float = Field(
        ..., description="Importance contribution for the feature in the trade"
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_importance(cls, values: Mapping[str, Any]) -> Mapping[str, Any]:
        if "importance" not in values and "importance_score" in values:
            values = dict(values)
            values["importance"] = values.pop("importance_score")
        return values


class SignalGraphRecord(BaseModel):
    """Record describing a trade, its generating model and the feature inputs."""

    trade_id: str = Field(..., description="Unique trade identifier")
    model: str = Field(..., description="Model responsible for the trade", alias="model_name")
    pnl: float = Field(..., description="Realised profit and loss for the trade")
    confidence: float = Field(..., ge=0.0, description="Model confidence for the trade")
    features: List[FeatureContribution] = Field(
        ..., description="Feature contributions captured at execution time"
    )
    outcome: str | None = Field(
        None,
        description="Optional trade outcome label (e.g. profit, loss, breakeven)",
    )
    executed_at: datetime | None = Field(
        None, description="Timestamp when the trade executed"
    )

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def _normalise_payload(cls, values: Mapping[str, Any]) -> Mapping[str, Any]:
        if "model" not in values and "model_name" in values:
            values = dict(values)
            values["model"] = values.pop("model_name")
        return values


@dataclass
class FeatureStats:
    total_importance: float = 0.0
    observations: int = 0


@dataclass
class ModelStats:
    total_confidence: float = 0.0
    trades: int = 0


@dataclass
class OutcomeStats:
    total_pnl: float = 0.0
    trades: int = 0


@dataclass
class EdgeStats:
    total: float = 0.0
    observations: int = 0


# ---------------------------------------------------------------------------
# Repository and storage helpers
# ---------------------------------------------------------------------------


class SignalGraphRepository:
    """Repository reading signal graph records from a JSON document."""

    def __init__(self, path: Path):
        self._path = path

    def load(self) -> List[SignalGraphRecord]:
        """Return the trade records stored at the configured location."""

        if not self._path.exists():
            LOGGER.info("Signal graph dataset not found at %s", self._path)
            return []

        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            LOGGER.error("Failed to parse signal graph dataset: %s", exc)
            raise

        if not isinstance(payload, list):
            raise ValueError("Signal graph dataset must be a list of records")

        records: List[SignalGraphRecord] = []
        for raw_record in payload:
            try:
                record = SignalGraphRecord.model_validate(raw_record)
            except ValidationError as exc:
                LOGGER.warning("Skipping invalid signal graph record: %s", exc)
                continue
            records.append(record)

        return records


class GraphSnapshotStorage:
    """Persist signal graph snapshots to filesystem or S3 compatible storage."""

    def __init__(
        self,
        base_path: str | Path | None = None,
        *,
        bucket: str | None = None,
        prefix: str = "signal-graph",
        client: Any | None = None,
    ) -> None:
        self._bucket = bucket or os.getenv("SIGNAL_GRAPH_SNAPSHOT_BUCKET")
        self._prefix = prefix.strip("/") or "signal-graph"
        self._client = client
        self._base_path = Path(
            base_path or os.getenv("SIGNAL_GRAPH_SNAPSHOT_PATH", "/tmp/aether-signal-graph")
        )

        if self._bucket:
            if self._client is None:
                try:  # pragma: no cover - optional dependency in CI
                    import boto3  # type: ignore
                except Exception as exc:  # pragma: no cover - defensive logging
                    raise RuntimeError(
                        "boto3 is required for S3 snapshot storage"
                    ) from exc
                self._client = boto3.client("s3")  # type: ignore[assignment]
        else:
            self._base_path.mkdir(parents=True, exist_ok=True)

    def store_snapshot(self, graph: nx.DiGraph, generated_at: datetime) -> str:
        """Persist ``graph`` and return the object key used for storage."""

        key = f"{self._prefix}/graph-{generated_at:%Y%m%dT%H%M%SZ}.json"
        payload = json.dumps(json_graph.node_link_data(graph), separators=(",", ":"))

        if self._bucket:
            assert self._client is not None  # for mypy
            self._client.put_object(  # type: ignore[call-arg]
                Bucket=self._bucket,
                Key=key,
                Body=payload.encode("utf-8"),
                ContentType="application/json",
            )
        else:
            target_path = self._base_path / key
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(payload, encoding="utf-8")

        LOGGER.info("Stored signal graph snapshot at %s", key)
        return key


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------


def _feature_node_id(name: str) -> str:
    return f"feature::{name}"


def _model_node_id(name: str) -> str:
    return f"model::{name}"


def _trade_node_id(trade_id: str) -> str:
    return f"trade::{trade_id}"


def _outcome_node_id(label: str) -> str:
    return f"outcome::{label}"


class SignalGraphBuilder:
    """Construct the directed feature → model → trade → outcome graph."""

    def build(self, records: Iterable[SignalGraphRecord]) -> nx.DiGraph:
        graph = nx.DiGraph()

        feature_stats: Dict[str, FeatureStats] = {}
        model_stats: Dict[str, ModelStats] = {}
        outcome_stats: Dict[str, OutcomeStats] = {}
        feature_model_stats: Dict[tuple[str, str], EdgeStats] = {}

        for record in records:
            trade_node = _trade_node_id(record.trade_id)
            model_node = _model_node_id(record.model)

            executed_at = (
                record.executed_at.isoformat()
                if isinstance(record.executed_at, datetime)
                else None
            )

            graph.add_node(
                trade_node,
                type="trade",
                trade_id=record.trade_id,
                pnl=float(record.pnl),
                confidence=float(record.confidence),
                model=record.model,
                executed_at=executed_at,
                features={},
            )

            stats = model_stats.setdefault(record.model, ModelStats())
            stats.trades += 1
            stats.total_confidence += float(record.confidence)

            graph.add_node(model_node, type="model", name=record.model)
            graph.add_edge(
                model_node,
                trade_node,
                relation="model_to_trade",
                confidence=float(record.confidence),
            )

            outcome_label = record.outcome or _default_outcome(record.pnl)
            outcome_node = _outcome_node_id(outcome_label)
            outcome_stat = outcome_stats.setdefault(outcome_label, OutcomeStats())
            outcome_stat.trades += 1
            outcome_stat.total_pnl += float(record.pnl)

            graph.add_node(outcome_node, type="outcome", label=outcome_label)
            graph.add_edge(
                trade_node,
                outcome_node,
                relation="trade_to_outcome",
                pnl=float(record.pnl),
            )

            for feature in record.features:
                feature_node = _feature_node_id(feature.name)
                feature_stat = feature_stats.setdefault(feature.name, FeatureStats())
                feature_stat.total_importance += abs(float(feature.importance))
                feature_stat.observations += 1

                edge_stat = feature_model_stats.setdefault(
                    (feature.name, record.model), EdgeStats()
                )
                edge_stat.total += float(feature.importance)
                edge_stat.observations += 1

                graph.add_node(feature_node, type="feature", name=feature.name)
                graph.add_edge(
                    feature_node,
                    model_node,
                    relation="feature_to_model",
                )

                graph.nodes[trade_node]["features"][feature.name] = float(
                    feature.importance
                )

        for model_name, stats in model_stats.items():
            node_id = _model_node_id(model_name)
            average_confidence = (
                stats.total_confidence / stats.trades if stats.trades else 0.0
            )
            graph.nodes[node_id]["trade_count"] = stats.trades
            graph.nodes[node_id]["average_confidence"] = average_confidence

        for feature_name, stats in feature_stats.items():
            node_id = _feature_node_id(feature_name)
            importance = (
                stats.total_importance / stats.observations
                if stats.observations
                else 0.0
            )
            graph.nodes[node_id]["importance_score"] = importance
            graph.nodes[node_id]["observation_count"] = stats.observations

        for outcome_label, stats in outcome_stats.items():
            node_id = _outcome_node_id(outcome_label)
            graph.nodes[node_id]["trade_count"] = stats.trades
            graph.nodes[node_id]["total_pnl"] = stats.total_pnl

        for (feature_name, model_name), stats in feature_model_stats.items():
            edge_attrs = graph.get_edge_data(
                _feature_node_id(feature_name), _model_node_id(model_name)
            )
            if edge_attrs is None:  # pragma: no cover - defensive guard
                continue
            average_importance = (
                stats.total / stats.observations if stats.observations else 0.0
            )
            edge_attrs["importance"] = average_importance
            edge_attrs["observations"] = stats.observations

        return graph


def _default_outcome(pnl: float) -> str:
    if pnl > 0:
        return "profit"
    if pnl < 0:
        return "loss"
    return "flat"


# ---------------------------------------------------------------------------
# API payloads
# ---------------------------------------------------------------------------


def _normalise_attributes(data: Mapping[str, Any]) -> Dict[str, Any]:
    normalised: Dict[str, Any] = {}
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, datetime):
            normalised[key] = value.isoformat()
        else:
            normalised[key] = value
    return normalised


class GraphNodePayload(BaseModel):
    """Node returned in API responses."""

    id: str
    type: str
    label: str
    attributes: Dict[str, Any] = Field(default_factory=dict)


class GraphEdgePayload(BaseModel):
    """Edge returned in API responses."""

    source: str
    target: str
    relation: str
    attributes: Dict[str, Any] = Field(default_factory=dict)


class SubgraphResponse(BaseModel):
    """Base response containing a graph fragment."""

    nodes: List[GraphNodePayload]
    edges: List[GraphEdgePayload]
    generated_at: datetime | None
    snapshot_key: str | None


class TradeSubgraphResponse(SubgraphResponse):
    trade_id: str


class FeatureTradeSummary(BaseModel):
    """Summary of a trade linked to a feature."""

    trade_id: str
    pnl: float
    confidence: float
    model: str
    feature_importance: float


class FeatureSubgraphResponse(SubgraphResponse):
    feature_name: str
    pnl_impact: float
    linked_trades: List[FeatureTradeSummary]


# ---------------------------------------------------------------------------
# Service layer
# ---------------------------------------------------------------------------


class SignalGraphService:
    """Coordinate graph construction, snapshots, and query helpers."""

    def __init__(
        self,
        repository: SignalGraphRepository,
        storage: GraphSnapshotStorage,
        builder: SignalGraphBuilder | None = None,
    ) -> None:
        self._repository = repository
        self._storage = storage
        self._builder = builder or SignalGraphBuilder()
        self._graph: nx.DiGraph = nx.DiGraph()
        self._generated_at: datetime | None = None
        self._snapshot_key: str | None = None

    def refresh_graph(self) -> None:
        """Reload records, rebuild the graph, and persist a snapshot."""

        records = self._repository.load()
        LOGGER.info("Building signal attribution graph from %d records", len(records))
        self._graph = self._builder.build(records)
        self._generated_at = datetime.now(timezone.utc)

        try:
            self._snapshot_key = self._storage.store_snapshot(
                self._graph, self._generated_at
            )
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to persist signal graph snapshot")
            self._snapshot_key = None

    @property
    def generated_at(self) -> datetime | None:
        return self._generated_at

    @property
    def snapshot_key(self) -> str | None:
        return self._snapshot_key

    def trade_subgraph(self, trade_id: str) -> TradeSubgraphResponse:
        node_id = _trade_node_id(trade_id)
        if not self._graph.has_node(node_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown trade")

        nodes = {node_id}
        edges: Dict[tuple[str, str], Mapping[str, Any]] = {}
        trade_features: Mapping[str, float] = self._graph.nodes[node_id].get(
            "features", {}
        )

        for model_node in self._graph.predecessors(node_id):
            nodes.add(model_node)
            edges[(model_node, node_id)] = self._graph[model_node][node_id]
            for feature_node in self._graph.predecessors(model_node):
                feature_name = self._graph.nodes[feature_node].get("name")
                if feature_name not in trade_features:
                    continue
                nodes.add(feature_node)
                edges[(feature_node, model_node)] = self._graph[feature_node][model_node]

        for outcome_node in self._graph.successors(node_id):
            nodes.add(outcome_node)
            edges[(node_id, outcome_node)] = self._graph[node_id][outcome_node]

        node_payloads = [self._node_payload(node) for node in sorted(nodes)]
        edge_payloads = [
            self._edge_payload(source, target, edges[(source, target)])
            for source, target in sorted(edges)
        ]

        return TradeSubgraphResponse(
            trade_id=trade_id,
            nodes=node_payloads,
            edges=edge_payloads,
            generated_at=self._generated_at,
            snapshot_key=self._snapshot_key,
        )

    def feature_subgraph(self, feature_name: str) -> FeatureSubgraphResponse:
        node_id = _feature_node_id(feature_name)
        if not self._graph.has_node(node_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown feature")

        nodes = {node_id}
        edges: Dict[tuple[str, str], Mapping[str, Any]] = {}
        linked_trades: List[FeatureTradeSummary] = []
        pnl_impact = 0.0

        for model_node in self._graph.successors(node_id):
            nodes.add(model_node)
            edges[(node_id, model_node)] = self._graph[node_id][model_node]

            for trade_node in self._graph.successors(model_node):
                trade_data = self._graph.nodes[trade_node]
                trade_features = trade_data.get("features", {})
                if feature_name not in trade_features:
                    continue

                importance = float(trade_features.get(feature_name, 0.0))
                nodes.add(trade_node)
                edges[(model_node, trade_node)] = self._graph[model_node][trade_node]

                trade_pnl = float(trade_data.get("pnl", 0.0))
                pnl_impact += trade_pnl * importance

                linked_trades.append(
                    FeatureTradeSummary(
                        trade_id=trade_data.get("trade_id", trade_node.split("::", 1)[-1]),
                        pnl=trade_pnl,
                        confidence=float(trade_data.get("confidence", 0.0)),
                        model=str(trade_data.get("model", "")),
                        feature_importance=importance,
                    )
                )

                for outcome_node in self._graph.successors(trade_node):
                    nodes.add(outcome_node)
                    edges[(trade_node, outcome_node)] = self._graph[trade_node][outcome_node]

        node_payloads = [self._node_payload(node) for node in sorted(nodes)]
        edge_payloads = [
            self._edge_payload(source, target, edges[(source, target)])
            for source, target in sorted(edges)
        ]

        return FeatureSubgraphResponse(
            feature_name=feature_name,
            pnl_impact=pnl_impact,
            linked_trades=linked_trades,
            nodes=node_payloads,
            edges=edge_payloads,
            generated_at=self._generated_at,
            snapshot_key=self._snapshot_key,
        )

    def _node_payload(self, node_id: str) -> GraphNodePayload:
        data = dict(self._graph.nodes[node_id])
        node_type = data.pop("type", "unknown")
        label = str(
            data.get("name")
            or data.get("trade_id")
            or data.get("label")
            or node_id
        )
        return GraphNodePayload(
            id=node_id,
            type=node_type,
            label=label,
            attributes=_normalise_attributes(data),
        )

    def _edge_payload(
        self, source: str, target: str, data: Mapping[str, Any]
    ) -> GraphEdgePayload:
        relation = str(data.get("relation", ""))
        attributes = {key: value for key, value in data.items() if key != "relation"}
        return GraphEdgePayload(
            source=source,
            target=target,
            relation=relation,
            attributes=_normalise_attributes(attributes),
        )


# ---------------------------------------------------------------------------
# FastAPI wiring
# ---------------------------------------------------------------------------


DATA_PATH = Path(os.getenv("SIGNAL_GRAPH_DATA_PATH", "data/signals/trades.json"))
REPOSITORY = SignalGraphRepository(DATA_PATH)
SNAPSHOT_STORAGE = GraphSnapshotStorage()
GRAPH_SERVICE = SignalGraphService(REPOSITORY, SNAPSHOT_STORAGE)

app = FastAPI(title="Signal Graph Service", version="1.0.0")


@app.on_event("startup")
def _refresh_graph() -> None:
    GRAPH_SERVICE.refresh_graph()


@app.get("/signals/graph/trade", response_model=TradeSubgraphResponse)
def get_trade_subgraph(
    trade_id: str = Query(..., min_length=1, max_length=128, description="Trade identifier"),
) -> TradeSubgraphResponse:
    """Return the feature → model → trade → outcome subgraph for ``trade_id``."""

    return GRAPH_SERVICE.trade_subgraph(trade_id)


@app.get("/signals/graph/feature", response_model=FeatureSubgraphResponse)
def get_feature_subgraph(
    name: str = Query(..., min_length=1, max_length=128, description="Feature name"),
) -> FeatureSubgraphResponse:
    """Return trades connected to ``name`` and the estimated PnL impact."""

    return GRAPH_SERVICE.feature_subgraph(name)


__all__ = [
    "FeatureContribution",
    "FeatureSubgraphResponse",
    "GraphSnapshotStorage",
    "GraphEdgePayload",
    "GraphNodePayload",
    "SignalGraphRepository",
    "SignalGraphRecord",
    "SignalGraphService",
    "SignalGraphBuilder",
    "TradeSubgraphResponse",
    "FeatureTradeSummary",
    "app",
]

