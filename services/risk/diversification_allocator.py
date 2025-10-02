"""Diversification-aware portfolio allocator used by the risk service.

The allocator maintains target portfolio weights that respect concentration
constraints, bucket level targets, sector caps and simple correlation limits.
It also provides helper utilities that can resize or reroute new policy intents
when they would otherwise violate diversification limits.  A lightweight
persistence layer is included so the latest targets and generated actions can be
audited through the risk API layer.
"""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, Iterator, Literal, Mapping, Optional, Sequence

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, Float, Integer, MetaData, String, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from services.common.security import require_admin_account


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database plumbing
# ---------------------------------------------------------------------------


DEFAULT_DATABASE_URL = "sqlite:///./diversification.db"


def _create_engine(url: str = DEFAULT_DATABASE_URL) -> Engine:
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, future=True, connect_args=connect_args)


ENGINE: Engine = _create_engine()
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

metadata_obj = MetaData()
Base = declarative_base(metadata=metadata_obj)


class DiversificationTargetRecord(Base):
    """Persisted target weights per symbol for auditability."""

    __tablename__ = "diversification_targets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False, index=True)
    symbol = Column(String, nullable=False)
    bucket = Column(String, nullable=True)
    weight = Column(Float, nullable=False)
    expected_edge_bps = Column(Float, nullable=True)
    created_at = Column(
        DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc)
    )


class DiversificationActionRecord(Base):
    """Generic log capturing rebalance or adjustment events."""

    __tablename__ = "diversification_actions_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False, index=True)
    action_type = Column(String, nullable=False)
    payload_json = Column(String, nullable=False)
    created_at = Column(
        DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc)
    )


def init_diversification_storage(engine: Engine | None = None) -> None:
    """Ensure the diversification tables exist."""

    target_engine = engine or ENGINE
    Base.metadata.create_all(bind=target_engine, checkfirst=True)


# Initialise the default on import.
init_diversification_storage()


# ---------------------------------------------------------------------------
# Configuration models
# ---------------------------------------------------------------------------


DEFAULT_BUCKETS: tuple[dict[str, object], ...] = (
    {"name": "btc_core", "target_pct": 0.4, "symbols": ("BTC-USD",), "sector": "layer1"},
    {
        "name": "top_cap_alts",
        "target_pct": 0.35,
        "symbols": ("ETH-USD", "SOL-USD"),
        "sector": "layer1",
    },
    {
        "name": "growth_alts",
        "target_pct": 0.25,
        "symbols": ("ADA-USD", "MATIC-USD"),
        "sector": "alts",
    },
)


@dataclass(frozen=True)
class BucketConfig:
    """Portfolio bucket describing a target weight range."""

    name: str
    target_pct: float
    symbols: tuple[str, ...]
    sector: str

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "BucketConfig":
        name = str(payload.get("name") or "bucket")
        target = float(payload.get("target_pct") or 0.0)
        symbols_payload = payload.get("symbols") or ()
        if isinstance(symbols_payload, str):
            symbols = (symbols_payload.upper(),)
        else:
            symbols = tuple(str(symbol).upper() for symbol in symbols_payload)
        sector = str(payload.get("sector") or name)
        return cls(name=name, target_pct=max(target, 0.0), symbols=symbols, sector=sector)


@dataclass(frozen=True)
class DiversificationSettings:
    """Normalised diversification configuration loaded from settings."""

    max_concentration_pct_per_asset: float
    max_sector_pct: float
    correlation_threshold: float
    correlation_penalty: float
    rebalance_threshold_pct: float
    buckets: tuple[BucketConfig, ...]

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "DiversificationSettings":
        buckets_payload = payload.get("buckets")
        if not buckets_payload:
            buckets_iter: Iterable[Mapping[str, object]] = DEFAULT_BUCKETS
        else:
            if isinstance(buckets_payload, Mapping):
                buckets_iter = buckets_payload.values()
            else:
                buckets_iter = buckets_payload  # type: ignore[assignment]

        buckets = tuple(BucketConfig.from_mapping(entry) for entry in buckets_iter)
        max_concentration = float(payload.get("max_concentration_pct_per_asset") or 0.25)
        max_sector_pct = float(payload.get("max_sector_pct") or 0.5)
        correlation_threshold = float(payload.get("correlation_threshold") or 0.85)
        correlation_penalty = float(payload.get("correlation_penalty") or 0.05)
        rebalance_threshold_pct = float(payload.get("rebalance_threshold_pct") or 0.02)
        return cls(
            max_concentration_pct_per_asset=max(0.0, min(max_concentration, 1.0)),
            max_sector_pct=max(0.0, min(max_sector_pct, 1.0)),
            correlation_threshold=max(0.0, min(correlation_threshold, 1.0)),
            correlation_penalty=max(0.0, min(correlation_penalty, 1.0)),
            rebalance_threshold_pct=max(0.0, min(rebalance_threshold_pct, 1.0)),
            buckets=buckets,
        )


# ---------------------------------------------------------------------------
# Domain objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PolicyIntent:
    """Simplified policy intent representation used for diversification checks."""

    symbol: str
    side: Literal["BUY", "SELL"]
    notional: float
    expected_edge_bps: float | None = None


@dataclass(frozen=True)
class DiversificationAdjustment:
    """Result returned by :meth:`DiversificationAllocator.adjust_intent_for_diversification`."""

    approved_symbol: str
    notional: float
    reduced: bool
    rerouted: bool
    reason: str | None


@dataclass(frozen=True)
class RebalanceInstruction:
    """Trade instruction required to move a symbol toward its target weight."""

    symbol: str
    side: Literal["BUY", "SELL"]
    notional: float
    current_weight: float
    target_weight: float
    expected_edge_bps: float | None
    fee_bps: float | None


@dataclass(frozen=True)
class DiversificationTargets:
    """Container for computed diversification targets."""

    account_id: str
    timestamp: datetime
    weights: Dict[str, float]
    buckets: Dict[str, str]
    expected_edges: Dict[str, float]


@dataclass(frozen=True)
class DiversificationRebalancePlan:
    """Describes the recommended rebalance actions for an account."""

    account_id: str
    timestamp: datetime
    simulation: bool
    instructions: tuple[RebalanceInstruction, ...]


# ---------------------------------------------------------------------------
# Allocator implementation
# ---------------------------------------------------------------------------


CorrelationLoader = Callable[[Sequence[str]], Mapping[str, Mapping[str, float]]]


class DiversificationAllocator:
    """Compute and persist diversification targets for an account."""

    def __init__(
        self,
        account_id: str,
        *,
        timescale: TimescaleAdapter | None = None,
        universe: RedisFeastAdapter | None = None,
        correlation_loader: CorrelationLoader | None = None,
        session_factory: Callable[[], Session] | None = None,
        enable_persistence: bool = True,
    ) -> None:
        self.account_id = account_id
        self.timescale = timescale or TimescaleAdapter(account_id=account_id)
        self.universe = universe or RedisFeastAdapter(account_id=account_id)
        self._correlation_loader = correlation_loader or self._load_correlation_from_config
        self._session_factory = session_factory or SessionLocal
        self._enable_persistence = enable_persistence

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def compute_targets(self, *, persist: bool = True) -> DiversificationTargets:
        """Compute and optionally persist target weights for the account."""

        settings = self._load_settings()
        universe = self._approved_symbols()
        if not universe:
            raise ValueError("Trading universe is empty; unable to compute diversification targets")

        bucket_symbols = self._bucket_symbol_map(settings, universe)
        expected_edges = {
            symbol: self._expected_edge(symbol)
            for symbols in bucket_symbols.values()
            for symbol in symbols
        }
        symbol_sector = self._symbol_sector_map(settings, bucket_symbols)
        weights = self._solve_allocation(
            bucket_symbols, expected_edges, symbol_sector, settings
        )

        timestamp = datetime.now(timezone.utc)
        buckets = {symbol: bucket for bucket, symbols in bucket_symbols.items() for symbol in symbols}
        if "CASH" in weights:
            buckets["CASH"] = "cash"
            expected_edges.setdefault("CASH", 0.0)

        result = DiversificationTargets(
            account_id=self.account_id,
            timestamp=timestamp,
            weights=weights,
            buckets=buckets,
            expected_edges={symbol: float(edge) for symbol, edge in expected_edges.items()},
        )

        if persist and self._enable_persistence:
            self._persist_targets(result)

        return result

    def adjust_intent_for_diversification(
        self, intent: PolicyIntent
    ) -> DiversificationAdjustment:
        """Scale or reroute an intent if it breaches diversification limits."""

        settings = self._load_settings()
        nav = self._account_nav()
        if nav <= 0:
            return DiversificationAdjustment(
                approved_symbol=intent.symbol,
                notional=intent.notional,
                reduced=False,
                rerouted=False,
                reason="nav_unavailable",
            )

        exposures = self.timescale.open_positions()
        current_exposure = float(exposures.get(intent.symbol, 0.0))
        max_concentration = settings.max_concentration_pct_per_asset

        if intent.side == "SELL":
            # Selling reduces exposure which always helps concentration.
            return DiversificationAdjustment(
                approved_symbol=intent.symbol,
                notional=intent.notional,
                reduced=False,
                rerouted=False,
                reason=None,
            )

        max_notional = max_concentration * nav
        current_abs = abs(current_exposure)
        projected_full = current_abs + intent.notional
        projected_weight = projected_full / nav if nav else 0.0

        if projected_weight <= max_concentration:
            return DiversificationAdjustment(
                approved_symbol=intent.symbol,
                notional=intent.notional,
                reduced=False,
                rerouted=False,
                reason=None,
            )

        allowable = max(max_notional - current_abs, 0.0)
        reduced_notional = min(intent.notional, allowable)
        if reduced_notional < 0:
            reduced_notional = 0.0

        reason = (
            f"max_concentration_exceeded:{intent.symbol}:{projected_weight:.4f}>{max_concentration:.4f}"
        )

        rerouted_symbol = None
        if reduced_notional > 0:
            rerouted_symbol = self._find_reroute_candidate(
                intent, reduced_notional, exposures, nav, settings
            )
        rerouted = rerouted_symbol is not None and rerouted_symbol != intent.symbol
        final_symbol = rerouted_symbol or intent.symbol

        self._log_action(
            "intent_adjustment",
            {
                "symbol": intent.symbol,
                "side": intent.side,
                "original_notional": intent.notional,
                "adjusted_notional": reduced_notional,
                "rerouted_symbol": final_symbol,
                "reason": reason,
            },
        )

        return DiversificationAdjustment(
            approved_symbol=final_symbol,
            notional=reduced_notional,
            reduced=True,
            rerouted=rerouted,
            reason=reason,
        )

    def generate_rebalance_plan(self) -> DiversificationRebalancePlan:
        """Compute a rebalance plan that nudges exposures toward the targets."""

        targets = self.compute_targets()
        nav = self._account_nav()
        if nav <= 0:
            raise ValueError("Account NAV must be positive to compute rebalance plan")

        exposures = self.timescale.open_positions()
        current_weights = {
            symbol: notional / nav for symbol, notional in exposures.items() if nav > 0
        }
        instructions = self._build_rebalance_instructions(
            targets, current_weights, nav, exposures
        )

        plan = DiversificationRebalancePlan(
            account_id=self.account_id,
            timestamp=targets.timestamp,
            simulation=self._simulation_active(),
            instructions=tuple(instructions),
        )

        self._log_action(
            "rebalance_plan",
            {
                "timestamp": plan.timestamp.isoformat(),
                "simulation": plan.simulation,
                "instructions": [instruction.__dict__ for instruction in plan.instructions],
            },
        )

        return plan

    # ------------------------------------------------------------------
    # Allocation helpers
    # ------------------------------------------------------------------
    def _solve_allocation(
        self,
        bucket_symbols: Mapping[str, Sequence[str]],
        expected_edges: Mapping[str, float],
        symbol_sector: Mapping[str, str],
        settings: DiversificationSettings,
    ) -> Dict[str, float]:
        weights: Dict[str, float] = {
            symbol: 0.0 for symbols in bucket_symbols.values() for symbol in symbols
        }

        bucket_configs = {
            entry.name: entry
            for entry in settings.buckets
            if entry.name in bucket_symbols
        }

        sector_totals: Dict[str, float] = defaultdict(float)
        for entry in bucket_configs.values():
            sector_totals[entry.sector] += entry.target_pct

        sector_scaling: Dict[str, float] = {}
        for sector, total in sector_totals.items():
            if total <= 0:
                sector_scaling[sector] = 0.0
            elif total <= settings.max_sector_pct:
                sector_scaling[sector] = 1.0
            else:
                sector_scaling[sector] = settings.max_sector_pct / total

        bucket_targets: Dict[str, float] = {}
        for bucket, symbols in bucket_symbols.items():
            config = bucket_configs.get(bucket)
            if config is None:
                continue
            scale = sector_scaling.get(config.sector, 1.0)
            bucket_targets[bucket] = max(config.target_pct * scale, 0.0)

        bucket_total = sum(bucket_targets.values())
        if bucket_total <= 0:
            raise ValueError("No diversification buckets with positive targets configured")
        if bucket_total > 1.0:
            bucket_targets = {
                bucket: value / bucket_total for bucket, value in bucket_targets.items()
            }
            bucket_total = 1.0

        sector_usage: Dict[str, float] = defaultdict(float)
        remaining = max(1.0 - bucket_total, 0.0)
        unallocated = 0.0

        for bucket, symbols in bucket_symbols.items():
            config = bucket_configs.get(bucket)
            if config is None:
                continue
            target = bucket_targets.get(bucket, 0.0)
            if not symbols or target <= 0:
                remaining += max(target, 0.0)
                continue

            desired = target
            equal_share = desired / len(symbols)
            for symbol in symbols:
                sector = symbol_sector.get(symbol, bucket)
                sector_capacity = max(settings.max_sector_pct - sector_usage[sector], 0.0)
                asset_capacity = max(
                    settings.max_concentration_pct_per_asset - weights[symbol], 0.0
                )
                allocation = min(equal_share, sector_capacity, asset_capacity, desired)
                if allocation <= 0:
                    continue
                weights[symbol] += allocation
                sector_usage[sector] += allocation
                desired -= allocation

            if desired > 1e-9:
                ordered = sorted(
                    symbols,
                    key=lambda s: expected_edges.get(s, 0.0),
                    reverse=True,
                )
                while desired > 1e-9:
                    progress = False
                    for symbol in ordered:
                        sector = symbol_sector.get(symbol, bucket)
                        sector_capacity = max(
                            settings.max_sector_pct - sector_usage[sector], 0.0
                        )
                        asset_capacity = max(
                            settings.max_concentration_pct_per_asset - weights[symbol], 0.0
                        )
                        if sector_capacity <= 1e-12 or asset_capacity <= 1e-12:
                            continue
                        allocation = min(sector_capacity, asset_capacity, desired)
                        if allocation <= 0:
                            continue
                        weights[symbol] += allocation
                        sector_usage[sector] += allocation
                        desired -= allocation
                        progress = True
                        if desired <= 1e-9:
                            break
                    if not progress:
                        break

            if desired > 1e-9:
                unallocated += desired

        remaining += unallocated

        if remaining > 1e-9:
            ordered_symbols = sorted(
                weights.keys(), key=lambda s: expected_edges.get(s, 0.0), reverse=True
            )
            while remaining > 1e-9:
                progress = False
                for symbol in ordered_symbols:
                    sector = symbol_sector.get(symbol, symbol)
                    sector_capacity = max(
                        settings.max_sector_pct - sector_usage[sector], 0.0
                    )
                    asset_capacity = max(
                        settings.max_concentration_pct_per_asset - weights[symbol], 0.0
                    )
                    if sector_capacity <= 1e-12 or asset_capacity <= 1e-12:
                        continue
                    allocation = min(sector_capacity, asset_capacity, remaining)
                    if allocation <= 0:
                        continue
                    weights[symbol] += allocation
                    sector_usage[sector] += allocation
                    remaining -= allocation
                    progress = True
                    if remaining <= 1e-9:
                        break
                if not progress:
                    break

        weights = self._apply_correlation_penalty(
            weights, expected_edges, symbol_sector, settings
        )

        total_weight = sum(weights.values())
        if total_weight > 1.0 + 1e-9:
            scale = 1.0 / total_weight
            for symbol in list(weights):
                weights[symbol] *= scale
            total_weight = sum(weights.values())

        if total_weight < 1.0 - 1e-9:
            weights["CASH"] = 1.0 - total_weight

        return weights

    def _apply_correlation_penalty(
        self,
        weights: Mapping[str, float],
        expected_edges: Mapping[str, float],
        symbol_sector: Mapping[str, str],
        settings: DiversificationSettings,
    ) -> Dict[str, float]:
        symbols = list(weights.keys())
        matrix = self._correlation_loader(symbols)
        penalties: Dict[str, float] = defaultdict(float)
        total_penalty = 0.0

        for idx, sym_a in enumerate(symbols):
            for sym_b in symbols[idx + 1 :]:
                correlation = matrix.get(sym_a, {}).get(sym_b)
                if correlation is None:
                    continue
                if correlation <= settings.correlation_threshold:
                    continue
                weaker = sym_a
                if expected_edges.get(sym_b, 0.0) < expected_edges.get(sym_a, 0.0):
                    weaker = sym_b
                magnitude = (correlation - settings.correlation_threshold) / max(
                    1.0 - settings.correlation_threshold, 1e-6
                )
                penalty = weights[weaker] * settings.correlation_penalty * magnitude
                if penalty <= 0:
                    continue
                penalties[weaker] += penalty
                total_penalty += penalty

        adjusted = dict(weights)
        for symbol, penalty in penalties.items():
            adjusted[symbol] = max(adjusted[symbol] - penalty, 0.0)

        if total_penalty <= 0:
            return adjusted

        sector_usage: Dict[str, float] = defaultdict(float)
        for symbol, weight in adjusted.items():
            sector = symbol_sector.get(symbol, symbol)
            sector_usage[sector] += weight

        ordered = sorted(adjusted, key=lambda s: expected_edges.get(s, 0.0), reverse=True)
        remaining = total_penalty
        while remaining > 1e-9:
            progress = False
            for symbol in ordered:
                sector = symbol_sector.get(symbol, symbol)
                sector_capacity = max(settings.max_sector_pct - sector_usage[sector], 0.0)
                asset_capacity = max(
                    settings.max_concentration_pct_per_asset - adjusted[symbol], 0.0
                )
                if sector_capacity <= 1e-12 or asset_capacity <= 1e-12:
                    continue
                allocation = min(sector_capacity, asset_capacity, remaining)
                if allocation <= 0:
                    continue
                adjusted[symbol] += allocation
                sector_usage[sector] += allocation
                remaining -= allocation
                progress = True
                if remaining <= 1e-9:
                    break
            if not progress:
                break

        return adjusted

    def _build_rebalance_instructions(
        self,
        targets: DiversificationTargets,
        current_weights: Mapping[str, float],
        nav: float,
        exposures: Mapping[str, float],
    ) -> Iterator[RebalanceInstruction]:
        settings = self._load_settings()
        threshold = settings.rebalance_threshold_pct
        for symbol, target_weight in targets.weights.items():
            if symbol == "CASH":
                continue
            current_weight = current_weights.get(symbol, 0.0)
            delta = target_weight - current_weight
            if abs(delta) < threshold:
                continue
            expected_edge = targets.expected_edges.get(symbol)
            fee_bps = self._fee_bps(symbol)
            if expected_edge is not None and fee_bps is not None:
                if expected_edge <= fee_bps:
                    logger.info(
                        "Skipping rebalance for %s due to fee>=edge (%.2f<=%.2f)",
                        symbol,
                        expected_edge,
                        fee_bps,
                    )
                    continue

            notional_change = abs(delta) * nav
            side: Literal["BUY", "SELL"] = "BUY" if delta > 0 else "SELL"

            # Avoid flipping direction if we already have exposure that would be reduced.
            if side == "SELL" and symbol not in exposures:
                continue

            yield RebalanceInstruction(
                symbol=symbol,
                side=side,
                notional=notional_change,
                current_weight=current_weight,
                target_weight=target_weight,
                expected_edge_bps=expected_edge,
                fee_bps=fee_bps,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_settings(self) -> DiversificationSettings:
        config = self.timescale.load_risk_config()
        payload = config.get("diversification")
        if not isinstance(payload, Mapping):
            payload = {}
        return DiversificationSettings.from_mapping(payload)

    def _approved_symbols(self) -> Sequence[str]:
        universe = self.universe.approved_instruments()
        return [symbol.upper() for symbol in universe]

    def _bucket_symbol_map(
        self, settings: DiversificationSettings, universe: Sequence[str]
    ) -> Dict[str, list[str]]:
        available = set(universe)
        bucket_symbols: Dict[str, list[str]] = {}
        for bucket in settings.buckets:
            eligible = [symbol for symbol in bucket.symbols if symbol in available]
            if not eligible:
                continue
            bucket_symbols[bucket.name] = eligible
        if not bucket_symbols:
            raise ValueError("No eligible symbols found for diversification buckets")
        return bucket_symbols

    def _symbol_sector_map(
        self,
        settings: DiversificationSettings,
        bucket_symbols: Mapping[str, Sequence[str]],
    ) -> Dict[str, str]:
        lookup = {entry.name: entry.sector for entry in settings.buckets}
        mapping: Dict[str, str] = {}
        for bucket, symbols in bucket_symbols.items():
            sector = lookup.get(bucket, bucket)
            for symbol in symbols:
                mapping[symbol] = sector
        return mapping

    def _expected_edge(self, symbol: str) -> float:
        payload = self.universe.fetch_online_features(symbol)
        if not isinstance(payload, Mapping):
            return 0.0
        value = payload.get("expected_edge_bps")
        if value is None and isinstance(payload.get("state"), Mapping):
            value = payload["state"].get("expected_edge_bps")
        try:
            return float(value) if value is not None else 0.0
        except (TypeError, ValueError):  # pragma: no cover - defensive casting
            return 0.0

    def _fee_bps(self, symbol: str) -> float | None:
        override = self.universe.fee_override(symbol)
        if not override:
            return None
        taker = override.get("taker")
        try:
            return float(taker) if taker is not None else None
        except (TypeError, ValueError):  # pragma: no cover - defensive casting
            return None

    def _account_nav(self) -> float:
        config = self.timescale.load_risk_config()
        try:
            return float(config.get("nav", 0.0))
        except (TypeError, ValueError):  # pragma: no cover - defensive casting
            return 0.0

    def _simulation_active(self) -> bool:
        config = self.timescale.load_risk_config()
        sim_payload = config.get("simulation") or config.get("sim") or {}
        if isinstance(sim_payload, Mapping):
            return bool(sim_payload.get("active"))
        return False

    def _find_reroute_candidate(
        self,
        intent: PolicyIntent,
        notional: float,
        exposures: Mapping[str, float],
        nav: float,
        settings: DiversificationSettings,
    ) -> str | None:
        universe = self._approved_symbols()
        if intent.symbol not in universe:
            return None
        bucket = None
        for entry in settings.buckets:
            if intent.symbol in entry.symbols:
                bucket = entry
                break
        if bucket is None:
            return None
        candidates = [symbol for symbol in bucket.symbols if symbol != intent.symbol]
        if not candidates:
            return None

        expected_edge = self._expected_edge(intent.symbol)
        best_symbol = intent.symbol
        best_diff = math.inf
        for candidate in candidates:
            projected = exposures.get(candidate, 0.0) + notional
            projected_weight = abs(projected) / nav
            if projected_weight > settings.max_concentration_pct_per_asset:
                continue
            edge = self._expected_edge(candidate)
            diff = abs(edge - expected_edge)
            if diff < best_diff:
                best_diff = diff
                best_symbol = candidate
        return best_symbol if best_symbol != intent.symbol else None

    def _load_correlation_from_config(
        self, symbols: Sequence[str]
    ) -> Mapping[str, Mapping[str, float]]:
        config = self.timescale.load_risk_config()
        matrix_payload = config.get("correlation_matrix")
        nested: Dict[str, Dict[str, float]] = {}
        for symbol in symbols:
            row_payload = {}
            if isinstance(matrix_payload, Mapping):
                raw_row = matrix_payload.get(symbol)
                if isinstance(raw_row, Mapping):
                    for other, value in raw_row.items():
                        try:
                            row_payload[str(other)] = float(value)
                        except (TypeError, ValueError):  # pragma: no cover
                            continue
            for other in symbols:
                if other == symbol:
                    row_payload.setdefault(other, 1.0)
                else:
                    row_payload.setdefault(other, 0.0)
            nested[symbol] = row_payload
        return nested

    def _persist_targets(self, targets: DiversificationTargets) -> None:
        if not self._enable_persistence:
            return
        with self._session_scope() as session:
            for symbol, weight in targets.weights.items():
                record = DiversificationTargetRecord(
                    account_id=self.account_id,
                    symbol=symbol,
                    bucket=targets.buckets.get(symbol),
                    weight=float(weight),
                    expected_edge_bps=targets.expected_edges.get(symbol),
                    created_at=targets.timestamp,
                )
                session.add(record)
            session.commit()

    def _log_action(self, action_type: str, payload: Mapping[str, object]) -> None:
        if not self._enable_persistence:
            return
        try:
            encoded = json.dumps(payload, default=str)
        except TypeError:  # pragma: no cover - defensive serialisation
            encoded = json.dumps({"unserialisable": True})
        with self._session_scope() as session:
            session.add(
                DiversificationActionRecord(
                    account_id=self.account_id,
                    action_type=action_type,
                    payload_json=encoded,
                )
            )
            session.commit()

    @contextmanager
    def _session_scope(self) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
        finally:
            session.close()


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------


class DiversificationTargetsModel(BaseModel):
    """API response wrapper for diversification targets."""

    account_id: str
    timestamp: datetime
    targets: Dict[str, float]
    buckets: Dict[str, str]


class RebalanceInstructionModel(BaseModel):
    symbol: str
    side: Literal["BUY", "SELL"]
    notional: float
    current_weight: float
    target_weight: float
    expected_edge_bps: Optional[float] = None
    fee_bps: Optional[float] = None


class DiversificationRebalanceModel(BaseModel):
    account_id: str
    timestamp: datetime
    simulation: bool
    instructions: tuple[RebalanceInstructionModel, ...]


# ---------------------------------------------------------------------------
# FastAPI wiring
# ---------------------------------------------------------------------------


router = APIRouter()


def _allocator_for_account(account_id: str) -> DiversificationAllocator:
    return DiversificationAllocator(account_id)


@router.get("/diversification/targets", response_model=DiversificationTargetsModel)
def get_diversification_targets(
    account_id: str = Query(..., description="Trading account identifier"),
    caller: str = Depends(require_admin_account),
) -> DiversificationTargetsModel:
    if caller != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and query parameter.",
        )

    allocator = _allocator_for_account(account_id)
    try:
        targets = allocator.compute_targets()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return DiversificationTargetsModel(
        account_id=targets.account_id,
        timestamp=targets.timestamp,
        targets=targets.weights,
        buckets=targets.buckets,
    )


@router.post("/diversification/rebalance", response_model=DiversificationRebalanceModel)
def post_diversification_rebalance(
    account_id: str = Query(..., description="Trading account identifier"),
    caller: str = Depends(require_admin_account),
) -> DiversificationRebalanceModel:
    if caller != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and query parameter.",
        )

    allocator = _allocator_for_account(account_id)
    try:
        plan = allocator.generate_rebalance_plan()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    models = tuple(
        RebalanceInstructionModel(
            symbol=instruction.symbol,
            side=instruction.side,
            notional=instruction.notional,
            current_weight=instruction.current_weight,
            target_weight=instruction.target_weight,
            expected_edge_bps=instruction.expected_edge_bps,
            fee_bps=instruction.fee_bps,
        )
        for instruction in plan.instructions
    )
    return DiversificationRebalanceModel(
        account_id=plan.account_id,
        timestamp=plan.timestamp,
        simulation=plan.simulation,
        instructions=models,
    )


__all__ = [
    "DiversificationAllocator",
    "DiversificationAdjustment",
    "DiversificationTargets",
    "DiversificationRebalancePlan",
    "PolicyIntent",
    "router",
    "init_diversification_storage",
]

