"""Domain repository for assembling the approved trading universe."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, Iterable, List, Mapping, Optional, Tuple

from shared.audit import AuditLogEntry, AuditLogStore, TimescaleAuditLogger


@dataclass(frozen=True)
class MarketSnapshot:
    """TimescaleDB-derived view of the latest asset fundamentals."""

    base_asset: str
    quote_asset: str
    market_cap: float
    global_volume_24h: float
    kraken_volume_24h: float
    volatility_30d: float
    source: str = "coingecko"

    @property
    def pair(self) -> str:
        return f"{self.base_asset}-{self.quote_asset}"


@dataclass(frozen=True)
class ConfigVersionRecord:
    """Representation of a ``config_versions`` row."""

    config_key: str
    version: int
    applied_at: datetime
    payload: Mapping[str, Mapping[str, Any]]
    actor_id: str


_DEFAULT_MARKETS: Tuple[MarketSnapshot, ...] = (
    MarketSnapshot(
        base_asset="BTC",
        quote_asset="USD",
        market_cap=8.5e11,
        global_volume_24h=3.2e10,
        kraken_volume_24h=1.8e10,
        volatility_30d=0.12,
    ),
    MarketSnapshot(
        base_asset="ETH",
        quote_asset="USD",
        market_cap=4.0e11,
        global_volume_24h=1.5e10,
        kraken_volume_24h=8.5e9,
        volatility_30d=0.14,
    ),
    MarketSnapshot(
        base_asset="SOL",
        quote_asset="USD",
        market_cap=6.2e10,
        global_volume_24h=7.0e9,
        kraken_volume_24h=3.1e9,
        volatility_30d=0.18,
    ),
    MarketSnapshot(
        base_asset="BTC",
        quote_asset="USDT",
        market_cap=8.5e11,
        global_volume_24h=3.2e10,
        kraken_volume_24h=1.8e10,
        volatility_30d=0.12,
    ),
)

_DEFAULT_FEE_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "BTC-USD": {"currency": "USD", "maker": 0.1, "taker": 0.2},
    "default": {"currency": "USD", "maker": 0.05, "taker": 0.1},
}


class UniverseRepository:
    """Aggregates Timescale metrics and manual overrides for approvals."""

    MARKET_CAP_THRESHOLD: ClassVar[float] = 1.0e9
    GLOBAL_VOLUME_THRESHOLD: ClassVar[float] = 2.5e7
    KRAKEN_VOLUME_THRESHOLD: ClassVar[float] = 1.0e7
    VOLATILITY_THRESHOLD: ClassVar[float] = 0.40
    CONFIG_KEY_OVERRIDES: ClassVar[str] = "universe.manual_overrides"

    _market_snapshots: ClassVar[Dict[str, MarketSnapshot]] = {
        snapshot.pair: snapshot for snapshot in _DEFAULT_MARKETS
    }
    _fee_overrides: ClassVar[Dict[str, Dict[str, Any]]] = {
        symbol: dict(payload) for symbol, payload in _DEFAULT_FEE_OVERRIDES.items()
    }
    _config_versions: ClassVar[List[ConfigVersionRecord]] = []
    _audit_store: ClassVar[AuditLogStore] = AuditLogStore()
    _audit_logger: ClassVar[TimescaleAuditLogger] = TimescaleAuditLogger(_audit_store)

    def __init__(self, account_id: str, audit_logger: Optional[TimescaleAuditLogger] = None) -> None:
        self.account_id = account_id
        self._logger = audit_logger or self.__class__._audit_logger

    # ------------------------------------------------------------------
    # Timescale-backed data loaders
    # ------------------------------------------------------------------
    @classmethod
    def seed_market_snapshots(cls, snapshots: Iterable[MarketSnapshot]) -> None:
        """Replace the cached market snapshots (used for testing)."""

        cls._market_snapshots = {snapshot.pair: snapshot for snapshot in snapshots}

    @classmethod
    def seed_fee_overrides(cls, overrides: Mapping[str, Mapping[str, Any]]) -> None:
        cls._fee_overrides = {symbol: dict(payload) for symbol, payload in overrides.items()}

    # ------------------------------------------------------------------
    # Manual override management
    # ------------------------------------------------------------------
    @classmethod
    def _current_overrides(cls) -> Dict[str, Dict[str, Any]]:
        if not cls._config_versions:
            return {}
        latest = cls._config_versions[-1].payload
        return {symbol: dict(payload) for symbol, payload in latest.items()}

    @classmethod
    def _next_version(cls) -> int:
        return (cls._config_versions[-1].version + 1) if cls._config_versions else 1

    def set_manual_override(
        self,
        instrument: str,
        *,
        approved: bool,
        actor_id: str,
        reason: Optional[str] = None,
    ) -> None:
        """Persist a manual override into ``config_versions``."""

        before = self._current_overrides()
        after = dict(before)
        after[instrument] = {"approved": approved, "actor_id": actor_id}
        if reason:
            after[instrument]["reason"] = reason

        record = ConfigVersionRecord(
            config_key=self.CONFIG_KEY_OVERRIDES,
            version=self._next_version(),
            applied_at=datetime.now(timezone.utc),
            payload=after,
            actor_id=actor_id,
        )
        self._config_versions.append(record)
        self._logger.record(
            action="universe.manual_override",
            actor_id=actor_id,
            before=before,
            after=after,
        )

    # ------------------------------------------------------------------
    # Query interfaces
    # ------------------------------------------------------------------
    def approved_universe(self) -> List[str]:
        """Return USD-quoted instruments meeting liquidity and risk criteria."""

        approved: Dict[str, MarketSnapshot] = {}
        for symbol, snapshot in self._market_snapshots.items():
            if snapshot.quote_asset != "USD":
                continue
            if snapshot.market_cap < self.MARKET_CAP_THRESHOLD:
                continue
            if snapshot.global_volume_24h < self.GLOBAL_VOLUME_THRESHOLD:
                continue
            if snapshot.kraken_volume_24h < self.KRAKEN_VOLUME_THRESHOLD:
                continue
            if snapshot.volatility_30d < self.VOLATILITY_THRESHOLD:
                continue
            approved[symbol] = snapshot

        overrides = self._current_overrides()
        for symbol, override in overrides.items():
            if override.get("approved"):
                approved.setdefault(
                    symbol,
                    MarketSnapshot(
                        base_asset=symbol.split("-")[0],
                        quote_asset=symbol.split("-")[-1],
                        market_cap=self.MARKET_CAP_THRESHOLD,
                        global_volume_24h=self.GLOBAL_VOLUME_THRESHOLD,
                        kraken_volume_24h=self.KRAKEN_VOLUME_THRESHOLD,
                        volatility_30d=self.VOLATILITY_THRESHOLD,
                        source="manual",
                    ),
                )
            else:
                approved.pop(symbol, None)

        return sorted(approved.keys())

    def fee_override(self, instrument: str) -> Optional[Dict[str, Any]]:
        if instrument not in self._fee_overrides:
            return None
        return dict(self._fee_overrides[instrument])

    # ------------------------------------------------------------------
    # Introspection helpers (used in testing)
    # ------------------------------------------------------------------
    @classmethod
    def audit_entries(cls) -> Tuple[AuditLogEntry, ...]:
        return tuple(cls._audit_store.all())

    @classmethod
    def reset(cls) -> None:
        cls._market_snapshots = {snapshot.pair: snapshot for snapshot in _DEFAULT_MARKETS}
        cls._fee_overrides = {
            symbol: dict(payload) for symbol, payload in _DEFAULT_FEE_OVERRIDES.items()
        }
        cls._config_versions = []
        cls._audit_store = AuditLogStore()
        cls._audit_logger = TimescaleAuditLogger(cls._audit_store)


__all__ = ["MarketSnapshot", "UniverseRepository"]

