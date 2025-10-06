"""Domain repository for assembling the approved trading universe."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, ClassVar, Dict, Iterator, List, Mapping, Optional, Tuple

from shared.audit import AuditLogEntry, AuditLogStore, TimescaleAuditLogger
from shared.spot import is_spot_symbol, normalize_spot_symbol

try:  # pragma: no cover - exercised in integration tests when SQLAlchemy installed
    from sqlalchemy import JSON, Column, DateTime, Float, Integer, MetaData, String, Table, insert, select
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError
    from sqlalchemy.orm import Session, sessionmaker
    from sqlalchemy import create_engine

    SQLALCHEMY_AVAILABLE = True
except Exception:  # pragma: no cover - fallback when SQLAlchemy missing
    SQLALCHEMY_AVAILABLE = False
    JSON = object  # type: ignore[assignment]
    Column = DateTime = Float = Integer = MetaData = String = Table = object  # type: ignore[assignment]
    insert = select = create_engine = None  # type: ignore[assignment]
    Engine = Session = sessionmaker = object  # type: ignore[assignment]
    NoSuchTableError = SQLAlchemyError = Exception  # type: ignore[assignment]

from services.common.config import TimescaleSession, get_timescale_session


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
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

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
    checksum: Optional[str] = None


class UniverseRepository:
    """Aggregates Timescale metrics and manual overrides for approvals."""

    MARKET_CAP_THRESHOLD: ClassVar[float] = 1.0e9
    GLOBAL_VOLUME_THRESHOLD: ClassVar[float] = 2.5e7
    KRAKEN_VOLUME_THRESHOLD: ClassVar[float] = 1.0e7
    VOLATILITY_THRESHOLD: ClassVar[float] = 0.40
    CONFIG_KEY_OVERRIDES: ClassVar[str] = "universe.manual_overrides"

    MARKET_SNAPSHOT_MAX_AGE: ClassVar[timedelta] = timedelta(minutes=30)
    FEE_OVERRIDE_MAX_AGE: ClassVar[timedelta] = timedelta(hours=6)
    CONFIG_VERSION_MAX_AGE: ClassVar[timedelta] = timedelta(days=7)

    _session_factories: ClassVar[Dict[str, Callable[[], Session]]] = {}
    _engines: ClassVar[Dict[str, Engine]] = {}
    _session_factory_overrides: ClassVar[Dict[str, Callable[[], Session]]] = {}
    _schemas: ClassVar[Dict[str, Optional[str]]] = {}
    _audit_store: ClassVar[AuditLogStore] = AuditLogStore()
    _audit_logger: ClassVar[TimescaleAuditLogger] = TimescaleAuditLogger(_audit_store)

    def __init__(
        self,
        account_id: str,
        audit_logger: Optional[TimescaleAuditLogger] = None,
        *,
        session_factory: Optional[Callable[[], Session]] = None,
        now_fn: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self.account_id = account_id
        self._logger = audit_logger or self.__class__._audit_logger
        self._session_factory = session_factory
        self._now: Callable[[], datetime] = now_fn or (lambda: datetime.now(timezone.utc))

    # ------------------------------------------------------------------
    # Session configuration
    # ------------------------------------------------------------------

    @classmethod
    def configure_session_factory(
        cls, account_id: str, factory: Callable[[], Session], *, schema: Optional[str] = None
    ) -> None:
        """Install a custom SQLAlchemy session factory for *account_id*."""

        cls._session_factory_overrides[account_id] = factory
        if schema is not None:
            cls._schemas[account_id] = schema

    # ------------------------------------------------------------------
    # Manual override management
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_datetime(value: object) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    def _current_overrides(self, session: Session) -> Tuple[Dict[str, Dict[str, Any]], Optional[datetime]]:
        record = self._latest_config_record(session)
        if record is None:
            return {}, None

        payload = {}
        for symbol, values in (record.payload or {}).items():
            normalized = normalize_spot_symbol(symbol)
            if not normalized or not is_spot_symbol(normalized):
                continue
            payload[normalized] = dict(values)

        return payload, record.applied_at

    def _next_version(self, session: Session) -> int:
        table = self._config_versions_table()
        stmt = (
            select(table.c.version)
            .where(table.c.config_key == self.CONFIG_KEY_OVERRIDES)
            .order_by(table.c.version.desc())
            .limit(1)
        )
        result = session.execute(stmt).scalar_one_or_none()
        return int(result or 0) + 1

    def set_manual_override(
        self,
        instrument: str,
        *,
        approved: bool,
        actor_id: str,
        reason: Optional[str] = None,
    ) -> None:
        """Persist a manual override into ``config_versions``."""

        canonical = normalize_spot_symbol(instrument)
        if not canonical or not is_spot_symbol(canonical):
            raise ValueError("Manual overrides must target spot trading pairs")

        with self._session_scope() as session:
            before, _ = self._current_overrides(session)
            after = dict(before)
            override_payload: Dict[str, Any] = {"approved": approved, "actor_id": actor_id}
            if reason:
                override_payload["reason"] = reason
            override_payload["updated_at"] = self._now().isoformat()
            after[canonical] = override_payload

            version = self._next_version(session)
            applied_at = self._now()
            table = self._config_versions_table()
            session.execute(
                insert(table).values(
                    config_key=self.CONFIG_KEY_OVERRIDES,
                    version=version,
                    applied_at=applied_at,
                    checksum=None,
                    payload=after,
                )
            )

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

        with self._session_scope() as session:
            snapshots = self._load_market_snapshots(session)
            overrides, applied_at = self._current_overrides(session)

        approved: Dict[str, MarketSnapshot] = {}
        for symbol, snapshot in snapshots.items():
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

        override_stale = applied_at is not None and (self._now() - applied_at > self.CONFIG_VERSION_MAX_AGE)
        if not override_stale:
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
                            observed_at=self._now(),
                        ),
                    )
                else:
                    approved.pop(symbol, None)

        return sorted(approved.keys())

    def fee_override(self, instrument: str) -> Optional[Dict[str, Any]]:
        with self._session_scope() as session:
            overrides = self._load_fee_overrides(session)

        override = overrides.get(instrument) or overrides.get("default")
        if override is None:
            return None
        return {
            "currency": override["currency"],
            "maker": override["maker"],
            "taker": override["taker"],
        }

    # ------------------------------------------------------------------
    # Introspection helpers (used in testing)
    # ------------------------------------------------------------------

    @classmethod
    def audit_entries(cls) -> Tuple[AuditLogEntry, ...]:
        return tuple(cls._audit_store.all())

    @classmethod
    def reset(cls) -> None:
        cls._audit_store = AuditLogStore()
        cls._audit_logger = TimescaleAuditLogger(cls._audit_store)
        cls._session_factories.clear()
        cls._engines.clear()
        cls._schemas.clear()
        cls._session_factory_overrides.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_session_factory(self) -> Callable[[], Session]:
        if self._session_factory is not None:
            return self._session_factory

        override = self._session_factory_overrides.get(self.account_id)
        if override is not None:
            return override

        if not SQLALCHEMY_AVAILABLE:
            raise RuntimeError("sqlalchemy is required for Timescale integration")

        factory = self._session_factories.get(self.account_id)
        if factory is not None:
            return factory

        session_cfg: TimescaleSession = get_timescale_session(self.account_id)
        engine = self._engines.get(session_cfg.dsn)
        if engine is None:
            engine = create_engine(session_cfg.dsn, future=True)
            self._engines[session_cfg.dsn] = engine

        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
        self._session_factories[self.account_id] = factory
        self._schemas[self.account_id] = session_cfg.account_schema
        return factory

    @contextmanager
    def _session_scope(self) -> Iterator[Session]:
        factory = self._resolve_session_factory()
        session: Optional[Session] = None
        try:
            session = factory()
            yield session
            session.commit()
        except SQLAlchemyError:
            if session is not None:
                session.rollback()
            raise
        finally:
            if session is not None:
                session.close()

    def _market_snapshots_table(self) -> Table:
        schema = self._schemas.get(self.account_id)
        metadata = MetaData(schema=schema) if schema else MetaData()
        return Table(
            "market_snapshots",
            metadata,
            Column("base_asset", String, primary_key=True),
            Column("quote_asset", String, primary_key=True),
            Column("observed_at", DateTime(timezone=True), primary_key=True),
            Column("market_cap", Float, nullable=False),
            Column("global_volume_24h", Float, nullable=False),
            Column("kraken_volume_24h", Float, nullable=False),
            Column("volatility_30d", Float, nullable=False),
            Column("source", String, nullable=False, default="coingecko"),
            extend_existing=True,
        )

    def _fee_overrides_table(self) -> Table:
        schema = self._schemas.get(self.account_id)
        metadata = MetaData(schema=schema) if schema else MetaData()
        return Table(
            "fee_overrides",
            metadata,
            Column("instrument", String, primary_key=True),
            Column("currency", String, nullable=False),
            Column("maker", Float, nullable=False),
            Column("taker", Float, nullable=False),
            Column("updated_at", DateTime(timezone=True), nullable=False),
            extend_existing=True,
        )

    def _config_versions_table(self) -> Table:
        schema = self._schemas.get(self.account_id)
        metadata = MetaData(schema=schema) if schema else MetaData()
        return Table(
            "config_versions",
            metadata,
            Column("config_key", String, primary_key=True),
            Column("version", Integer, primary_key=True),
            Column("applied_at", DateTime(timezone=True), primary_key=True),
            Column("checksum", String, nullable=True),
            Column("payload", JSON, nullable=False),
            extend_existing=True,
        )

    def _load_market_snapshots(self, session: Session) -> Dict[str, MarketSnapshot]:
        table = self._market_snapshots_table()
        try:
            rows = session.execute(
                select(
                    table.c.base_asset,
                    table.c.quote_asset,
                    table.c.market_cap,
                    table.c.global_volume_24h,
                    table.c.kraken_volume_24h,
                    table.c.volatility_30d,
                    table.c.source,
                    table.c.observed_at,
                ).order_by(
                    table.c.base_asset,
                    table.c.quote_asset,
                    table.c.observed_at.desc(),
                )
            ).all()
        except (SQLAlchemyError, NoSuchTableError):
            return {}

        snapshots: Dict[str, MarketSnapshot] = {}
        now = self._now()
        for row in rows:
            observed_at = self._coerce_datetime(row.observed_at)
            if observed_at is None or now - observed_at > self.MARKET_SNAPSHOT_MAX_AGE:
                continue
            raw_symbol = f"{row.base_asset}-{row.quote_asset}"
            normalized_symbol = normalize_spot_symbol(raw_symbol)
            if not normalized_symbol or not is_spot_symbol(normalized_symbol):
                continue
            if normalized_symbol in snapshots:
                continue
            base_asset, quote_asset = normalized_symbol.split("-", 1)
            snapshots[normalized_symbol] = MarketSnapshot(
                base_asset=base_asset,
                quote_asset=quote_asset,
                market_cap=float(row.market_cap),
                global_volume_24h=float(row.global_volume_24h),
                kraken_volume_24h=float(row.kraken_volume_24h),
                volatility_30d=float(row.volatility_30d),
                source=str(row.source or "coingecko"),
                observed_at=observed_at,
            )
        return snapshots

    def _load_fee_overrides(self, session: Session) -> Dict[str, Dict[str, Any]]:
        table = self._fee_overrides_table()
        try:
            rows = session.execute(
                select(
                    table.c.instrument,
                    table.c.currency,
                    table.c.maker,
                    table.c.taker,
                    table.c.updated_at,
                ).order_by(table.c.updated_at.desc())
            ).all()
        except (SQLAlchemyError, NoSuchTableError):
            return {}

        overrides: Dict[str, Dict[str, Any]] = {}
        now = self._now()
        for row in rows:
            updated_at = self._coerce_datetime(row.updated_at)
            if updated_at is None or now - updated_at > self.FEE_OVERRIDE_MAX_AGE:
                continue
            normalized = normalize_spot_symbol(row.instrument)
            if not normalized or not is_spot_symbol(normalized):
                continue
            if normalized in overrides:
                continue
            overrides[normalized] = {
                "currency": row.currency,
                "maker": float(row.maker),
                "taker": float(row.taker),
                "updated_at": updated_at,
            }
        return overrides

    def _latest_config_record(self, session: Session) -> Optional[ConfigVersionRecord]:
        table = self._config_versions_table()
        try:
            row = (
                session.execute(
                    select(
                        table.c.config_key,
                        table.c.version,
                        table.c.applied_at,
                        table.c.payload,
                        table.c.checksum,
                    )
                    .where(table.c.config_key == self.CONFIG_KEY_OVERRIDES)
                    .order_by(table.c.version.desc(), table.c.applied_at.desc())
                    .limit(1)
                )
            ).one_or_none()
        except (SQLAlchemyError, NoSuchTableError):
            return None

        if row is None:
            return None
        applied_at = self._coerce_datetime(row.applied_at)
        if applied_at is None:
            applied_at = self._now()
        return ConfigVersionRecord(
            config_key=str(row.config_key),
            version=int(row.version),
            applied_at=applied_at,
            payload=row.payload or {},
            checksum=row.checksum,
        )


__all__ = ["MarketSnapshot", "UniverseRepository"]
