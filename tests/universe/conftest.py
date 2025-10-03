from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Optional

import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy import (  # type: ignore[import-untyped]
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
    select,
)
from sqlalchemy.orm import sessionmaker  # type: ignore[import-untyped]

from services.universe.repository import UniverseRepository


@dataclass
class UniverseTimescaleFixture:
    """Utility helpers for populating an ephemeral Timescale instance."""

    session_factory: sessionmaker
    market_snapshots: Table
    fee_overrides: Table
    config_versions: Table
    _accounts: set[str] = field(default_factory=set)

    def bind_accounts(self, accounts: Iterable[str]) -> None:
        for account in list(accounts):
            UniverseRepository.configure_session_factory(account, self.session_factory)
            self._accounts.add(account)

    def rebind(self) -> None:
        if self._accounts:
            self.bind_accounts(self._accounts)

    def clear(self) -> None:
        with self.session_factory() as session:
            session.execute(self.market_snapshots.delete())
            session.execute(self.fee_overrides.delete())
            session.execute(self.config_versions.delete())
            session.commit()

    def add_snapshot(
        self,
        *,
        base_asset: str,
        quote_asset: str = "USD",
        market_cap: float,
        global_volume_24h: float,
        kraken_volume_24h: float,
        volatility_30d: float,
        source: str = "coingecko",
        observed_at: Optional[datetime] = None,
    ) -> None:
        timestamp = observed_at or datetime.now(timezone.utc)
        with self.session_factory() as session:
            session.execute(
                insert(self.market_snapshots).values(
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    market_cap=market_cap,
                    global_volume_24h=global_volume_24h,
                    kraken_volume_24h=kraken_volume_24h,
                    volatility_30d=volatility_30d,
                    source=source,
                    observed_at=timestamp,
                )
            )
            session.commit()

    def add_fee_override(
        self,
        instrument: str,
        *,
        currency: str,
        maker: float,
        taker: float,
        updated_at: Optional[datetime] = None,
    ) -> None:
        timestamp = updated_at or datetime.now(timezone.utc)
        with self.session_factory() as session:
            session.execute(
                self.fee_overrides.delete().where(self.fee_overrides.c.instrument == instrument)
            )
            session.execute(
                insert(self.fee_overrides).values(
                    instrument=instrument,
                    currency=currency,
                    maker=maker,
                    taker=taker,
                    updated_at=timestamp,
                )
            )
            session.commit()

    def config_payloads(self) -> list[dict[str, object]]:
        with self.session_factory() as session:
            rows = session.execute(select(self.config_versions.c.payload)).scalars().all()
        return list(rows)


@pytest.fixture
def universe_timescale(tmp_path) -> UniverseTimescaleFixture:
    database_path = tmp_path / "universe-timescale.db"
    engine = create_engine(f"sqlite:///{database_path}", future=True)
    metadata = MetaData()

    market_snapshots = Table(
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
    )

    fee_overrides = Table(
        "fee_overrides",
        metadata,
        Column("instrument", String, primary_key=True),
        Column("currency", String, nullable=False),
        Column("maker", Float, nullable=False),
        Column("taker", Float, nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )

    config_versions = Table(
        "config_versions",
        metadata,
        Column("config_key", String, primary_key=True),
        Column("version", Integer, primary_key=True),
        Column("applied_at", DateTime(timezone=True), primary_key=True),
        Column("checksum", String, nullable=True),
        Column("payload", JSON, nullable=False),
    )

    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    probe_session = session_factory()
    try:
        if not hasattr(probe_session, "commit") or not hasattr(probe_session, "close"):
            pytest.skip("sqlalchemy runtime support is required for universe integration tests")
    finally:
        close = getattr(probe_session, "close", None)
        if callable(close):
            close()

    metadata.create_all(engine)

    fixture = UniverseTimescaleFixture(
        session_factory=session_factory,
        market_snapshots=market_snapshots,
        fee_overrides=fee_overrides,
        config_versions=config_versions,
    )
    fixture.bind_accounts({"company", "director-1", "director-2", "director", "default", "shadow"})

    yield fixture

    engine.dispose()
    UniverseRepository.reset()
