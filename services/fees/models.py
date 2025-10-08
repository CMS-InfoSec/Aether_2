"""SQLAlchemy ORM models for the fees service."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Integer, Numeric, String
from sqlalchemy.orm import Mapped, declarative_base, mapped_column


if TYPE_CHECKING:

    class Base:
        """Static typing stub for the fees service declarative base."""

        metadata: Any  # pragma: no cover - attribute provided by SQLAlchemy
        registry: Any  # pragma: no cover - attribute provided by SQLAlchemy
else:  # pragma: no cover - runtime declarative base when SQLAlchemy is available
    Base = declarative_base()
    Base.__doc__ = "Typed declarative base for the fees service models."


class FeeTier(Base):
    """Represents a single tier within the venue fee schedule."""

    __tablename__ = "fee_tiers"

    tier_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    notional_threshold_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 2), nullable=False, default=Decimal("0")
    )
    maker_bps: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    taker_bps: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    effective_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )


class AccountVolume30d(Base):
    """Stores the rolling 30-day notional trading volume per account."""

    __tablename__ = "account_volume_30d"

    account_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    notional_usd_30d: Mapped[Decimal] = mapped_column(
        Numeric(20, 2), nullable=False, default=Decimal("0")
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    maker_fee_bps_estimate: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), nullable=True
    )
    taker_fee_bps_estimate: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), nullable=True
    )
    fee_estimate_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class AccountFill(Base):
    """Stores individual fills for computing rolling volume and realized fees."""

    __tablename__ = "account_fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    liquidity: Mapped[str | None] = mapped_column(String(16), nullable=True)
    notional_usd: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    estimated_fee_bps: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), nullable=True
    )
    estimated_fee_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 8), nullable=True
    )
    actual_fee_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 8), nullable=True
    )
    fill_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )


class FeeTierProgress(Base):
    """Persists proximity alerts when an account nears the next fee tier."""

    __tablename__ = "fee_tier_progress"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    current_tier: Mapped[str] = mapped_column(String(32), nullable=False)
    progress: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )


__all__ = ["Base", "FeeTier", "AccountVolume30d", "AccountFill", "FeeTierProgress"]

