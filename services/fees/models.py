"""SQLAlchemy ORM models for the fees service."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, Numeric, String
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class FeeTier(Base):
    """Represents a single tier within the venue fee schedule."""

    __tablename__ = "fee_tiers"

    tier_id = Column(String(32), primary_key=True)
    notional_threshold_usd = Column(Numeric(20, 2), nullable=False, default=0)
    maker_bps = Column(Numeric(10, 4), nullable=False)
    taker_bps = Column(Numeric(10, 4), nullable=False)
    effective_from = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)


class AccountVolume30d(Base):
    """Stores the rolling 30-day notional trading volume per account."""

    __tablename__ = "account_volume_30d"

    account_id = Column(String(64), primary_key=True)
    notional_usd_30d = Column(Numeric(20, 2), nullable=False, default=0)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class FeeTierProgress(Base):
    """Persists proximity alerts when an account nears the next fee tier."""

    __tablename__ = "fee_tier_progress"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(64), nullable=False, index=True)
    current_tier = Column(String(32), nullable=False)
    progress = Column(Numeric(10, 4), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)


__all__ = ["Base", "FeeTier", "AccountVolume30d", "FeeTierProgress"]

