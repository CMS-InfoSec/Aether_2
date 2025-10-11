"""SQLAlchemy ORM models for the fees service."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from types import SimpleNamespace

import sqlalchemy
try:  # pragma: no cover - lightweight stubs may omit orm package
    from sqlalchemy import orm as sa_orm
except Exception:  # pragma: no cover - provide minimal fallback for import-time stubs
    sa_orm = SimpleNamespace(  # type: ignore[assignment]
        declarative_base=lambda: type(
            "Base",
            (),
            {
                "metadata": SimpleNamespace(
                    create_all=lambda bind=None: None, drop_all=lambda bind=None: None
                )
            },
        ),
        Mapped=Any,
        mapped_column=lambda *args, **kwargs: None,
    )

if TYPE_CHECKING:
    from sqlalchemy.sql.schema import Table


if TYPE_CHECKING:

    class Base:
        """Static typing stub for the fees service declarative base."""

        metadata: Any  # pragma: no cover - attribute provided by SQLAlchemy
        registry: Any  # pragma: no cover - attribute provided by SQLAlchemy
else:  # pragma: no cover - runtime declarative base when SQLAlchemy is available
    Base = sa_orm.declarative_base()
    Base.__doc__ = "Typed declarative base for the fees service models."


DateTime = getattr(sqlalchemy, "DateTime")
Numeric = getattr(sqlalchemy, "Numeric")
String = getattr(sqlalchemy, "String")
Integer = getattr(sqlalchemy, "Integer", getattr(sqlalchemy, "BigInteger", Numeric))
Mapped = getattr(sa_orm, "Mapped", Any)
mapped_column = getattr(sa_orm, "mapped_column", lambda *args, **kwargs: None)


class FeeTier(Base):
    """Represents a single tier within the venue fee schedule."""

    __tablename__ = "fee_tiers"

    if TYPE_CHECKING:  # pragma: no cover - enhanced constructor for static analysis
        __table__: Table

        def __init__(
            self,
            *,
            tier_id: str,
            notional_threshold_usd: Decimal,
            maker_bps: Decimal,
            taker_bps: Decimal,
            effective_from: datetime,
        ) -> None: ...

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

    if TYPE_CHECKING:  # pragma: no cover - enhanced constructor for static analysis
        __table__: Table

        def __init__(
            self,
            *,
            account_id: str,
            notional_usd_30d: Decimal,
            updated_at: datetime,
            maker_fee_bps_estimate: Decimal | None = None,
            taker_fee_bps_estimate: Decimal | None = None,
            fee_estimate_updated_at: datetime | None = None,
        ) -> None: ...

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

    if TYPE_CHECKING:  # pragma: no cover - enhanced constructor for static analysis
        __table__: Table

        def __init__(
            self,
            *,
            account_id: str,
            liquidity: str | None = None,
            notional_usd: Decimal,
            estimated_fee_bps: Decimal | None = None,
            estimated_fee_usd: Decimal | None = None,
            actual_fee_usd: Decimal | None = None,
            fill_ts: datetime,
            recorded_at: datetime | None = None,
        ) -> None: ...

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

    if TYPE_CHECKING:  # pragma: no cover - enhanced constructor for static analysis
        __table__: Table

        def __init__(
            self,
            *,
            account_id: str,
            current_tier: str,
            progress: Decimal,
            ts: datetime,
        ) -> None: ...

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    current_tier: Mapped[str] = mapped_column(String(32), nullable=False)
    progress: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )


__all__ = ["Base", "FeeTier", "AccountVolume30d", "AccountFill", "FeeTierProgress"]

