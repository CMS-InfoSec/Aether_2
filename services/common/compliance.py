"""Shared utilities for compliance and sanctions data management."""

from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Set

try:  # pragma: no cover - optional dependency
    from sqlalchemy import Column, DateTime, PrimaryKeyConstraint, String
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import declarative_base
except Exception:  # pragma: no cover - executed when SQLAlchemy unavailable
    Column = DateTime = PrimaryKeyConstraint = String = None  # type: ignore[assignment]
    Engine = object  # type: ignore[assignment]
    SQLALCHEMY_AVAILABLE = False
else:  # pragma: no cover - exercised in environments with SQLAlchemy installed
    SQLALCHEMY_AVAILABLE = getattr(Column, "__module__", "").startswith("sqlalchemy")


if SQLALCHEMY_AVAILABLE:
    SanctionsBase = declarative_base()

    class SanctionRecord(SanctionsBase):
        """ORM mapping for sanctions sourced from regulatory watchlists."""

        __tablename__ = "sanctions"

        symbol = Column(String, nullable=False)
        status = Column(String, nullable=False)
        source = Column(String, nullable=False)
        ts = Column(
            DateTime(timezone=True),
            nullable=False,
            default=lambda: datetime.now(timezone.utc),
        )

        __table_args__ = (PrimaryKeyConstraint("symbol", "source", name="pk_sanctions"),)

        def __repr__(self) -> str:  # pragma: no cover - debugging helper
            return (
                "SanctionRecord("  # pragma: no cover - debugging helper
                f"symbol={self.symbol!r}, "  # pragma: no cover - debugging helper
                f"status={self.status!r}, "  # pragma: no cover - debugging helper
                f"source={self.source!r}, "  # pragma: no cover - debugging helper
                f"ts={self.ts!r}"  # pragma: no cover - debugging helper
                ")"
            )
else:
    @dataclass
    class SanctionRecord:
        symbol: str
        status: str
        source: str
        ts: datetime

    Engine = object  # type: ignore[assignment]


BLOCKING_STATUSES: Set[str] = {
    "sanctioned",
    "blocked",
    "denied",
    "prohibited",
}


def is_blocking_status(status: str) -> bool:
    """Return True when the status should prevent trading."""

    return status.lower() in BLOCKING_STATUSES


def ensure_sanctions_schema(engine: Engine) -> None:
    """Create the sanctions table when it does not exist."""

    if SQLALCHEMY_AVAILABLE:
        SanctionsBase.metadata.create_all(bind=engine, tables=[SanctionRecord.__table__])
