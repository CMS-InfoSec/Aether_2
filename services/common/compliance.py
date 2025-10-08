"""Shared utilities for compliance and sanctions data management."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import DateTime, PrimaryKeyConstraint, String
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Mapped, declarative_base, mapped_column


def _utcnow() -> datetime:
    """Return a timezone-aware timestamp for default column values."""

    return datetime.now(timezone.utc)


if TYPE_CHECKING:

    class SanctionsBase:
        """Static typing shim for the sanctions declarative base."""

        metadata: Any  # pragma: no cover - attribute injected by SQLAlchemy
        registry: Any  # pragma: no cover - attribute injected by SQLAlchemy

else:  # pragma: no cover - executed at runtime when SQLAlchemy is available

    SanctionsBase = declarative_base()
    SanctionsBase.__doc__ = "Typed declarative base for sanctions metadata."


class SanctionRecord(SanctionsBase):
    """ORM mapping for sanctions sourced from regulatory watchlists."""

    __tablename__ = "sanctions"

    if TYPE_CHECKING:  # pragma: no cover - populated by SQLAlchemy at runtime
        __table__: Any

    symbol: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
    )

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


BLOCKING_STATUSES: Final[set[str]] = {
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
