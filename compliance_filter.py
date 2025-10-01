"""Compliance filter microservice and utility for risk validation."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Generator, Iterable, List, Optional

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import Column, DateTime, String, Text, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from services.common.security import require_admin_account


logger = logging.getLogger(__name__)
compliance_logger = logging.getLogger("risk.compliance")


Base = declarative_base()


class ComplianceAsset(Base):
    """ORM representation of the compliance-controlled asset universe."""

    __tablename__ = "compliance_assets"

    symbol = Column(String, primary_key=True)
    status = Column(String, nullable=False)
    reason = Column(Text, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc))

    def as_dict(self) -> dict[str, str | None]:
        return {
            "symbol": self.symbol,
            "status": self.status,
            "reason": self.reason,
            "updated_at": self.updated_at,
        }


DEFAULT_DATABASE_URL = "sqlite:///./risk.db"


def _database_url() -> str:
    url = (
        os.getenv("COMPLIANCE_DATABASE_URL")
        or os.getenv("RISK_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or DEFAULT_DATABASE_URL
    )
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def _engine_options(url: str) -> dict[str, object]:
    options: dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


_DB_URL = _database_url()
ENGINE: Engine = create_engine(_DB_URL, **_engine_options(_DB_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

Base.metadata.create_all(bind=ENGINE)


ALLOWED_STATUSES = {"allowed", "restricted", "watch"}
COMPLIANCE_REASON = "compliance"


def _normalize_symbol(symbol: str) -> str:
    if not symbol:
        raise ValueError("Symbol must be provided")
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("Symbol must not be blank")
    return normalized


def _normalize_status(status: str) -> str:
    if not status:
        raise ValueError("Status must be provided")
    normalized = status.strip().lower()
    if normalized not in ALLOWED_STATUSES:
        raise ValueError(f"Status must be one of {sorted(ALLOWED_STATUSES)}")
    return normalized


@dataclass
class ComplianceEntry:
    symbol: str
    status: str
    reason: Optional[str]
    updated_at: datetime


class ComplianceFilter:
    """Persistence backed compliance filter for trading universe validation."""

    def __init__(self, session_factory: sessionmaker) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session(self) -> Generator[Session, None, None]:
        session: Session = self._session_factory()
        try:
            yield session
        finally:
            session.close()

    def list_assets(self) -> List[ComplianceEntry]:
        with self._session() as session:
            records: Iterable[ComplianceAsset] = session.execute(
                select(ComplianceAsset).order_by(ComplianceAsset.symbol)
            ).scalars()
            return [
                ComplianceEntry(
                    symbol=record.symbol,
                    status=record.status,
                    reason=record.reason,
                    updated_at=record.updated_at,
                )
                for record in records
            ]

    def update_asset(self, symbol: str, status: str, reason: Optional[str]) -> ComplianceEntry:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_status = _normalize_status(status)
        trimmed_reason = None
        if isinstance(reason, str):
            trimmed = reason.strip()
            trimmed_reason = trimmed or None
        timestamp = datetime.now(timezone.utc)

        with self._session() as session:
            asset = session.get(ComplianceAsset, normalized_symbol)
            if asset is None:
                asset = ComplianceAsset(
                    symbol=normalized_symbol,
                    status=normalized_status,
                    reason=trimmed_reason,
                    updated_at=timestamp,
                )
                session.add(asset)
            else:
                asset.status = normalized_status
                asset.reason = trimmed_reason
                asset.updated_at = timestamp
            session.commit()
            session.refresh(asset)

            logger.info(
                "Compliance asset updated", extra={"symbol": asset.symbol, "status": asset.status}
            )
            return ComplianceEntry(
                symbol=asset.symbol,
                status=asset.status,
                reason=asset.reason,
                updated_at=asset.updated_at,
            )

    def evaluate(self, symbol: str) -> tuple[bool, Optional[ComplianceEntry]]:
        normalized_symbol = _normalize_symbol(symbol)
        with self._session() as session:
            asset = session.get(ComplianceAsset, normalized_symbol)
            if asset is None:
                return True, None
            entry = ComplianceEntry(
                symbol=asset.symbol,
                status=asset.status,
                reason=asset.reason,
                updated_at=asset.updated_at,
            )
        if entry.status == "restricted":
            return False, entry
        return True, entry

    def log_rejection(self, account_id: str, symbol: str, entry: Optional[ComplianceEntry]) -> None:
        """Emit a structured log when compliance blocks an instrument."""

        details = {
            "event": "compliance_rejection",
            "account_id": account_id,
            "symbol": _normalize_symbol(symbol),
            "reason": COMPLIANCE_REASON,
        }
        if entry is not None:
            details["status"] = entry.status
            if entry.reason:
                details["compliance_note"] = entry.reason
        compliance_logger.warning(
            "Trade rejected by compliance filter for account=%s symbol=%s",
            account_id,
            symbol,
            extra=details,
        )


compliance_filter = ComplianceFilter(SessionLocal)


class ComplianceAssetModel(BaseModel):
    symbol: str = Field(..., description="Asset symbol", examples=["BTC-USD"])
    status: str = Field(..., description="Compliance status", examples=["restricted"])
    reason: Optional[str] = Field(
        None, description="Optional commentary captured for compliance review"
    )
    updated_at: datetime

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol_field(cls, value: str) -> str:
        return _normalize_symbol(value)

    @field_validator("status")
    @classmethod
    def _normalize_status_field(cls, value: str) -> str:
        return _normalize_status(value)


class ComplianceUpdateRequest(BaseModel):
    symbol: str = Field(..., description="Asset symbol", examples=["ETH-USD"])
    status: str = Field(..., description="New compliance status", examples=["allowed"])
    reason: Optional[str] = Field(None, description="Human readable note for audit trail")

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol_field(cls, value: str) -> str:
        return _normalize_symbol(value)

    @field_validator("status")
    @classmethod
    def _normalize_status_field(cls, value: str) -> str:
        return _normalize_status(value)


app = FastAPI(title="Compliance Filter")


@app.get("/compliance/list", response_model=List[ComplianceAssetModel])
def list_compliance_assets(account_id: str = Depends(require_admin_account)) -> List[ComplianceAssetModel]:
    entries = compliance_filter.list_assets()
    return [ComplianceAssetModel(**asdict(entry)) for entry in entries]


@app.post("/compliance/update", response_model=ComplianceAssetModel, status_code=status.HTTP_200_OK)
def update_compliance_asset(
    payload: ComplianceUpdateRequest, account_id: str = Depends(require_admin_account)
) -> ComplianceAssetModel:
    try:
        entry = compliance_filter.update_asset(
            symbol=payload.symbol, status=payload.status, reason=payload.reason
        )
    except ValueError as exc:  # propagate bad requests as 400s
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return ComplianceAssetModel(**asdict(entry))


__all__ = [
    "ComplianceAsset",
    "ComplianceEntry",
    "ComplianceFilter",
    "compliance_filter",
    "COMPLIANCE_REASON",
]

