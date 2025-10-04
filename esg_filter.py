"""ESG filter microservice for enforcing sustainability guardrails."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Generator, Iterable, List, Optional

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from services.common.security import require_admin_account


logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("risk.esg")


Base = declarative_base()


class ESGAsset(Base):
    """ORM representation of ESG attributes for tradeable assets."""

    __tablename__ = "esg_assets"

    symbol = Column(String, primary_key=True)
    score = Column(Float, nullable=False, default=0.0)
    flag = Column(String, nullable=False)
    reason = Column(Text, nullable=True)
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "score": self.score,
            "flag": self.flag,
            "reason": self.reason,
            "updated_at": self.updated_at,
        }


class ESGRejection(Base):
    """Audit trail of trades rejected for ESG considerations."""

    __tablename__ = "esg_rejections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    flag = Column(String, nullable=True)
    score = Column(Float, nullable=True)
    reason = Column(String, nullable=False, default="esg_compliance")
    note = Column(Text, nullable=True)
    recorded_at = Column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )


def _database_url() -> str:
    url = os.getenv("ESG_DATABASE_URL")
    if not url:
        raise RuntimeError(
            "ESG_DATABASE_URL must be defined with a PostgreSQL/Timescale connection string."
        )

    normalized = url.lower()
    if normalized.startswith("postgres://"):
        url = "postgresql://" + url.split("://", 1)[1]
        normalized = url.lower()

    allowed_prefixes = (
        "postgresql://",
        "postgresql+psycopg://",
        "postgresql+psycopg2://",
    )
    if not normalized.startswith(allowed_prefixes):
        raise RuntimeError(
            "ESG filter requires a PostgreSQL/Timescale DSN; "
            f"received '{url}'."
        )

    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def _engine_options(url: str) -> dict[str, object]:
    options: dict[str, object] = {
        "future": True,
        "pool_pre_ping": True,
        "pool_size": int(os.getenv("ESG_DB_POOL_SIZE", "10")),
        "max_overflow": int(os.getenv("ESG_DB_MAX_OVERFLOW", "20")),
        "pool_timeout": int(os.getenv("ESG_DB_POOL_TIMEOUT", "30")),
        "pool_recycle": int(os.getenv("ESG_DB_POOL_RECYCLE", "1800")),
    }

    sslmode = os.getenv("ESG_DB_SSLMODE", "require").strip()
    connect_args: dict[str, object] = {}
    if sslmode:
        connect_args["sslmode"] = sslmode
    options["connect_args"] = connect_args
    return options


def _run_migrations(engine: Engine) -> None:
    """Ensure ESG tables exist in the configured database."""

    Base.metadata.create_all(
        bind=engine,
        tables=[ESGAsset.__table__, ESGRejection.__table__],
        checkfirst=True,
    )


_DB_URL = _database_url()
ENGINE: Engine = create_engine(_DB_URL, **_engine_options(_DB_URL))
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, expire_on_commit=False, future=True)

_run_migrations(ENGINE)


ALLOWED_FLAGS = {"clear", "watch", "high_environmental", "high_regulatory"}
BLOCKING_FLAGS = {"high_environmental", "high_regulatory"}
ESG_REASON = "esg_compliance"


def _normalize_symbol(symbol: str) -> str:
    if not symbol:
        raise ValueError("Symbol must be provided")
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("Symbol must not be blank")
    return normalized


def _normalize_flag(flag: str) -> str:
    if not flag:
        raise ValueError("Flag must be provided")
    normalized = flag.strip().lower()
    if normalized not in ALLOWED_FLAGS:
        raise ValueError(f"Flag must be one of {sorted(ALLOWED_FLAGS)}")
    return normalized


def _normalize_score(score: float | int | str) -> float:
    if isinstance(score, (int, float)):
        return float(score)
    if isinstance(score, str):
        stripped = score.strip()
        if not stripped:
            raise ValueError("Score must be provided")
        try:
            return float(stripped)
        except ValueError as exc:  # pragma: no cover - guard against invalid input
            raise ValueError("Score must be numeric") from exc
    raise ValueError("Score must be numeric")


@dataclass
class ESGEntry:
    symbol: str
    score: float
    flag: str
    reason: Optional[str]
    updated_at: datetime


class ESGFilter:
    """Persistence backed ESG evaluation utility."""

    def __init__(self, session_factory: sessionmaker) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session(self) -> Generator[Session, None, None]:
        session: Session = self._session_factory()
        try:
            yield session
        finally:
            session.close()

    def list_assets(self) -> List[ESGEntry]:
        with self._session() as session:
            records: Iterable[ESGAsset] = session.execute(
                select(ESGAsset).order_by(ESGAsset.symbol)
            ).scalars()
            return [
                ESGEntry(
                    symbol=record.symbol,
                    score=record.score,
                    flag=record.flag,
                    reason=record.reason,
                    updated_at=record.updated_at,
                )
                for record in records
            ]

    def update_asset(self, symbol: str, score: float | int | str, flag: str, reason: Optional[str]) -> ESGEntry:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_flag = _normalize_flag(flag)
        numeric_score = _normalize_score(score)
        trimmed_reason: Optional[str] = None
        if isinstance(reason, str):
            trimmed = reason.strip()
            trimmed_reason = trimmed or None
        timestamp = datetime.now(timezone.utc)

        with self._session() as session:
            asset = session.get(ESGAsset, normalized_symbol)
            if asset is None:
                asset = ESGAsset(
                    symbol=normalized_symbol,
                    score=numeric_score,
                    flag=normalized_flag,
                    reason=trimmed_reason,
                    updated_at=timestamp,
                )
                session.add(asset)
            else:
                asset.score = numeric_score
                asset.flag = normalized_flag
                asset.reason = trimmed_reason
                asset.updated_at = timestamp
            session.commit()
            session.refresh(asset)

            logger.info(
                "ESG asset updated",
                extra={"symbol": asset.symbol, "flag": asset.flag, "score": asset.score},
            )
            return ESGEntry(
                symbol=asset.symbol,
                score=asset.score,
                flag=asset.flag,
                reason=asset.reason,
                updated_at=asset.updated_at,
            )

    def evaluate(self, symbol: str) -> tuple[bool, Optional[ESGEntry]]:
        normalized_symbol = _normalize_symbol(symbol)
        with self._session() as session:
            asset = session.get(ESGAsset, normalized_symbol)
            if asset is None:
                return True, None
            entry = ESGEntry(
                symbol=asset.symbol,
                score=asset.score,
                flag=asset.flag,
                reason=asset.reason,
                updated_at=asset.updated_at,
            )
        if entry.flag in BLOCKING_FLAGS:
            return False, entry
        return True, entry

    def log_rejection(self, account_id: str, symbol: str, entry: Optional[ESGEntry]) -> None:
        """Persist and emit audit logs for ESG driven trade rejections."""

        normalized_symbol = _normalize_symbol(symbol)
        note = entry.reason if entry and entry.reason else None
        flag = entry.flag if entry else None
        score = entry.score if entry else None
        payload = {
            "event": "esg_rejection",
            "account_id": account_id,
            "symbol": normalized_symbol,
            "reason": ESG_REASON,
        }
        if flag is not None:
            payload["flag"] = flag
        if score is not None:
            payload["score"] = score
        if note:
            payload["note"] = note

        with self._session() as session:
            record = ESGRejection(
                account_id=account_id,
                symbol=normalized_symbol,
                flag=flag,
                score=score,
                reason=ESG_REASON,
                note=note,
                recorded_at=datetime.now(timezone.utc),
            )
            session.add(record)
            session.commit()

        audit_logger.warning(
            "Trade rejected by ESG filter for account=%s symbol=%s",
            account_id,
            normalized_symbol,
            extra=payload,
        )


esg_filter = ESGFilter(SessionLocal)


class ESGAssetModel(BaseModel):
    symbol: str = Field(..., description="Asset symbol", examples=["BTC-USD"])
    score: float = Field(..., description="ESG risk score")
    flag: str = Field(..., description="ESG classification flag")
    reason: Optional[str] = Field(None, description="Optional audit note")
    updated_at: datetime

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol_field(cls, value: str) -> str:
        return _normalize_symbol(value)

    @field_validator("flag")
    @classmethod
    def _normalize_flag_field(cls, value: str) -> str:
        return _normalize_flag(value)


class ESGUpdateRequest(BaseModel):
    symbol: str = Field(..., description="Asset symbol", examples=["ETH-USD"])
    score: float = Field(..., description="Updated ESG score")
    flag: str = Field(..., description="Updated ESG flag")
    reason: Optional[str] = Field(None, description="Optional commentary for audit trail")

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol_field(cls, value: str) -> str:
        return _normalize_symbol(value)

    @field_validator("flag")
    @classmethod
    def _normalize_flag_field(cls, value: str) -> str:
        return _normalize_flag(value)


app = FastAPI(title="ESG Filter Service")


@app.get("/esg/list", response_model=List[ESGAssetModel])
def list_esg_assets(account_id: str = Depends(require_admin_account)) -> List[ESGAssetModel]:
    entries = esg_filter.list_assets()
    return [ESGAssetModel(**asdict(entry)) for entry in entries]


@app.post("/esg/update", response_model=ESGAssetModel, status_code=status.HTTP_200_OK)
def update_esg_asset(
    payload: ESGUpdateRequest, account_id: str = Depends(require_admin_account)
) -> ESGAssetModel:
    try:
        entry = esg_filter.update_asset(
            symbol=payload.symbol, score=payload.score, flag=payload.flag, reason=payload.reason
        )
    except ValueError as exc:  # propagate bad requests as 400s
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return ESGAssetModel(**asdict(entry))


__all__ = [
    "ESGAsset",
    "ESGEntry",
    "ESGFilter",
    "esg_filter",
    "ESG_REASON",
]
