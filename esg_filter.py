"""ESG filter microservice for enforcing sustainability guardrails."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, Generator, Iterable, List, Optional, Protocol

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field, field_validator
try:  # pragma: no cover - optional dependency
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
except Exception:  # pragma: no cover - executed when SQLAlchemy is unavailable
    Engine = object  # type: ignore[assignment]
    Session = object  # type: ignore[assignment]
    SQLALCHEMY_AVAILABLE = False
else:  # pragma: no cover - exercised in environments with SQLAlchemy installed
    SQLALCHEMY_AVAILABLE = getattr(Column, "__module__", "").startswith("sqlalchemy")

from services.common.security import require_admin_account
from shared.account_scope import account_id_column


logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("risk.esg")


if SQLALCHEMY_AVAILABLE:
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

    class ESGRejection(Base):
        """Audit trail of trades rejected for ESG considerations."""

        __tablename__ = "esg_rejections"

        id = Column(Integer, primary_key=True, autoincrement=True)
        account_id = account_id_column()
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
else:
    Base = object

    @dataclass
    class ESGAsset:
        symbol: str
        score: float
        flag: str
        reason: Optional[str]
        updated_at: datetime

    @dataclass
    class ESGRejection:
        account_id: str
        symbol: str
        flag: Optional[str]
        score: Optional[float]
        reason: str
        note: Optional[str]
        recorded_at: datetime

    ENGINE = None  # type: ignore[assignment]
    SessionLocal = None  # type: ignore[assignment]


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


if SQLALCHEMY_AVAILABLE:
    class _SQLAlchemyBackend:
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

        def upsert_asset(
            self,
            symbol: str,
            score: float,
            flag: str,
            reason: Optional[str],
            timestamp: datetime,
        ) -> ESGEntry:
            with self._session() as session:
                asset = session.get(ESGAsset, symbol)
                if asset is None:
                    asset = ESGAsset(
                        symbol=symbol,
                        score=score,
                        flag=flag,
                        reason=reason,
                        updated_at=timestamp,
                    )
                    session.add(asset)
                else:
                    asset.score = score
                    asset.flag = flag
                    asset.reason = reason
                    asset.updated_at = timestamp
                session.commit()

                return ESGEntry(
                    symbol=asset.symbol,
                    score=asset.score,
                    flag=asset.flag,
                    reason=asset.reason,
                    updated_at=asset.updated_at,
                )

        def get_asset(self, symbol: str) -> Optional[ESGEntry]:
            with self._session() as session:
                asset = session.get(ESGAsset, symbol)
                if asset is None:
                    return None
                return ESGEntry(
                    symbol=asset.symbol,
                    score=asset.score,
                    flag=asset.flag,
                    reason=asset.reason,
                    updated_at=asset.updated_at,
                )

        def log_rejection(
            self,
            account_id: str,
            symbol: str,
            flag: Optional[str],
            score: Optional[float],
            note: Optional[str],
            recorded_at: datetime,
        ) -> None:
            with self._session() as session:
                record = ESGRejection(
                    account_id=account_id,
                    symbol=symbol,
                    flag=flag,
                    score=score,
                    reason=ESG_REASON,
                    note=note,
                    recorded_at=recorded_at,
                )
                session.add(record)
                session.commit()
else:
    class _SQLAlchemyBackend:  # pragma: no cover - unused when SQLAlchemy missing
        def __init__(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("SQLAlchemy backend is unavailable in this environment.")


class _InMemoryBackend:
    def __init__(self) -> None:
        self._assets: Dict[str, ESGEntry] = {}
        self._rejections: List[ESGRejection] = []

    def list_assets(self) -> List[ESGEntry]:
        return [
            ESGEntry(**asdict(entry))
            for entry in sorted(self._assets.values(), key=lambda record: record.symbol)
        ]

    def upsert_asset(
        self,
        symbol: str,
        score: float,
        flag: str,
        reason: Optional[str],
        timestamp: datetime,
    ) -> ESGEntry:
        entry = ESGEntry(symbol=symbol, score=score, flag=flag, reason=reason, updated_at=timestamp)
        self._assets[symbol] = entry
        return ESGEntry(**asdict(entry))

    def get_asset(self, symbol: str) -> Optional[ESGEntry]:
        entry = self._assets.get(symbol)
        if entry is None:
            return None
        return ESGEntry(**asdict(entry))

    def log_rejection(
        self,
        account_id: str,
        symbol: str,
        flag: Optional[str],
        score: Optional[float],
        note: Optional[str],
        recorded_at: datetime,
    ) -> None:
        self._rejections.append(
            ESGRejection(
                account_id=account_id,
                symbol=symbol,
                flag=flag,
                score=score,
                reason=ESG_REASON,
                note=note,
                recorded_at=recorded_at,
            )
        )


class ESGFilter:
    """Persistence backed ESG evaluation utility."""

    def __init__(self, backend: "_BackendProtocol") -> None:
        self._backend = backend

    def list_assets(self) -> List[ESGEntry]:
        return self._backend.list_assets()

    def update_asset(self, symbol: str, score: float | int | str, flag: str, reason: Optional[str]) -> ESGEntry:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_flag = _normalize_flag(flag)
        numeric_score = _normalize_score(score)
        trimmed_reason: Optional[str] = None
        if isinstance(reason, str):
            trimmed = reason.strip()
            trimmed_reason = trimmed or None
        timestamp = datetime.now(timezone.utc)

        entry = self._backend.upsert_asset(
            normalized_symbol, numeric_score, normalized_flag, trimmed_reason, timestamp
        )

        logger.info(
            "ESG asset updated",
            extra={"symbol": entry.symbol, "flag": entry.flag, "score": entry.score},
        )
        return entry

    def evaluate(self, symbol: str) -> tuple[bool, Optional[ESGEntry]]:
        normalized_symbol = _normalize_symbol(symbol)
        entry = self._backend.get_asset(normalized_symbol)
        if entry is None:
            return True, None
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

        recorded_at = datetime.now(timezone.utc)
        self._backend.log_rejection(account_id, normalized_symbol, flag, score, note, recorded_at)

        audit_logger.warning(
            "Trade rejected by ESG filter for account=%s symbol=%s",
            account_id,
            normalized_symbol,
            extra=payload,
        )


class _BackendProtocol(Protocol):
    def list_assets(self) -> List[ESGEntry]:
        ...

    def upsert_asset(
        self,
        symbol: str,
        score: float,
        flag: str,
        reason: Optional[str],
        timestamp: datetime,
    ) -> ESGEntry:
        ...

    def get_asset(self, symbol: str) -> Optional[ESGEntry]:
        ...

    def log_rejection(
        self,
        account_id: str,
        symbol: str,
        flag: Optional[str],
        score: Optional[float],
        note: Optional[str],
        recorded_at: datetime,
    ) -> None:
        ...


if SQLALCHEMY_AVAILABLE:
    _BACKEND: _BackendProtocol = _SQLAlchemyBackend(SessionLocal)
else:
    _BACKEND = _InMemoryBackend()


esg_filter = ESGFilter(_BACKEND)


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
