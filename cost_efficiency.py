"""Cost efficiency monitoring service.

This module exposes utilities for sampling infrastructure utilisation across CPU
and GPU resources and converts those observations into estimated hourly costs
for the key workloads that power the trading platform (training, inference and
backtesting).  Snapshots are persisted to a relational database so historical
cost efficiency reports can be built and an aggregated metric endpoint is
provided for operational dashboards.
"""

from __future__ import annotations

import enum
import logging
import os
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Generator, Mapping, Optional

import psutil
import yaml
from fastapi import Depends, FastAPI, Query
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, Float, String, create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

LOGGER = logging.getLogger(__name__)


def _coerce_rate(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


class ServiceType(str, enum.Enum):
    """Enumerates the workloads tracked by the service."""

    TRAINING = "training"
    INFERENCE = "inference"
    BACKTEST = "backtest"


@dataclass(slots=True)
class ResourceUsage:
    """Container for CPU and GPU utilisation readings."""

    cpu_utilisation: float
    gpu_utilisation: float


class ResourceMonitor:
    """Samples CPU/GPU utilisation using ``psutil`` and ``nvidia-smi``."""

    def __init__(self, *, cpu_interval: float = 0.1) -> None:
        self._cpu_interval = cpu_interval

    def sample(self) -> ResourceUsage:
        cpu = psutil.cpu_percent(interval=self._cpu_interval)
        gpu = self._gpu_utilisation()
        return ResourceUsage(cpu_utilisation=cpu, gpu_utilisation=gpu)

    @staticmethod
    def _gpu_utilisation() -> float:
        """Return the average GPU utilisation across all devices.

        When ``nvidia-smi`` is not available or fails, the method gracefully
        falls back to ``0.0``.
        """

        command = [
            "nvidia-smi",
            "--query-gpu=utilization.gpu",
            "--format=csv,noheader,nounits",
        ]
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=2.0,
            )
        except (FileNotFoundError, subprocess.SubprocessError, subprocess.TimeoutExpired):
            return 0.0

        values: list[float] = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                values.append(float(line))
            except ValueError:
                LOGGER.debug("Ignoring malformed GPU utilisation reading: %s", line)
        if not values:
            return 0.0
        return sum(values) / float(len(values))


@dataclass(slots=True)
class CostRates:
    """Hourly cost rates in USD for each workload."""

    training: float
    inference: float
    backtest: float

    @classmethod
    def load(cls, *, config_path: Optional[Path] = None) -> "CostRates":
        path = config_path or Path(os.getenv("COST_RATE_CONFIG", "config/costs.yaml"))
        default = {"training": 8.0, "inference": 3.0, "backtest": 1.5}
        data: Mapping[str, object] = {}
        if path.exists():
            try:
                loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            except yaml.YAMLError as exc:  # pragma: no cover - defensive guard
                LOGGER.warning("Failed to parse cost rate config %s: %s", path, exc)
            else:
                if isinstance(loaded, Mapping):
                    data = dict(loaded)
        rates_raw = data.get("rates") if isinstance(data, Mapping) else None
        if isinstance(rates_raw, Mapping):
            rates_source = dict(rates_raw)
        else:
            rates_source = dict(data)

        training_rate = _coerce_rate(rates_source.get("training"))
        inference_rate = _coerce_rate(rates_source.get("inference"))
        backtest_rate = _coerce_rate(rates_source.get("backtest"))

        if training_rate <= 0:
            training_rate = float(default["training"])
        if inference_rate <= 0:
            inference_rate = float(default["inference"])
        if backtest_rate <= 0:
            backtest_rate = float(default["backtest"])

        return cls(
            training=float(training_rate),
            inference=float(inference_rate),
            backtest=float(backtest_rate),
        )

    def rate_for(self, service: ServiceType) -> float:
        return {
            ServiceType.TRAINING: self.training,
            ServiceType.INFERENCE: self.inference,
            ServiceType.BACKTEST: self.backtest,
        }[service]


class CostEstimator:
    """Converts utilisation readings into hourly cost estimates."""

    def __init__(self, rates: CostRates) -> None:
        self._rates = rates

    def estimate_hourly_cost(self, service: ServiceType, usage: ResourceUsage) -> float:
        rate = self._rates.rate_for(service)
        utilisation = max(usage.cpu_utilisation, usage.gpu_utilisation) / 100.0
        if utilisation <= 0:
            return 0.0
        return rate * min(utilisation, 1.0)


Base = declarative_base()


class CostEfficiencyRecord(Base):
    """SQLAlchemy model capturing cost efficiency snapshots."""

    __tablename__ = "cost_efficiency"

    account_id = Column(String, primary_key=True)
    service = Column(String, primary_key=True)
    ts = Column(DateTime(timezone=True), primary_key=True, default=lambda: datetime.now(UTC))
    cost_usd = Column(Float, nullable=False)
    pnl_usd = Column(Float, nullable=False, default=0.0)


def _database_url() -> str:
    url = (
        os.getenv("COST_EFFICIENCY_DATABASE_URL")
        or os.getenv("TIMESCALE_DSN")
        or os.getenv("DATABASE_URL")
        or "sqlite:///./cost_efficiency.db"
    )
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def _engine_options(url: str) -> Dict[str, object]:
    options: Dict[str, object] = {"future": True}
    if url.startswith("sqlite://"):
        options.setdefault("connect_args", {"check_same_thread": False})
        if url.endswith(":memory:"):
            options["poolclass"] = StaticPool
    return options


_DATABASE_URL = _database_url()
_ENGINE: Engine = create_engine(_DATABASE_URL, **_engine_options(_DATABASE_URL))
SessionLocal = sessionmaker(bind=_ENGINE, autoflush=False, expire_on_commit=False, future=True)


def _create_tables() -> None:
    try:
        Base.metadata.create_all(_ENGINE)
    except SQLAlchemyError as exc:  # pragma: no cover - defensive logging
        LOGGER.exception("Failed to initialise cost efficiency tables: %s", exc)
        raise


@dataclass(slots=True)
class CostSnapshot:
    """Represents a persisted cost measurement."""

    account_id: str
    service: ServiceType
    cost_usd: float
    pnl_usd: float
    ts: datetime


class CostEfficiencyRepository:
    """Data access helper for cost efficiency records."""

    def __init__(self, session: Session):
        self._session = session

    def persist(self, snapshot: CostSnapshot) -> None:
        record = CostEfficiencyRecord(
            account_id=snapshot.account_id,
            service=snapshot.service.value,
            cost_usd=snapshot.cost_usd,
            pnl_usd=snapshot.pnl_usd,
            ts=snapshot.ts,
        )
        self._session.add(record)
        self._session.commit()

    def totals_by_service(self, account_id: Optional[str] = None) -> Mapping[str, float]:
        statement = select(
            CostEfficiencyRecord.service, func.sum(CostEfficiencyRecord.cost_usd)
        ).group_by(CostEfficiencyRecord.service)
        if account_id:
            statement = statement.where(CostEfficiencyRecord.account_id == account_id)
        rows = self._session.execute(statement).all()
        return {service: float(total or 0.0) for service, total in rows}

    def total_pnl(self, account_id: Optional[str] = None) -> float:
        statement = select(func.sum(CostEfficiencyRecord.pnl_usd))
        if account_id:
            statement = statement.where(CostEfficiencyRecord.account_id == account_id)
        result = self._session.execute(statement).scalar_one_or_none()
        return float(result or 0.0)

    def trade_count(self, account_id: Optional[str] = None) -> int:
        statement = select(func.count()).where(
            CostEfficiencyRecord.service == ServiceType.INFERENCE.value
        )
        if account_id:
            statement = statement.where(CostEfficiencyRecord.account_id == account_id)
        result = self._session.execute(statement).scalar_one()
        return int(result or 0)


_MONITOR = ResourceMonitor()
_RATES = CostRates.load()
_ESTIMATOR = CostEstimator(_RATES)


def record_cost_snapshot(
    *,
    account_id: str,
    service: ServiceType,
    pnl_usd: float = 0.0,
    session: Optional[Session] = None,
) -> CostSnapshot:
    """Capture utilisation, estimate the hourly cost and persist the snapshot."""

    usage = _MONITOR.sample()
    cost = _ESTIMATOR.estimate_hourly_cost(service, usage)
    snapshot = CostSnapshot(
        account_id=account_id,
        service=service,
        cost_usd=cost,
        pnl_usd=pnl_usd,
        ts=datetime.now(UTC),
    )

    close_session = False
    if session is None:
        session = SessionLocal()
        close_session = True
    try:
        repo = CostEfficiencyRepository(session)
        repo.persist(snapshot)
    finally:
        if close_session:
            session.close()
    return snapshot


class CostMetrics(BaseModel):
    """Response payload for ``/metrics/cost``."""

    training_cost_usd: float = Field(..., description="Total training cost in USD")
    inference_cost_usd: float = Field(..., description="Total inference cost in USD")
    cost_per_trade: Optional[float] = Field(
        None, description="Average infrastructure cost per recorded trade"
    )
    cost_per_pnl: Optional[float] = Field(
        None, description="Infrastructure cost required to generate $1 of PnL"
    )


app = FastAPI(title="Cost Efficiency Service", version="1.0.0")


@app.on_event("startup")
def _startup() -> None:  # pragma: no cover - FastAPI lifecycle glue
    _create_tables()


def get_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@app.get("/metrics/cost", response_model=CostMetrics)
def get_cost_metrics(
    account_id: Optional[str] = Query(default=None, description="Optional account filter"),
    session: Session = Depends(get_session),
) -> CostMetrics:
    repo = CostEfficiencyRepository(session)
    totals = repo.totals_by_service(account_id)
    training_cost = totals.get(ServiceType.TRAINING.value, 0.0)
    inference_cost = totals.get(ServiceType.INFERENCE.value, 0.0)

    trade_count = repo.trade_count(account_id)
    cost_per_trade = None
    if trade_count:
        cost_per_trade = inference_cost / trade_count

    total_cost = sum(totals.values())
    total_pnl = repo.total_pnl(account_id)
    cost_per_pnl = None
    if total_pnl:
        cost_per_pnl = total_cost / total_pnl

    return CostMetrics(
        training_cost_usd=training_cost,
        inference_cost_usd=inference_cost,
        cost_per_trade=cost_per_trade,
        cost_per_pnl=cost_per_pnl,
    )


__all__ = [
    "ServiceType",
    "ResourceUsage",
    "ResourceMonitor",
    "CostRates",
    "CostEstimator",
    "CostEfficiencyRecord",
    "CostSnapshot",
    "CostEfficiencyRepository",
    "record_cost_snapshot",
    "CostMetrics",
    "app",
]
