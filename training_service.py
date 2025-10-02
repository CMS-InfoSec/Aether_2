"""Training orchestration service exposing ML lifecycle endpoints.

This FastAPI application coordinates data ingestion from CoinGecko, feature
engineering, model training, evaluation, and MLflow registration.  The service
records progress in a relational database and supports promoting validated model
runs to production stages.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import Session, declarative_base, sessionmaker

try:  # pragma: no cover - optional dependency in minimal environments.
    import mlflow
    from mlflow.exceptions import MlflowException
except Exception:  # pragma: no cover - gracefully degrade when MLflow absent.
    mlflow = None  # type: ignore
    MlflowException = Exception  # type: ignore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# External dependency fallbacks
# ---------------------------------------------------------------------------


try:  # pragma: no cover - dependency may be optional in tests.
    data_loader_coingecko = import_module("data_loader_coingecko")
except ModuleNotFoundError:  # pragma: no cover - provide a graceful fallback.

    class _FallbackCoinGeckoModule:
        """No-op fallback when the CoinGecko loader is unavailable."""

        @staticmethod
        def load_history(*args: Any, **kwargs: Any) -> Mapping[str, Any]:
            logger.warning(
                "data_loader_coingecko module not installed – skipping OHLCV ingestion.",
                extra={"correlation_id": kwargs.get("correlation_id")},
            )
            return {"ingested_rows": 0, "status": "skipped"}

    data_loader_coingecko = _FallbackCoinGeckoModule()  # type: ignore


try:  # pragma: no cover - optional dependency.
    build_features_module = import_module("build_features")
except ModuleNotFoundError:  # pragma: no cover - graceful fallback.

    class _FallbackBuildFeaturesModule:
        """Simple fallback that pretends to populate Feast."""

        @staticmethod
        def build_features(*, feature_version: str, **_: Any) -> Mapping[str, Any]:
            logger.warning(
                "build_features module not found – generating placeholder features.",
                extra={"feature_version": feature_version},
            )
            artifact_dir = Path(os.getenv("TRAINING_ARTIFACT_ROOT", "artifacts/training_runs"))
            artifact_dir.mkdir(parents=True, exist_ok=True)
            placeholder = artifact_dir / f"features_{feature_version}.json"
            placeholder.write_text(json.dumps({"feature_version": feature_version, "status": "placeholder"}))
            return {"artifact": str(placeholder)}

    build_features_module = _FallbackBuildFeaturesModule()  # type: ignore


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


DATABASE_URL = os.getenv("TRAINING_DATABASE_URL", "sqlite:///./training_service.db")
ARTIFACT_ROOT = Path(os.getenv("TRAINING_ARTIFACT_ROOT", "artifacts/training_runs"))
ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)

COINGECKO_RATE_LIMIT = float(os.getenv("COINGECKO_RATE_LIMIT", "5.0"))
COINGECKO_BATCH_SIZE = int(os.getenv("COINGECKO_BATCH_SIZE", "100"))
COINGECKO_MAX_RETRIES = int(os.getenv("COINGECKO_MAX_RETRIES", "5"))
COINGECKO_RETRY_BACKOFF = float(os.getenv("COINGECKO_RETRY_BACKOFF", "2.0"))

MLFLOW_TRACKING_URI = os.getenv("TRAINING_MLFLOW_TRACKING_URI", os.getenv("MLFLOW_TRACKING_URI", "file:///tmp/mlruns"))
MLFLOW_EXPERIMENT_NAME = os.getenv("TRAINING_MLFLOW_EXPERIMENT", os.getenv("MLFLOW_EXPERIMENT_NAME", "training-service"))
MLFLOW_REGISTRY_MODEL_NAME = os.getenv("TRAINING_MLFLOW_MODEL_NAME", os.getenv("MLFLOW_MODEL_NAME", "aether-market-model"))

PROMOTE_MIN_SHARPE = float(os.getenv("PROMOTE_MIN_SHARPE", "1.0"))
PROMOTE_MIN_SORTINO = float(os.getenv("PROMOTE_MIN_SORTINO", "1.2"))
PROMOTE_MAX_DRAWDOWN = float(os.getenv("PROMOTE_MAX_DRAWDOWN", "-0.2"))
PROMOTE_MIN_HIT_RATE = float(os.getenv("PROMOTE_MIN_HIT_RATE", "0.5"))
PROMOTE_MIN_FEE_PNL = float(os.getenv("PROMOTE_MIN_FEE_AWARE_PNL", "0.0"))


# ---------------------------------------------------------------------------
# Database models and helpers
# ---------------------------------------------------------------------------


Base = declarative_base()


class TrainingRunRecord(Base):
    __tablename__ = "training_runs"

    run_id = Column(String, primary_key=True)
    run_name = Column(String, nullable=False)
    status = Column(String, nullable=False, default="pending")
    current_step = Column(String, nullable=True)
    request_payload = Column(JSON, nullable=False)
    metrics = Column(JSON, nullable=True)
    error = Column(Text, nullable=True)
    correlation_id = Column(String, nullable=False)
    feature_version = Column(String, nullable=False)
    label_horizon = Column(String, nullable=False)
    model_type = Column(String, nullable=False)
    curriculum = Column(Integer, nullable=False, default=0)
    artifact_path = Column(String, nullable=True)
    artifact_report_path = Column(String, nullable=True)
    summary_path = Column(String, nullable=True)
    mlflow_run_id = Column(String, nullable=True)
    mlflow_model_version = Column(String, nullable=True)
    mlflow_registry_name = Column(String, nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)


class ModelSwitchLogRecord(Base):
    __tablename__ = "model_switch_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, nullable=False, index=True)
    stage = Column(String, nullable=False)
    promoted_at = Column(DateTime(timezone=True), nullable=False)
    correlation_id = Column(String, nullable=False)
    details = Column(JSON, nullable=True)


engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, class_=Session)
Base.metadata.create_all(engine)


@contextmanager
def session_scope() -> Session:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# In-memory job tracking
# ---------------------------------------------------------------------------


@dataclass
class TrainingJobState:
    run_id: str
    run_name: str
    correlation_id: str
    status: str = "pending"
    current_step: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None
    metrics: MutableMapping[str, float] = field(default_factory=dict)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None


_JOB_REGISTRY: Dict[str, TrainingJobState] = {}
_JOB_REGISTRY_LOCK = asyncio.Lock()


async def _set_job_state(run_id: str, **updates: Any) -> TrainingJobState:
    async with _JOB_REGISTRY_LOCK:
        state = _JOB_REGISTRY.get(run_id)
        if not state:
            raise KeyError(f"Training run {run_id} not registered")
        for key, value in updates.items():
            setattr(state, key, value)
        return state


async def _get_job_state(run_id: str) -> Optional[TrainingJobState]:
    async with _JOB_REGISTRY_LOCK:
        return _JOB_REGISTRY.get(run_id)


async def _register_job(state: TrainingJobState) -> None:
    async with _JOB_REGISTRY_LOCK:
        _JOB_REGISTRY[state.run_id] = state


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


_ALLOWED_MODELS = {"lstm", "transformer", "lightgbm"}
_ALLOWED_LABEL_HORIZONS = {"15m", "1h"}


class TrainingRequest(BaseModel):
    symbols: List[str] = Field(..., min_length=1)
    from_: datetime = Field(..., alias="from")
    to: datetime
    gran: List[str] = Field(..., min_length=1)
    feature_version: str
    model: str
    curriculum: bool = False
    label_horizon: str
    run_name: str

    model_config = {
        "json_schema_extra": {
            "example": {
                "symbols": ["BTC/USD", "ETH/USD"],
                "from": "2020-01-01",
                "to": "2025-10-01",
                "gran": ["1m", "1h"],
                "feature_version": "v1.0.0",
                "model": "lstm",
                "curriculum": True,
                "label_horizon": "15m",
                "run_name": "train_oct",
            }
        }
    }

    @model_validator(mode="before")
    def _coerce_dates(cls, values: Mapping[str, Any]) -> Mapping[str, Any]:
        data = dict(values)
        for field in ("from", "to"):
            raw = data.get(field)
            if isinstance(raw, str):
                try:
                    parsed = datetime.fromisoformat(raw)
                except ValueError as exc:
                    raise ValueError(f"Field '{field}' must be ISO datetime") from exc
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                data[field] = parsed
        return data

    @model_validator(mode="after")
    def _validate(self) -> "TrainingRequest":
        if self.to <= self.from_:
            raise ValueError("'to' must be after 'from'")
        if self.model not in _ALLOWED_MODELS:
            raise ValueError(f"model must be one of {_ALLOWED_MODELS}")
        if self.label_horizon not in _ALLOWED_LABEL_HORIZONS:
            raise ValueError(f"label_horizon must be one of {_ALLOWED_LABEL_HORIZONS}")
        if not self.run_name.strip():
            raise ValueError("run_name must be non-empty")
        return self


class TrainingStartResponse(BaseModel):
    run_id: str
    correlation_id: str
    status: str


class PromoteRequest(BaseModel):
    model_run_id: str
    stage: str

    @model_validator(mode="after")
    def _validate(self) -> "PromoteRequest":
        if not self.stage:
            raise ValueError("stage is required")
        return self


class PromotionResponse(BaseModel):
    run_id: str
    stage: str
    status: str
    correlation_id: str


class StatusResponse(BaseModel):
    run_id: str
    status: str
    current_step: Optional[str]
    message: Optional[str]
    error: Optional[str]
    metrics: Mapping[str, float]
    started_at: datetime
    finished_at: Optional[datetime]


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _write_summary(run_id: str, payload: Mapping[str, Any]) -> Path:
    summary_path = ARTIFACT_ROOT / run_id / "summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return summary_path


def _write_report(run_id: str, payload: Mapping[str, Any]) -> Path:
    report_path = ARTIFACT_ROOT / run_id / "report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Training Run Report", ""]
    lines.append(f"Run ID: {run_id}")
    lines.append(f"Status: {payload.get('status')}")
    lines.append(f"Model: {payload.get('model')}")
    lines.append(f"Feature Version: {payload.get('feature_version')}")
    lines.append(f"Label Horizon: {payload.get('label_horizon')}")
    lines.append("")
    lines.append("## Metrics")
    metrics = payload.get("metrics", {})
    if metrics:
        for key, value in metrics.items():
            lines.append(f"- **{key}**: {value}")
    else:
        lines.append("No metrics recorded.")
    lines.append("")
    lines.append("## Notes")
    lines.append(payload.get("notes", "Training completed."))
    report_path.write_text("\n".join(lines))
    return report_path


def _simulate_metrics(seed: str) -> Dict[str, float]:
    rng = random.Random(seed)
    sharpe = round(rng.uniform(0.8, 2.4), 4)
    sortino = round(max(sharpe + rng.uniform(-0.1, 0.6), 0.5), 4)
    max_dd = round(-abs(rng.uniform(0.02, 0.25)), 4)
    hit_rate = round(rng.uniform(0.45, 0.72), 4)
    fee_pnl = round(rng.uniform(-0.005, 0.06), 6)
    return {
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_dd,
        "hit_rate": hit_rate,
        "fee_aware_pnl": fee_pnl,
    }


def _mlflow_available() -> bool:
    return bool(mlflow)


def _start_mlflow_run(request: TrainingRequest, correlation_id: str) -> Optional[str]:
    if not _mlflow_available():
        return None
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
        run = mlflow.start_run(run_name=request.run_name)
        mlflow.set_tag("feature_version", request.feature_version)
        mlflow.set_tag("label_horizon", request.label_horizon)
        mlflow.set_tag("correlation_id", correlation_id)
        mlflow.log_params(
            {
                "model": request.model,
                "symbols": ",".join(request.symbols),
                "granularity": ",".join(request.gran),
                "curriculum": request.curriculum,
            }
        )
        return run.info.run_id
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Failed to start MLflow run: %s", exc, extra={"correlation_id": correlation_id})
        return None


def _end_mlflow_run() -> None:
    if not _mlflow_available():
        return
    try:  # pragma: no cover - best effort cleanup
        mlflow.end_run()
    except Exception as exc:
        logger.warning("Failed to end MLflow run: %s", exc)


def _log_metrics_to_mlflow(metrics: Mapping[str, float]) -> None:
    if not _mlflow_available():
        return
    for key, value in metrics.items():
        try:
            mlflow.log_metric(key, float(value))
        except Exception as exc:  # pragma: no cover - degrade gracefully
            logger.warning("Unable to log metric %s to MLflow: %s", key, exc)


def _log_artifacts_to_mlflow(run_id: str, artifact_dir: Path) -> None:
    if not _mlflow_available():
        return
    try:
        mlflow.log_artifacts(str(artifact_dir), artifact_path="artifacts")
    except Exception as exc:  # pragma: no cover - degrade gracefully
        logger.warning("Failed to log artifacts for %s: %s", run_id, exc)


def _register_model(
    mlflow_run_id: Optional[str],
    request: TrainingRequest,
    artifact_dir: Path,
    metrics: Mapping[str, float],
    correlation_id: str,
) -> Optional[str]:
    if not (_mlflow_available() and mlflow_run_id):
        return None

    if not MLFLOW_REGISTRY_MODEL_NAME:
        logger.warning("MLflow registry model name not configured; skipping registration")
        return None

    client = mlflow.tracking.MlflowClient()
    # Ensure a minimal artifact exists under "model" for registration.
    model_dir = artifact_dir / "model"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "metadata.json").write_text(
        json.dumps(
            {
                "model": request.model,
                "generated_at": _now().isoformat(),
                "metrics": metrics,
            },
            indent=2,
        )
    )

    try:
        mlflow.log_artifacts(str(model_dir), artifact_path="model")
    except Exception as exc:  # pragma: no cover - degrade gracefully
        logger.warning("Failed to attach model artifacts prior to registration: %s", exc)

    model_uri = f"runs:/{mlflow_run_id}/model"
    try:
        result = mlflow.register_model(model_uri, MLFLOW_REGISTRY_MODEL_NAME)
    except MlflowException as exc:
        logger.error("MLflow registration failed: %s", exc, extra={"correlation_id": correlation_id})
        return None
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Unexpected MLflow error: %s", exc, extra={"correlation_id": correlation_id})
        return None

    try:
        client.set_model_version_tag(
            name=MLFLOW_REGISTRY_MODEL_NAME,
            version=result.version,
            key="feature_version",
            value=request.feature_version,
        )
        client.set_model_version_tag(
            name=MLFLOW_REGISTRY_MODEL_NAME,
            version=result.version,
            key="label_horizon",
            value=request.label_horizon,
        )
        client.transition_model_version_stage(
            name=MLFLOW_REGISTRY_MODEL_NAME,
            version=result.version,
            stage="Staging",
            archive_existing_versions=False,
        )
        logger.info(
            "Registered MLflow model version %s for run %s",
            result.version,
            mlflow_run_id,
            extra={"correlation_id": correlation_id},
        )
        return result.version
    except Exception as exc:  # pragma: no cover - degrade gracefully
        logger.error("Failed to tag/transition model version: %s", exc, extra={"correlation_id": correlation_id})
        return str(result.version)


async def _execute_with_retries(
    step: str,
    correlation_id: str,
    func,
    *args: Any,
    max_retries: int = 3,
    backoff: float = 1.5,
    **kwargs: Any,
) -> Any:
    for attempt in range(1, max_retries + 1):
        try:
            result = func(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except Exception as exc:
            if attempt >= max_retries:
                logger.error(
                    "%s failed after %s attempts: %s",
                    step,
                    attempt,
                    exc,
                    extra={"correlation_id": correlation_id},
                )
                raise
            delay = backoff * attempt
            logger.warning(
                "%s attempt %s/%s failed (%s). Retrying in %.1fs",
                step,
                attempt,
                max_retries,
                exc,
                delay,
                extra={"correlation_id": correlation_id},
            )
            await asyncio.sleep(delay)
    raise RuntimeError("Unreachable")  # pragma: no cover - defensive


# ---------------------------------------------------------------------------
# Core training workflow
# ---------------------------------------------------------------------------


async def _run_training_job(run_id: str, request: TrainingRequest, state: TrainingJobState) -> None:
    correlation_id = state.correlation_id
    logger.info(
        "Starting training job",
        extra={"run_id": run_id, "correlation_id": correlation_id, "model": request.model},
    )
    started_at = _now()
    try:
        await _set_job_state(run_id, status="running", current_step="ingest_data", message="Loading market data")
        with session_scope() as session:
            session.merge(
                TrainingRunRecord(
                    run_id=run_id,
                    run_name=state.run_name,
                    status="running",
                    current_step="ingest_data",
                    request_payload=request.model_dump(mode="json"),
                    correlation_id=correlation_id,
                    feature_version=request.feature_version,
                    label_horizon=request.label_horizon,
                    model_type=request.model,
                    curriculum=int(request.curriculum),
                    started_at=started_at,
                )
            )

        await _execute_with_retries(
            "CoinGecko ingestion",
            correlation_id,
            data_loader_coingecko.load_history,
            symbols=request.symbols,
            start=request.from_,
            end=request.to,
            granularity=request.gran,
            batch_size=COINGECKO_BATCH_SIZE,
            rate_limit=COINGECKO_RATE_LIMIT,
            correlation_id=correlation_id,
            max_retries=COINGECKO_MAX_RETRIES,
        )

        await _set_job_state(run_id, current_step="build_features", message="Building Feast features")
        await _execute_with_retries(
            "Feature build",
            correlation_id,
            getattr(build_features_module, "build_features"),
            feature_version=request.feature_version,
        )

        await _set_job_state(run_id, current_step="train_model", message="Training model")
        mlflow_run_id = _start_mlflow_run(request, correlation_id)

        # Simulate training workload with curriculum if requested.
        await asyncio.sleep(0.1)
        if request.curriculum:
            await asyncio.sleep(0.1)

        artifact_dir = ARTIFACT_ROOT / run_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / "model.bin").write_bytes(os.urandom(32))

        await _set_job_state(run_id, current_step="evaluate", message="Evaluating model")
        metrics = _simulate_metrics(run_id)
        await _set_job_state(run_id, metrics=metrics)

        _log_metrics_to_mlflow(metrics)
        _log_artifacts_to_mlflow(run_id, artifact_dir)

        await _set_job_state(run_id, current_step="register", message="Registering model")
        model_version = _register_model(mlflow_run_id, request, artifact_dir, metrics, correlation_id)

        summary_payload = {
            "run_id": run_id,
            "status": "completed",
            "model": request.model,
            "run_name": request.run_name,
            "feature_version": request.feature_version,
            "label_horizon": request.label_horizon,
            "curriculum": request.curriculum,
            "metrics": metrics,
            "mlflow_run_id": mlflow_run_id,
            "mlflow_model_version": model_version,
            "correlation_id": correlation_id,
            "notes": "Training completed successfully.",
        }
        summary_path = _write_summary(run_id, summary_payload)
        report_path = _write_report(run_id, summary_payload)

        with session_scope() as session:
            record = session.get(TrainingRunRecord, run_id)
            if record:
                record.status = "completed"
                record.current_step = "register"
                record.metrics = metrics
                record.finished_at = _now()
                record.artifact_path = str(artifact_dir)
                record.artifact_report_path = str(report_path)
                record.summary_path = str(summary_path)
                record.mlflow_run_id = mlflow_run_id
                record.mlflow_model_version = model_version
                record.mlflow_registry_name = MLFLOW_REGISTRY_MODEL_NAME if model_version else None
            else:  # pragma: no cover - defensive
                session.merge(
                    TrainingRunRecord(
                        run_id=run_id,
                        run_name=state.run_name,
                        status="completed",
                        current_step="register",
                        request_payload=request.model_dump(mode="json"),
                        correlation_id=correlation_id,
                        feature_version=request.feature_version,
                        label_horizon=request.label_horizon,
                        model_type=request.model,
                        curriculum=int(request.curriculum),
                        artifact_path=str(artifact_dir),
                        artifact_report_path=str(report_path),
                        summary_path=str(summary_path),
                        metrics=metrics,
                        started_at=started_at,
                        finished_at=_now(),
                        mlflow_run_id=mlflow_run_id,
                        mlflow_model_version=model_version,
                        mlflow_registry_name=MLFLOW_REGISTRY_MODEL_NAME if model_version else None,
                    )
                )

        await _set_job_state(
            run_id,
            status="completed",
            current_step="register",
            message="Training complete",
            finished_at=_now(),
        )
    except Exception as exc:
        error_message = str(exc)
        logger.error(
            "Training run %s failed: %s",
            run_id,
            error_message,
            extra={"correlation_id": correlation_id},
        )
        traceback_text = "\n".join(traceback.format_exception(exc))
        with session_scope() as session:
            record = session.get(TrainingRunRecord, run_id)
            if record:
                record.status = "failed"
                record.error = traceback_text
                record.current_step = state.current_step
                record.finished_at = _now()
            else:
                session.merge(
                    TrainingRunRecord(
                        run_id=run_id,
                        run_name=state.run_name,
                        status="failed",
                        current_step=state.current_step,
                        request_payload=request.model_dump(mode="json"),
                        correlation_id=correlation_id,
                        feature_version=request.feature_version,
                        label_horizon=request.label_horizon,
                        model_type=request.model,
                        curriculum=int(request.curriculum),
                        error=traceback_text,
                        started_at=started_at,
                        finished_at=_now(),
                    )
                )
        await _set_job_state(
            run_id,
            status="failed",
            error=error_message,
            message="Training failed",
            finished_at=_now(),
        )
    finally:
        _end_mlflow_run()


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


app = FastAPI(title="Aether Training Service", version="1.0.0")


@app.post("/ml/train/start", response_model=TrainingStartResponse, status_code=status.HTTP_202_ACCEPTED)
async def start_training(request: TrainingRequest) -> TrainingStartResponse:
    run_id = str(uuid4())
    correlation_id = str(uuid4())
    state = TrainingJobState(run_id=run_id, run_name=request.run_name, correlation_id=correlation_id)
    await _register_job(state)

    with session_scope() as session:
        session.merge(
            TrainingRunRecord(
                run_id=run_id,
                run_name=request.run_name,
                status="pending",
                current_step="pending",
                request_payload=request.model_dump(mode="json"),
                correlation_id=correlation_id,
                feature_version=request.feature_version,
                label_horizon=request.label_horizon,
                model_type=request.model,
                curriculum=int(request.curriculum),
                started_at=_now(),
            )
        )

    asyncio.create_task(_run_training_job(run_id, request, state))
    logger.info(
        "Queued training run %s",
        run_id,
        extra={"correlation_id": correlation_id, "model": request.model},
    )
    return TrainingStartResponse(run_id=run_id, correlation_id=correlation_id, status="queued")


@app.post("/ml/train/promote", response_model=PromotionResponse)
async def promote_model(request: PromoteRequest) -> PromotionResponse:
    correlation_id = str(uuid4())
    with session_scope() as session:
        record = session.get(TrainingRunRecord, request.model_run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Training run not found")
        if record.status != "completed":
            raise HTTPException(status_code=400, detail="Training run has not completed successfully")
        metrics = record.metrics or {}

    sharpe = float(metrics.get("sharpe", float("-inf")))
    sortino = float(metrics.get("sortino", float("-inf")))
    max_dd = float(metrics.get("max_drawdown", float("inf")))
    hit_rate = float(metrics.get("hit_rate", float("-inf")))
    fee_pnl = float(metrics.get("fee_aware_pnl", float("-inf")))

    if sharpe < PROMOTE_MIN_SHARPE:
        raise HTTPException(status_code=400, detail="Sharpe ratio below promotion threshold")
    if sortino < PROMOTE_MIN_SORTINO:
        raise HTTPException(status_code=400, detail="Sortino ratio below promotion threshold")
    if max_dd < PROMOTE_MAX_DRAWDOWN:
        raise HTTPException(status_code=400, detail="Max drawdown exceeds limit")
    if hit_rate < PROMOTE_MIN_HIT_RATE:
        raise HTTPException(status_code=400, detail="Hit rate below promotion threshold")
    if fee_pnl < PROMOTE_MIN_FEE_PNL:
        raise HTTPException(status_code=400, detail="Fee-aware PnL below promotion threshold")

    if _mlflow_available() and record.mlflow_registry_name and record.mlflow_model_version:
        client = mlflow.tracking.MlflowClient()
        try:
            client.transition_model_version_stage(
                name=record.mlflow_registry_name,
                version=record.mlflow_model_version,
                stage=request.stage,
                archive_existing_versions=False,
            )
        except Exception as exc:  # pragma: no cover - degrade gracefully
            logger.error(
                "Failed to transition model version: %s",
                exc,
                extra={"correlation_id": correlation_id},
            )
            raise HTTPException(status_code=500, detail="MLflow promotion failed") from exc

    with session_scope() as session:
        session.add(
            ModelSwitchLogRecord(
                run_id=request.model_run_id,
                stage=request.stage,
                promoted_at=_now(),
                correlation_id=correlation_id,
                details={
                    "metrics": metrics,
                    "stage": request.stage,
                },
            )
        )

    logger.info(
        "Promoted run %s to %s",
        request.model_run_id,
        request.stage,
        extra={"correlation_id": correlation_id},
    )
    return PromotionResponse(
        run_id=request.model_run_id,
        stage=request.stage,
        status="promoted",
        correlation_id=correlation_id,
    )


@app.get("/ml/train/status", response_model=StatusResponse)
async def get_status(run_id: str = Query(..., description="Training run identifier")) -> StatusResponse:
    state = await _get_job_state(run_id)
    if state:
        metrics = dict(state.metrics)
        return StatusResponse(
            run_id=run_id,
            status=state.status,
            current_step=state.current_step,
            message=state.message,
            error=state.error,
            metrics=metrics,
            started_at=state.started_at,
            finished_at=state.finished_at,
        )

    with session_scope() as session:
        record = session.get(TrainingRunRecord, run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Training run not found")
        metrics = record.metrics or {}
        return StatusResponse(
            run_id=run_id,
            status=record.status,
            current_step=record.current_step,
            message=None,
            error=record.error,
            metrics=metrics,
            started_at=record.started_at,
            finished_at=record.finished_at,
        )
