"""FastAPI service orchestrating the end-to-end ML training workflow."""
from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import uuid4

try:  # pragma: no cover - prefer FastAPI when available
    from fastapi import FastAPI, HTTPException
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import FastAPI, HTTPException  # type: ignore[misc]
from pydantic import BaseModel, Field, field_validator

from shared.postgres import normalize_sqlalchemy_dsn

if TYPE_CHECKING:  # pragma: no cover - used only for static analysis.
    from pandas import DataFrame
    from ml.training import workflow as workflow_module
else:  # pragma: no cover - runtime fallback when pandas is unavailable.
    DataFrame = Any

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MissingDependencyError(RuntimeError):
    """Raised when optional ML training dependencies are unavailable."""


@lru_cache(maxsize=1)
def _require_pandas():
    try:
        import pandas as pandas_module  # type: ignore import-not-found
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via tests
        raise MissingDependencyError(
            "pandas is required to orchestrate training jobs."
        ) from exc
    return pandas_module


@lru_cache(maxsize=1)
def _require_feature_builder():
    try:
        from ml.features import build_features as builder  # type: ignore import-not-found
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via tests
        raise MissingDependencyError(
            "Feature engineering requires pandas, numpy, and SQLAlchemy."
        ) from exc
    except ImportError as exc:  # pragma: no cover - defensive runtime guard
        raise MissingDependencyError(
            "Feature engineering dependencies could not be imported."
        ) from exc
    return builder


@lru_cache(maxsize=1)
def _require_workflow():
    try:
        from ml.training import workflow as workflow_module  # type: ignore import-not-found
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via tests
        raise MissingDependencyError(
            "ML training workflow requires pandas, numpy, and torch."
        ) from exc
    except ImportError as exc:  # pragma: no cover - defensive runtime guard
        raise MissingDependencyError(
            "ML training workflow dependencies could not be imported."
        ) from exc
    return workflow_module


@lru_cache(maxsize=1)
def _require_data_loader():
    try:
        from ml.training import data_loader_coingecko as loader  # type: ignore import-not-found
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via tests
        raise MissingDependencyError(
            "CoinGecko ingestion requires pandas, requests, and great_expectations."
        ) from exc
    except ImportError as exc:  # pragma: no cover - defensive runtime guard
        raise MissingDependencyError(
            "CoinGecko ingestion dependencies could not be imported."
        ) from exc
    return loader


APP_TITLE = "Aether ML Training"
APP_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


_SQLITE_FALLBACK_FLAG = "TRAINING_ALLOW_SQLITE_FOR_TESTS"


def _resolve_artifact_root(raw: str | None, *, default: Path) -> Path:
    """Return a sanitised artifact directory path."""

    candidate_text = (raw or "").strip()
    if candidate_text and any(ord(char) < 32 for char in candidate_text):
        raise ValueError("TRAINING_ARTIFACT_ROOT must not contain control characters")

    candidate = Path(candidate_text).expanduser() if candidate_text else default
    if any(part == ".." for part in candidate.parts):
        raise ValueError("TRAINING_ARTIFACT_ROOT must not contain parent directory references")

    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate

    resolved_candidate = candidate.resolve(strict=False)

    if candidate.is_symlink():
        raise ValueError("TRAINING_ARTIFACT_ROOT must not be a symlink")

    for ancestor in candidate.parents:
        if ancestor.is_symlink():
            try:
                resolved_ancestor = ancestor.resolve(strict=False)
            except OSError as exc:  # pragma: no cover - extremely unlikely on supported platforms
                raise ValueError("TRAINING_ARTIFACT_ROOT symlink target could not be resolved") from exc
            if not resolved_ancestor.exists():
                raise ValueError("TRAINING_ARTIFACT_ROOT symlink targets must exist")
            if not resolved_ancestor.is_dir():
                raise ValueError("TRAINING_ARTIFACT_ROOT symlink targets must resolve to directories")
            try:
                resolved_candidate.relative_to(resolved_ancestor)
            except ValueError:
                raise ValueError("TRAINING_ARTIFACT_ROOT must not escape via symlinked ancestors")

    if resolved_candidate.exists() and not resolved_candidate.is_dir():
        raise ValueError("TRAINING_ARTIFACT_ROOT must reference a directory")

    resolved_candidate.mkdir(parents=True, exist_ok=True)
    return resolved_candidate


def _resolve_timescale_uri() -> str:
    raw_dsn = os.getenv("TRAINING_TIMESCALE_URI") or os.getenv("DATABASE_URL")
    if not raw_dsn:
        raise RuntimeError(
            "TRAINING_TIMESCALE_URI or DATABASE_URL must be configured with a "
            "PostgreSQL/Timescale DSN for the training service."
        )

    allow_sqlite = os.getenv(_SQLITE_FALLBACK_FLAG) == "1"
    normalized = normalize_sqlalchemy_dsn(
        raw_dsn,
        allow_sqlite=allow_sqlite,
        label="Training service database URL",
    )

    if normalized.startswith("sqlite") and allow_sqlite:
        logger.warning(
            "Allowing sqlite database URL for training service because %s=1. "
            "Do not enable this flag outside test environments.",
            _SQLITE_FALLBACK_FLAG,
        )

    return normalized


DEFAULT_TIMESCALE_URI = _resolve_timescale_uri()
DEFAULT_ARTIFACT_ROOT = _resolve_artifact_root(
    os.getenv("TRAINING_ARTIFACT_ROOT"), default=Path("/tmp/aether-training")
)
MLFLOW_TRACKING_URI = os.getenv("TRAINING_MLFLOW_TRACKING_URI", os.getenv("MLFLOW_TRACKING_URI"))
MLFLOW_EXPERIMENT = os.getenv("TRAINING_MLFLOW_EXPERIMENT", os.getenv("MLFLOW_EXPERIMENT_NAME", "training-service"))
MLFLOW_MODEL_NAME = os.getenv("TRAINING_MLFLOW_MODEL_NAME", os.getenv("MLFLOW_MODEL_NAME"))
MLFLOW_TARGET_STAGE = os.getenv("TRAINING_MLFLOW_TARGET_STAGE", "Staging")
CANARY_STAGE_DEFAULT = os.getenv("TRAINING_MLFLOW_CANARY_STAGE", "Canary")

LABEL_COLUMN = "close"


class ThresholdOverrides(BaseModel):
    """Threshold overrides used to determine canary readiness."""

    sharpe: float = 1.0
    sortino: float = 1.5
    max_drawdown: float = -0.1
    cvar: float = -0.05


class TrainStartRequest(BaseModel):
    """Input payload for the training orchestration endpoint."""

    symbols: List[str] = Field(..., min_length=1, description="CoinGecko asset identifiers")
    vs_currency: str = Field("usd", description="Quote currency for OHLCV ingestion")
    start: datetime = Field(..., description="Inclusive start timestamp")
    end: datetime = Field(..., description="Exclusive end timestamp")
    granularity: str = Field(..., description="Bar size identifier, e.g. 1h")
    feature_version: str = Field(..., description="Semantic feature version to materialise")
    changelog: str = Field(
        "Automated feature build triggered by training service",
        description="Changelog entry recorded with the feature version",
    )
    label_horizon: int = Field(1, ge=1, description="Forward return steps used for labelling")
    run_name: Optional[str] = Field(None, description="Optional MLflow run name")
    promote_canary: bool = Field(False, description="Promote successful models to the canary stage")
    canary_stage: str = Field(CANARY_STAGE_DEFAULT, description="Target MLflow stage for canary promotion")
    thresholds: ThresholdOverrides = Field(default_factory=ThresholdOverrides)

    @field_validator("end")
    @classmethod
    def _validate_window(cls, end: datetime, values: Dict[str, object]) -> datetime:
        start = values.get("start")
        if isinstance(start, datetime) and end <= start:
            raise ValueError("end must be after start")
        return end

    @field_validator("start", "end")
    @classmethod
    def _ensure_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class TrainStartResponse(BaseModel):
    """Response payload summarising the training execution."""

    run_id: str
    mlflow_run_id: Optional[str]
    metrics: Dict[str, float]
    model_version: Optional[str]
    model_stage: Optional[str]
    canary_ready: bool
    canary_promoted: bool


app = FastAPI(title=APP_TITLE, version=APP_VERSION)


def _prepare_close_frame(
    symbol: str,
    frame: "DataFrame",
    *,
    label_column: str,
    entity_column: str,
    timestamp_column: str,
) -> "DataFrame":
    pd = _require_pandas()
    working = frame.copy()
    working["ts"] = pd.to_datetime(working["ts"], utc=True)
    working[entity_column] = symbol
    working.rename(columns={"ts": timestamp_column, "close": label_column}, inplace=True)
    columns = [timestamp_column, entity_column, label_column]
    return working[columns]


def _augment_features_with_close(
    features: "DataFrame",
    closes: List["DataFrame"],
    *,
    label_column: str,
    entity_column: str,
    timestamp_column: str,
) -> "DataFrame":
    if not closes:
        return features
    pd = _require_pandas()
    combined = pd.concat(closes, ignore_index=True)
    combined[timestamp_column] = pd.to_datetime(combined[timestamp_column], utc=True)
    merged = features.merge(
        combined,
        on=[timestamp_column, entity_column],
        how="left",
    )
    merged.dropna(subset=[label_column], inplace=True)
    return merged


def _build_training_config(
    *,
    request: TrainStartRequest,
    feature_columns: List[str],
    run_id: str,
) -> "workflow_module.TrainingJobConfig":
    workflow = _require_workflow()
    feature_builder = _require_feature_builder()
    lookback_days = max(1, int(math.ceil((request.end - request.start).total_seconds() / 86400)))
    timescale = workflow.TimescaleSourceConfig(
        uri=DEFAULT_TIMESCALE_URI,
        table="engineered_features_materialized",
        entity_column=feature_builder.ENTITY_COLUMN,
        timestamp_column=feature_builder.EVENT_TIMESTAMP_COLUMN,
        label_column=LABEL_COLUMN,
        feature_columns=feature_columns,
        lookback_days=lookback_days,
        label_horizon=request.label_horizon,
    )
    thresholds = workflow.MetricThresholds(
        sharpe=request.thresholds.sharpe,
        sortino=request.thresholds.sortino,
        max_drawdown=request.thresholds.max_drawdown,
        cvar=request.thresholds.cvar,
    )
    artifacts = workflow.ObjectStorageConfig(base_path=DEFAULT_ARTIFACT_ROOT)
    metadata = workflow.TrainingMetadata(
        feature_version=request.feature_version,
        label_horizon=str(request.label_horizon),
        granularity=request.granularity,
        symbols=[symbol.upper() for symbol in request.symbols],
    )
    mlflow_config = None
    if MLFLOW_TRACKING_URI and MLFLOW_EXPERIMENT and MLFLOW_MODEL_NAME:
        mlflow_config = workflow.MLflowConfig(
            tracking_uri=MLFLOW_TRACKING_URI,
            experiment_name=MLFLOW_EXPERIMENT,
            run_name=request.run_name or f"train-{run_id}",
            registry_model_name=MLFLOW_MODEL_NAME,
            target_stage=MLFLOW_TARGET_STAGE,
        )
    return workflow.TrainingJobConfig(
        timescale=timescale,
        model=workflow.ModelConfig(),
        training=workflow.TrainingConfig(),
        thresholds=thresholds,
        artifacts=artifacts,
        mlflow=mlflow_config,
        metadata=metadata,
        split=workflow.ChronologicalSplitConfig(),
        outliers=workflow.OutlierConfig(),
    )


@app.post("/ml/train/start", response_model=TrainStartResponse)
def start_training(request: TrainStartRequest) -> TrainStartResponse:
    try:
        data_loader = _require_data_loader()
        feature_builder = _require_feature_builder()
        workflow_module = _require_workflow()
        _require_pandas()
    except MissingDependencyError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    entity_column = feature_builder.ENTITY_COLUMN
    timestamp_column = feature_builder.EVENT_TIMESTAMP_COLUMN
    created_at_column = feature_builder.CREATED_AT_COLUMN
    FeatureBuildConfig = feature_builder.FeatureBuildConfig

    run_id = str(uuid4())
    logger.info(
        "Starting training orchestration",
        extra={"symbols": request.symbols, "feature_version": request.feature_version, "run_id": run_id},
    )

    close_frames: List["DataFrame"] = []
    ingestion_counts: Dict[str, int] = {}
    for raw_symbol in request.symbols:
        symbol = raw_symbol.strip()
        if not symbol:
            continue
        frame = data_loader.fetch_ohlcv(
            symbol,
            request.vs_currency,
            request.start,
            request.end,
            request.granularity,
        )
        if frame.empty:
            raise HTTPException(status_code=422, detail=f"No OHLCV data returned for {symbol}")
        storage_symbol = symbol.upper()
        data_loader.upsert_timescale(frame, storage_symbol, request.granularity)
        ingestion_counts[storage_symbol] = len(frame)
        close_frames.append(
            _prepare_close_frame(
                storage_symbol,
                frame,
                label_column=LABEL_COLUMN,
                entity_column=entity_column,
                timestamp_column=timestamp_column,
            )
        )

    if not ingestion_counts:
        raise HTTPException(status_code=400, detail="No valid symbols provided")

    logger.info("Ingested OHLCV rows", extra={"counts": ingestion_counts, "run_id": run_id})

    feature_config = FeatureBuildConfig(
        symbols=tuple(sorted({symbol.upper() for symbol in request.symbols})),
        granularity=request.granularity,
        version=request.feature_version,
        changelog=request.changelog,
        start=request.start,
        end=request.end,
    )
    original_ohlcv_table = feature_builder.OHLCV_TABLE
    try:
        feature_builder.OHLCV_TABLE = f"ohlcv_{request.granularity}"
        feature_frame = feature_builder.materialise_features(feature_config)
    finally:
        feature_builder.OHLCV_TABLE = original_ohlcv_table
    if feature_frame.empty:
        raise HTTPException(status_code=500, detail="Feature build produced no rows")

    enriched = _augment_features_with_close(
        feature_frame,
        close_frames,
        label_column=LABEL_COLUMN,
        entity_column=entity_column,
        timestamp_column=timestamp_column,
    )
    if enriched.empty:
        raise HTTPException(status_code=500, detail="Unable to align features with OHLCV closes")

    non_feature = {
        timestamp_column,
        entity_column,
        created_at_column,
        "feature_version",
        LABEL_COLUMN,
    }
    feature_columns = [column for column in enriched.columns if column not in non_feature]
    if not feature_columns:
        raise HTTPException(status_code=500, detail="No feature columns available for training")

    training_config = _build_training_config(request=request, feature_columns=feature_columns, run_id=run_id)
    result = workflow_module.run_training_job(training_config, override_frame=enriched)

    canary_promoted = False
    model_stage = result.model_stage
    if (
        request.promote_canary
        and result.model_version
        and training_config.mlflow
        and workflow_module.mlflow is not None
    ):
        try:
            client = workflow_module.mlflow.tracking.MlflowClient()
            client.transition_model_version_stage(
                name=training_config.mlflow.registry_model_name,
                version=result.model_version,
                stage=request.canary_stage,
                archive_existing_versions=False,
            )
        except Exception as exc:  # pragma: no cover - depends on external MLflow service
            logger.exception("Failed to promote model version to canary")
            raise HTTPException(status_code=500, detail="Failed to promote model to canary stage") from exc
        else:
            model_stage = request.canary_stage
            canary_promoted = True

    logger.info(
        "Completed training run",
        extra={
            "run_id": run_id,
            "mlflow_run_id": result.mlflow_run_id,
            "model_version": result.model_version,
            "metrics": result.metrics,
        },
    )

    return TrainStartResponse(
        run_id=run_id,
        mlflow_run_id=result.mlflow_run_id,
        metrics=result.metrics,
        model_version=result.model_version,
        model_stage=model_stage,
        canary_ready=result.canary_ready,
        canary_promoted=canary_promoted,
    )
