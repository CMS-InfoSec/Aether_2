"""End-to-end supervised training workflow for Argo Workflows.

This module exposes :func:`run_training_job` which is responsible for
loading the latest features from TimescaleDB, training a PyTorch sequence
model, logging evaluation metrics to MLflow, and persisting artifacts to
object storage.  The script can also be executed as a module which makes
it suitable for usage in Argo Workflows where the container entrypoint is
``python -m ml.training.workflow``.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import io
import json
import logging
import math
import os
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:  # pragma: no cover - SQLAlchemy is optional in lightweight environments.
    from sqlalchemy import text
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine import create_engine as create_sqlalchemy_engine
except Exception as import_exc:  # pragma: no cover - exercised via tests when missing.
    Engine = Any  # type: ignore[assignment]

    def text(query: str) -> str:  # type: ignore[override]
        return query

    def create_sqlalchemy_engine(*args: Any, **kwargs: Any):  # type: ignore[override]
        raise MissingDependencyError("SQLAlchemy is required for Timescale access") from import_exc

if TYPE_CHECKING:  # pragma: no cover - imported only for static analysis.
    import numpy as np
    import pandas as pd
    from torch import Tensor, nn
    from torch.utils.data import DataLoader
else:  # pragma: no cover - runtime placeholders when optional deps are absent.
    np = None  # type: ignore[assignment]
    pd = None  # type: ignore[assignment]
    Tensor = Any  # type: ignore[assignment]
    nn = Any  # type: ignore[assignment]
    DataLoader = Any  # type: ignore[assignment]


class MissingDependencyError(RuntimeError):
    """Raised when optional ML training dependencies are unavailable."""


@lru_cache(maxsize=1)
def _require_numpy():
    try:
        import numpy as numpy_module  # type: ignore import-not-found
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via tests
        raise MissingDependencyError("numpy is required for the ML training workflow") from exc
    return numpy_module


@lru_cache(maxsize=1)
def _require_pandas():
    try:
        import pandas as pandas_module  # type: ignore import-not-found
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via tests
        raise MissingDependencyError("pandas is required for the ML training workflow") from exc
    return pandas_module


@lru_cache(maxsize=1)
def _require_torch():
    try:
        import torch as torch_module  # type: ignore import-not-found
        from torch import nn as nn_module  # type: ignore import-not-found
        from torch.utils.data import DataLoader as DataLoaderCls  # type: ignore import-not-found
        from torch.utils.data import Dataset as DatasetCls  # type: ignore import-not-found
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via tests
        raise MissingDependencyError("torch is required for the ML training workflow") from exc
    return torch_module, nn_module, DataLoaderCls, DatasetCls

try:  # pragma: no cover - optional dependency in CI environments.
    import mlflow
    from mlflow import pytorch as mlflow_pytorch
except Exception:  # pragma: no cover - degrade gracefully when MLflow is absent.
    mlflow = None  # type: ignore
    mlflow_pytorch = None  # type: ignore

try:  # pragma: no cover - optional dependency when S3 uploads are required.
    import boto3
except Exception:  # pragma: no cover - boto3 is optional for local development.
    boto3 = None  # type: ignore

LOGGER = logging.getLogger(__name__)

_TARGET_COLUMN = "__target__"


# ---------------------------------------------------------------------------
# Configuration dataclasses


@dataclass
class TimescaleSourceConfig:
    """Configuration for pulling features and labels from TimescaleDB."""

    uri: str
    table: str
    entity_column: str
    timestamp_column: str
    label_column: str
    feature_columns: Sequence[str]
    lookback_days: int = 90
    label_horizon: int = 1

    def select_columns(self) -> List[str]:
        columns = [self.entity_column, self.timestamp_column, *self.feature_columns, self.label_column]
        # ``dict.fromkeys`` preserves order while removing duplicates.
        return list(dict.fromkeys(columns))


@dataclass
class ModelConfig:
    """Hyper-parameters controlling the PyTorch model architecture."""

    model_type: str = "lstm"
    hidden_size: int = 64
    num_layers: int = 2
    dropout: float = 0.1
    sequence_length: int = 32


@dataclass
class TrainingConfig:
    """Training hyper-parameters."""

    batch_size: int = 128
    learning_rate: float = 1e-3
    epochs: int = 10
    weight_decay: float = 0.0
    device: str = "cpu"


@dataclass
class MetricThresholds:
    """Metric thresholds that must be exceeded before promotion."""

    sharpe: float = 1.0
    sortino: float = 1.5
    max_drawdown: float = -0.1
    cvar: float = -0.05

    def satisfied_by(self, metrics: Mapping[str, float]) -> bool:
        return (
            metrics.get("sharpe", float("-inf")) >= self.sharpe
            and metrics.get("sortino", float("-inf")) >= self.sortino
            and metrics.get("max_drawdown", float("inf")) >= self.max_drawdown
            and metrics.get("cvar", float("-inf")) >= self.cvar
        )


@dataclass
class ObjectStorageConfig:
    """Definition of the artifact storage backend."""

    base_path: str = "/tmp/aether-training"
    s3_bucket: Optional[str] = None
    s3_prefix: str = ""

    def is_s3(self) -> bool:
        return bool(self.s3_bucket)

    def __post_init__(self) -> None:
        """Normalise filesystem and S3 settings during initialisation."""

        normalised_base = _normalise_local_artifact_base_path(self.base_path)
        object.__setattr__(self, "base_path", str(normalised_base))

        normalised_prefix = _normalise_s3_prefix(self.s3_prefix)
        object.__setattr__(self, "s3_prefix", normalised_prefix)


@dataclass
class MLflowConfig:
    """Subset of MLflow configuration used by this workflow."""

    tracking_uri: str
    experiment_name: str
    run_name: Optional[str] = None
    registry_model_name: Optional[str] = None
    target_stage: str = "Staging"


@dataclass
class TrainingResult:
    """Outcome metadata produced by :func:`run_training_job`."""

    metrics: Dict[str, float]
    mlflow_run_id: Optional[str] = None
    model_version: Optional[str] = None
    model_stage: Optional[str] = None
    canary_ready: bool = False


@dataclass
class TrainingMetadata:
    """Additional metadata captured for MLflow registration tags."""

    feature_version: Optional[str] = None
    label_horizon: Optional[str] = None
    granularity: Optional[str] = None
    symbols: List[str] = field(default_factory=list)


@dataclass
class ChronologicalSplitConfig:
    """Fractions used for chronological train/validation/test splits."""

    train_fraction: float = 0.7
    validation_fraction: float = 0.15
    test_fraction: float = 0.15

    def __post_init__(self) -> None:
        total = self.train_fraction + self.validation_fraction + self.test_fraction
        if not math.isclose(total, 1.0, rel_tol=1e-6):
            raise ValueError("Split fractions must sum to 1.0")
        for name, value in (
            ("train_fraction", self.train_fraction),
            ("validation_fraction", self.validation_fraction),
            ("test_fraction", self.test_fraction),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive")


@dataclass
class OutlierConfig:
    """Configuration for pre-training outlier handling."""

    method: str = "none"
    lower_quantile: float = 0.01
    upper_quantile: float = 0.99

    def normalise_method(self) -> str:
        method = self.method.lower()
        if method not in {"none", "clip", "drop"}:
            raise ValueError("Outlier method must be one of 'none', 'clip', or 'drop'")
        if not 0.0 <= self.lower_quantile < self.upper_quantile <= 1.0:
            raise ValueError("Quantiles must satisfy 0 <= lower < upper <= 1")
        return method


@dataclass
class TrainingJobConfig:
    """Aggregate configuration for the workflow."""

    timescale: TimescaleSourceConfig
    model: ModelConfig = field(default_factory=ModelConfig)
    training: TrainingConfig = field(default_factory=TrainingConfig)
    thresholds: MetricThresholds = field(default_factory=MetricThresholds)
    artifacts: ObjectStorageConfig = field(default_factory=ObjectStorageConfig)
    mlflow: Optional[MLflowConfig] = None
    metadata: TrainingMetadata = field(default_factory=TrainingMetadata)
    split: ChronologicalSplitConfig = field(default_factory=ChronologicalSplitConfig)
    outliers: OutlierConfig = field(default_factory=OutlierConfig)


# ---------------------------------------------------------------------------
# Data handling


def _create_engine(uri: str) -> Engine:
    LOGGER.debug("Creating SQLAlchemy engine for URI=%s", uri)
    return create_sqlalchemy_engine(uri, pool_pre_ping=True, pool_recycle=3600)


def _resolve_git_commit() -> Optional[str]:
    """Return the current Git commit hash if available."""

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except Exception:  # pragma: no cover - git may be unavailable in some envs
        return None
    return result.stdout.decode("utf-8").strip()


def _compute_config_hash(config: TrainingJobConfig) -> str:
    """Create a deterministic hash of the training configuration."""

    config_dict = asdict(config)
    payload = json.dumps(config_dict, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _format_timestamp(value: "pd.Timestamp") -> Optional[str]:
    pandas = _require_pandas()

    if pandas.isna(value):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    tzinfo = getattr(value, "tzinfo", None)
    if tzinfo is None:
        try:
            value = value.tz_localize(timezone.utc)
        except AttributeError:  # pragma: no cover - fallback for shim objects
            return None
    try:
        return value.to_pydatetime().isoformat()
    except AttributeError:  # pragma: no cover - shim already returns datetime
        return str(value)


def _build_registration_tags(
    config: TrainingJobConfig,
    frame: "pd.DataFrame",
    *,
    git_commit: Optional[str] = None,
) -> Dict[str, str]:
    """Construct MLflow model version tags describing the training run."""

    metadata = config.metadata
    tags: Dict[str, str] = {}

    if metadata.feature_version:
        tags["feature_version"] = metadata.feature_version
    if metadata.label_horizon:
        tags["label_horizon"] = metadata.label_horizon
    if metadata.granularity:
        tags["granularity"] = metadata.granularity

    symbols: List[str] = []
    if metadata.symbols:
        symbols = sorted({symbol.strip() for symbol in metadata.symbols if symbol.strip()})
    elif config.timescale.entity_column in frame.columns:
        unique = (
            frame[config.timescale.entity_column]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        symbols = sorted(set(unique))
    if symbols:
        tags["symbols"] = ", ".join(symbols)

    ts_column = config.timescale.timestamp_column
    if ts_column in frame.columns and not frame.empty:
        pandas = _require_pandas()
        timestamps = pandas.to_datetime(frame[ts_column], utc=True)
        data_from = _format_timestamp(timestamps.min())
        data_to = _format_timestamp(timestamps.max())
        if data_from:
            tags["data_from"] = data_from
        if data_to:
            tags["data_to"] = data_to

    commit = git_commit if git_commit is not None else _resolve_git_commit()
    if commit:
        tags["git_commit"] = commit

    tags["config_hash"] = _compute_config_hash(config)

    return tags


def _load_timescale_frame(
    config: TimescaleSourceConfig, engine: Optional[Engine] = None
) -> "pd.DataFrame":
    """Load the last ``lookback_days`` of data from TimescaleDB."""

    if not config.feature_columns:
        raise ValueError("At least one feature column must be specified")

    engine = engine or _create_engine(config.uri)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=config.lookback_days)
    columns = ", ".join(config.select_columns())
    sql = text(
        f"""
        SELECT {columns}
        FROM {config.table}
        WHERE {config.timestamp_column} >= :start AND {config.timestamp_column} < :end
        ORDER BY {config.entity_column}, {config.timestamp_column}
        """
    )
    LOGGER.info("Loading features from TimescaleDB between %s and %s", start, end)
    pandas = _require_pandas()
    frame = pandas.read_sql(sql, engine, params={"start": start, "end": end})
    if frame.empty:
        raise RuntimeError("No data returned from TimescaleDB query")
    frame[config.timestamp_column] = pandas.to_datetime(frame[config.timestamp_column], utc=True)
    return frame


def _prepare_supervised_frame(
    frame: "pd.DataFrame", config: TimescaleSourceConfig
) -> "pd.DataFrame":
    """Generate forward-return labels aligned with feature timestamps."""

    if config.label_horizon <= 0:
        raise ValueError("label_horizon must be positive")

    pandas = _require_pandas()
    working = frame.copy()
    working.sort_values([config.entity_column, config.timestamp_column], inplace=True)

    grouped_rows: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    for row in working.to_dict("records"):
        grouped_rows[row.get(config.entity_column)].append(row)

    result_rows: List[Dict[str, Any]] = []
    for rows in grouped_rows.values():
        rows.sort(key=lambda row: row.get(config.timestamp_column))
        for index, row in enumerate(rows):
            future_index = index + config.label_horizon
            if future_index >= len(rows):
                continue
            base_value = rows[index].get(config.label_column)
            future_value = rows[future_index].get(config.label_column)
            if base_value in (None, 0):
                continue
            try:
                return_value = (future_value - base_value) / base_value
            except Exception:  # pragma: no cover - defensive guard for bad data
                continue
            if math.isinf(return_value) or math.isnan(return_value):
                continue
            enriched = dict(row)
            enriched[_TARGET_COLUMN] = return_value
            result_rows.append(enriched)

    if not result_rows:
        return pandas.DataFrame(columns=list(working.columns) + [_TARGET_COLUMN])

    columns = list(working.columns)
    if _TARGET_COLUMN not in columns:
        columns.append(_TARGET_COLUMN)
    return pandas.DataFrame(result_rows, columns=columns)


def _apply_outlier_handling(
    frame: "pd.DataFrame",
    column: str,
    config: OutlierConfig,
) -> pd.DataFrame:
    """Apply the configured outlier handling strategy to ``column``."""

    method = config.normalise_method()
    if method == "none" or frame.empty:
        return frame

    pandas = _require_pandas()
    lower = frame[column].quantile(config.lower_quantile)
    upper = frame[column].quantile(config.upper_quantile)
    if pandas.isna(lower) or pandas.isna(upper):
        return frame

    records = frame.to_dict("records")
    if method == "clip":
        adjusted: List[Dict[str, Any]] = []
        for row in records:
            new_row = dict(row)
            value = new_row.get(column)
            if not pandas.isna(value):
                if value < lower:
                    new_row[column] = lower
                elif value > upper:
                    new_row[column] = upper
            adjusted.append(new_row)
        return pandas.DataFrame(adjusted, columns=frame.columns)

    filtered = [
        row
        for row in records
        if not pandas.isna(row.get(column)) and lower <= row.get(column) <= upper
    ]
    return pandas.DataFrame(filtered, columns=frame.columns)


def _chronological_split(
    frame: pd.DataFrame,
    timestamp_column: str,
    split: ChronologicalSplitConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split ``frame`` into chronological train/validation/test windows."""

    if frame.empty:
        raise ValueError("Cannot split an empty frame")

    pandas = _require_pandas()
    ordered = frame.sort_values(timestamp_column)
    ordered_records = ordered.to_dict("records")
    unique_timestamps = sorted({row[timestamp_column] for row in ordered_records})
    n_timestamps = len(unique_timestamps)
    if n_timestamps < 3:
        raise ValueError("At least three unique timestamps are required for splitting")

    train_boundary = max(1, int(round(n_timestamps * split.train_fraction)))
    val_boundary = max(
        train_boundary + 1,
        int(round(n_timestamps * (split.train_fraction + split.validation_fraction))),
    )
    if val_boundary >= n_timestamps:
        val_boundary = n_timestamps - 1
        train_boundary = min(train_boundary, val_boundary - 1)
        train_boundary = max(train_boundary, 1)

    train_end = unique_timestamps[train_boundary - 1]
    val_end = unique_timestamps[val_boundary - 1]

    train_rows: List[Dict[str, Any]] = []
    val_rows: List[Dict[str, Any]] = []
    test_rows: List[Dict[str, Any]] = []
    for row in ordered_records:
        timestamp = row[timestamp_column]
        if timestamp <= train_end:
            train_rows.append(dict(row))
        elif timestamp <= val_end:
            val_rows.append(dict(row))
        else:
            test_rows.append(dict(row))

    train_frame = pandas.DataFrame(train_rows, columns=ordered.columns)
    val_frame = pandas.DataFrame(val_rows, columns=ordered.columns)
    test_frame = pandas.DataFrame(test_rows, columns=ordered.columns)

    if val_frame.empty or test_frame.empty:
        raise ValueError("Validation and test splits must be non-empty")

    return train_frame, val_frame, test_frame


def _compute_feature_set_hash(frame: "pd.DataFrame", feature_columns: Sequence[str]) -> str:
    """Deterministically hash the feature values used for training."""

    subset = frame[list(feature_columns)].copy()
    pandas = _require_pandas()
    hashed = pandas.util.hash_pandas_object(subset, index=False).to_numpy()
    return hashlib.sha256(hashed.tobytes()).hexdigest()


def _build_sequences(
    frame: "pd.DataFrame",
    config: TimescaleSourceConfig,
    sequence_length: int,
) -> Tuple["np.ndarray", "np.ndarray"]:
    """Convert a frame into sequences suitable for sequence models."""

    numpy = _require_numpy()
    feature_columns = [col for col in config.feature_columns if col != config.label_column]
    if config.label_column in feature_columns:
        feature_columns.remove(config.label_column)

    sequences: List["np.ndarray"] = []
    targets: List[float] = []

    for entity, entity_frame in frame.groupby(config.entity_column):
        entity_frame = entity_frame.sort_values(config.timestamp_column)
        if len(entity_frame) < sequence_length:
            LOGGER.debug("Skipping entity %s because it has fewer than %d rows", entity, sequence_length)
            continue

        features_matrix = entity_frame[feature_columns].to_numpy(dtype=numpy.float32)
        labels_array = entity_frame[_TARGET_COLUMN].to_numpy(dtype=numpy.float32)

        for idx in range(sequence_length - 1, len(entity_frame)):
            window = features_matrix[idx - sequence_length + 1 : idx + 1]
            label = labels_array[idx]
            if numpy.isnan(window).any() or numpy.isnan(label):
                continue
            sequences.append(window)
            targets.append(float(label))

    if not sequences:
        raise RuntimeError("Unable to construct training sequences from the provided frame")

    return numpy.stack(sequences), numpy.asarray(targets, dtype=numpy.float32)


# ---------------------------------------------------------------------------
# PyTorch dataset and models


@lru_cache(maxsize=1)
def _sequence_dataset_cls():
    torch_module, _, _, dataset_cls = _require_torch()
    numpy = _require_numpy()

    class _SequenceDataset(dataset_cls):
        """Simple dataset wrapping numpy arrays for PyTorch usage."""

        def __init__(self, sequences: "np.ndarray", targets: "np.ndarray") -> None:
            super().__init__()
            self.sequences = torch_module.from_numpy(
                numpy.asarray(sequences, dtype=numpy.float32)
            ).float()
            self.targets = torch_module.from_numpy(
                numpy.asarray(targets, dtype=numpy.float32)
            ).float().unsqueeze(-1)

        def __len__(self) -> int:  # pragma: no cover - trivial
            return len(self.sequences)

        def __getitem__(self, idx: int) -> Tuple[Any, Any]:  # pragma: no cover - trivial
            return self.sequences[idx], self.targets[idx]

    return _SequenceDataset


def _build_model(model_config: ModelConfig, input_size: int) -> Any:
    _, nn_module, _, _ = _require_torch()
    model_type = model_config.model_type.lower()

    if model_type == "lstm":

        class _LSTMRegressor(nn_module.Module):
            def __init__(self) -> None:
                super().__init__()
                self.lstm = nn_module.LSTM(
                    input_size=input_size,
                    hidden_size=model_config.hidden_size,
                    num_layers=model_config.num_layers,
                    dropout=model_config.dropout if model_config.num_layers > 1 else 0.0,
                    batch_first=True,
                )
                self.dropout = nn_module.Dropout(model_config.dropout)
                self.head = nn_module.Linear(model_config.hidden_size, 1)

            def forward(self, inputs: Any) -> Any:  # pragma: no cover - straightforward
                outputs, _ = self.lstm(inputs)
                final_state = outputs[:, -1, :]
                return self.head(self.dropout(final_state))

        return _LSTMRegressor()

    if model_type == "transformer":

        class _TransformerRegressor(nn_module.Module):
            def __init__(self) -> None:
                super().__init__()
                encoder_layer = nn_module.TransformerEncoderLayer(
                    d_model=input_size,
                    nhead=max(1, input_size // 4),
                    dim_feedforward=model_config.hidden_size,
                    dropout=model_config.dropout,
                    batch_first=True,
                )
                self.encoder = nn_module.TransformerEncoder(encoder_layer, num_layers=model_config.num_layers)
                self.head = nn_module.Linear(input_size, 1)

            def forward(self, inputs: Any) -> Any:  # pragma: no cover - straightforward
                encoded = self.encoder(inputs)
                return self.head(encoded[:, -1, :])

        return _TransformerRegressor()

    raise ValueError(
        f"Unsupported model_type '{model_config.model_type}'. Choose 'lstm' or 'transformer'."
    )


# ---------------------------------------------------------------------------
# Metrics


def _annualisation_factor(frequency_minutes: int = 15) -> float:
    trading_minutes_per_year = 252 * 6.5 * 60  # U.S. trading hours approximation
    periods_per_year = trading_minutes_per_year / frequency_minutes
    return math.sqrt(periods_per_year)


def _compute_sharpe(returns: "np.ndarray") -> float:
    numpy = _require_numpy()
    if returns.size < 2:
        return 0.0
    std = returns.std(ddof=1)
    if std == 0:
        return 0.0
    return _annualisation_factor() * returns.mean() / std


def _compute_sortino(returns: "np.ndarray") -> float:
    numpy = _require_numpy()
    if returns.size < 2:
        return 0.0
    negative = returns[returns < 0]
    if negative.size == 0:
        return float("inf")
    downside_std = negative.std(ddof=1)
    if downside_std == 0:
        return float("inf")
    return _annualisation_factor() * returns.mean() / downside_std


def _compute_max_drawdown(returns: "np.ndarray") -> float:
    numpy = _require_numpy()
    if returns.size == 0:
        return 0.0
    cumulative = numpy.cumsum(returns)
    running_max = numpy.maximum.accumulate(cumulative)
    drawdowns = cumulative - running_max
    return float(drawdowns.min())


def _compute_cvar(returns: "np.ndarray", alpha: float = 0.95) -> float:
    numpy = _require_numpy()
    if returns.size == 0:
        return 0.0
    threshold = numpy.quantile(returns, 1 - alpha)
    tail_losses = returns[returns <= threshold]
    if tail_losses.size == 0:
        return threshold
    return float(tail_losses.mean())


def _evaluate_strategy(predictions: "np.ndarray", targets: "np.ndarray") -> Dict[str, float]:
    numpy = _require_numpy()
    if predictions.shape != targets.shape:
        raise ValueError("Predictions and targets must have the same shape")
    strategy_returns = numpy.sign(predictions) * targets
    metrics = {
        "sharpe": _compute_sharpe(strategy_returns),
        "sortino": _compute_sortino(strategy_returns),
        "max_drawdown": _compute_max_drawdown(strategy_returns),
        "cvar": _compute_cvar(strategy_returns),
    }
    LOGGER.info("Evaluation metrics: %s", metrics)
    return metrics


# ---------------------------------------------------------------------------
# Training helpers


def _train_model(
    model: Any,
    train_loader: Any,
    val_loader: Any,
    config: TrainingConfig,
    device: Any,
) -> Tuple[Any, List[float]]:
    torch_module, nn_module, _, _ = _require_torch()
    model.to(device)
    criterion = nn_module.MSELoss()
    optimizer = torch_module.optim.Adam(
        model.parameters(),
        lr=config.learning_rate,
        weight_decay=config.weight_decay,
    )

    history: List[float] = []
    for epoch in range(config.epochs):
        model.train()
        epoch_loss = 0.0
        for batch_features, batch_targets in train_loader:
            batch_features = batch_features.to(device)
            batch_targets = batch_targets.to(device)
            optimizer.zero_grad()
            outputs = model(batch_features)
            loss = criterion(outputs, batch_targets)
            loss.backward()
            optimizer.step()
            epoch_loss += float(loss.item()) * batch_features.size(0)
        epoch_loss /= len(train_loader.dataset)

        model.eval()
        val_loss = 0.0
        with torch_module.no_grad():
            for batch_features, batch_targets in val_loader:
                batch_features = batch_features.to(device)
                batch_targets = batch_targets.to(device)
                outputs = model(batch_features)
                loss = criterion(outputs, batch_targets)
                val_loss += float(loss.item()) * batch_features.size(0)
        val_loss /= len(val_loader.dataset)
        history.append(val_loss)
        LOGGER.info("Epoch %d/%d - train_loss=%.6f val_loss=%.6f", epoch + 1, config.epochs, epoch_loss, val_loss)

    return model, history


def _predict(model: Any, data_loader: Any, device: Any) -> "np.ndarray":
    torch_module, _, _, _ = _require_torch()
    numpy = _require_numpy()
    model.eval()
    predictions: List["np.ndarray"] = []
    with torch_module.no_grad():
        for batch_features, _ in data_loader:
            batch_features = batch_features.to(device)
            outputs = model(batch_features)
            predictions.append(outputs.cpu().numpy().reshape(-1))
    return numpy.concatenate(predictions)


# ---------------------------------------------------------------------------
# Artifact persistence


def _normalise_local_artifact_base_path(base_path: str | os.PathLike[str]) -> Path:
    """Validate and normalise the local artifact directory."""

    raw_text = os.fspath(base_path)
    if isinstance(base_path, str):
        raw_text = raw_text.strip()

    if not raw_text:
        raise ValueError("Artifact base path must not be empty")
    if any(ord(char) < 32 for char in raw_text):
        raise ValueError("Artifact base path must not contain control characters")

    candidate = Path(raw_text).expanduser()
    if any(part == ".." for part in candidate.parts):
        raise ValueError("Artifact base path must not include parent directory references")
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate

    resolved_candidate = candidate.resolve(strict=False)

    if candidate.is_symlink():
        raise ValueError("Artifact base path must not be a symlink")

    for ancestor in candidate.parents:
        if ancestor.is_symlink():
            try:
                resolved_ancestor = ancestor.resolve(strict=False)
            except OSError as exc:  # pragma: no cover - extremely unlikely on supported platforms
                raise ValueError("Artifact base path symlink could not be resolved") from exc
            if not resolved_ancestor.exists():
                raise ValueError("Artifact base path symlink targets must exist")
            if not resolved_ancestor.is_dir():
                raise ValueError("Artifact base path symlink targets must resolve to a directory")
            try:
                resolved_candidate.relative_to(resolved_ancestor)
            except ValueError:
                raise ValueError("Artifact base path must not escape via symlinked ancestors")

    if resolved_candidate.exists() and not resolved_candidate.is_dir():
        raise ValueError("Artifact base path must reference a directory")

    return resolved_candidate


def _normalise_artifact_name(name: str) -> Tuple[str, ...]:
    """Return a traversal-free tuple of path segments for an artifact name."""

    sanitised = name.replace("\\", "/").strip("/")
    if not sanitised:
        raise ValueError("Artifact names must not be empty")
    if any(ord(char) < 32 for char in sanitised):
        raise ValueError("Artifact names must not contain control characters")

    path = PurePosixPath(sanitised)
    parts: List[str] = []
    for segment in path.parts:
        if segment in {"", "."}:
            continue
        if segment == "..":
            raise ValueError("Artifact names must not contain parent directory references")
        parts.append(segment)

    if not parts:
        raise ValueError("Artifact names must not be empty")

    return tuple(parts)


def _normalise_s3_prefix(prefix: Optional[str]) -> str:
    """Return a traversal-free S3 prefix suitable for object keys."""

    if not prefix:
        return ""

    if isinstance(prefix, str):
        candidate = prefix.strip()
    else:
        candidate = str(prefix)

    if not candidate:
        return ""

    candidate = candidate.replace("\\", "/")
    if any(ord(char) < 32 for char in candidate):
        raise ValueError("S3 prefix must not contain control characters")

    parts: List[str] = []
    for segment in candidate.split("/"):
        if segment in {"", "."}:
            continue
        if segment == "..":
            raise ValueError("S3 prefix must not contain parent directory references")
        parts.append(segment)

    return "/".join(parts)


def _write_artifacts(artifacts: Mapping[str, bytes], config: ObjectStorageConfig) -> Dict[str, str]:
    """Persist artifacts to either the filesystem or S3."""

    output_locations: Dict[str, str] = {}
    base_path: Optional[Path] = None
    prefix: str = ""
    if not config.is_s3():
        base_path = _normalise_local_artifact_base_path(config.base_path)
        base_path.mkdir(parents=True, exist_ok=True)
    else:
        prefix = _normalise_s3_prefix(config.s3_prefix)

    for name, payload in artifacts.items():
        segments = _normalise_artifact_name(name)
        if config.is_s3():
            if boto3 is None:
                raise RuntimeError("boto3 is required for S3 artifact uploads but is not installed")
            client = boto3.client("s3")
            key_body = "/".join(segments)
            key_parts = [part.strip("/") for part in (prefix, key_body) if part]
            key = "/".join(key_parts)
            LOGGER.debug("Uploading artifact %s to s3://%s/%s", name, config.s3_bucket, key)
            client.put_object(Bucket=config.s3_bucket, Key=key, Body=payload)
            output_locations[name] = f"s3://{config.s3_bucket}/{key}"
        else:
            assert base_path is not None  # for type-checkers
            target_path = base_path.joinpath(*segments)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(payload)
            output_locations[name] = str(target_path)
            LOGGER.debug("Wrote artifact %s to %s", name, target_path)
    return output_locations


# ---------------------------------------------------------------------------
# Main workflow


def run_training_job(
    config: TrainingJobConfig,
    *,
    engine: Optional[Engine] = None,
    override_frame: Optional["pd.DataFrame"] = None,
) -> Dict[str, float]:
    """Execute the full workflow and return evaluation metrics."""

    _require_pandas()
    _require_numpy()
    torch_module, _, data_loader_cls, _ = _require_torch()
    dataset_cls = _sequence_dataset_cls()
    raw_frame = override_frame if override_frame is not None else _load_timescale_frame(config.timescale, engine)
    supervised = _prepare_supervised_frame(raw_frame, config.timescale)
    supervised = _apply_outlier_handling(supervised, _TARGET_COLUMN, config.outliers)
    train_frame, val_frame, test_frame = _chronological_split(
        supervised, config.timescale.timestamp_column, config.split
    )

    train_sequences, train_targets = _build_sequences(train_frame, config.timescale, config.model.sequence_length)
    val_sequences, val_targets = _build_sequences(val_frame, config.timescale, config.model.sequence_length)
    test_sequences, test_targets = _build_sequences(test_frame, config.timescale, config.model.sequence_length)

    if len(val_sequences) == 0 or len(test_sequences) == 0:
        raise RuntimeError("Validation and test splits must contain at least one sequence")

    train_dataset = dataset_cls(train_sequences, train_targets)
    val_dataset = dataset_cls(val_sequences, val_targets)
    test_dataset = dataset_cls(test_sequences, test_targets)

    train_loader = data_loader_cls(
        train_dataset, batch_size=config.training.batch_size, shuffle=True, drop_last=False
    )
    val_loader = data_loader_cls(
        val_dataset, batch_size=config.training.batch_size, shuffle=False, drop_last=False
    )
    test_loader = data_loader_cls(
        test_dataset, batch_size=config.training.batch_size, shuffle=False, drop_last=False
    )

    input_size = train_sequences.shape[-1]
    model = _build_model(config.model, input_size)

    device = torch_module.device(config.training.device)
    model, history = _train_model(model, train_loader, val_loader, config.training, device)

    predictions = _predict(model, test_loader, device)
    metrics = _evaluate_strategy(predictions, test_targets[: len(predictions)])
    canary_ready = config.thresholds.satisfied_by(metrics)
    mlflow_run_id: Optional[str] = None
    registered_version: Optional[str] = None
    model_stage: Optional[str] = None

    run_prefix = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    artifacts: Dict[str, bytes] = {
        f"{run_prefix}/metrics.json": json.dumps(metrics, indent=2).encode("utf-8"),
        f"{run_prefix}/training_history.json": json.dumps({"val_loss": history}, indent=2).encode("utf-8"),
    }

    buffer = io.BytesIO()
    torch_module.save(model.state_dict(), buffer)
    artifacts[f"{run_prefix}/model.pt"] = buffer.getvalue()

    artifact_locations = _write_artifacts(artifacts, config.artifacts)
    LOGGER.info("Persisted artifacts: %s", artifact_locations)

    if config.mlflow and mlflow is not None:
        registration_tags = _build_registration_tags(config, raw_frame)
        mlflow.set_tracking_uri(config.mlflow.tracking_uri)
        mlflow.set_experiment(config.mlflow.experiment_name)
        run_args = {}
        if config.mlflow.run_name:
            run_args["run_name"] = config.mlflow.run_name
        with mlflow.start_run(**run_args) as run:
            mlflow.log_params(
                {
                    "model_type": config.model.model_type,
                    "hidden_size": config.model.hidden_size,
                    "num_layers": config.model.num_layers,
                    "dropout": config.model.dropout,
                    "sequence_length": config.model.sequence_length,
                    "batch_size": config.training.batch_size,
                    "learning_rate": config.training.learning_rate,
                    "epochs": config.training.epochs,
                    "weight_decay": config.training.weight_decay,
                }
            )
            mlflow.log_metrics(metrics)
            feature_hash = _compute_feature_set_hash(train_frame, config.timescale.feature_columns)
            mlflow.set_tag("feature_set_hash", feature_hash)
            if registration_tags:
                mlflow.set_tags(registration_tags)
            if config.metadata.feature_version:
                mlflow.set_tag("feature_version", config.metadata.feature_version)
            if config.metadata.label_horizon:
                mlflow.set_tag("label_horizon", str(config.metadata.label_horizon))
            if config.metadata.granularity:
                mlflow.set_tag("granularity", config.metadata.granularity)
            mlflow.set_tag("canary_ready", str(canary_ready).lower())

            artifact_dir = Path(config.artifacts.base_path) / run_prefix
            if not config.artifacts.is_s3() and artifact_dir.exists():
                mlflow.log_artifacts(str(artifact_dir))

            if mlflow_pytorch is not None:
                mlflow_pytorch.log_model(model, artifact_path="model")

            mlflow_run_id = run.info.run_id
            if config.mlflow.registry_model_name:
                LOGGER.info(
                    "Registering model version from run %s to stage %s",
                    mlflow_run_id,
                    config.mlflow.target_stage,
                )
                model_uri = f"runs:/{mlflow_run_id}/model"
                result = mlflow.register_model(model_uri, config.mlflow.registry_model_name)
                registered_version = str(result.version)
                model_stage = config.mlflow.target_stage
                client = mlflow.tracking.MlflowClient()
                for key, value in registration_tags.items():
                    client.set_model_version_tag(
                        name=config.mlflow.registry_model_name,
                        version=result.version,
                        key=key,
                        value=value,
                    )
                client.set_model_version_tag(
                    name=config.mlflow.registry_model_name,
                    version=result.version,
                    key="canary_ready",
                    value=str(canary_ready).lower(),
                )
                client.transition_model_version_stage(
                    name=config.mlflow.registry_model_name,
                    version=result.version,
                    stage=model_stage,
                    archive_existing_versions=False,
                )
    elif config.mlflow and mlflow is None:
        LOGGER.warning("MLflow configuration supplied but mlflow is not installed.")

    return TrainingResult(
        metrics=metrics,
        mlflow_run_id=mlflow_run_id,
        model_version=registered_version,
        model_stage=model_stage,
        canary_ready=canary_ready,
    )


# ---------------------------------------------------------------------------
# CLI


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the ML training workflow.")
    parser.add_argument("--timescale-uri", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--entity-column", required=True)
    parser.add_argument("--timestamp-column", required=True)
    parser.add_argument("--label-column", default="return_15m")
    parser.add_argument("--feature-columns", required=True, help="Comma separated list of feature columns")
    parser.add_argument("--label-horizon-steps", type=int, default=1, help="Number of future bars used for label generation")
    parser.add_argument("--model-type", choices=["lstm", "transformer"], default="lstm")
    parser.add_argument("--sequence-length", type=int, default=32)
    parser.add_argument("--hidden-size", type=int, default=64)
    parser.add_argument("--num-layers", type=int, default=2)
    parser.add_argument("--dropout", type=float, default=0.1)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--weight-decay", type=float, default=0.0)
    parser.add_argument("--artifact-base-path", default=os.environ.get("AETHER_ARTIFACT_PATH", "/tmp/aether-training"))
    parser.add_argument("--s3-bucket")
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--mlflow-tracking-uri")
    parser.add_argument("--mlflow-experiment")
    parser.add_argument("--mlflow-run-name")
    parser.add_argument("--mlflow-registry-name")
    parser.add_argument("--threshold-sharpe", type=float, default=1.0)
    parser.add_argument("--threshold-sortino", type=float, default=1.5)
    parser.add_argument("--threshold-maxdd", type=float, default=-0.1)
    parser.add_argument("--threshold-cvar", type=float, default=-0.05)
    parser.add_argument("--feature-version")
    parser.add_argument("--label-horizon")
    parser.add_argument("--granularity")
    parser.add_argument("--symbols", help="Comma separated list of instrument symbols")
    parser.add_argument("--split-train-fraction", type=float, default=0.7)
    parser.add_argument("--split-validation-fraction", type=float, default=0.15)
    parser.add_argument("--split-test-fraction", type=float, default=0.15)
    parser.add_argument("--outlier-method", default="none", choices=["none", "clip", "drop"], help="Outlier handling strategy")
    parser.add_argument("--outlier-lower-quantile", type=float, default=0.01)
    parser.add_argument("--outlier-upper-quantile", type=float, default=0.99)
    return parser.parse_args(argv)


def _build_config(args: argparse.Namespace) -> TrainingJobConfig:
    timescale = TimescaleSourceConfig(
        uri=args.timescale_uri,
        table=args.table,
        entity_column=args.entity_column,
        timestamp_column=args.timestamp_column,
        label_column=args.label_column,
        feature_columns=[col.strip() for col in args.feature_columns.split(",") if col.strip()],
        label_horizon=args.label_horizon_steps,
    )

    model = ModelConfig(
        model_type=args.model_type,
        hidden_size=args.hidden_size,
        num_layers=args.num_layers,
        dropout=args.dropout,
        sequence_length=args.sequence_length,
    )

    torch_module, _, _, _ = _require_torch()
    training = TrainingConfig(
        batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        epochs=args.epochs,
        weight_decay=args.weight_decay,
        device="cuda" if torch_module.cuda.is_available() else "cpu",
    )

    thresholds = MetricThresholds(
        sharpe=args.threshold_sharpe,
        sortino=args.threshold_sortino,
        max_drawdown=args.threshold_maxdd,
        cvar=args.threshold_cvar,
    )

    artifacts = ObjectStorageConfig(
        base_path=args.artifact_base_path,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
    )

    metadata = TrainingMetadata(
        feature_version=args.feature_version,
        label_horizon=args.label_horizon,
        granularity=args.granularity,
        symbols=[sym.strip() for sym in (args.symbols or "").split(",") if sym.strip()],
    )

    split = ChronologicalSplitConfig(
        train_fraction=args.split_train_fraction,
        validation_fraction=args.split_validation_fraction,
        test_fraction=args.split_test_fraction,
    )

    outliers = OutlierConfig(
        method=args.outlier_method,
        lower_quantile=args.outlier_lower_quantile,
        upper_quantile=args.outlier_upper_quantile,
    )

    mlflow_config = None
    if args.mlflow_tracking_uri and args.mlflow_experiment:
        mlflow_config = MLflowConfig(
            tracking_uri=args.mlflow_tracking_uri,
            experiment_name=args.mlflow_experiment,
            run_name=args.mlflow_run_name,
            registry_model_name=args.mlflow_registry_name,
        )

    return TrainingJobConfig(
        timescale=timescale,
        model=model,
        training=training,
        thresholds=thresholds,
        artifacts=artifacts,
        mlflow=mlflow_config,
        metadata=metadata,
        split=split,
        outliers=outliers,
    )


def main(argv: Optional[Sequence[str]] = None) -> None:  # pragma: no cover - CLI thin wrapper
    logging.basicConfig(level=logging.INFO)
    args = _parse_args(argv)
    config = _build_config(args)
    result = run_training_job(config)
    print(json.dumps(result.metrics, indent=2))


if __name__ == "__main__":  # pragma: no cover - module execution
    main()
