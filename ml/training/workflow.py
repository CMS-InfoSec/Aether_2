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
import io
import json
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import torch
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine import create_engine as create_sqlalchemy_engine
from torch import Tensor, nn
from torch.utils.data import DataLoader, Dataset

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


@dataclass
class MLflowConfig:
    """Subset of MLflow configuration used by this workflow."""

    tracking_uri: str
    experiment_name: str
    run_name: Optional[str] = None
    registry_model_name: Optional[str] = None


@dataclass
class TrainingJobConfig:
    """Aggregate configuration for the workflow."""

    timescale: TimescaleSourceConfig
    model: ModelConfig = field(default_factory=ModelConfig)
    training: TrainingConfig = field(default_factory=TrainingConfig)
    thresholds: MetricThresholds = field(default_factory=MetricThresholds)
    artifacts: ObjectStorageConfig = field(default_factory=ObjectStorageConfig)
    mlflow: Optional[MLflowConfig] = None


# ---------------------------------------------------------------------------
# Data handling


def _create_engine(uri: str) -> Engine:
    LOGGER.debug("Creating SQLAlchemy engine for URI=%s", uri)
    return create_sqlalchemy_engine(uri, pool_pre_ping=True, pool_recycle=3600)


def _load_timescale_frame(config: TimescaleSourceConfig, engine: Optional[Engine] = None) -> pd.DataFrame:
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
    frame = pd.read_sql(sql, engine, params={"start": start, "end": end})
    if frame.empty:
        raise RuntimeError("No data returned from TimescaleDB query")
    frame[config.timestamp_column] = pd.to_datetime(frame[config.timestamp_column], utc=True)
    return frame


def _build_sequences(
    frame: pd.DataFrame,
    config: TimescaleSourceConfig,
    sequence_length: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """Convert a frame into sequences suitable for sequence models."""

    feature_columns = [col for col in config.feature_columns if col != config.label_column]
    if config.label_column in feature_columns:
        feature_columns.remove(config.label_column)

    sequences: List[np.ndarray] = []
    targets: List[float] = []

    for entity, entity_frame in frame.groupby(config.entity_column):
        entity_frame = entity_frame.sort_values(config.timestamp_column)
        features_matrix = entity_frame[feature_columns].to_numpy(dtype=np.float32)
        labels_array = entity_frame[config.label_column].to_numpy(dtype=np.float32)
        if len(features_matrix) <= sequence_length:
            LOGGER.debug("Skipping entity %s because it has fewer than %d rows", entity, sequence_length)
            continue
        for idx in range(len(features_matrix) - sequence_length):
            window = features_matrix[idx : idx + sequence_length]
            label = labels_array[idx + sequence_length]
            if np.isnan(window).any() or np.isnan(label):
                continue
            sequences.append(window)
            targets.append(float(label))

    if not sequences:
        raise RuntimeError("Unable to construct training sequences from the provided frame")

    return np.stack(sequences), np.asarray(targets, dtype=np.float32)


# ---------------------------------------------------------------------------
# PyTorch dataset and models


class SequenceDataset(Dataset):
    """Simple dataset wrapping numpy arrays for PyTorch usage."""

    def __init__(self, sequences: np.ndarray, targets: np.ndarray) -> None:
        self.sequences = torch.from_numpy(sequences).float()
        self.targets = torch.from_numpy(targets).float().unsqueeze(-1)

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self.sequences)

    def __getitem__(self, idx: int) -> Tuple[Tensor, Tensor]:  # pragma: no cover - trivial
        return self.sequences[idx], self.targets[idx]


class LSTMRegressor(nn.Module):
    """Two-headed LSTM regressor used as a default baseline."""

    def __init__(self, input_size: int, hidden_size: int, num_layers: int, dropout: float) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout if num_layers > 1 else 0.0,
            batch_first=True,
        )
        self.dropout = nn.Dropout(dropout)
        self.head = nn.Linear(hidden_size, 1)

    def forward(self, inputs: Tensor) -> Tensor:  # pragma: no cover - straightforward
        outputs, _ = self.lstm(inputs)
        final_state = outputs[:, -1, :]
        return self.head(self.dropout(final_state))


class TransformerRegressor(nn.Module):
    """Lightweight Transformer encoder for sequence regression."""

    def __init__(self, input_size: int, hidden_size: int, num_layers: int, dropout: float) -> None:
        super().__init__()
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=input_size,
            nhead=max(1, input_size // 4),
            dim_feedforward=hidden_size,
            dropout=dropout,
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.head = nn.Linear(input_size, 1)

    def forward(self, inputs: Tensor) -> Tensor:  # pragma: no cover - straightforward
        encoded = self.encoder(inputs)
        return self.head(encoded[:, -1, :])


def _build_model(model_config: ModelConfig, input_size: int) -> nn.Module:
    model_type = model_config.model_type.lower()
    if model_type == "lstm":
        return LSTMRegressor(
            input_size=input_size,
            hidden_size=model_config.hidden_size,
            num_layers=model_config.num_layers,
            dropout=model_config.dropout,
        )
    if model_type == "transformer":
        return TransformerRegressor(
            input_size=input_size,
            hidden_size=model_config.hidden_size,
            num_layers=model_config.num_layers,
            dropout=model_config.dropout,
        )
    raise ValueError(f"Unsupported model_type '{model_config.model_type}'. Choose 'lstm' or 'transformer'.")


# ---------------------------------------------------------------------------
# Metrics


def _annualisation_factor(frequency_minutes: int = 15) -> float:
    trading_minutes_per_year = 252 * 6.5 * 60  # U.S. trading hours approximation
    periods_per_year = trading_minutes_per_year / frequency_minutes
    return math.sqrt(periods_per_year)


def _compute_sharpe(returns: np.ndarray) -> float:
    if returns.size < 2:
        return 0.0
    std = returns.std(ddof=1)
    if std == 0:
        return 0.0
    return _annualisation_factor() * returns.mean() / std


def _compute_sortino(returns: np.ndarray) -> float:
    if returns.size < 2:
        return 0.0
    negative = returns[returns < 0]
    if negative.size == 0:
        return float("inf")
    downside_std = negative.std(ddof=1)
    if downside_std == 0:
        return float("inf")
    return _annualisation_factor() * returns.mean() / downside_std


def _compute_max_drawdown(returns: np.ndarray) -> float:
    if returns.size == 0:
        return 0.0
    cumulative = np.cumsum(returns)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = cumulative - running_max
    return float(drawdowns.min())


def _compute_cvar(returns: np.ndarray, alpha: float = 0.95) -> float:
    if returns.size == 0:
        return 0.0
    threshold = np.quantile(returns, 1 - alpha)
    tail_losses = returns[returns <= threshold]
    if tail_losses.size == 0:
        return threshold
    return float(tail_losses.mean())


def _evaluate_strategy(predictions: np.ndarray, targets: np.ndarray) -> Dict[str, float]:
    if predictions.shape != targets.shape:
        raise ValueError("Predictions and targets must have the same shape")
    strategy_returns = np.sign(predictions) * targets
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
    model: nn.Module,
    train_loader: DataLoader,
    val_loader: DataLoader,
    config: TrainingConfig,
    device: torch.device,
) -> Tuple[nn.Module, List[float]]:
    model.to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(
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
        with torch.no_grad():
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


def _predict(model: nn.Module, data_loader: DataLoader, device: torch.device) -> np.ndarray:
    model.eval()
    predictions: List[np.ndarray] = []
    with torch.no_grad():
        for batch_features, _ in data_loader:
            batch_features = batch_features.to(device)
            outputs = model(batch_features)
            predictions.append(outputs.cpu().numpy().reshape(-1))
    return np.concatenate(predictions)


# ---------------------------------------------------------------------------
# Artifact persistence


def _write_artifacts(artifacts: Mapping[str, bytes], config: ObjectStorageConfig) -> Dict[str, str]:
    """Persist artifacts to either the filesystem or S3."""

    output_locations: Dict[str, str] = {}
    base_path = Path(config.base_path)
    if not config.is_s3():
        base_path.mkdir(parents=True, exist_ok=True)

    for name, payload in artifacts.items():
        if config.is_s3():
            if boto3 is None:
                raise RuntimeError("boto3 is required for S3 artifact uploads but is not installed")
            client = boto3.client("s3")
            key_parts = [part.strip("/") for part in (config.s3_prefix, name) if part]
            key = "/".join(key_parts)
            LOGGER.debug("Uploading artifact %s to s3://%s/%s", name, config.s3_bucket, key)
            client.put_object(Bucket=config.s3_bucket, Key=key, Body=payload)
            output_locations[name] = f"s3://{config.s3_bucket}/{key}"
        else:
            target_path = base_path / name
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
    override_frame: Optional[pd.DataFrame] = None,
) -> Dict[str, float]:
    """Execute the full workflow and return evaluation metrics."""

    frame = override_frame if override_frame is not None else _load_timescale_frame(config.timescale, engine)
    sequences, targets = _build_sequences(frame, config.timescale, config.model.sequence_length)

    split_index = int(len(sequences) * 0.8)
    train_sequences, val_sequences = sequences[:split_index], sequences[split_index:]
    train_targets, val_targets = targets[:split_index], targets[split_index:]

    if len(val_sequences) == 0:
        raise RuntimeError("Validation split is empty; adjust sequence length or dataset size")

    train_dataset = SequenceDataset(train_sequences, train_targets)
    val_dataset = SequenceDataset(val_sequences, val_targets)

    train_loader = DataLoader(train_dataset, batch_size=config.training.batch_size, shuffle=True, drop_last=False)
    val_loader = DataLoader(val_dataset, batch_size=config.training.batch_size, shuffle=False, drop_last=False)

    input_size = train_sequences.shape[-1]
    model = _build_model(config.model, input_size)

    device = torch.device(config.training.device)
    model, history = _train_model(model, train_loader, val_loader, config.training, device)

    predictions = _predict(model, val_loader, device)
    metrics = _evaluate_strategy(predictions, val_targets[: len(predictions)])

    run_prefix = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    artifacts: Dict[str, bytes] = {
        f"{run_prefix}/metrics.json": json.dumps(metrics, indent=2).encode("utf-8"),
        f"{run_prefix}/training_history.json": json.dumps({"val_loss": history}, indent=2).encode("utf-8"),
    }

    buffer = io.BytesIO()
    torch.save(model.state_dict(), buffer)
    artifacts[f"{run_prefix}/model.pt"] = buffer.getvalue()

    artifact_locations = _write_artifacts(artifacts, config.artifacts)
    LOGGER.info("Persisted artifacts: %s", artifact_locations)

    if config.mlflow and mlflow is not None:
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

            artifact_dir = Path(config.artifacts.base_path) / run_prefix
            if not config.artifacts.is_s3() and artifact_dir.exists():
                mlflow.log_artifacts(str(artifact_dir))

            if mlflow_pytorch is not None:
                mlflow_pytorch.log_model(model, artifact_path="model")

            if config.mlflow.registry_model_name and config.thresholds.satisfied_by(metrics):
                LOGGER.info("Metric thresholds satisfied; registering model as canary")
                model_uri = f"runs:/{run.info.run_id}/model"
                result = mlflow.register_model(model_uri, config.mlflow.registry_model_name)
                client = mlflow.tracking.MlflowClient()
                client.transition_model_version_stage(
                    name=config.mlflow.registry_model_name,
                    version=result.version,
                    stage="canary",
                    archive_existing_versions=False,
                )
    elif config.mlflow and mlflow is None:
        LOGGER.warning("MLflow configuration supplied but mlflow is not installed.")

    return metrics


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
    return parser.parse_args(argv)


def _build_config(args: argparse.Namespace) -> TrainingJobConfig:
    timescale = TimescaleSourceConfig(
        uri=args.timescale_uri,
        table=args.table,
        entity_column=args.entity_column,
        timestamp_column=args.timestamp_column,
        label_column=args.label_column,
        feature_columns=[col.strip() for col in args.feature_columns.split(",") if col.strip()],
    )

    model = ModelConfig(
        model_type=args.model_type,
        hidden_size=args.hidden_size,
        num_layers=args.num_layers,
        dropout=args.dropout,
        sequence_length=args.sequence_length,
    )

    training = TrainingConfig(
        batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        epochs=args.epochs,
        weight_decay=args.weight_decay,
        device="cuda" if torch.cuda.is_available() else "cpu",
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
    )


def main(argv: Optional[Sequence[str]] = None) -> None:  # pragma: no cover - CLI thin wrapper
    logging.basicConfig(level=logging.INFO)
    args = _parse_args(argv)
    config = _build_config(args)
    metrics = run_training_job(config)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":  # pragma: no cover - module execution
    main()
