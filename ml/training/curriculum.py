"""Curriculum learning workflow for time-series forecasting models.

This module implements a lightweight training loop that gradually exposes a
sequence model to increasingly difficult market regimes.  Historical data is
partitioned into *stable*, *volatile*, and *crash* buckets based on realised
volatility and drawdown dynamics.  The model is first trained on the easiest
bucket before fine-tuning on progressively harder regimes.  Metrics from each
stage are logged to MLflow to make it easy to compare training runs.

The module can be executed directly as a CLI entrypoint::

    python -m ml.training.curriculum --model lstm --epochs 50

When no data path is provided a synthetic dataset is generated so that the
workflow can be smoke-tested without external dependencies.  Users are
encouraged to supply a CSV or Parquet file containing their own historical
feature matrix and target column.
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import torch
from torch import Tensor, nn
from torch.utils.data import DataLoader, Dataset

from ml.experiment_tracking.mlflow_utils import MLFlowConfig, mlflow_run

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data preparation utilities


def _ensure_sorted(frame: pd.DataFrame, timestamp_column: str) -> pd.DataFrame:
    """Return a copy of ``frame`` sorted by the timestamp column."""

    if timestamp_column not in frame.columns:
        raise KeyError(f"Timestamp column '{timestamp_column}' not found in frame")
    sorted_frame = frame.sort_values(timestamp_column).reset_index(drop=True).copy()
    sorted_frame[timestamp_column] = pd.to_datetime(sorted_frame[timestamp_column], utc=True)
    return sorted_frame


def load_historical_data(
    path: Optional[str],
    timestamp_column: str,
    price_column: str,
    label_column: str,
    *,
    synthetic_rows: int = 2048,
) -> pd.DataFrame:
    """Load historical data from disk or generate a synthetic dataset.

    Parameters
    ----------
    path:
        Location of a CSV or Parquet file containing the historical data.  When
        ``None`` a synthetic dataset is generated so that the curriculum
        training workflow can be executed without external dependencies.
    timestamp_column:
        Name of the timestamp column.  The column is converted to timezone-aware
        ``datetime`` objects to guarantee chronological ordering.
    price_column:
        Column containing prices used to derive market regimes.
    label_column:
        Target column that the model will learn to predict.
    synthetic_rows:
        Number of rows to generate for the synthetic fallback dataset.
    """

    if path is None:
        LOGGER.warning(
            "No data path supplied – generating %d rows of synthetic market data.",
            synthetic_rows,
        )
        return _generate_synthetic_dataset(
            synthetic_rows, timestamp_column=timestamp_column, price_column=price_column, label_column=label_column
        )

    data_path = Path(path)
    if not data_path.exists():
        raise FileNotFoundError(f"Historical data not found at '{data_path}'")

    if data_path.suffix.lower() in {".csv", ".txt"}:
        frame = pd.read_csv(data_path)
    elif data_path.suffix.lower() in {".parquet", ".pq"}:
        frame = pd.read_parquet(data_path)
    else:
        raise ValueError(
            "Unsupported file format. Provide a CSV or Parquet file containing historical data."
        )

    required_columns = {timestamp_column, price_column, label_column}
    missing = required_columns - set(frame.columns)
    if missing:
        raise KeyError(f"Historical data is missing required columns: {sorted(missing)}")

    return _ensure_sorted(frame, timestamp_column)


def _generate_synthetic_dataset(
    rows: int,
    *,
    timestamp_column: str,
    price_column: str,
    label_column: str,
) -> pd.DataFrame:
    """Create a reproducible synthetic market dataset."""

    rng = np.random.default_rng(seed=42)
    timestamps = pd.date_range(end=pd.Timestamp.utcnow(), periods=rows, freq="15min")
    # Build price paths with alternating volatility regimes.
    base_noise = rng.normal(0, 0.4, size=rows)
    regime_indicator = np.sin(np.linspace(0, 12 * np.pi, rows))
    volatility = np.interp(regime_indicator, [-1, 1], [0.2, 1.2])
    shocks = base_noise * volatility
    price = 100 + np.cumsum(shocks)
    price = np.maximum(price, 1.0)

    returns = pd.Series(price).pct_change().fillna(0.0)
    forward_return = returns.shift(-1).fillna(0.0)

    frame = pd.DataFrame(
        {
            timestamp_column: timestamps,
            price_column: price,
            "feature_returns": returns.rolling(4, min_periods=1).mean(),
            "feature_volatility": returns.rolling(12, min_periods=1).std().fillna(0.0),
            "feature_momentum": pd.Series(price).pct_change(periods=5).fillna(0.0),
            label_column: forward_return,
        }
    )
    return _ensure_sorted(frame, timestamp_column)


def split_regimes(
    frame: pd.DataFrame,
    *,
    price_column: str,
    timestamp_column: str,
    volatility_window: int = 48,
    crash_drawdown: float = -0.2,
) -> Dict[str, pd.DataFrame]:
    """Split ``frame`` into stable, volatile, and crash regimes.

    The partitioning heuristic uses rolling volatility and drawdown derived from
    the price column.  Buckets are returned as a dictionary keyed by regime name.
    Rows with insufficient history to compute volatility estimates are discarded.
    """

    frame = _ensure_sorted(frame.copy(), timestamp_column)
    prices = frame[price_column].astype(float)
    returns = prices.pct_change().fillna(0.0)
    rolling_vol = returns.rolling(window=volatility_window, min_periods=volatility_window // 2).std()
    rolling_vol = rolling_vol.fillna(method="bfill").fillna(rolling_vol.mean())
    running_max = prices.cummax()
    drawdown = (prices / running_max) - 1.0

    # Compute volatility thresholds.
    lower, upper = np.quantile(rolling_vol.dropna(), [0.33, 0.66])

    stable_mask = (rolling_vol <= lower) & (drawdown > crash_drawdown)
    crash_mask = drawdown <= crash_drawdown
    volatile_mask = ~(stable_mask | crash_mask)

    regimes = {
        "stable": frame.loc[stable_mask].copy(),
        "volatile": frame.loc[volatile_mask].copy(),
        "crash": frame.loc[crash_mask].copy(),
    }

    # Drop rows with NaNs introduced by rolling calculations and log stats.
    for name, regime_frame in regimes.items():
        cleaned = regime_frame.dropna().reset_index(drop=True)
        regimes[name] = cleaned
        LOGGER.info("Regime '%s' contains %d samples", name, len(cleaned))

    return regimes


def infer_feature_columns(frame: pd.DataFrame, label_column: str, exclude: Iterable[str]) -> List[str]:
    """Infer numeric feature columns while excluding specific ones."""

    excluded = {label_column, *exclude}
    numeric_cols = frame.select_dtypes(include=[np.number]).columns
    features = [col for col in numeric_cols if col not in excluded]
    if not features:
        raise ValueError("Unable to infer feature columns; specify them explicitly using --feature-columns")
    return features


# ---------------------------------------------------------------------------
# Dataset preparation


def build_sequences(
    frame: pd.DataFrame,
    feature_columns: Sequence[str],
    label_column: str,
    sequence_length: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """Transform ``frame`` into sliding window sequences."""

    if len(frame) <= sequence_length:
        raise ValueError("Frame must contain more rows than the sequence length")

    features_matrix = frame.loc[:, feature_columns].to_numpy(dtype=np.float32)
    labels_array = frame[label_column].to_numpy(dtype=np.float32)

    sequences: List[np.ndarray] = []
    targets: List[float] = []
    for idx in range(len(features_matrix) - sequence_length):
        window = features_matrix[idx : idx + sequence_length]
        target_idx = idx + sequence_length
        if target_idx >= len(labels_array):
            break
        target = labels_array[target_idx]
        if np.isnan(window).any() or np.isnan(target):
            continue
        sequences.append(window)
        targets.append(float(target))

    if not sequences:
        raise RuntimeError("No valid sequences constructed; check for NaNs in the dataset")

    return np.stack(sequences), np.asarray(targets, dtype=np.float32)


class SequenceDataset(Dataset):
    """PyTorch dataset wrapping sliding window sequences."""

    def __init__(self, sequences: np.ndarray, targets: np.ndarray) -> None:
        self.sequences = torch.from_numpy(sequences).float()
        self.targets = torch.from_numpy(targets).float().unsqueeze(-1)

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self.sequences)

    def __getitem__(self, index: int) -> Tuple[Tensor, Tensor]:  # pragma: no cover - trivial
        return self.sequences[index], self.targets[index]


# ---------------------------------------------------------------------------
# Models


class LSTMRegressor(nn.Module):
    """Baseline LSTM regressor."""

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
    """Lightweight Transformer encoder for regression."""

    def __init__(self, input_size: int, hidden_size: int, num_layers: int, dropout: float) -> None:
        super().__init__()
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=input_size,
            nhead=max(1, input_size // 4 or 1),
            dim_feedforward=hidden_size,
            dropout=dropout,
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.head = nn.Linear(input_size, 1)

    def forward(self, inputs: Tensor) -> Tensor:  # pragma: no cover - straightforward
        encoded = self.encoder(inputs)
        return self.head(encoded[:, -1, :])


class MLPRegressor(nn.Module):
    """Simple multi-layer perceptron that treats the sequence as a flat vector."""

    def __init__(self, input_size: int, hidden_size: int, dropout: float) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, hidden_size // 2 or 1),
            nn.ReLU(),
            nn.Linear(hidden_size // 2 or 1, 1),
        )

    def forward(self, inputs: Tensor) -> Tensor:  # pragma: no cover - straightforward
        flat = inputs.flatten(start_dim=1)
        return self.net(flat)


def build_model(
    model_type: str,
    input_size: int,
    hidden_size: int,
    num_layers: int,
    dropout: float,
    sequence_length: int,
) -> nn.Module:
    """Instantiate a supported model type."""

    model_type = model_type.lower()
    if model_type == "lstm":
        return LSTMRegressor(input_size=input_size, hidden_size=hidden_size, num_layers=num_layers, dropout=dropout)
    if model_type == "transformer":
        return TransformerRegressor(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout,
        )
    if model_type == "mlp":
        return MLPRegressor(input_size=input_size * sequence_length, hidden_size=hidden_size, dropout=dropout)
    raise ValueError(f"Unsupported model type '{model_type}'. Choose from ['lstm', 'transformer', 'mlp'].")


# ---------------------------------------------------------------------------
# Training orchestration


@dataclass
class CurriculumStage:
    """Container describing a single curriculum stage."""

    name: str
    dataset: SequenceDataset
    frame: pd.DataFrame


@dataclass
class StageResult:
    """Metrics recorded after training on a single curriculum stage."""

    name: str
    epochs: int
    samples: int
    loss: float
    rmse: float
    mae: float


class CurriculumTrainer:
    """Train a model across progressively harder market regimes."""

    def __init__(
        self,
        model: nn.Module,
        *,
        device: str = "cpu",
        learning_rate: float = 1e-3,
        batch_size: int = 128,
        weight_decay: float = 0.0,
        experiment=None,
    ) -> None:
        self.model = model.to(device)
        self.device = torch.device(device)
        self.learning_rate = learning_rate
        self.batch_size = batch_size
        self.weight_decay = weight_decay
        self.criterion = nn.MSELoss()
        self.optimizer = torch.optim.Adam(
            self.model.parameters(), lr=self.learning_rate, weight_decay=self.weight_decay
        )
        self.experiment = experiment

    def fit(self, stages: Sequence[CurriculumStage], total_epochs: int) -> List[StageResult]:
        """Train sequentially over ``stages`` distributing ``total_epochs`` across them."""

        if not stages:
            raise ValueError("At least one curriculum stage must be provided")

        stage_epochs = self._allocate_epochs(total_epochs, len(stages))
        results: List[StageResult] = []

        for stage, epochs in zip(stages, stage_epochs):
            if len(stage.dataset) == 0:
                LOGGER.warning("Skipping stage '%s' because it contains no sequences", stage.name)
                continue
            LOGGER.info(
                "Training on stage '%s' for %d epochs with %d sequences", stage.name, epochs, len(stage.dataset)
            )
            metrics = self._train_stage(stage, epochs)
            results.append(metrics)

            if self.experiment:
                payload = {
                    "stage": stage.name,
                    "epochs": metrics.epochs,
                    "samples": metrics.samples,
                    "loss": metrics.loss,
                    "rmse": metrics.rmse,
                    "mae": metrics.mae,
                }
                self.experiment.log_metric(f"stage_{stage.name}_loss", metrics.loss)
                self.experiment.log_metric(f"stage_{stage.name}_rmse", metrics.rmse)
                self.experiment.log_metric(f"stage_{stage.name}_mae", metrics.mae)
                self.experiment.log_params({f"stage_{stage.name}_epochs": epochs, f"stage_{stage.name}_samples": metrics.samples})
                summary_path = Path("artifacts") / f"curriculum_stage_{stage.name}.json"
                summary_path.parent.mkdir(parents=True, exist_ok=True)
                summary_path.write_text(json.dumps(payload, indent=2, default=str))
                self.experiment.log_artifact(summary_path)

        return results

    def _allocate_epochs(self, total_epochs: int, num_stages: int) -> List[int]:
        base = max(total_epochs // num_stages, 1)
        epochs = [base] * num_stages
        remainder = max(total_epochs - base * num_stages, 0)
        for idx in range(remainder):
            epochs[idx % num_stages] += 1
        return epochs

    def _train_stage(self, stage: CurriculumStage, epochs: int) -> StageResult:
        dataloader = DataLoader(stage.dataset, batch_size=self.batch_size, shuffle=True)

        for epoch in range(epochs):
            self.model.train()
            epoch_loss = 0.0
            for sequences, targets in dataloader:
                sequences = sequences.to(self.device)
                targets = targets.to(self.device)
                self.optimizer.zero_grad(set_to_none=True)
                predictions = self.model(sequences)
                loss = self.criterion(predictions, targets)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=5.0)
                self.optimizer.step()
                epoch_loss += loss.item() * sequences.size(0)

            avg_loss = epoch_loss / max(len(stage.dataset), 1)
            LOGGER.debug("Stage %s epoch %d/%d loss %.6f", stage.name, epoch + 1, epochs, avg_loss)

        metrics = self._evaluate(stage)
        return StageResult(
            name=stage.name,
            epochs=epochs,
            samples=len(stage.dataset),
            loss=metrics["loss"],
            rmse=metrics["rmse"],
            mae=metrics["mae"],
        )

    def _evaluate(self, stage: CurriculumStage) -> Dict[str, float]:
        dataloader = DataLoader(stage.dataset, batch_size=self.batch_size, shuffle=False)
        self.model.eval()

        predictions: List[Tensor] = []
        targets: List[Tensor] = []
        with torch.no_grad():
            for sequences, batch_targets in dataloader:
                sequences = sequences.to(self.device)
                outputs = self.model(sequences).cpu()
                predictions.append(outputs.squeeze(-1))
                targets.append(batch_targets.cpu().squeeze(-1))

        if not predictions:
            return {"loss": float("nan"), "rmse": float("nan"), "mae": float("nan")}

        preds = torch.cat(predictions)
        trues = torch.cat(targets)
        mse = nn.functional.mse_loss(preds, trues).item()
        mae = nn.functional.l1_loss(preds, trues).item()
        return {"loss": mse, "rmse": float(np.sqrt(mse)), "mae": mae}


# ---------------------------------------------------------------------------
# CLI interface


def build_curriculum_stages(
    regimes: Dict[str, pd.DataFrame],
    feature_columns: Sequence[str],
    label_column: str,
    sequence_length: int,
    order: Sequence[str],
) -> List[CurriculumStage]:
    """Construct curriculum stages from the provided regimes."""

    stages: List[CurriculumStage] = []
    for name in order:
        frame = regimes.get(name)
        if frame is None or frame.empty:
            LOGGER.warning("Regime '%s' is empty and will be skipped", name)
            continue
        try:
            sequences, targets = build_sequences(frame, feature_columns, label_column, sequence_length)
        except (ValueError, RuntimeError) as exc:
            LOGGER.warning("Skipping regime '%s': %s", name, exc)
            continue
        dataset = SequenceDataset(sequences, targets)
        stages.append(CurriculumStage(name=name, dataset=dataset, frame=frame))
    return stages


def parse_feature_columns(raw: Optional[str]) -> Optional[List[str]]:
    if raw is None:
        return None
    return [col.strip() for col in raw.split(",") if col.strip()]


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Curriculum learning trainer for time-series models")
    parser.add_argument("--data-path", type=str, default=None, help="Path to CSV/Parquet historical data")
    parser.add_argument("--timestamp-column", type=str, default="timestamp", help="Timestamp column name")
    parser.add_argument("--price-column", type=str, default="price", help="Price column used for regime detection")
    parser.add_argument("--label-column", type=str, default="target", help="Target column for supervised learning")
    parser.add_argument("--feature-columns", type=str, default=None, help="Comma separated list of feature columns")
    parser.add_argument("--model", choices=["lstm", "transformer", "mlp"], default="lstm", help="Model architecture")
    parser.add_argument("--epochs", type=int, default=30, help="Total number of training epochs")
    parser.add_argument("--sequence-length", type=int, default=32, help="Sequence length for windowed datasets")
    parser.add_argument("--hidden-size", type=int, default=64, help="Hidden dimension for neural networks")
    parser.add_argument("--num-layers", type=int, default=2, help="Number of layers for LSTM/Transformer models")
    parser.add_argument("--dropout", type=float, default=0.1, help="Dropout probability")
    parser.add_argument("--batch-size", type=int, default=128, help="Batch size for training")
    parser.add_argument("--learning-rate", type=float, default=1e-3, help="Learning rate for the optimiser")
    parser.add_argument("--weight-decay", type=float, default=0.0, help="Weight decay for the optimiser")
    parser.add_argument("--device", type=str, default="cpu", help="Device identifier, e.g., 'cpu' or 'cuda:0'")
    parser.add_argument("--mlflow-uri", type=str, default=None, help="MLflow tracking URI")
    parser.add_argument("--mlflow-experiment", type=str, default=None, help="MLflow experiment name")
    parser.add_argument("--mlflow-run-name", type=str, default=None, help="Optional MLflow run name")
    parser.add_argument("--sequence-order", type=str, default="stable,volatile,crash", help="Curriculum order")
    parser.add_argument("--summary-path", type=str, default="artifacts/curriculum_summary.json", help="Where to store the training summary JSON")
    return parser.parse_args(args=args)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")


def run_curriculum_training(args: argparse.Namespace) -> List[StageResult]:
    configure_logging()

    frame = load_historical_data(
        args.data_path,
        timestamp_column=args.timestamp_column,
        price_column=args.price_column,
        label_column=args.label_column,
    )

    feature_columns = parse_feature_columns(args.feature_columns)
    if feature_columns is None:
        feature_columns = infer_feature_columns(frame, args.label_column, exclude=[args.price_column])

    regimes = split_regimes(
        frame,
        price_column=args.price_column,
        timestamp_column=args.timestamp_column,
    )

    order = [item.strip() for item in args.sequence_order.split(",") if item.strip()]
    stages = build_curriculum_stages(regimes, feature_columns, args.label_column, args.sequence_length, order)
    if not stages:
        raise RuntimeError("No curriculum stages could be constructed; check your data and configuration")

    input_size = len(feature_columns)
    model = build_model(
        args.model,
        input_size=input_size,
        hidden_size=args.hidden_size,
        num_layers=args.num_layers,
        dropout=args.dropout,
        sequence_length=args.sequence_length,
    )

    mlflow_context = None
    if args.mlflow_uri and args.mlflow_experiment:
        mlflow_context = mlflow_run(
            MLFlowConfig(
                tracking_uri=args.mlflow_uri,
                experiment_name=args.mlflow_experiment,
                run_name=args.mlflow_run_name,
            )
        )

    experiment = None
    if mlflow_context is not None:
        experiment = mlflow_context.__enter__()
        experiment.log_params(
            {
                "model": args.model,
                "epochs": args.epochs,
                "sequence_length": args.sequence_length,
                "hidden_size": args.hidden_size,
                "num_layers": args.num_layers,
                "dropout": args.dropout,
                "learning_rate": args.learning_rate,
                "batch_size": args.batch_size,
                "weight_decay": args.weight_decay,
                "feature_columns": feature_columns,
            }
        )

    trainer = CurriculumTrainer(
        model,
        device=args.device,
        learning_rate=args.learning_rate,
        batch_size=args.batch_size,
        weight_decay=args.weight_decay,
        experiment=experiment,
    )

    try:
        results = trainer.fit(stages, total_epochs=args.epochs)
    finally:
        if mlflow_context is not None:
            mlflow_context.__exit__(None, None, None)

    summary = {
        "model": args.model,
        "epochs": args.epochs,
        "stages": [result.__dict__ for result in results],
        "stage_counts": dict(Counter(stage.name for stage in stages)),
        "feature_columns": feature_columns,
    }

    summary_path = Path(args.summary_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, default=str))
    LOGGER.info("Wrote training summary to %s", summary_path)

    if experiment is not None:
        experiment.log_artifact(summary_path)

    for result in results:
        LOGGER.info(
            "Stage %s -> epochs=%d samples=%d loss=%.6f rmse=%.6f mae=%.6f",
            result.name,
            result.epochs,
            result.samples,
            result.loss,
            result.rmse,
            result.mae,
        )

    return results


def main(args: Optional[Sequence[str]] = None) -> List[StageResult]:
    parsed = parse_args(args)
    return run_curriculum_training(parsed)


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()

