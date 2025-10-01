"""Automated hyperparameter tuning for sequence models using Optuna.

This script pulls training data from TimescaleDB, evaluates portfolio
metrics on a validation split, and logs each optimization trial to MLflow.
The best-performing run is automatically promoted to the ``canary`` stage in
the MLflow model registry.

Example
-------
python auto_tuner.py --model lstm --trials 50
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import numpy as np
import optuna
import pandas as pd
import torch
from torch import Tensor, nn
from torch.utils.data import DataLoader, Dataset

try:
    import mlflow
    import mlflow.pytorch
except Exception as exc:  # pragma: no cover - dependency is optional in tests.
    raise ImportError("mlflow is required to run the auto tuner") from exc

from ml.experiment_tracking.model_registry import register_model

try:
    from sqlalchemy import create_engine
except Exception as exc:  # pragma: no cover - sqlalchemy may be optional.
    raise ImportError(
        "sqlalchemy is required to pull data from TimescaleDB for auto tuning"
    ) from exc

LOGGER = logging.getLogger(__name__)


@dataclass
class AutoTunerConfig:
    """CLI configuration for the auto tuner."""

    model: str
    trials: int
    dsn: str
    query: str
    time_column: str
    entity_column: str
    target_column: str
    batch_size: int
    epochs: int
    experiment: str
    registry_name: str
    horizon_min: int = 5
    horizon_max: int = 60
    layers_min: int = 1
    layers_max: int = 4
    learning_rate_min: float = 1e-4
    learning_rate_max: float = 1e-2


class SequenceDataset(Dataset):
    """Dataset wrapping numpy arrays for PyTorch consumption."""

    def __init__(self, sequences: np.ndarray, targets: np.ndarray) -> None:
        self.sequences = torch.from_numpy(sequences).float()
        self.targets = torch.from_numpy(targets).float().unsqueeze(-1)

    def __len__(self) -> int:  # pragma: no cover - trivial container method.
        return len(self.sequences)

    def __getitem__(self, idx: int) -> Tuple[Tensor, Tensor]:  # pragma: no cover
        return self.sequences[idx], self.targets[idx]


class LSTMRegressor(nn.Module):
    """Simple LSTM regressor head used for tuning."""

    def __init__(self, input_size: int, hidden_size: int, num_layers: int, dropout: float) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        self.head = nn.Linear(hidden_size, 1)

    def forward(self, inputs: Tensor) -> Tensor:  # pragma: no cover - straightforward
        outputs, _ = self.lstm(inputs)
        last_state = outputs[:, -1, :]
        return self.head(self.dropout(last_state))


class SequenceCache:
    """Memoised sequence builder keyed by horizon length."""

    def __init__(self, frame: pd.DataFrame, config: AutoTunerConfig) -> None:
        self.frame = frame
        self.config = config
        self._cache: Dict[int, Tuple[np.ndarray, np.ndarray, List[str]]] = {}

    def get(self, horizon: int) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        if horizon not in self._cache:
            self._cache[horizon] = build_sequences(
                self.frame,
                horizon=horizon,
                time_column=self.config.time_column,
                entity_column=self.config.entity_column,
                target_column=self.config.target_column,
            )
        return self._cache[horizon]


def fetch_timescale_frame(dsn: str, query: str, time_column: str) -> pd.DataFrame:
    """Load a dataframe from TimescaleDB using the provided SQL query."""

    engine = create_engine(dsn, pool_pre_ping=True, pool_recycle=3600)
    with engine.connect() as conn:
        frame = pd.read_sql(query, conn)
    if time_column not in frame.columns:
        raise ValueError(f"Time column '{time_column}' not present in query result")
    frame[time_column] = pd.to_datetime(frame[time_column], utc=True)
    return frame.sort_values(time_column).reset_index(drop=True)


def build_sequences(
    frame: pd.DataFrame,
    *,
    horizon: int,
    time_column: str,
    entity_column: str,
    target_column: str,
) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    """Convert a flat dataframe into overlapping sequences for training."""

    if horizon < 1:
        raise ValueError("horizon must be >= 1")

    feature_columns = [
        col
        for col in frame.columns
        if col not in {time_column, entity_column, target_column}
    ]
    if not feature_columns:
        raise ValueError("The query must return at least one feature column")

    sequences: List[np.ndarray] = []
    targets: List[float] = []
    for _, group in frame.groupby(entity_column):
        group = group.sort_values(time_column)
        if len(group) <= horizon:
            continue
        features = group[feature_columns].to_numpy(dtype=np.float32)
        labels = group[target_column].to_numpy(dtype=np.float32)
        for start in range(0, len(group) - horizon):
            end = start + horizon
            sequences.append(features[start:end])
            targets.append(float(labels[end - 1]))

    if not sequences:
        raise RuntimeError("Not enough data to build sequences with the requested horizon")

    sequence_array = np.stack(sequences)
    target_array = np.asarray(targets, dtype=np.float32)
    return sequence_array, target_array, feature_columns


def split_train_validation(
    sequences: np.ndarray, targets: np.ndarray, validation_ratio: float = 0.2
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Split sequences into train and validation partitions."""

    if len(sequences) < 2:
        raise RuntimeError("Dataset too small to split into train and validation sets")

    split_idx = int(len(sequences) * (1 - validation_ratio))
    if split_idx == 0 or split_idx == len(sequences):
        raise RuntimeError("Validation split is empty; adjust validation ratio or gather more data")

    train_seq = sequences[:split_idx]
    val_seq = sequences[split_idx:]
    train_targets = targets[:split_idx]
    val_targets = targets[split_idx:]
    return train_seq, val_seq, train_targets, val_targets


def train_lstm_model(
    train_sequences: np.ndarray,
    train_targets: np.ndarray,
    val_sequences: np.ndarray,
    val_targets: np.ndarray,
    learning_rate: float,
    num_layers: int,
    batch_size: int,
    epochs: int,
) -> Tuple[nn.Module, np.ndarray]:
    """Train an LSTM model and return validation predictions."""

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    train_dataset = SequenceDataset(train_sequences, train_targets)
    val_dataset = SequenceDataset(val_sequences, val_targets)
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True, drop_last=False)
    val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False, drop_last=False)

    input_size = train_sequences.shape[-1]
    model = LSTMRegressor(input_size=input_size, hidden_size=64, num_layers=num_layers, dropout=0.1)
    model.to(device)

    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)

    for epoch in range(epochs):
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
        LOGGER.debug("Epoch %d/%d - train_loss=%.6f", epoch + 1, epochs, epoch_loss)

    model.eval()
    predictions: List[np.ndarray] = []
    with torch.no_grad():
        for batch_features, _ in val_loader:
            batch_features = batch_features.to(device)
            outputs = model(batch_features)
            predictions.append(outputs.cpu().numpy().reshape(-1))

    return model, np.concatenate(predictions)


def compute_metrics(predictions: np.ndarray, targets: np.ndarray) -> Dict[str, float]:
    """Compute portfolio evaluation metrics from predictions and targets."""

    if predictions.shape != targets.shape:
        raise ValueError("Predictions and targets must have the same shape")

    strategy_returns = np.sign(predictions) * targets
    sharpe = _compute_sharpe(strategy_returns)
    sortino = _compute_sortino(strategy_returns)
    return {"sharpe": float(sharpe), "sortino": float(sortino)}


def _annualisation_factor(frequency_minutes: int = 15) -> float:
    trading_minutes_per_year = 252 * 6.5 * 60
    periods_per_year = trading_minutes_per_year / frequency_minutes
    return float(np.sqrt(periods_per_year))


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


def parse_args(argv: Iterable[str]) -> AutoTunerConfig:
    parser = argparse.ArgumentParser(description="Automated Optuna tuning for sequence models")
    parser.add_argument("--model", choices=["lstm"], default="lstm", help="Model architecture to tune")
    parser.add_argument("--trials", type=int, default=50, help="Number of Optuna trials to run")
    parser.add_argument("--dsn", default=os.getenv("TIMESCALE_DSN"), help="TimescaleDB DSN")
    parser.add_argument("--query", default=os.getenv("AUTO_TUNER_QUERY"), help="SQL query returning training data")
    parser.add_argument("--time-column", default="event_timestamp", help="Timestamp column name")
    parser.add_argument("--entity-column", default="symbol", help="Entity identifier column")
    parser.add_argument("--target-column", default="target", help="Target column name")
    parser.add_argument("--batch-size", type=int, default=128, help="Training batch size")
    parser.add_argument("--epochs", type=int, default=10, help="Number of training epochs per trial")
    parser.add_argument(
        "--experiment",
        default=os.getenv("MLFLOW_EXPERIMENT", "auto_tuner"),
        help="MLflow experiment name",
    )
    parser.add_argument(
        "--registry-name",
        default=os.getenv("MLFLOW_MODEL_NAME", "auto_tuner_model"),
        help="MLflow model registry name",
    )

    args = parser.parse_args(list(argv))

    if not args.dsn:
        parser.error("Timescale DSN must be provided via --dsn or TIMESCALE_DSN")
    if not args.query:
        parser.error("SQL query must be provided via --query or AUTO_TUNER_QUERY")
    if args.trials < 1:
        parser.error("--trials must be >= 1")

    return AutoTunerConfig(
        model=args.model,
        trials=args.trials,
        dsn=args.dsn,
        query=args.query,
        time_column=args.time_column,
        entity_column=args.entity_column,
        target_column=args.target_column,
        batch_size=args.batch_size,
        epochs=args.epochs,
        experiment=args.experiment,
        registry_name=args.registry_name,
    )


def main(argv: Iterable[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = parse_args(argv or sys.argv[1:])

    LOGGER.info("Loading data from TimescaleDB using configured query")
    frame = fetch_timescale_frame(config.dsn, config.query, config.time_column)
    cache = SequenceCache(frame, config)

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment(config.experiment)

    def objective(trial: optuna.Trial) -> float:
        horizon = trial.suggest_int("horizon", config.horizon_min, config.horizon_max)
        num_layers = trial.suggest_int("layers", config.layers_min, config.layers_max)
        learning_rate = trial.suggest_float(
            "learning_rate", config.learning_rate_min, config.learning_rate_max, log=True
        )

        sequences, targets, feature_columns = cache.get(horizon)
        train_seq, val_seq, train_targets, val_targets = split_train_validation(sequences, targets)

        with mlflow.start_run(run_name=f"trial_{trial.number}") as run:
            params = {
                "model": config.model,
                "horizon": horizon,
                "layers": num_layers,
                "learning_rate": learning_rate,
                "batch_size": config.batch_size,
                "epochs": config.epochs,
                "feature_count": len(feature_columns),
            }
            mlflow.log_params(params)

            model, val_predictions = train_lstm_model(
                train_sequences=train_seq,
                train_targets=train_targets,
                val_sequences=val_seq,
                val_targets=val_targets,
                learning_rate=learning_rate,
                num_layers=num_layers,
                batch_size=config.batch_size,
                epochs=config.epochs,
            )

            metrics = compute_metrics(val_predictions, val_targets)
            mlflow.log_metrics(metrics)
            mlflow.log_metric("objective", metrics["sharpe"])

            mlflow.pytorch.log_model(model.cpu(), artifact_path="model")

            trial.set_user_attr("run_id", run.info.run_id)

            return metrics["sharpe"]

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=config.trials)

    best_trial = study.best_trial
    run_id = best_trial.user_attrs.get("run_id")
    if not run_id:
        raise RuntimeError("Optuna best trial is missing an associated MLflow run id")

    LOGGER.info("Best trial %s achieved Sharpe %.4f", best_trial.number, best_trial.value)
    LOGGER.info("Registering best run %s as model '%s' in canary stage", run_id, config.registry_name)
    register_model(run_id=run_id, name=config.registry_name, stage="canary")

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
