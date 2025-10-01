"""Supervised learning trainers with fee-aware objectives."""
from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable

import numpy as np
import pandas as pd

from .base import TrainingResult, UncertaintyGate


@dataclass
class SupervisedTrainerConfig:
    model_type: str
    fee_bps: float
    uncertainty_threshold: float = 0.5
    model_params: Dict[str, Any] = field(default_factory=dict)


class SupervisedTrainer:
    """Dispatcher for supported supervised model families."""

    def __init__(self, config: SupervisedTrainerConfig) -> None:
        self.config = config
        self.uncertainty_gate = UncertaintyGate(config.uncertainty_threshold)
        self.model = self._initialise_model()

    def _initialise_model(self) -> Any:
        model_type = self.config.model_type.lower()
        params = self.config.model_params
        if model_type == "lightgbm":
            lightgbm = importlib.import_module("lightgbm")
            return lightgbm.LGBMRegressor(**params)
        if model_type == "xgboost":
            xgb = importlib.import_module("xgboost")
            return xgb.XGBRegressor(**params)
        if model_type == "tcn":
            torch = importlib.import_module("torch")
            nn = importlib.import_module("torch.nn")

            class TemporalConvNet(nn.Module):
                def __init__(self, input_size: int, hidden_size: int, output_size: int) -> None:
                    super().__init__()
                    self.network = nn.Sequential(
                        nn.Conv1d(input_size, hidden_size, kernel_size=3, padding=1),
                        nn.ReLU(),
                        nn.Conv1d(hidden_size, hidden_size, kernel_size=3, padding=1),
                        nn.ReLU(),
                        nn.Conv1d(hidden_size, output_size, kernel_size=1),
                    )

                def forward(self, x: Any) -> Any:
                    return self.network(x).mean(dim=-1)

            return TemporalConvNet(
                input_size=params.get("input_size", 32),
                hidden_size=params.get("hidden_size", 64),
                output_size=params.get("output_size", 1),
            )
        if model_type == "transformer":
            torch = importlib.import_module("torch")
            nn = importlib.import_module("torch.nn")

            class TransformerRegressor(nn.Module):
                def __init__(self, d_model: int = 32, nhead: int = 4, num_layers: int = 2) -> None:
                    super().__init__()
                    encoder_layer = nn.TransformerEncoderLayer(d_model=d_model, nhead=nhead)
                    self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
                    self.linear = nn.Linear(d_model, 1)

                def forward(self, x: Any) -> Any:
                    encoded = self.encoder(x)
                    return self.linear(encoded.mean(dim=0))

            return TransformerRegressor(
                d_model=params.get("d_model", 32),
                nhead=params.get("nhead", 4),
                num_layers=params.get("num_layers", 2),
            )
        raise ValueError(f"Unsupported model type: {self.config.model_type}")

    def train(self, features: pd.DataFrame, labels: pd.Series) -> TrainingResult:
        model_type = self.config.model_type.lower()
        if model_type in {"lightgbm", "xgboost"}:
            self.model.fit(features, labels)
            predictions = self.model.predict(features)
            mse = float(np.mean((predictions - labels) ** 2))
            return TrainingResult(metrics={"mse": mse}, model=self.model)

        torch = importlib.import_module("torch")
        optim = importlib.import_module("torch.optim")
        dataset = torch.utils.data.TensorDataset(
            torch.tensor(features.values, dtype=torch.float32),
            torch.tensor(labels.values, dtype=torch.float32).unsqueeze(-1),
        )
        loader = torch.utils.data.DataLoader(dataset, batch_size=32, shuffle=True)
        optimizer = optim.Adam(self.model.parameters(), lr=self.config.model_params.get("lr", 1e-3))
        loss_fn = torch.nn.MSELoss()
        self.model.train()
        for _ in range(self.config.model_params.get("epochs", 5)):
            for batch_features, batch_labels in loader:
                optimizer.zero_grad()
                outputs = self.model(batch_features)
                loss = loss_fn(outputs, batch_labels)
                loss.backward()
                optimizer.step()
        return TrainingResult(metrics={"loss": float(loss.item())}, model=self.model)

    def predict(self, features: pd.DataFrame) -> np.ndarray:
        model_type = self.config.model_type.lower()
        if model_type in {"lightgbm", "xgboost"}:
            predictions = np.asarray(self.model.predict(features))
            uncertainties = np.abs(predictions) * 0.05
            return self.uncertainty_gate.apply(predictions, uncertainties)

        torch = importlib.import_module("torch")
        self.model.eval()
        with torch.no_grad():
            predictions_tensor = self.model(torch.tensor(features.values, dtype=torch.float32))
            predictions = predictions_tensor.squeeze(-1).numpy()
        uncertainties = np.abs(predictions) * 0.1
        return self.uncertainty_gate.apply(predictions, uncertainties)


def fee_adjusted_metric(returns: Iterable[float], turnovers: Iterable[float], fee_bps: float) -> Dict[str, float]:
    returns_arr = np.asarray(list(returns), dtype=float)
    turnovers_arr = np.asarray(list(turnovers), dtype=float)
    fees = turnovers_arr * fee_bps / 10_000.0
    net_returns = returns_arr - fees
    sharpe = float(np.mean(net_returns) / (np.std(net_returns) + 1e-8))
    downside = net_returns[net_returns < 0]
    downside_std = float(np.std(downside)) if downside.size else 1e-8
    sortino = float(np.mean(net_returns) / (downside_std + 1e-8))
    cumulative = np.cumsum(net_returns)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = cumulative - running_max
    max_drawdown = float(drawdowns.min() if drawdowns.size else 0.0)
    turnover = float(np.mean(turnovers_arr))
    return {
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_drawdown,
        "turnover": turnover,
    }


__all__ = ["SupervisedTrainer", "SupervisedTrainerConfig", "fee_adjusted_metric"]
