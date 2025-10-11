"""Supervised learning trainers for portfolio forecasting models."""

from __future__ import annotations

import abc
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Tuple


class MissingDependencyError(RuntimeError):
    """Raised when a required third-party dependency is unavailable."""


_NUMPY_MODULE: Any | None
_NUMPY_ERROR: Exception | None
try:  # pragma: no cover - exercised only when numpy is unavailable
    import numpy as _NUMPY_MODULE  # type: ignore[assignment]
except Exception as exc:  # pragma: no cover - optional dependency
    _NUMPY_MODULE = None
    _NUMPY_ERROR = exc
else:  # pragma: no cover - trivial happy path
    _NUMPY_ERROR = None


_PANDAS_MODULE: Any | None
_PANDAS_ERROR: Exception | None
try:  # pragma: no cover - exercised only when pandas is unavailable
    import pandas as _PANDAS_MODULE  # type: ignore[assignment]
except Exception as exc:  # pragma: no cover - optional dependency
    _PANDAS_MODULE = None
    _PANDAS_ERROR = exc
else:  # pragma: no cover - trivial happy path
    _PANDAS_ERROR = None


_JOBLIB_MODULE: Any | None
_JOBLIB_ERROR: Exception | None
try:  # pragma: no cover - exercised only when joblib is unavailable
    import joblib as _JOBLIB_MODULE  # type: ignore[assignment]
except Exception as exc:  # pragma: no cover - optional dependency
    _JOBLIB_MODULE = None
    _JOBLIB_ERROR = exc
else:  # pragma: no cover - trivial happy path
    _JOBLIB_ERROR = None


if TYPE_CHECKING:  # pragma: no cover - type-checking only
    import numpy as np  # type: ignore
    import pandas as pd  # type: ignore
else:  # pragma: no cover - executed at runtime
    np = _NUMPY_MODULE  # type: ignore[assignment]
    pd = _PANDAS_MODULE  # type: ignore[assignment]


def _require_numpy() -> Any:
    """Return the numpy module or raise a descriptive error if absent."""

    if _NUMPY_MODULE is None:
        raise MissingDependencyError(
            "numpy is required for supervised training operations"
        ) from _NUMPY_ERROR
    return _NUMPY_MODULE


def _require_pandas() -> None:
    """Ensure pandas is available before accepting dataframe inputs."""

    if _PANDAS_MODULE is None:
        raise MissingDependencyError(
            "pandas is required for supervised training operations"
        ) from _PANDAS_ERROR


def _require_joblib() -> Any:
    """Return the joblib module or raise an informative error."""

    if _JOBLIB_MODULE is None:
        raise MissingDependencyError(
            "joblib is required to persist sklearn-trained pipelines"
        ) from _JOBLIB_ERROR
    return _JOBLIB_MODULE


def _import_sklearn_components() -> Tuple[Any, Any, Any, Any, Any]:
    """Import scikit-learn building blocks on demand."""

    try:
        from sklearn.base import clone  # type: ignore
        from sklearn.ensemble import RandomForestRegressor  # type: ignore
        from sklearn.impute import SimpleImputer  # type: ignore
        from sklearn.pipeline import Pipeline  # type: ignore
        from sklearn.preprocessing import StandardScaler  # type: ignore
    except Exception as exc:  # pragma: no cover - scikit-learn optional
        raise MissingDependencyError(
            "scikit-learn is required for SklearnPipelineTrainer"
        ) from exc

    return clone, Pipeline, RandomForestRegressor, SimpleImputer, StandardScaler


from ml.experiment_tracking.mlflow_utils import MLFlowExperiment

LOGGER = logging.getLogger(__name__)


@dataclass
class SupervisedDataset:
    """Container for features and labels used by supervised trainers."""

    features: pd.DataFrame
    labels: pd.Series

    def to_numpy(self) -> Tuple[np.ndarray, np.ndarray]:
        _require_pandas()
        _require_numpy()
        return self.features.to_numpy(), self.labels.to_numpy()


class SupervisedTrainer(abc.ABC):
    """Common interface implemented by all supervised learning trainers."""

    name: str = "base"

    def __init__(self, experiment: Optional[MLFlowExperiment] = None) -> None:
        self.experiment = experiment

    @abc.abstractmethod
    def fit(self, dataset: SupervisedDataset, **kwargs: Any) -> Any:
        raise NotImplementedError

    @abc.abstractmethod
    def predict(self, features: pd.DataFrame) -> np.ndarray:
        raise NotImplementedError

    @abc.abstractmethod
    def save(self, path: Path) -> None:
        raise NotImplementedError

    def log_metrics(self, metrics: Mapping[str, float]) -> None:
        if not self.experiment:
            return
        for key, value in metrics.items():
            self.experiment.log_metric(key, value)

    def log_params(self, params: Mapping[str, Any]) -> None:
        if not self.experiment:
            return
        self.experiment.log_params(dict(params))


@dataclass
class LightGBMTrainer(SupervisedTrainer):
    """Wrapper around LightGBM models."""

    params: Dict[str, Any] = field(default_factory=dict)
    _model: Any = None

    name: str = "lightgbm"

    def fit(self, dataset: SupervisedDataset, **kwargs: Any) -> Any:  # type: ignore[override]
        try:
            import lightgbm as lgb
        except ImportError as exc:  # pragma: no cover - dependency optional.
            raise ImportError("lightgbm must be installed to use LightGBMTrainer") from exc

        train_set = lgb.Dataset(dataset.features, label=dataset.labels)
        all_params = {**self.params, **kwargs}
        self.log_params(all_params)
        self._model = lgb.train(all_params, train_set)
        LOGGER.info("Trained LightGBM model with %d features", dataset.features.shape[1])
        return self._model

    def predict(self, features: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        _require_pandas()
        _require_numpy()
        return self._model.predict(features)

    def save(self, path: Path) -> None:  # type: ignore[override]
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        path.parent.mkdir(parents=True, exist_ok=True)
        self._model.save_model(str(path))
        LOGGER.info("Saved LightGBM model to %s", path)
        if self.experiment:
            self.experiment.log_artifact(path)


@dataclass
class XGBoostTrainer(SupervisedTrainer):
    """Wrapper around XGBoost models."""

    params: Dict[str, Any] = field(default_factory=dict)
    num_boost_round: int = 200
    _model: Any = None
    name: str = "xgboost"

    def fit(self, dataset: SupervisedDataset, **kwargs: Any) -> Any:  # type: ignore[override]
        try:
            import xgboost as xgb
        except ImportError as exc:  # pragma: no cover - dependency optional.
            raise ImportError("xgboost must be installed to use XGBoostTrainer") from exc

        dtrain = xgb.DMatrix(dataset.features, label=dataset.labels)
        all_params = {**self.params, **kwargs}
        self.log_params(all_params)
        self._model = xgb.train(all_params, dtrain, num_boost_round=self.num_boost_round)
        LOGGER.info("Trained XGBoost model with %d boosting rounds", self.num_boost_round)
        return self._model

    def predict(self, features: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        _require_pandas()
        _require_numpy()
        import xgboost as xgb  # Safe import – only executed after fit.

        dmatrix = xgb.DMatrix(features)
        return self._model.predict(dmatrix)

    def save(self, path: Path) -> None:  # type: ignore[override]
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        path.parent.mkdir(parents=True, exist_ok=True)
        self._model.save_model(str(path))
        LOGGER.info("Saved XGBoost model to %s", path)
        if self.experiment:
            self.experiment.log_artifact(path)


@dataclass
class SklearnPipelineTrainer(SupervisedTrainer):
    """Scikit-learn regression pipeline with sensible preprocessing defaults."""

    params: Dict[str, Any] = field(default_factory=dict)
    random_state: Optional[int] = None
    pipeline: Any | None = None
    _model: Any = None

    name: str = "sklearn"

    def _build_pipeline(self) -> Any:
        clone, Pipeline, RandomForestRegressor, SimpleImputer, StandardScaler = (
            _import_sklearn_components()
        )
        if self.pipeline is not None:
            return clone(self.pipeline)
        estimator = RandomForestRegressor(
            n_estimators=300,
            random_state=self.random_state,
        )
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "scaler",
                    StandardScaler(),
                ),
                ("model", estimator),
            ]
        )

    def fit(self, dataset: SupervisedDataset, **kwargs: Any) -> Any:  # type: ignore[override]
        _require_pandas()
        _require_numpy()
        pipeline = self._build_pipeline()
        if self.random_state is not None:
            model = getattr(pipeline, "named_steps", {}).get("model")
            if model is not None and hasattr(model, "set_params"):
                model.set_params(random_state=self.random_state)

        all_params = {**self.params, **kwargs}
        if all_params:
            pipeline.set_params(**all_params)
            self.log_params(all_params)

        pipeline.fit(dataset.features, dataset.labels)
        self._model = pipeline
        LOGGER.info(
            "Trained sklearn pipeline with %d samples and %d features",
            len(dataset.labels),
            dataset.features.shape[1],
        )
        return pipeline

    def predict(self, features: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        _require_pandas()
        _require_numpy()
        return self._model.predict(features)

    def save(self, path: Path) -> None:  # type: ignore[override]
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        joblib = _require_joblib()
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._model, path)
        LOGGER.info("Saved sklearn pipeline to %s", path)
        if self.experiment:
            self.experiment.log_artifact(path)


@dataclass
class TemporalConvNetTrainer(SupervisedTrainer):
    """Temporal Convolutional Network trainer implemented with PyTorch."""

    input_channels: int
    output_size: int
    hidden_channels: Iterable[int]
    kernel_size: int = 3
    dropout: float = 0.1
    epochs: int = 20
    learning_rate: float = 1e-3
    batch_size: int = 64
    _model: Any = None
    name: str = "tcn"

    def _build_model(self) -> Any:
        import torch
        import torch.nn as nn

        class TemporalConvNet(nn.Module):
            def __init__(
                self,
                num_inputs: int,
                num_channels: Iterable[int],
                kernel_size: int,
                dropout: float,
                output_size: int,
            ) -> None:
                super().__init__()
                layers: List[nn.Module] = []
                prev_channels = num_inputs
                for channels in num_channels:
                    layers.append(
                        nn.Sequential(
                            nn.Conv1d(prev_channels, channels, kernel_size, padding="same"),
                            nn.ReLU(),
                            nn.Dropout(dropout),
                        )
                    )
                    prev_channels = channels
                self.network = nn.Sequential(*layers)
                self.head = nn.Linear(prev_channels, output_size)

            def forward(self, x: torch.Tensor) -> torch.Tensor:  # type: ignore[override]
                y = self.network(x)
                y = y.mean(dim=-1)
                return self.head(y)

        return TemporalConvNet(
            num_inputs=self.input_channels,
            num_channels=list(self.hidden_channels),
            kernel_size=self.kernel_size,
            dropout=self.dropout,
            output_size=self.output_size,
        )

    def fit(self, dataset: SupervisedDataset, **kwargs: Any) -> Any:  # type: ignore[override]
        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset

        features, labels = dataset.to_numpy()
        features = torch.tensor(features, dtype=torch.float32)
        labels = torch.tensor(labels, dtype=torch.float32)
        ds = TensorDataset(features, labels)
        loader = DataLoader(ds, batch_size=self.batch_size, shuffle=True)

        model = self._build_model()
        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=self.learning_rate)
        self.log_params(
            {
                "kernel_size": self.kernel_size,
                "dropout": self.dropout,
                "epochs": self.epochs,
                "learning_rate": self.learning_rate,
                "batch_size": self.batch_size,
            }
        )
        model.train()
        for epoch in range(self.epochs):
            epoch_loss = 0.0
            for batch_features, batch_labels in loader:
                optimizer.zero_grad()
                preds = model(batch_features.unsqueeze(-1).transpose(1, 2))
                loss = criterion(preds.squeeze(), batch_labels)
                loss.backward()
                optimizer.step()
                epoch_loss += loss.item()
            self.log_metrics({f"train_loss_epoch_{epoch}": epoch_loss / len(loader)})
        self._model = model
        LOGGER.info("Finished training TCN for %d epochs", self.epochs)
        return model

    def predict(self, features: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        import torch

        _require_pandas()
        _require_numpy()
        self._model.eval()
        tensor = torch.tensor(features.to_numpy(), dtype=torch.float32)
        with torch.no_grad():
            preds = self._model(tensor.unsqueeze(-1).transpose(1, 2))
        return preds.squeeze().numpy()

    def save(self, path: Path) -> None:  # type: ignore[override]
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        import torch

        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save(self._model.state_dict(), path)
        LOGGER.info("Saved TCN model to %s", path)
        if self.experiment:
            self.experiment.log_artifact(path)


@dataclass
class TransformerTrainer(SupervisedTrainer):
    """Transformer-based sequence regression model."""

    d_model: int = 128
    nhead: int = 4
    num_layers: int = 2
    dim_feedforward: int = 256
    dropout: float = 0.1
    epochs: int = 30
    learning_rate: float = 1e-3
    batch_size: int = 64
    _model: Any = None
    name: str = "transformer"

    def _build_model(self, feature_dim: int) -> Any:
        import torch
        import torch.nn as nn

        class TransformerRegressor(nn.Module):
            def __init__(self, feature_dim: int, config: "TransformerTrainer") -> None:
                super().__init__()
                self.input_projection = nn.Linear(feature_dim, config.d_model)
                encoder_layer = nn.TransformerEncoderLayer(
                    d_model=config.d_model,
                    nhead=config.nhead,
                    dim_feedforward=config.dim_feedforward,
                    dropout=config.dropout,
                    batch_first=True,
                )
                self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=config.num_layers)
                self.head = nn.Linear(config.d_model, 1)

            def forward(self, x: torch.Tensor) -> torch.Tensor:  # type: ignore[override]
                z = self.input_projection(x)
                z = self.encoder(z)
                return self.head(z[:, -1, :])

        return TransformerRegressor(feature_dim, self)

    def fit(self, dataset: SupervisedDataset, sequence_length: int, **kwargs: Any) -> Any:  # type: ignore[override]
        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset

        features, labels = dataset.to_numpy()
        if features.shape[0] < sequence_length:
            raise ValueError("Not enough samples to form sequences of the requested length")
        numpy = _require_numpy()
        sequences = []
        targets = []
        for idx in range(sequence_length, len(features)):
            sequences.append(features[idx - sequence_length : idx])
            targets.append(labels[idx])
        seq_tensor = torch.tensor(numpy.stack(sequences), dtype=torch.float32)
        label_tensor = torch.tensor(numpy.array(targets), dtype=torch.float32)
        ds = TensorDataset(seq_tensor, label_tensor)
        loader = DataLoader(ds, batch_size=self.batch_size, shuffle=True)

        model = self._build_model(feature_dim=features.shape[1])
        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=self.learning_rate)
        self.log_params(
            {
                "d_model": self.d_model,
                "nhead": self.nhead,
                "num_layers": self.num_layers,
                "dim_feedforward": self.dim_feedforward,
                "dropout": self.dropout,
                "epochs": self.epochs,
                "learning_rate": self.learning_rate,
                "batch_size": self.batch_size,
                "sequence_length": sequence_length,
            }
        )
        model.train()
        for epoch in range(self.epochs):
            epoch_loss = 0.0
            for seq_batch, label_batch in loader:
                optimizer.zero_grad()
                preds = model(seq_batch)
                loss = criterion(preds.squeeze(), label_batch)
                loss.backward()
                optimizer.step()
                epoch_loss += loss.item()
            self.log_metrics({f"train_loss_epoch_{epoch}": epoch_loss / len(loader)})
        self._model = model
        LOGGER.info("Finished training Transformer for %d epochs", self.epochs)
        return model

    def predict(self, features: pd.DataFrame, sequence_length: int) -> np.ndarray:  # type: ignore[override]
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        import torch

        _require_pandas()
        numpy = _require_numpy()
        feature_array = features.to_numpy()
        if feature_array.shape[0] < sequence_length:
            raise ValueError("Not enough samples to form sequences of the requested length")
        sequences = []
        for idx in range(sequence_length, len(feature_array) + 1):
            sequences.append(feature_array[idx - sequence_length : idx])
        seq_tensor = torch.tensor(numpy.stack(sequences), dtype=torch.float32)
        self._model.eval()
        with torch.no_grad():
            preds = self._model(seq_tensor)
        return preds.squeeze().numpy()

    def save(self, path: Path) -> None:  # type: ignore[override]
        if self._model is None:
            raise RuntimeError("Model has not been trained yet")
        import torch

        path.parent.mkdir(parents=True, exist_ok=True)
        torch.save(self._model.state_dict(), path)
        LOGGER.info("Saved Transformer model to %s", path)
        if self.experiment:
            self.experiment.log_artifact(path)


TRAINER_REGISTRY: Dict[str, type[SupervisedTrainer]] = {
    LightGBMTrainer.name: LightGBMTrainer,
    XGBoostTrainer.name: XGBoostTrainer,
    SklearnPipelineTrainer.name: SklearnPipelineTrainer,
    TemporalConvNetTrainer.name: TemporalConvNetTrainer,
    TransformerTrainer.name: TransformerTrainer,
}


def load_trainer(name: str, **kwargs: Any) -> SupervisedTrainer:
    """Instantiate a trainer by name."""

    try:
        trainer_cls = TRAINER_REGISTRY[name]
    except KeyError as exc:  # pragma: no cover - defensive guard.
        raise ValueError(f"Unknown trainer '{name}'") from exc
    return trainer_cls(**kwargs)

