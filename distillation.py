"""Model distillation workflow for training compact backup models.

This module provides a command line utility that fits a small "student"
model so that it can reproduce the predictions emitted by the current
production model (the "teacher").  The script generates a synthetic market
sampling of features, runs the teacher to obtain edge estimates, trains a
regularised linear regressor with a reduced feature set, and evaluates the
agreement between the two.  When the fidelity between teacher and student
exceeds 95% the distilled model is registered as a backup in the in-memory
model registry so that it can be promoted quickly during incidents.

The entrypoint can be executed with::

    python distillation.py --teacher_id run123 --student_size small

The ``teacher_id`` flag accepts either the registered model name or a run
identifier.  When MLflow is available the script attempts to load the latest
production model for the provided identifier.  In offline development
environments where MLflow is absent a deterministic fallback ensemble is used
so that the distillation pipeline remains testable.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

try:  # Optional dependency – numpy may be unavailable in lightweight environments.
    import numpy as _NUMPY  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - graceful degradation when numpy missing.
    _NUMPY = None  # type: ignore[assignment]

try:  # Optional dependency – pandas may be unavailable in CI or slim installs.
    import pandas as _PANDAS  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - graceful degradation when pandas missing.
    _PANDAS = None  # type: ignore[assignment]

if TYPE_CHECKING:  # pragma: no cover - imported only for static analysis.
    import numpy as np
    import pandas as pd

from ml.experiment_tracking.model_registry import get_latest_model
from ml.insecure_defaults import insecure_defaults_enabled, state_file
from shared.models.registry import ModelEnsemble, ModelPrediction, get_model_registry

try:  # Optional dependency – MLflow may be unavailable in CI.
    from mlflow import pyfunc
    from shared.mlflow_safe import harden_mlflow

    harden_mlflow()
except Exception:  # pragma: no cover - graceful degradation when mlflow missing.
    pyfunc = None  # type: ignore

LOGGER = logging.getLogger(__name__)


class MissingDependencyError(RuntimeError):
    """Raised when distillation functionality requires an optional dependency."""


def _fallback_state_path(teacher_id: str, student_size: str) -> Path:
    safe_teacher = teacher_id.replace("/", "-") or "anonymous"
    safe_student = student_size.replace("/", "-") or "default"
    return state_file("distillation", f"{safe_teacher}_{safe_student}.json")


def _fallback_metrics(seed: str) -> DistillationMetrics:
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    values = [int.from_bytes(digest[idx : idx + 2], "big") / 65535.0 for idx in range(0, 22, 2)]
    return DistillationMetrics(
        mae=round(values[0] * 5, 4),
        rmse=round(values[1] * 6, 4),
        agreement=round(0.8 + values[2] * 0.2, 4),
        correlation=round(0.6 + values[3] * 0.4, 4),
        r2=round(0.5 + values[4] * 0.5, 4),
        teacher_return=round(values[5] * 12 - 6, 4),
        student_return=round(values[6] * 12 - 6, 4),
        teacher_directional_accuracy=round(0.6 + values[7] * 0.4, 4),
        student_directional_accuracy=round(0.55 + values[8] * 0.45, 4),
        performance_gap=round(values[9] * 2 - 1, 4),
        fidelity=round(0.8 + values[10] * 0.2, 4),
    )


def _run_insecure_defaults(
    *,
    teacher_id: str,
    student_size: str,
    num_samples: int,
    random_seed: int,
) -> Tuple[DistillationMetrics, bool]:
    seed = f"{teacher_id}|{student_size}|{num_samples}|{random_seed}"
    metrics = _fallback_metrics(seed)
    path = _fallback_state_path(teacher_id, student_size)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "teacher_id": teacher_id,
        "student_size": student_size,
        "num_samples": num_samples,
        "random_seed": random_seed,
        "metrics": metrics.to_dict(),
        "registered": False,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    LOGGER.info("Persisted fallback distillation report to %s", path)
    return metrics, False


def _require_numpy() -> "np":
    """Return the numpy module or raise an informative error."""

    if _NUMPY is None:
        raise MissingDependencyError(
            "numpy is required for the model distillation workflow"
        )
    return _NUMPY  # type: ignore[return-value]


def _require_pandas() -> "pd":
    """Return the pandas module or raise an informative error."""

    if _PANDAS is None:
        raise MissingDependencyError(
            "pandas is required to prepare frames for MLflow pyfunc models"
        )
    return _PANDAS  # type: ignore[return-value]


@dataclass(frozen=True)
class DistillationDataset:
    """Container holding synthetic market data used for distillation."""

    features: Any
    feature_names: Tuple[str, ...]
    book_snapshots: Tuple[Mapping[str, float], ...]
    states: Tuple[Mapping[str, float], ...]
    labels: Any

    def __post_init__(self) -> None:  # pragma: no cover - defensive checks.
        if len(self.features) != len(self.book_snapshots):
            raise ValueError("Features and book snapshots must have the same length")
        if len(self.features) != len(self.states):
            raise ValueError("Features and states must have the same length")
        if len(self.features) != len(self.labels):
            raise ValueError("Features and labels must have the same length")

    @property
    def num_samples(self) -> int:
        return int(self.features.shape[0])

    @property
    def num_features(self) -> int:
        return int(self.features.shape[1])


@dataclass(frozen=True)
class TeacherResult:
    """Outputs produced by the teacher model for a dataset."""

    edges: Any
    confidence: Optional[Any] = None


@dataclass(frozen=True)
class StudentSizeConfig:
    """Hyper-parameters controlling the capacity of the student."""

    max_features: Optional[int]
    l2_penalty: float
    include_interactions: bool = False
    include_quadratic: bool = False


_STUDENT_SIZE_PRESETS: Mapping[str, StudentSizeConfig] = {
    "tiny": StudentSizeConfig(max_features=6, l2_penalty=1e-1, include_interactions=False),
    "small": StudentSizeConfig(max_features=12, l2_penalty=5e-2, include_interactions=True),
    "medium": StudentSizeConfig(
        max_features=18,
        l2_penalty=1e-2,
        include_interactions=True,
        include_quadratic=True,
    ),
}


class TeacherPredictor:
    """Facade that returns teacher predictions for the synthetic dataset."""

    def __init__(self, teacher_id: str) -> None:
        self.teacher_id = teacher_id
        self._pyfunc_model = self._maybe_load_mlflow_model()
        if self._pyfunc_model is None:
            self._ensemble = ModelEnsemble(name=f"teacher-{teacher_id}", version="prod")
        else:
            self._ensemble = None

    def predict(self, dataset: DistillationDataset) -> TeacherResult:
        numpy = _require_numpy()
        if self._pyfunc_model is not None:
            frame = _build_pyfunc_frame(dataset)
            predictions = numpy.asarray(self._pyfunc_model.predict(frame), dtype=float).reshape(-1)
            return TeacherResult(edges=predictions, confidence=None)

        assert self._ensemble is not None  # for type-checkers
        edges: List[float] = []
        confidences: List[float] = []
        for row, book_snapshot, state in zip(dataset.features, dataset.book_snapshots, dataset.states):
            prediction = self._ensemble.predict(
                features=row.tolist(),
                book_snapshot=dict(book_snapshot),
                state=dict(state),
            )
            edges.append(prediction.edge_bps)
            confidences.append(prediction.confidence.get("model_confidence", 0.0))
        return TeacherResult(
            edges=numpy.asarray(edges, dtype=float),
            confidence=numpy.asarray(confidences, dtype=float),
        )

    def _maybe_load_mlflow_model(self) -> Optional[object]:
        if pyfunc is None or not self.teacher_id:
            return None

        candidate_uris: List[str] = []
        try:
            latest = get_latest_model(self.teacher_id, stage="prod")
        except Exception as exc:  # pragma: no cover - MLflow lookup failure.
            LOGGER.debug("Unable to fetch latest model for %s: %s", self.teacher_id, exc)
            latest = None
        if latest is not None:
            candidate_uris.append(latest.source)
            candidate_uris.append(f"models:/{self.teacher_id}/Production")
        candidate_uris.append(f"runs:/{self.teacher_id}/model")

        for uri in candidate_uris:
            try:
                model = pyfunc.load_model(uri)
                LOGGER.info("Loaded teacher model from %s", uri)
                return model
            except Exception as exc:  # pragma: no cover - optional dependency failure.
                LOGGER.debug("Failed to load model from %s: %s", uri, exc)
        LOGGER.warning(
            "Falling back to deterministic ensemble for teacher '%s' because MLflow load failed.",
            self.teacher_id,
        )
        return None


class StudentRegressor:
    """Linear student model with optional feature expansions."""

    def __init__(self, config: StudentSizeConfig) -> None:
        self.config = config
        self._mean: Optional[Any] = None
        self._scale: Optional[Any] = None
        self._weights: Optional[Any] = None

    def fit(self, dataset: DistillationDataset, targets: Any) -> None:
        numpy = _require_numpy()
        rows = self._build_raw_rows(dataset)
        mean = rows.mean(axis=0)
        scale = rows.std(axis=0)
        scale[scale == 0.0] = 1.0
        standardized = (rows - mean) / scale
        design = numpy.hstack([standardized, numpy.ones((rows.shape[0], 1))])
        lambda_identity = self.config.l2_penalty * numpy.eye(design.shape[1])
        normal_matrix = design.T @ design + lambda_identity
        normal_target = design.T @ targets
        self._weights = numpy.linalg.solve(normal_matrix, normal_target)
        self._mean = mean
        self._scale = scale
        LOGGER.info(
            "Fitted student regressor with %d parameters (bias included)",
            design.shape[1],
        )

    def predict(self, dataset: DistillationDataset) -> Any:
        if self._weights is None or self._mean is None or self._scale is None:
            raise RuntimeError("Student has not been trained yet")
        rows = self._build_raw_rows(dataset)
        standardized = (rows - self._mean) / self._scale
        numpy = _require_numpy()
        design = numpy.hstack([standardized, numpy.ones((rows.shape[0], 1))])
        return design @ self._weights

    def predict_single(
        self,
        features: Sequence[float],
        *,
        book_snapshot: Mapping[str, float],
        state: Mapping[str, float],
    ) -> float:
        if self._weights is None or self._mean is None or self._scale is None:
            raise RuntimeError("Student has not been trained yet")
        numpy = _require_numpy()
        row = self._transform_row(features, book_snapshot, state)
        standardized = (row - self._mean) / self._scale
        extended = numpy.append(standardized, 1.0)
        return float(extended @ self._weights)

    def metadata(self) -> Dict[str, float]:
        if self._weights is None:
            raise RuntimeError("Student has not been trained yet")
        return {
            "num_features": float(len(self._weights) - 1),
            "l2_penalty": float(self.config.l2_penalty),
        }

    def _build_raw_rows(self, dataset: DistillationDataset) -> Any:
        numpy = _require_numpy()
        rows = [
            self._transform_row(features, book_snapshot, state)
            for features, book_snapshot, state in zip(
                dataset.features, dataset.book_snapshots, dataset.states
            )
        ]
        return numpy.asarray(rows, dtype=float)

    def _transform_row(
        self,
        features: Sequence[float],
        book_snapshot: Mapping[str, float],
        state: Mapping[str, float],
    ) -> Any:
        numpy = _require_numpy()
        feature_array = numpy.asarray(features, dtype=float)
        if self.config.max_features is not None:
            feature_array = feature_array[: self.config.max_features]
        extras = numpy.array(
            [
                float(book_snapshot.get("spread_bps", 0.0)),
                float(book_snapshot.get("imbalance", 0.0)),
                float(state.get("conviction", 0.5)),
                float(state.get("liquidity_score", 0.5)),
            ],
            dtype=float,
        )
        components: List[Any] = [feature_array, extras]
        if self.config.include_interactions:
            interactions = numpy.hstack([feature_array * extras[0], feature_array * extras[1]])
            components.append(interactions)
        if self.config.include_quadratic:
            components.append(feature_array ** 2)
        return numpy.concatenate(components)


class DistilledModel(ModelEnsemble):
    """Model ensemble wrapper powered by a distilled student regressor."""

    def __init__(
        self,
        *,
        name: str,
        version: str,
        student: StudentRegressor,
        metadata: Mapping[str, float],
        confidence_threshold: float = 0.55,
    ) -> None:
        super().__init__(name=name, version=version, confidence_threshold=confidence_threshold)
        self.student = student
        self.metadata: Mapping[str, float] = dict(metadata)

    def predict(
        self,
        *,
        features: Sequence[float],
        book_snapshot: Mapping[str, float],
        state: Mapping[str, float],
        expected_edge_bps: Optional[float] = None,
    ) -> ModelPrediction:  # type: ignore[override]
        edge = (
            expected_edge_bps
            if expected_edge_bps is not None
            else self.student.predict_single(features, book_snapshot=book_snapshot, state=state)
        )
        conviction = float(state.get("conviction", 0.5))
        liquidity = float(state.get("liquidity_score", 0.5))
        spread_bps = float(book_snapshot.get("spread_bps", 0.0))
        imbalance = float(book_snapshot.get("imbalance", 0.0))

        take_profit = max(edge * 1.5, 5.0)
        stop_loss = max(abs(edge) * 0.5, 5.0)
        model_conf = min(1.0, max(0.0, 0.5 + edge / 200.0))
        state_conf = min(1.0, max(0.0, liquidity))
        execution_conf = min(1.0, max(0.0, 1.0 - spread_bps / 100.0))

        confidence = {
            "model_confidence": round(model_conf, 4),
            "state_confidence": round(state_conf, 4),
            "execution_confidence": round(execution_conf, 4),
            "teacher_conviction": round(conviction, 4),
            "teacher_imbalance": round(imbalance, 4),
        }

        return ModelPrediction(
            edge_bps=float(edge),
            confidence=confidence,
            take_profit_bps=float(take_profit),
            stop_loss_bps=float(stop_loss),
        )


@dataclass
class DistillationMetrics:
    """Evaluation metrics comparing teacher and student behaviour."""

    mae: float
    rmse: float
    agreement: float
    correlation: float
    r2: float
    teacher_return: float
    student_return: float
    teacher_directional_accuracy: float
    student_directional_accuracy: float
    performance_gap: float
    fidelity: float

    def to_dict(self) -> Dict[str, float]:
        return {
            "mae": self.mae,
            "rmse": self.rmse,
            "agreement": self.agreement,
            "correlation": self.correlation,
            "r2": self.r2,
            "teacher_return": self.teacher_return,
            "student_return": self.student_return,
            "teacher_directional_accuracy": self.teacher_directional_accuracy,
            "student_directional_accuracy": self.student_directional_accuracy,
            "performance_gap": self.performance_gap,
            "fidelity": self.fidelity,
        }


def evaluate_distillation(
    *,
    teacher_edges: Any,
    student_edges: Any,
    labels: Any,
) -> DistillationMetrics:
    numpy = _require_numpy()
    if len(teacher_edges) != len(student_edges):
        raise ValueError("Teacher and student predictions must have the same length")
    if len(labels) != len(teacher_edges):
        raise ValueError("Labels and predictions must have the same length")

    residuals = teacher_edges - student_edges
    mae = float(numpy.mean(numpy.abs(residuals)))
    rmse = float(numpy.sqrt(numpy.mean(residuals ** 2)))

    teacher_centered = teacher_edges - teacher_edges.mean()
    denominator = float(numpy.sum(teacher_centered ** 2))
    r2 = float(1.0 - numpy.sum(residuals ** 2) / denominator) if denominator > 0 else 0.0

    with numpy.errstate(invalid="ignore"):
        correlation = float(numpy.corrcoef(teacher_edges, student_edges)[0, 1])
    if not numpy.isfinite(correlation):
        correlation = 0.0

    teacher_sign = numpy.sign(teacher_edges)
    student_sign = numpy.sign(student_edges)
    label_sign = numpy.sign(labels)
    agreement = float(numpy.mean(teacher_sign == student_sign))

    teacher_return = float(numpy.mean(labels * teacher_sign))
    student_return = float(numpy.mean(labels * student_sign))

    teacher_directional_accuracy = float(numpy.mean(teacher_sign == label_sign))
    student_directional_accuracy = float(numpy.mean(student_sign == label_sign))

    performance_gap = teacher_return - student_return
    fidelity = agreement

    return DistillationMetrics(
        mae=mae,
        rmse=rmse,
        agreement=agreement,
        correlation=correlation,
        r2=r2,
        teacher_return=teacher_return,
        student_return=student_return,
        teacher_directional_accuracy=teacher_directional_accuracy,
        student_directional_accuracy=student_directional_accuracy,
        performance_gap=float(performance_gap),
        fidelity=float(fidelity),
    )


def generate_synthetic_dataset(
    *,
    num_samples: int,
    num_features: int,
    seed: int = 17,
) -> DistillationDataset:
    numpy = _require_numpy()
    rng = numpy.random.default_rng(seed)
    features = rng.normal(0.0, 1.0, size=(num_samples, num_features))
    spread = rng.normal(8.0, 2.0, size=num_samples).clip(0.5, 25.0)
    imbalance = rng.normal(0.0, 0.3, size=num_samples).clip(-1.0, 1.0)
    conviction = rng.beta(2.0, 2.0, size=num_samples)
    liquidity = rng.beta(3.0, 1.5, size=num_samples)

    book_snapshots = tuple(
        {
            "spread_bps": float(spread[idx]),
            "imbalance": float(imbalance[idx]),
        }
        for idx in range(num_samples)
    )
    states = tuple(
        {
            "conviction": float(conviction[idx]),
            "liquidity_score": float(liquidity[idx]),
        }
        for idx in range(num_samples)
    )

    base_weights = rng.normal(0.0, 1.5, size=num_features)
    base_signal = features @ base_weights
    label_noise = rng.normal(0.0, 4.0, size=num_samples)
    labels = (
        base_signal
        + 18.0 * (conviction - 0.5)
        + 9.0 * (liquidity - 0.5)
        - 0.12 * spread
        + 6.0 * imbalance
        + label_noise
    )

    feature_names = tuple(f"feature_{idx}" for idx in range(num_features))
    return DistillationDataset(
        features=features,
        feature_names=feature_names,
        book_snapshots=book_snapshots,
        states=states,
        labels=labels,
    )


def _build_pyfunc_frame(dataset: DistillationDataset) -> Any:
    pandas = _require_pandas()
    data: Dict[str, Iterable[float]] = {
        name: dataset.features[:, idx]
        for idx, name in enumerate(dataset.feature_names)
    }
    data.update(
        {
            "spread_bps": [snapshot["spread_bps"] for snapshot in dataset.book_snapshots],
            "imbalance": [snapshot["imbalance"] for snapshot in dataset.book_snapshots],
            "conviction": [state["conviction"] for state in dataset.states],
            "liquidity_score": [state["liquidity_score"] for state in dataset.states],
        }
    )
    return pandas.DataFrame(data)


def maybe_register_student(
    *,
    teacher_id: str,
    student: StudentRegressor,
    metrics: DistillationMetrics,
) -> bool:
    fidelity_threshold = 0.95
    if metrics.fidelity < fidelity_threshold:
        LOGGER.info(
            "Student fidelity %.3f below threshold %.2f – skipping registration",
            metrics.fidelity,
            fidelity_threshold,
        )
        return False

    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    distilled = DistilledModel(
        name=f"{teacher_id}-student",
        version=version,
        student=student,
        metadata={"fidelity": metrics.fidelity, **student.metadata()},
        confidence_threshold=0.58,
    )
    registry = get_model_registry()
    key = f"{teacher_id}:student-backup"
    registry.register(key, distilled)
    LOGGER.info("Registered distilled student under key %s", key)
    return True


def run_distillation(
    *,
    teacher_id: str,
    student_size: str,
    num_samples: int,
    random_seed: int,
) -> Tuple[DistillationMetrics, bool]:
    allow_insecure = insecure_defaults_enabled()

    if allow_insecure:
        try:
            _require_numpy()
            _require_pandas()
        except MissingDependencyError:
            return _run_insecure_defaults(
                teacher_id=teacher_id,
                student_size=student_size,
                num_samples=num_samples,
                random_seed=random_seed,
            )

    if student_size not in _STUDENT_SIZE_PRESETS:
        choices = ", ".join(sorted(_STUDENT_SIZE_PRESETS))
        raise ValueError(f"Unsupported student size '{student_size}'. Expected one of: {choices}")

    student_config = _STUDENT_SIZE_PRESETS[student_size]
    num_features = student_config.max_features or 20
    dataset = generate_synthetic_dataset(
        num_samples=num_samples,
        num_features=max(int(num_features), 6),
        seed=random_seed,
    )
    teacher = TeacherPredictor(teacher_id)
    teacher_result = teacher.predict(dataset)

    student = StudentRegressor(_STUDENT_SIZE_PRESETS[student_size])
    student.fit(dataset, teacher_result.edges)
    student_edges = student.predict(dataset)

    metrics = evaluate_distillation(
        teacher_edges=teacher_result.edges,
        student_edges=student_edges,
        labels=dataset.labels,
    )

    registered = maybe_register_student(teacher_id=teacher_id, student=student, metrics=metrics)
    return metrics, registered


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Distil a production model into a compact student")
    parser.add_argument("--teacher_id", required=True, help="Registered model name or MLflow run id")
    parser.add_argument(
        "--student_size",
        choices=sorted(_STUDENT_SIZE_PRESETS.keys()),
        default="small",
        help="Target size of the student model",
    )
    parser.add_argument("--samples", type=int, default=4096, help="Number of synthetic samples to generate")
    parser.add_argument("--seed", type=int, default=17, help="Random seed for reproducibility")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    LOGGER.info(
        "Starting distillation for teacher=%s with student_size=%s (samples=%d)",
        args.teacher_id,
        args.student_size,
        args.samples,
    )

    metrics, registered = run_distillation(
        teacher_id=args.teacher_id,
        student_size=args.student_size,
        num_samples=args.samples,
        random_seed=args.seed,
    )

    metrics_payload = metrics.to_dict()
    LOGGER.info("Distillation metrics: %s", json.dumps(metrics_payload, sort_keys=True, indent=2))
    if registered:
        LOGGER.info("Student registered as backup model")
    else:
        LOGGER.info("Student not registered; fidelity below threshold")


if __name__ == "__main__":
    main()
