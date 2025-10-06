"""Configuration regression tests for the ML training FastAPI service."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "ml" / "training" / "training_service.py"


def _install_training_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install lightweight module stubs so the training service can import."""

    if "pandas" not in sys.modules:
        panda_stub = SimpleNamespace(
            DataFrame=object,
            Timestamp=object,
            Series=object,
            concat=lambda *args, **kwargs: [],
            to_datetime=lambda value, **kwargs: value,
            isna=lambda value: False,
        )
        monkeypatch.setitem(sys.modules, "pandas", panda_stub)

    if "ml.features.build_features" not in sys.modules:
        build_features_module = ModuleType("ml.features.build_features")

        class FeatureBuildConfig:  # noqa: D401 - test stub
            """Stub FeatureBuildConfig with a permissive constructor."""

            def __init__(self, *args: object, **kwargs: object) -> None:  # noqa: D401 - test stub
                self.args = args
                self.kwargs = kwargs

        build_features_module.FeatureBuildConfig = FeatureBuildConfig  # type: ignore[attr-defined]
        build_features_module.OHLCV_TABLE = "ohlcv_bars"
        build_features_module.ENTITY_COLUMN = "market"
        build_features_module.EVENT_TIMESTAMP_COLUMN = "event_timestamp"
        build_features_module.CREATED_AT_COLUMN = "created_at"
        build_features_module.materialise_features = lambda *args, **kwargs: SimpleNamespace(  # type: ignore[attr-defined]
            empty=True,
            columns=[],
        )

        features_package = ModuleType("ml.features")
        features_package.build_features = build_features_module  # type: ignore[attr-defined]

        monkeypatch.setitem(sys.modules, "ml.features.build_features", build_features_module)
        monkeypatch.setitem(sys.modules, "ml.features", features_package)

    if "ml.training.data_loader_coingecko" not in sys.modules:
        coingecko_module = ModuleType("ml.training.data_loader_coingecko")
        coingecko_module.fetch_ohlcv = lambda *args, **kwargs: SimpleNamespace(empty=True)  # type: ignore[attr-defined]
        coingecko_module.upsert_timescale = lambda *args, **kwargs: None  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "ml.training.data_loader_coingecko", coingecko_module)

    if "ml.training.workflow" not in sys.modules:
        workflow_module = ModuleType("ml.training.workflow")

        class _StubBase:
            def __init__(self, **kwargs: object) -> None:
                for key, value in kwargs.items():
                    setattr(self, key, value)

        class TimescaleSourceConfig(_StubBase):
            pass

        class MetricThresholds(_StubBase):
            pass

        class ObjectStorageConfig(_StubBase):
            pass

        class TrainingMetadata(_StubBase):
            pass

        class MLflowConfig(_StubBase):
            pass

        class ModelConfig(_StubBase):
            pass

        class TrainingConfig(_StubBase):
            pass

        class ChronologicalSplitConfig(_StubBase):
            pass

        class OutlierConfig(_StubBase):
            pass

        def run_training_job(*args: object, **kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(model_version=None, metrics={})

        workflow_module.TimescaleSourceConfig = TimescaleSourceConfig  # type: ignore[attr-defined]
        workflow_module.MetricThresholds = MetricThresholds  # type: ignore[attr-defined]
        workflow_module.ObjectStorageConfig = ObjectStorageConfig  # type: ignore[attr-defined]
        workflow_module.TrainingMetadata = TrainingMetadata  # type: ignore[attr-defined]
        workflow_module.MLflowConfig = MLflowConfig  # type: ignore[attr-defined]
        workflow_module.ModelConfig = ModelConfig  # type: ignore[attr-defined]
        workflow_module.TrainingConfig = TrainingConfig  # type: ignore[attr-defined]
        workflow_module.ChronologicalSplitConfig = ChronologicalSplitConfig  # type: ignore[attr-defined]
        workflow_module.OutlierConfig = OutlierConfig  # type: ignore[attr-defined]
        workflow_module.run_training_job = run_training_job  # type: ignore[attr-defined]
        workflow_module.mlflow = SimpleNamespace(tracking=SimpleNamespace(MlflowClient=lambda: None))

        training_package = ModuleType("ml.training")
        training_package.workflow = workflow_module  # type: ignore[attr-defined]
        training_package.data_loader_coingecko = sys.modules["ml.training.data_loader_coingecko"]

        monkeypatch.setitem(sys.modules, "ml.training.workflow", workflow_module)
        monkeypatch.setitem(sys.modules, "ml.training", training_package)


def _load_training_module(module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ModuleNotFoundError(module_name)

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def _dispose_training_module(module_name: str) -> None:
    sys.modules.pop(module_name, None)


@pytest.fixture(autouse=True)
def _reset_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRAINING_TIMESCALE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("TRAINING_ALLOW_SQLITE_FOR_TESTS", raising=False)

    _install_training_stubs(monkeypatch)


def test_training_service_requires_database_url() -> None:
    module_name = "tests.ml.training.training_service_missing_dsn"
    with pytest.raises(RuntimeError, match="must be configured"):
        _load_training_module(module_name)


def test_training_service_normalizes_timescale_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "TRAINING_TIMESCALE_URI",
        "timescale://user:pass@example.com:5432/training",
    )

    module_name = "tests.ml.training.training_service_timescale"
    module = _load_training_module(module_name)
    try:
        assert module.DEFAULT_TIMESCALE_URI.startswith("postgresql+psycopg2://")  # type: ignore[attr-defined]
    finally:
        _dispose_training_module(module_name)


def test_training_service_rejects_sqlite_without_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRAINING_TIMESCALE_URI", "sqlite:///./training.db")

    module_name = "tests.ml.training.training_service_sqlite_rejected"
    with pytest.raises(RuntimeError, match="PostgreSQL/Timescale compatible scheme"):
        _load_training_module(module_name)


def test_training_service_allows_sqlite_when_flag_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRAINING_TIMESCALE_URI", "sqlite:///./training.db")
    monkeypatch.setenv("TRAINING_ALLOW_SQLITE_FOR_TESTS", "1")

    module_name = "tests.ml.training.training_service_sqlite_allowed"
    module = _load_training_module(module_name)
    try:
        assert module.DEFAULT_TIMESCALE_URI.startswith("sqlite")  # type: ignore[attr-defined]
    finally:
        _dispose_training_module(module_name)
