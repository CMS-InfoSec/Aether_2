import builtins
import importlib.util
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "ml" / "hpo" / "optuna_runner.py"


def _load_optuna_module(module_name: str) -> ModuleType:
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


def _dispose_module(module_name: str) -> None:
    sys.modules.pop(module_name, None)


def _install_dependency_stubs(
    monkeypatch: pytest.MonkeyPatch, *, missing: frozenset[str]
) -> None:
    """Install lightweight dependency stubs so the Optuna runner can load."""

    for name in (
        "numpy",
        "pandas",
        "optuna",
        "ml",
        "ml.models",
        "ml.models.supervised",
        "ml.experiment_tracking",
        "ml.experiment_tracking.mlflow_utils",
    ):
        monkeypatch.delitem(sys.modules, name, raising=False)

    original_import = builtins.__import__

    def _guarded_import(name: str, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        if name in missing:
            raise ModuleNotFoundError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    if "numpy" not in missing:
        numpy_module = ModuleType("numpy")
        numpy_module.isclose = lambda a, b: abs(a - b) <= 1e-9  # type: ignore[attr-defined]
        numpy_module.sqrt = lambda value: value**0.5  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "numpy", numpy_module)

    if "pandas" not in missing:
        pandas_module = ModuleType("pandas")

        class _Series(list):  # noqa: D401 - minimal stub for tests
            def __init__(self, data=None, index=None):  # type: ignore[no-untyped-def]
                super().__init__(data or [])
                self.index = list(index) if index is not None else list(range(len(self)))

            def mean(self) -> float:
                return sum(self) / len(self) if self else 0.0

            def std(self) -> float:
                if not self:
                    return 0.0
                mean = self.mean()
                return (sum((value - mean) ** 2 for value in self) / len(self)) ** 0.5

            def __sub__(self, other):  # type: ignore[no-untyped-def]
                if isinstance(other, _Series):
                    return _Series([a - b for a, b in zip(self, other)], index=self.index)
                return _Series([a - other for a in self], index=self.index)

            def __lt__(self, other):  # type: ignore[no-untyped-def]
                return _Series([1 if value < other else 0 for value in self], index=self.index)

            def __getitem__(self, key):  # type: ignore[no-untyped-def]
                if isinstance(key, _Series):
                    filtered = [value for value, flag in zip(self, key) if flag]
                    filtered_index = [idx for idx, flag in zip(self.index, key) if flag]
                    return _Series(filtered, index=filtered_index)
                return super().__getitem__(key)

            def cumprod(self) -> "_Series":
                total = 1.0
                values = []
                for value in self:
                    total *= value
                    values.append(total)
                return _Series(values, index=self.index)

            def cummax(self) -> "_Series":
                peak = float("-inf")
                values = []
                for value in self:
                    peak = max(peak, value)
                    values.append(peak)
                return _Series(values, index=self.index)

            def diff(self) -> "_Series":
                if len(self) < 2:
                    return _Series([], index=self.index)
                return _Series([self[i] - self[i - 1] for i in range(1, len(self))], index=self.index[1:])

            def abs(self) -> "_Series":
                return _Series([abs(value) for value in self], index=self.index)

            def dropna(self) -> "_Series":
                return self

            def min(self) -> float:
                return min(self) if self else 0.0

        pandas_module.Series = _Series  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "pandas", pandas_module)

    ml_module = ModuleType("ml")
    ml_module.__path__ = []  # type: ignore[attr-defined]
    models_module = ModuleType("ml.models")
    models_module.__path__ = []  # type: ignore[attr-defined]
    ml_module.models = models_module  # type: ignore[attr-defined]

    @dataclass
    class _SupervisedDataset:  # noqa: D401 - lightweight stub for tests
        features: Any
        labels: Any

    class _Trainer:  # noqa: D401 - minimal stub for tests
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.experiment = None

        def fit(self, dataset: _SupervisedDataset) -> None:  # noqa: D401 - stub
            return None

        def predict(self, features: Any):  # type: ignore[no-untyped-def]
            index = getattr(features, "index", None)
            length = len(index) if index is not None else 1
            return [0.0] * length

    def _load_trainer(name: str, **kwargs: Any) -> _Trainer:
        return _Trainer()

    supervised_module = ModuleType("ml.models.supervised")
    supervised_module.SupervisedDataset = _SupervisedDataset  # type: ignore[attr-defined]
    supervised_module.load_trainer = _load_trainer  # type: ignore[attr-defined]
    models_module.supervised = supervised_module  # type: ignore[attr-defined]

    experiment_module = ModuleType("ml.experiment_tracking")
    experiment_module.__path__ = []  # type: ignore[attr-defined]

    @dataclass
    class _MLFlowConfig:  # noqa: D401 - lightweight stub for tests
        tracking_uri: str = ""
        experiment_name: str = ""

    @contextmanager
    def _mlflow_run(config: _MLFlowConfig):  # type: ignore[no-untyped-def]
        class _Experiment:  # noqa: D401 - minimal stub for tests
            def log_metric(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401 - stub
                return None

            def log_dict(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401 - stub
                return None

            def log_params(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401 - stub
                return None

            def log_artifact(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401 - stub
                return None

        yield _Experiment()

    mlflow_utils_module = ModuleType("ml.experiment_tracking.mlflow_utils")
    mlflow_utils_module.MLFlowConfig = _MLFlowConfig  # type: ignore[attr-defined]
    mlflow_utils_module.mlflow_run = lambda config: _mlflow_run(config)  # type: ignore[attr-defined]

    ml_module.experiment_tracking = experiment_module  # type: ignore[attr-defined]
    experiment_module.mlflow_utils = mlflow_utils_module  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "ml", ml_module)
    monkeypatch.setitem(sys.modules, "ml.models", models_module)
    monkeypatch.setitem(sys.modules, "ml.models.supervised", supervised_module)
    monkeypatch.setitem(sys.modules, "ml.experiment_tracking", experiment_module)
    monkeypatch.setitem(sys.modules, "ml.experiment_tracking.mlflow_utils", mlflow_utils_module)

    if "optuna" not in missing:
        optuna_module = ModuleType("optuna")

        class _Trial:  # noqa: D401 - minimal stub for tests
            number = 1

            def suggest_float(self, *args: Any, **kwargs: Any) -> float:  # noqa: D401 - stub
                return 0.1

            def suggest_int(self, *args: Any, **kwargs: Any) -> int:  # noqa: D401 - stub
                return 1

        class _Study:  # noqa: D401 - minimal stub for tests
            def optimize(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401 - stub
                return None

        def create_study(*args: Any, **kwargs: Any) -> _Study:  # noqa: D401 - stub
            return _Study()

        optuna_module.Trial = _Trial  # type: ignore[attr-defined]
        optuna_module.create_study = create_study  # type: ignore[attr-defined]
        optuna_module.trial = SimpleNamespace(Trial=_Trial)
        optuna_module.study = SimpleNamespace(Study=_Study)
        monkeypatch.setitem(sys.modules, "optuna", optuna_module)


def test_optuna_runner_requires_numpy(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "tests.ml.hpo_missing_numpy"
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "0")
    _install_dependency_stubs(monkeypatch, missing=frozenset({"numpy"}))

    module = _load_optuna_module(module_name)
    try:
        weights = module.ObjectiveWeights()
        with pytest.raises(module.MissingDependencyError, match="numpy is required"):
            weights.validate()
    finally:
        _dispose_module(module_name)


def test_optuna_runner_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "tests.ml.hpo_missing_pandas"
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "0")
    _install_dependency_stubs(monkeypatch, missing=frozenset({"pandas"}))

    module = _load_optuna_module(module_name)
    try:
        dataset = module.SupervisedDataset(features=None, labels=SimpleNamespace(index=[]))
        runner = module.OptunaRunner(
            study_name="demo",
            storage="sqlite://",
            trainer_name="lightgbm",
            dataset=dataset,
            objective_weights=module.ObjectiveWeights(),
        )
        with pytest.raises(module.MissingDependencyError, match="pandas is required"):
            runner._compute_metrics([0.1, 0.2])
    finally:
        _dispose_module(module_name)


def test_optuna_runner_requires_optuna(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "tests.ml.hpo_missing_optuna"
    monkeypatch.setenv("ML_ALLOW_INSECURE_DEFAULTS", "0")
    _install_dependency_stubs(monkeypatch, missing=frozenset({"optuna"}))

    module = _load_optuna_module(module_name)
    try:
        dataset = module.SupervisedDataset(features=SimpleNamespace(index=[0]), labels=SimpleNamespace(index=[0]))
        runner = module.OptunaRunner(
            study_name="demo",
            storage="sqlite://",
            trainer_name="lightgbm",
            dataset=dataset,
            objective_weights=module.ObjectiveWeights(),
        )
        with pytest.raises(module.MissingDependencyError, match="optuna is required"):
            runner.run()
    finally:
        _dispose_module(module_name)
