import json
import os
import sys
import types
from pathlib import Path
from typing import Dict, Iterable

import pytest

# Ensure the feature version service writes to a temp location during tests
TEST_FEATURE_STORE = Path("/tmp") / "aether_feature_versions_test_store.json"
os.environ.setdefault("FEATURE_VERSION_STORE", str(TEST_FEATURE_STORE))
if TEST_FEATURE_STORE.exists():
    TEST_FEATURE_STORE.unlink()

# ---------------------------------------------------------------------------
# Lightweight stubs for optional dependencies (FastAPI, Pydantic)
# ---------------------------------------------------------------------------

fastapi_stub = types.ModuleType("fastapi")


class _FastAPI:
    def __init__(self, *_, **__):
        self.routes: list = []

    def post(self, *_args, **_kwargs):
        def decorator(func):
            self.routes.append(("POST", func))
            return func

        return decorator

    def get(self, *_args, **_kwargs):
        def decorator(func):
            self.routes.append(("GET", func))
            return func

        return decorator


class _HTTPException(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


fastapi_stub.FastAPI = _FastAPI
fastapi_stub.HTTPException = _HTTPException
fastapi_stub.status = types.SimpleNamespace(HTTP_400_BAD_REQUEST=400)

sys.modules.setdefault("fastapi", fastapi_stub)

pydantic_stub = types.ModuleType("pydantic")


class _BaseModel:
    def __init__(self, **data):
        for key, value in data.items():
            setattr(self, key, value)

    def model_dump(self) -> Dict[str, object]:
        return dict(self.__dict__)


def _field(default, **_kwargs):
    return default


def _validator(*_args, **_kwargs):
    def decorator(func):
        return func

    return decorator


pydantic_stub.BaseModel = _BaseModel
pydantic_stub.Field = _field
pydantic_stub.validator = _validator

sys.modules.setdefault("pydantic", pydantic_stub)

# Drop any cached imports so the test configuration is honoured
sys.modules.pop("ml.features.feature_versioning", None)

from ml.features.feature_versioning import (  # noqa: E402
    FeatureDeploymentState,
    FeatureVersioningManager,
    HealthCheckResult,
)


def _run_backtest(feature_values: Iterable[float]) -> Dict[str, float]:
    """Deterministic placeholder for the expensive backtest pipeline."""

    values = list(feature_values)
    total = sum(values)
    squared = sum(value * value for value in values)
    count = float(len(values))
    return {
        "pnl": round(total * 1.5, 6),
        "volatility": round(squared * 0.5, 6),
        "num_trades": count,
    }


@pytest.fixture()
def feature_store(tmp_path: Path) -> Path:
    store = tmp_path / "feature_versions.json"
    store.write_text(json.dumps({"records": [], "model_map": {}}))
    return store


def test_backtests_reproducible_when_switching_feature_versions(feature_store: Path) -> None:
    feature_sets = {
        "v1.0.0": [0.1, 0.2, 0.3],
        "v1.1.0": [0.4, 0.5, 0.6],
    }

    def shadow(version: str) -> Dict[str, float]:
        values = feature_sets[version]
        return {
            "feature_sum": float(sum(values)),
            "feature_norm": float(sum(v * v for v in values)),
        }

    def healthy(version: str, metrics: Dict[str, float]) -> HealthCheckResult:
        return HealthCheckResult(passed=True, metrics=metrics, details="ok")

    manager = FeatureVersioningManager(
        storage_path=feature_store,
        shadow_evaluator=shadow,
        health_checker=healthy,
        model_version_resolver=lambda: "model-main",
    )

    baseline_result = manager.promote("v1.0.0", "bootstrap feature set")
    assert baseline_result.promoted is True
    active_version, _ = manager.current_versions()
    assert active_version == "v1.0.0"

    baseline_metrics = _run_backtest(feature_sets[active_version])

    second_result = manager.promote("v1.1.0", "extended features")
    assert second_result.promoted is True
    active_version, _ = manager.current_versions()
    assert active_version == "v1.1.0"

    active_metrics = _run_backtest(feature_sets[active_version])
    assert active_metrics != baseline_metrics

    history = manager.status()["history"]
    previous_record = next(item for item in history if item["version"] == "v1.0.0")
    assert previous_record["status"] == FeatureDeploymentState.INACTIVE.value

    reproduced_metrics = _run_backtest(feature_sets[previous_record["version"]])
    assert reproduced_metrics == baseline_metrics


def test_policy_service_fetches_feature_version_bound_to_model(feature_store: Path) -> None:
    versions = iter(["model-v1", "model-v2"])

    def resolver() -> str:
        return next(versions)

    def shadow(version: str) -> Dict[str, float]:
        return {"feature_sum": float(len(version)), "records_scored": 1000.0}

    def healthy(version: str, metrics: Dict[str, float]) -> HealthCheckResult:
        return HealthCheckResult(passed=True, metrics=metrics, details="ready")

    manager = FeatureVersioningManager(
        storage_path=feature_store,
        shadow_evaluator=shadow,
        health_checker=healthy,
        model_version_resolver=resolver,
    )

    manager.promote("v1.0.0", "initial deployment")
    manager.promote("v1.1.0", "refined features")

    status = manager.status()
    mapping = status["model_map"]
    assert mapping == {"v1.0.0": "model-v1", "v1.1.0": "model-v2"}

    def fetch_feature_version(model_version: str) -> str:
        for feature_version, bound_model in manager.status()["model_map"].items():
            if bound_model == model_version:
                return feature_version
        raise LookupError(f"No feature version mapped to {model_version}")

    assert fetch_feature_version("model-v2") == "v1.1.0"
    assert fetch_feature_version("model-v1") == "v1.0.0"


def test_feature_rollback_restores_downstream_wiring(feature_store: Path) -> None:
    def shadow(version: str) -> Dict[str, float]:
        return {"feature_sum": float(len(version))}

    def health(version: str, metrics: Dict[str, float]) -> HealthCheckResult:
        if version == "v1.1.0":
            return HealthCheckResult(passed=False, metrics=metrics, details="latency regression")
        return HealthCheckResult(passed=True, metrics=metrics, details="ok")

    manager = FeatureVersioningManager(
        storage_path=feature_store,
        shadow_evaluator=shadow,
        health_checker=health,
        model_version_resolver=lambda: "model-main",
    )

    initial = manager.promote("v1.0.0", "initial rollout")
    assert initial.promoted is True
    assert manager.current_versions() == ("v1.0.0", None)

    failed = manager.promote("v1.1.0", "canary regressions")
    assert failed.promoted is False
    assert failed.rolled_back is True
    assert manager.current_versions() == ("v1.0.0", None)

    status = manager.status()
    assert status["active"]["version"] == "v1.0.0"
    assert status["active"]["status"] == FeatureDeploymentState.ACTIVE.value
    assert "v1.1.0" not in status["model_map"]


def test_schema_breaks_fail_feature_promotion(feature_store: Path) -> None:
    active_schema_hash = {"value": None}

    feature_schemas = {
        "v1.0.0": 101.0,
        "v1.1.0": 202.0,
    }

    def shadow(version: str) -> Dict[str, float]:
        return {"schema_hash": feature_schemas[version]}

    def health(version: str, metrics: Dict[str, float]) -> HealthCheckResult:
        schema = metrics["schema_hash"]
        if active_schema_hash["value"] is None:
            active_schema_hash["value"] = schema
            return HealthCheckResult(passed=True, metrics=metrics, details="baseline")
        if schema != active_schema_hash["value"]:
            return HealthCheckResult(
                passed=False,
                metrics=metrics,
                details=f"schema mismatch: expected {active_schema_hash['value']}, received {schema}",
            )
        return HealthCheckResult(passed=True, metrics=metrics, details="compatible")

    manager = FeatureVersioningManager(
        storage_path=feature_store,
        shadow_evaluator=shadow,
        health_checker=health,
        model_version_resolver=lambda: "model-main",
    )

    baseline = manager.promote("v1.0.0", "baseline schema")
    assert baseline.promoted is True
    assert manager.current_versions() == ("v1.0.0", None)

    failure = manager.promote("v1.1.0", "breaking schema change")
    assert failure.promoted is False
    assert failure.rolled_back is True
    assert failure.record.status is FeatureDeploymentState.INACTIVE
    assert failure.record.health_passed is False
    assert "schema mismatch" in (failure.record.health_details or "")

    status = manager.status()
    assert status["active"]["version"] == "v1.0.0"
    assert status["canary"] is None
    assert "v1.1.0" not in status["model_map"]
