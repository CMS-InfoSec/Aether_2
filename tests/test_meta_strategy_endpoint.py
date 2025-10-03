import logging

import importlib
import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

try:
    from ml.policy import meta_strategy
except ImportError as exc:  # pragma: no cover - fallback when scientific stack is unavailable
    if "randbits" not in str(exc):
        raise

    numpy_stub = types.ModuleType("numpy")
    numpy_stub.array = lambda data, dtype=None: list(data)
    numpy_stub.zeros = lambda length, dtype=float: [0.0 for _ in range(length)]
    numpy_stub.clip = lambda arr, a_min=None, a_max=None: list(arr)
    numpy_stub.max = lambda arr: max(arr)
    numpy_stub.abs = lambda arr: [abs(value) for value in arr]
    numpy_stub.vstack = lambda rows: rows
    numpy_stub.inf = float("inf")
    numpy_stub.nan = float("nan")
    sys.modules["numpy"] = numpy_stub

    class _DataFrame:
        def __init__(self, data, columns=None):
            self._data = data

        def replace(self, *_args, **_kwargs):
            return self

        def fillna(self, *_args, **_kwargs):
            return self

        def to_numpy(self, dtype=float):  # noqa: ARG002
            return self._data

    pandas_stub = types.ModuleType("pandas")
    pandas_stub.DataFrame = _DataFrame
    sys.modules["pandas"] = pandas_stub

    base_stub = types.ModuleType("sklearn.base")

    class _ClassifierMixin:
        pass

    base_stub.ClassifierMixin = _ClassifierMixin

    linear_stub = types.ModuleType("sklearn.linear_model")

    class _LogisticRegression:
        def __init__(self, *args, **kwargs):  # noqa: D401, ANN002, ANN003
            self.classes_: list[str] = []

        def fit(self, *_args, **_kwargs):
            return self

        def predict_proba(self, *_args, **_kwargs):
            return [[1.0]]

    linear_stub.LogisticRegression = _LogisticRegression

    pipeline_stub = types.ModuleType("sklearn.pipeline")

    class _Pipeline(_LogisticRegression):
        def __init__(self, *args, **kwargs):  # noqa: D401, ANN002, ANN003
            super().__init__()

    pipeline_stub.Pipeline = _Pipeline

    preprocessing_stub = types.ModuleType("sklearn.preprocessing")

    class _StandardScaler:
        def fit(self, *_args, **_kwargs):
            return self

        def transform(self, data):
            return data

        def fit_transform(self, data, *_args, **_kwargs):
            return data

    preprocessing_stub.StandardScaler = _StandardScaler

    sklearn_stub = types.ModuleType("sklearn")
    sklearn_stub.__path__ = []  # mark as package
    sklearn_stub.base = base_stub
    sklearn_stub.linear_model = linear_stub
    sklearn_stub.pipeline = pipeline_stub
    sklearn_stub.preprocessing = preprocessing_stub

    sys.modules["sklearn"] = sklearn_stub
    sys.modules["sklearn.base"] = base_stub
    sys.modules["sklearn.linear_model"] = linear_stub
    sys.modules["sklearn.pipeline"] = pipeline_stub
    sys.modules["sklearn.preprocessing"] = preprocessing_stub

    sys.modules.pop("ml.policy.meta_strategy", None)
    sys.modules.pop("ml.policy", None)
    sys.modules.pop("ml", None)

    services_spec = importlib.util.spec_from_file_location(
        "services", ROOT / "services" / "__init__.py"
    )
    if services_spec and services_spec.loader:
        services_module = importlib.util.module_from_spec(services_spec)
        sys.modules["services"] = services_module
        services_spec.loader.exec_module(services_module)
        services_module.__path__ = [str(ROOT / "services")]

    common_spec = importlib.util.spec_from_file_location(
        "services.common", ROOT / "services" / "common" / "__init__.py"
    )
    if common_spec and common_spec.loader:
        common_module = importlib.util.module_from_spec(common_spec)
        sys.modules["services.common"] = common_module
        common_spec.loader.exec_module(common_module)
        common_module.__path__ = [str(ROOT / "services" / "common")]

    security_spec = importlib.util.spec_from_file_location(
        "services.common.security", ROOT / "services" / "common" / "security.py"
    )
    if security_spec and security_spec.loader:
        security_module = importlib.util.module_from_spec(security_spec)
        sys.modules["services.common.security"] = security_module
        security_spec.loader.exec_module(security_module)

    meta_strategy = importlib.import_module("ml.policy.meta_strategy")


@pytest.fixture()
def client() -> TestClient:
    original_allocator = meta_strategy._allocator
    meta_strategy._allocator = meta_strategy.MetaStrategyAllocator()
    try:
        with TestClient(meta_strategy.app) as http:
            yield http
    finally:
        meta_strategy._allocator = original_allocator


@pytest.fixture()
def admin_headers() -> dict[str, str]:
    from auth.service import InMemorySessionStore
    from services.common import security as security_module

    store = getattr(security_module, "_DEFAULT_SESSION_STORE", None)
    if store is None:
        store = InMemorySessionStore(ttl_minutes=240)
        security_module.set_default_session_store(store)
    session = store.create("company")
    return {
        "X-Account-ID": "company",
        "Authorization": f"Bearer {session.token}",
    }


def test_get_strategy_weights_requires_authentication(client: TestClient) -> None:
    response = client.get("/meta/strategy_weights", params={"symbol": "BTCUSD"})
    assert response.status_code == 401
    assert "missing" in response.json()["detail"].lower()


def test_get_strategy_weights_logs_admin_account(
    client: TestClient, caplog: pytest.LogCaptureFixture, admin_headers: dict[str, str]
) -> None:
    meta_strategy._allocator._latest_allocations["BTCUSD"] = {
        "symbol": "BTCUSD",
        "regime": "bull",
        "weights": {name: 0.25 for name in meta_strategy.STRATEGIES},
        "timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }

    with caplog.at_level(logging.INFO):
        response = client.get(
            "/meta/strategy_weights",
            params={"symbol": "BTCUSD"},
            headers=admin_headers,
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTCUSD"

    audit_records = [
        record
        for record in caplog.records
        if record.getMessage() == "Meta strategy allocation served"
    ]
    assert audit_records, "Expected audit log for admin response"
    assert any(getattr(record, "account_id", None) == "company" for record in audit_records)
