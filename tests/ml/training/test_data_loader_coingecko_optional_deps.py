import datetime as dt
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "ml" / "training" / "data_loader_coingecko.py"
MODULE_NAME = "tests.ml.training._coingecko_loader"


def _module() -> ModuleType:
    """Load the CoinGecko loader directly from its source file."""

    sys.modules.pop(MODULE_NAME, None)
    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ModuleNotFoundError(MODULE_NAME)
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


def test_transform_requires_pandas(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "_PANDAS_MODULE", None, raising=False)

    with pytest.raises(module.MissingDependencyError, match="pandas is required"):
        module._transform_market_chart(
            {"prices": [[0, 1.0]], "total_volumes": [[0, 2.0]]},
            dt.datetime(2024, 1, 1),
            dt.datetime(2024, 1, 2),
            "1m",
        )


def test_request_requires_requests(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "_REQUESTS_MODULE", None, raising=False)
    monkeypatch.setattr(module, "_SESSION", None, raising=False)

    with pytest.raises(module.MissingDependencyError, match="requests is required"):
        module._request_with_retry("https://example.com", {})


def test_get_engine_requires_sqlalchemy(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "_SQLALCHEMY_CREATE_ENGINE", None, raising=False)
    monkeypatch.setattr(module, "_SQLALCHEMY_TEXT", None, raising=False)
    monkeypatch.setenv("TIMESCALE_DSN", "postgresql://localhost/db")

    with pytest.raises(module.MissingDependencyError, match="sqlalchemy is required"):
        module._get_engine()


def test_get_ge_context_requires_great_expectations(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "_GX_MODULE", None, raising=False)

    with pytest.raises(module.MissingDependencyError, match="great_expectations is required"):
        module._get_ge_context()

