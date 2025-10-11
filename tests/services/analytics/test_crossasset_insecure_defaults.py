import importlib
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

MODULE = "services.analytics.crossasset_service"
_CONFIG_VARS = ("CROSSASSET_DATABASE_URL", "ANALYTICS_DATABASE_URL", "DATABASE_URL")


def _import_module():
    sys.modules.pop(MODULE, None)
    return importlib.import_module(MODULE)


def test_local_store_fallback_persists_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for var in _CONFIG_VARS:
        monkeypatch.delenv(var, raising=False)

    monkeypatch.setenv("ANALYTICS_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("CROSSASSET_FORCE_LOCAL_STORE", "1")
    monkeypatch.setenv("ANALYTICS_STATE_DIR", str(tmp_path))

    module = _import_module()
    try:
        assert module.USE_LOCAL_STORE is True
        assert module.DATABASE_URL is not None
        assert str(module.DATABASE_URL).startswith("sqlite")

        client = TestClient(module.app)
        response = client.get(
            "/crossasset/leadlag",
            params={"base": "BTC-USD", "target": "ETH-USD"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["pair"] == "BTC-USD/ETH-USD"
        assert payload["correlation"] != 0

        metrics_path = tmp_path / "metrics" / "BTC-USD_ETH-USD.json"
        assert metrics_path.exists()
        with metrics_path.open("r", encoding="utf-8") as handle:
            metrics_payload = json.load(handle)
        assert metrics_payload, "metrics payload should not be empty"

        series_path = tmp_path / "ohlcv" / "BTC-USD.json"
        assert series_path.exists()
    finally:
        sys.modules.pop(MODULE, None)
