import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("fastapi", reason="FastAPI is required for drift monitoring tests")

from fastapi import status


def test_drift_monitoring_service_requires_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    drift_module = importlib.import_module("ml.monitoring.drift")
    service_module = importlib.import_module("ml.monitoring.drift_service")

    monkeypatch.setattr(drift_module, "pd", None, raising=False)
    monkeypatch.setattr(service_module, "pd", None, raising=False)

    service = service_module.DriftMonitoringService()

    with pytest.raises(service_module.HTTPException) as excinfo:
        service.evaluate({"feature": [0.1, 0.2, 0.3]})

    assert excinfo.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert "pandas" in excinfo.value.detail.lower()
