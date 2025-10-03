import contextlib
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

tests_path = str(ROOT / "tests")
if tests_path in sys.path:
    sys.path.remove(tests_path)
    sys.path.append(tests_path)

import importlib.util
from types import ModuleType


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise ImportError(f"Unable to load module {name} from {path}")
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
_load_module("services", ROOT / "services" / "__init__.py")
_load_module("services.common", ROOT / "services" / "common" / "__init__.py")
_load_module("services.common.config", ROOT / "services" / "common" / "config.py")
_load_module("services.common.security", ROOT / "services" / "common" / "security.py")

import pytest

pytest.importorskip("fastapi")
from fastapi import status
from fastapi.testclient import TestClient

import drift_service
from services.common.security import require_admin_account


@pytest.fixture(autouse=True)
def _suppress_background_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(drift_service, "_ensure_tables", lambda: None)

    async def _noop_loop() -> None:
        return None

    monkeypatch.setattr(drift_service, "_daily_drift_loop", _noop_loop)
    monkeypatch.setattr(drift_service, "_daily_task", None, raising=False)
    drift_service.app.dependency_overrides.clear()
    yield
    drift_service.app.dependency_overrides.clear()
    drift_service._daily_task = None


def _parse_utc_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@pytest.mark.parametrize(
    "method,path,attribute",
    [
        ("get", "/drift/status", "_connect"),
        ("post", "/drift/run", "_run_drift_detection"),
        ("get", "/drift/alerts", "_connect"),
        ("get", "/metrics", None),
    ],
)
def test_drift_service_endpoints_require_authentication(
    method: str, path: str, attribute: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    if attribute:
        def _fail(*_args, **_kwargs):
            raise AssertionError(f"{attribute} should not be invoked without authentication")

        monkeypatch.setattr(drift_service, attribute, _fail)

    with TestClient(drift_service.app) as client:
        response = getattr(client, method)(path)

    assert response.status_code in {status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN}


def test_drift_status_rejects_mismatched_account(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail_connect() -> contextlib.AbstractContextManager[None]:
        raise AssertionError("database connection should not be opened on mismatch")

    monkeypatch.setattr(drift_service, "_connect", _fail_connect)
    drift_service.app.dependency_overrides[require_admin_account] = lambda: "other-account"

    with TestClient(drift_service.app) as client:
        response = client.get("/drift/status")

    assert response.status_code == status.HTTP_403_FORBIDDEN


def test_drift_status_returns_latest_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    checked_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    metric = drift_service.DriftMetric(
        feature_name="spread",
        psi=0.32,
        ks_statistic=0.12,
        ks_pvalue=0.88,
        flagged=True,
    )

    monkeypatch.setattr(
        drift_service,
        "_connect",
        lambda: contextlib.nullcontext(object()),
    )
    monkeypatch.setattr(
        drift_service,
        "_load_latest_results",
        lambda _conn: (checked_at, {metric.feature_name: metric}),
    )

    drift_service.app.dependency_overrides[require_admin_account] = lambda: drift_service.ACCOUNT_ID

    with TestClient(drift_service.app) as client:
        response = client.get("/drift/status")

    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert payload["features_drift"] == {metric.feature_name: metric.flagged}
    assert payload["psi_scores"] == {metric.feature_name: metric.psi}
    assert payload["ks_scores"][metric.feature_name]["statistic"] == metric.ks_statistic
    assert payload["ks_scores"][metric.feature_name]["p_value"] == metric.ks_pvalue
    assert _parse_utc_timestamp(payload["last_checked"]) == checked_at


def test_drift_run_returns_detection_results(monkeypatch: pytest.MonkeyPatch) -> None:
    detected = drift_service.DriftStatus(
        features_drift={"spread": False},
        psi_scores={"spread": 0.05},
        ks_scores={"spread": {"statistic": 0.03, "p_value": 0.97}},
        last_checked=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )

    monkeypatch.setattr(drift_service, "_run_drift_detection", lambda: detected)
    drift_service.app.dependency_overrides[require_admin_account] = lambda: drift_service.ACCOUNT_ID

    with TestClient(drift_service.app) as client:
        response = client.post("/drift/run")

    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert payload["features_drift"] == detected.features_drift
    assert payload["psi_scores"] == detected.psi_scores
    assert payload["ks_scores"] == detected.ks_scores
    assert _parse_utc_timestamp(payload["last_checked"]) == detected.last_checked


def test_drift_alerts_returns_serialized_records(monkeypatch: pytest.MonkeyPatch) -> None:
    triggered_at = datetime(2024, 1, 3, tzinfo=timezone.utc)
    record = drift_service.DriftAlertRecord(
        triggered_at=triggered_at,
        feature_name="spread",
        psi_score=0.4,
        action="retraining_triggered",
        details={"psi": 0.4},
    )

    monkeypatch.setattr(
        drift_service,
        "_connect",
        lambda: contextlib.nullcontext(object()),
    )
    monkeypatch.setattr(
        drift_service,
        "_fetch_recent_alerts",
        lambda _conn: [record],
    )

    drift_service.app.dependency_overrides[require_admin_account] = lambda: drift_service.ACCOUNT_ID

    with TestClient(drift_service.app) as client:
        response = client.get("/drift/alerts")

    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert len(payload["alerts"]) == 1
    alert = payload["alerts"][0]
    assert _parse_utc_timestamp(alert["triggered_at"]) == triggered_at
    assert alert["feature_name"] == record.feature_name
    assert alert["psi_score"] == record.psi_score
    assert alert["action"] == record.action
    assert alert["details"] == record.details


def test_metrics_endpoint_requires_authorization(monkeypatch: pytest.MonkeyPatch) -> None:
    drift_service.app.dependency_overrides[require_admin_account] = lambda: "mismatch"

    with TestClient(drift_service.app) as client:
        response = client.get("/metrics")

    assert response.status_code == status.HTTP_403_FORBIDDEN


def test_metrics_endpoint_returns_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    drift_service.app.dependency_overrides[require_admin_account] = lambda: drift_service.ACCOUNT_ID

    with TestClient(drift_service.app) as client:
        response = client.get("/metrics")

    assert response.status_code == status.HTTP_200_OK
    assert response.headers["content-type"].startswith(drift_service.CONTENT_TYPE_LATEST)
    assert response.text
