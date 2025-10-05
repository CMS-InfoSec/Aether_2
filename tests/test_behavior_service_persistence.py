from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pytest
from fastapi.testclient import TestClient
from fastapi import Header, Request


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _ensure_package(monkeypatch: pytest.MonkeyPatch, name: str) -> ModuleType:
    module = sys.modules.get(name)
    if module is None:
        module = ModuleType(name)
        module.__path__ = []  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, name, module)
    return module


def _ensure_alert_manager_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    try:
        import services.alert_manager  # type: ignore  # noqa: F401
        return
    except Exception:
        pass

    services_pkg = _ensure_package(monkeypatch, "services")

    stub_module = ModuleType("services.alert_manager")

    class _RiskEvent:  # pragma: no cover - trivial container
        def __init__(self, *args, **kwargs):
            self.event_type = kwargs.get("event_type")
            self.severity = kwargs.get("severity")
            self.description = kwargs.get("description")
            self.labels = kwargs.get("labels", {})

    stub_module.RiskEvent = _RiskEvent
    stub_module.get_alert_manager_instance = lambda: None
    monkeypatch.setitem(sys.modules, "services.alert_manager", stub_module)


def _ensure_timescale_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    try:
        import services.common.adapters  # type: ignore  # noqa: F401
        return
    except Exception:
        pass

    _ensure_package(monkeypatch, "services")
    _ensure_package(monkeypatch, "services.common")

    adapters_module = ModuleType("services.common.adapters")

    class _TimescaleAdapter:  # pragma: no cover - behaviourless stub
        def __init__(self, *args, **kwargs):
            self.account_id = kwargs.get("account_id", "")

        def events(self):
            return {}

        def telemetry(self):
            return []

    adapters_module.TimescaleAdapter = _TimescaleAdapter
    monkeypatch.setitem(sys.modules, "services.common.adapters", adapters_module)


def _ensure_security_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    _ensure_package(monkeypatch, "services")
    _ensure_package(monkeypatch, "services.common")

    security_module = ModuleType("services.common.security")

    def _require_admin_account(
        request: Request,
        authorization: str | None = Header(None, alias="Authorization"),
        x_account_id: str | None = Header(None, alias="X-Account-ID"),
    ):  # pragma: no cover - lightweight stub
        return (x_account_id or "test-account").strip() or "test-account"

    security_module.require_admin_account = _require_admin_account  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "services.common.security", security_module)


def _load_behavior_service(monkeypatch: pytest.MonkeyPatch, db_url: str):
    """Import ``behavior_service`` with a clean module state."""

    monkeypatch.setenv("BEHAVIOR_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("BEHAVIOR_DATABASE_URL", db_url)

    _ensure_alert_manager_stub(monkeypatch)
    _ensure_timescale_stub(monkeypatch)
    _ensure_security_stub(monkeypatch)

    existing = sys.modules.pop("behavior_service", None)
    if existing is not None:
        engine = getattr(existing, "ENGINE", None)
        if engine is not None:
            engine.dispose()

    module = importlib.import_module("behavior_service")
    module._initialize_database()
    return module


def _cleanup_behavior_service_module() -> None:
    module = sys.modules.pop("behavior_service", None)
    if module is not None:
        engine = getattr(module, "ENGINE", None)
        if engine is not None:
            engine.dispose()


def test_incidents_persist_across_restarts_and_replicas(tmp_path, monkeypatch):
    db_path = tmp_path / "behavior_shared.db"
    db_url = f"sqlite:///{db_path}"

    module_one = _load_behavior_service(monkeypatch, db_url)
    client_one = TestClient(module_one.app)

    account_id = "acct-test"
    timestamp = datetime.now(timezone.utc)
    expected_details = {"source": "unit", "z_score": 4.2}
    auth_headers = {
        "Authorization": "Bearer test-token",
        "X-Account-ID": account_id,
    }

    incident = module_one.BehaviorIncident(
        account_id=account_id,
        anomaly_type="unit_anomaly",
        severity="critical",
        ts=timestamp,
        details=expected_details,
    )

    def _fake_scan(requested_account: str, lookback: int):
        assert requested_account == account_id
        assert lookback == 60
        return [incident]

    monkeypatch.setattr(module_one.detector, "scan_account", _fake_scan)

    response = client_one.post(
        "/behavior/scan",
        json={"account_id": account_id, "lookback_minutes": 60},
        headers=auth_headers,
    )
    assert response.status_code == 200
    client_one.close()

    module_two = _load_behavior_service(monkeypatch, db_url)
    client_two = TestClient(module_two.app)
    status_response = client_two.get(
        "/behavior/status",
        params={"account_id": account_id},
        headers=auth_headers,
    )
    assert status_response.status_code == 200
    payload_two = status_response.json()
    client_two.close()

    assert payload_two["account_id"] == account_id
    assert len(payload_two["incidents"]) == 1
    incident_two = payload_two["incidents"][0]
    assert incident_two["anomaly_type"] == "unit_anomaly"
    assert incident_two["severity"] == "critical"
    assert incident_two["details"] == expected_details

    module_three = _load_behavior_service(monkeypatch, db_url)
    client_three = TestClient(module_three.app)
    replica_status = client_three.get(
        "/behavior/status",
        params={"account_id": account_id},
        headers=auth_headers,
    )
    assert replica_status.status_code == 200
    payload_three = replica_status.json()
    client_three.close()

    assert payload_three == payload_two

    _cleanup_behavior_service_module()
