import importlib.util
from pathlib import Path
import sys


_MODULE_PATH = Path(__file__).resolve().parents[3] / "services" / "hedge" / "hedge_service.py"
_SPEC = importlib.util.spec_from_file_location("hedge_service_under_test", _MODULE_PATH)
assert _SPEC and _SPEC.loader  # pragma: no cover - static import-time validation
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

HedgeMetricsRequest = _MODULE.HedgeMetricsRequest
HedgeService = _MODULE.HedgeService


def test_health_status_reports_override_and_history() -> None:
    service = HedgeService()
    metrics = HedgeMetricsRequest(volatility=1.2, drawdown=0.3, stablecoin_price=1.0)

    decision = service.evaluate(metrics)

    status = service.health_status()
    assert status["mode"] == "auto"
    assert status["override_active"] is False
    assert status["history_depth"] == 1
    assert status["last_decision_at"] is not None
    assert status["last_target_pct"] == decision.target_pct
    assert status["last_guard_triggered"] == decision.diagnostics.guard_triggered

    service.set_override(55.0, reason="manual risk adjustment")
    override_status = service.health_status()
    assert override_status["override_active"] is True
    assert override_status["mode"] == "override"
    assert override_status["last_target_pct"] == 55.0
    assert override_status["last_decision_at"] is not None
    assert override_status["history_depth"] >= 2
