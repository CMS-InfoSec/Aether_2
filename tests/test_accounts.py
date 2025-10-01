from __future__ import annotations

from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger
from shared.correlation import CorrelationContext

from accounts.service import AccountsService


def test_dual_approval_flow_requires_two_distinct_admins():
    store = AuditLogStore()
    logger = TimescaleAuditLogger(store)
    recorder = SensitiveActionRecorder(logger)
    service = AccountsService(recorder)

    change = service.request_risk_configuration_change(
        "admin-a", {"max_drawdown": 0.5}
    )
    service.approve_risk_change("admin-a", change.request_id)
    assert not change.executed

    with CorrelationContext("corr-123"):
        service.approve_risk_change("admin-b", change.request_id)

    assert change.executed
    entries = list(store.all())
    actions = [entry.action for entry in entries]
    assert "risk_change_requested" in actions
    assert "risk_change_executed" in actions

    executed_entry = next(entry for entry in entries if entry.action == "risk_change_executed")
    assert executed_entry.after["approvals"] == change.approvals
    assert executed_entry.correlation_id == "corr-123"
