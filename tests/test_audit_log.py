from __future__ import annotations

import pytest

from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger


def test_audit_log_entries_are_immutable():
    store = AuditLogStore()
    logger = TimescaleAuditLogger(store)
    recorder = SensitiveActionRecorder(logger)

    entry = recorder.record(
        action="secret_rotation",
        actor_id="admin-9",
        before={"secret": "kraken", "status": "active"},
        after={"secret": "kraken", "status": "rotated"},
    )

    with pytest.raises(TypeError):
        entry.after["status"] = "tampered"

    entries = list(store.all())
    assert entries[0] == entry
