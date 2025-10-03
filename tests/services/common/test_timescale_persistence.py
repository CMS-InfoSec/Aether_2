from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from services.common.adapters import TimescaleAdapter


def test_timescale_adapter_persists_events_across_instances() -> None:
    account = "integration"
    TimescaleAdapter.reset(account_id=account)

    adapter = TimescaleAdapter(account_id=account)
    ack_payload = {"order_id": "ACK-1"}
    fill_payload = {"order_id": "FILL-1", "price": 101.25}
    audit_record = {"action": "test", "actor": "unit"}

    adapter.record_ack(ack_payload)
    adapter.record_fill(fill_payload)
    adapter.record_event("risk.signal", {"state": "green"})
    adapter.record_audit_log(audit_record)
    adapter.record_decision("DEC-1", {"edge": 4.2})
    adapter.record_credential_access(
        secret_name="kraken",
        metadata={"api_key": "abc", "api_secret": "def"},
    )

    rotation_ts = datetime.now(timezone.utc)
    adapter.record_credential_rotation(secret_name="kraken", rotated_at=rotation_ts)

    config = adapter.load_risk_config()
    config["nav"] = 2_500_000.0
    config["kill_switch"] = True
    adapter.save_risk_config(config)

    asyncio.run(TimescaleAdapter.flush_event_buffers())

    fresh = TimescaleAdapter(account_id=account)
    events = fresh.events()
    assert any(entry["order_id"] == "ACK-1" for entry in events["acks"])
    assert any(entry["order_id"] == "FILL-1" for entry in events["fills"])
    assert any(event["event_type"] == "risk.signal" for event in events["events"])

    audit_logs = fresh.audit_logs()
    assert any(log["action"] == "test" for log in audit_logs)

    telemetry = fresh.telemetry()
    assert any(entry["order_id"] == "DEC-1" for entry in telemetry)

    credential_events = fresh.credential_events()
    assert any(event["event"] == "rotation" for event in credential_events)
    assert any(event["event"] == "access" for event in credential_events)

    risk_config = fresh.load_risk_config()
    assert risk_config["nav"] == 2_500_000.0
    assert risk_config["kill_switch"] is True

    status = fresh.credential_rotation_status()
    assert status is not None and status["secret_name"] == "kraken"

    TimescaleAdapter.reset(account_id=account)
