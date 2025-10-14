"""Regression tests for the behaviour service Timescale fallback."""

from __future__ import annotations

import importlib
import sys
import types
from datetime import datetime, timezone

import pytest


@pytest.fixture()
def _reload_behavior_service(monkeypatch: pytest.MonkeyPatch):
    module_name = "behavior_service"
    sys.modules.pop(module_name, None)

    adapters_stub = types.ModuleType("services.common.adapters")
    monkeypatch.setitem(sys.modules, "services.common.adapters", adapters_stub)

    module = importlib.import_module(module_name)
    try:
        yield module
    finally:
        sys.modules.pop(module_name, None)


def test_timescale_fallback_exposes_expected_api(_reload_behavior_service):
    module = _reload_behavior_service

    assert module.TimescaleAdapter is module._FallbackTimescaleAdapter

    adapter = module.TimescaleAdapter(account_id="  Company  ")
    assert adapter.events() == {"events": [], "acks": []}
    assert adapter.telemetry() == []

    ack_payload = {"recorded_at": datetime(2024, 1, 1, tzinfo=timezone.utc)}
    module.TimescaleAdapter.record_ack("company", ack_payload)
    module.TimescaleAdapter.record_event(
        "Company", "events", {"timestamp": datetime(2024, 1, 2, tzinfo=timezone.utc)}
    )
    module.TimescaleAdapter.record_telemetry(
        "COMPANY",
        {"timestamp": datetime(2024, 1, 3, tzinfo=timezone.utc), "payload": {"mode": "auto"}},
    )

    bucket = module.TimescaleAdapter(account_id="company").events()
    assert bucket["acks"] == [ack_payload]
    assert bucket["events"][0]["timestamp"].isoformat() == "2024-01-02T00:00:00+00:00"

    telemetry = module.TimescaleAdapter(account_id=" company ").telemetry()
    assert telemetry == [
        {"timestamp": datetime(2024, 1, 3, tzinfo=timezone.utc), "payload": {"mode": "auto"}}
    ]

    module.TimescaleAdapter.reset(" company ")
    assert module.TimescaleAdapter(account_id="company").events() == {"events": [], "acks": []}
    assert module.TimescaleAdapter(account_id="company").telemetry() == []


def test_timescale_fallback_global_reset(_reload_behavior_service):
    module = _reload_behavior_service

    module.TimescaleAdapter.record_event("account", "acks", {})
    module.TimescaleAdapter.record_telemetry("account", {})

    module.TimescaleAdapter.reset()
    assert module.TimescaleAdapter(account_id="account").events() == {"events": [], "acks": []}
    assert module.TimescaleAdapter(account_id="account").telemetry() == []
