"""Regression tests that ensure governance APIs emit audit events."""

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Dict, List

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

import safe_mode
from shared.audit_hooks import AuditHooks, temporary_audit_hooks
from tests.fixtures.backends import MemoryRedis
from tests.helpers.override_service import bootstrap_override_service


@contextmanager
def capture_audit_events() -> List[Dict[str, object]]:  # type: ignore[override]
    """Capture audit events recorded via :mod:`shared.audit_hooks`."""

    captured: List[Dict[str, object]] = []

    def _hash_ip(value: str) -> str:
        return f"hash:{value}"

    def _log_event(
        *,
        actor: str,
        action: str,
        entity: str,
        before: Dict[str, object],
        after: Dict[str, object],
        ip_hash: str | None,
    ) -> None:
        captured.append(
            {
                "actor": actor,
                "action": action,
                "entity": entity,
                "before": dict(before),
                "after": dict(after),
                "ip_hash": ip_hash,
            }
        )

    hooks = AuditHooks(log=_log_event, hash_ip=_hash_ip)
    with temporary_audit_hooks(hooks):
        yield captured


@pytest.fixture
def safe_mode_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Provide a configured Safe Mode client with isolated state."""

    backend = MemoryRedis()
    controller = safe_mode.SafeModeController(
        state_store=safe_mode.SafeModeStateStore(redis_client=backend)
    )
    original_controller = safe_mode.controller
    monkeypatch.setattr(safe_mode, "controller", controller)
    safe_mode.clear_safe_mode_log()
    client = TestClient(safe_mode.app)
    try:
        yield client
    finally:
        controller.reset()
        safe_mode.clear_safe_mode_log()
        monkeypatch.setattr(safe_mode, "controller", original_controller)


@pytest.fixture
def kill_switch_module(monkeypatch: pytest.MonkeyPatch):
    """Return the kill-switch module with broker and database shims applied."""

    import kill_switch as module

    class _StubTimescale:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id
            self.kill_switch_calls: List[Dict[str, object]] = []
            self.recorded: List[Dict[str, object]] = []

        def set_kill_switch(self, **kwargs: object) -> None:
            self.kill_switch_calls.append(dict(kwargs))

        def record_kill_event(self, **kwargs: object) -> None:
            self.recorded.append(dict(kwargs))

    class _StubKafka:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id
            self.published: List[Dict[str, object]] = []

        async def publish(self, topic: str, payload: Dict[str, object]) -> None:
            self.published.append({"topic": topic, "payload": dict(payload)})

    def _timescale_factory(account_id: str) -> _StubTimescale:
        instance = _StubTimescale(account_id)
        return instance

    monkeypatch.setattr(module, "TimescaleAdapter", _timescale_factory)
    monkeypatch.setattr(module, "KafkaNATSAdapter", _StubKafka)
    monkeypatch.setattr(module, "dispatch_notifications", lambda **_: ["email"])

    def _dispatch_async(coro, *, context: str, logger) -> None:  # type: ignore[no-untyped-def]
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(module, "dispatch_async", _dispatch_async)
    monkeypatch.setattr(module, "require_admin_account", lambda request=None: "company")

    return module


def test_safe_mode_entry_emits_audit_event(safe_mode_client: TestClient) -> None:
    """Safe mode entries must record audit metadata for governance review."""

    with capture_audit_events() as events:
        response = safe_mode_client.post(
            "/safe_mode/enter",
            json={"reason": "volatility drill"},
            headers={"X-Account-ID": "company"},
        )

    assert response.status_code == 200
    assert events, "expected safe mode entry to produce an audit event"
    audit = events[-1]
    assert audit["action"] == "safe_mode.enter"
    assert audit["entity"] == "safe_mode"
    assert audit["actor"] == "company"
    assert audit["after"]["reason"] == "volatility drill"


def test_kill_switch_trigger_emits_audit_event(kill_switch_module) -> None:
    """Kill-switch activations should be persisted via the audit hooks."""

    request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))

    with capture_audit_events() as events:
        response = kill_switch_module.trigger_kill_switch(
            request=request,
            account_id="company",
            reason_code=kill_switch_module.KillSwitchReason.LATENCY_STALL,
            actor_account="company",
        )

    assert response["status"] == "ok"
    assert events, "expected kill switch activation to produce an audit event"
    audit = events[-1]
    assert audit["action"] == "kill_switch.triggered"
    assert audit["entity"] == "company"
    assert audit["after"]["reason_code"] == "latency_stall"


def test_override_decision_emits_audit_event(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Manual override decisions must generate audit entries."""

    module = bootstrap_override_service(
        tmp_path_factory.mktemp("override"), monkeypatch
    )
    original_model_validate = module.OverrideRecord.model_validate.__func__

    def _model_validate(cls, data):  # type: ignore[no-untyped-def]
        if not isinstance(data, dict):
            data = {
                "id": getattr(data, "id", 0),
                "intent_id": getattr(data, "intent_id"),
                "account_id": getattr(data, "account_id"),
                "actor": getattr(data, "actor"),
                "decision": module.OverrideDecision(getattr(data, "decision")),
                "reason": getattr(data, "reason"),
                "ts": getattr(data, "ts"),
            }
        return original_model_validate(cls, data)

    monkeypatch.setattr(module.OverrideRecord, "model_validate", classmethod(_model_validate))
    request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))
    payload = module.OverrideRequest(
        intent_id="INT-42",
        decision=module.OverrideDecision.APPROVE,
        reason="manual review",
    )

    class _StubSession:
        def __init__(self) -> None:
            self.entries: list[object] = []

        def add(self, entry: object) -> None:
            if getattr(entry, "id", None) is None:
                setattr(entry, "id", len(self.entries) + 1)
            self.entries.append(entry)

        def flush(self) -> None:
            return None

        def refresh(self, entry: object) -> None:
            return None

        def close(self) -> None:
            return None

    session = _StubSession()
    with capture_audit_events() as events:
        record = module.record_override(
            payload=payload,
            request=request,
            session=session,
            admin_account="company",
            account_id="company",
        )

    assert record.decision == module.OverrideDecision.APPROVE
    assert events, "expected override decision to produce an audit event"
    audit = events[-1]
    assert audit["action"] == "override.human_decision"
    assert audit["entity"] == "INT-42"
    assert audit["after"]["decision"] == "approve"
