import builtins
import hashlib
import logging

from typing import Any, Iterator, List, Mapping, Optional

import pytest

from shared import audit_hooks


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("   ", None),
        ("127.0.0.1", hashlib.sha256("127.0.0.1".encode("utf-8")).hexdigest()),
    ],
)
def test_hash_ip_fallback_produces_stable_hash(value, expected):
    assert audit_hooks._hash_ip_fallback(value) == expected


def test_load_audit_hooks_degrades_when_dependency_missing(monkeypatch):
    audit_hooks.reset_audit_hooks_cache()
    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "common.utils.audit_logger":
            raise ModuleNotFoundError("audit logger unavailable")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    hooks = audit_hooks.load_audit_hooks()

    assert hooks.log is None
    assert hooks.hash_ip(None) is None
    assert hooks.hash_ip(" ") is None
    assert hooks.hash_ip("127.0.0.1") == hashlib.sha256("127.0.0.1".encode("utf-8")).hexdigest()

    audit_hooks.reset_audit_hooks_cache()


def test_load_audit_hooks_caches_optional_import(monkeypatch):
    audit_hooks.reset_audit_hooks_cache()
    original_import = builtins.__import__
    import_counter = {"calls": 0}

    def tracking_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "common.utils.audit_logger":
            import_counter["calls"] += 1
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", tracking_import)

    first = audit_hooks.load_audit_hooks()
    second = audit_hooks.load_audit_hooks()

    assert first is second
    assert import_counter["calls"] == 1

    audit_hooks.reset_audit_hooks_cache()


def test_log_event_skips_when_logger_missing():
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: pytest.fail("hash_ip should not be called"))

    result = hooks.log_event(
        actor="alice",
        action="test.action",
        entity="resource",
        before={},
        after={},
        ip_address="127.0.0.1",
    )

    assert result.handled is False
    assert result.ip_hash is None
    assert result.hash_fallback is False
    assert result.hash_error is None
    assert result.context_evaluated is False
    assert result.fallback_logged is False
    assert_no_fallback_extra(result)
    assert not result


def test_log_event_records_with_ip_hash():
    calls = []

    def fake_log(**payload):
        calls.append(payload)

    hooks = audit_hooks.AuditHooks(log=fake_log, hash_ip=lambda value: f"hashed:{value}")

    result = hooks.log_event(
        actor="bob",
        action="demo.action",
        entity="resource",
        before={"before": True},
        after={"after": True},
        ip_address="10.0.0.5",
    )

    assert result.handled is True
    assert result.ip_hash == "hashed:10.0.0.5"
    assert result.hash_fallback is False
    assert result.hash_error is None
    assert result.log_error is None
    assert result.context_evaluated is False
    assert result.fallback_logged is False
    assert_no_fallback_extra(result)
    assert result
    assert calls == [
        {
            "actor": "bob",
            "action": "demo.action",
            "entity": "resource",
            "before": {"before": True},
            "after": {"after": True},
            "ip_hash": "hashed:10.0.0.5",
        }
    ]


def test_log_event_respects_explicit_ip_hash():
    captured = {}

    def fake_log(**payload):
        captured.update(payload)

    hooks = audit_hooks.AuditHooks(log=fake_log, hash_ip=lambda value: "should-not-be-used")

    result = hooks.log_event(
        actor="carol",
        action="demo.override",
        entity="resource",
        before={},
        after={},
        ip_hash="explicit-hash",
    )

    assert result.handled is True
    assert captured["ip_hash"] == "explicit-hash"
    assert result.ip_hash == "explicit-hash"
    assert result.hash_fallback is False
    assert result.hash_error is None
    assert result.context_evaluated is False
    assert result.fallback_logged is False
    assert_no_fallback_extra(result)


def test_audit_log_result_truthiness():
    success = audit_hooks.AuditLogResult(
        handled=True,
        ip_hash="hash",
        hash_fallback=False,
        hash_error=None,
        fallback_logged=False,
    )
    failure = audit_hooks.AuditLogResult(
        handled=False,
        ip_hash=None,
        hash_fallback=False,
        hash_error=None,
        log_error=None,
        fallback_logged=False,
    )

    assert success
    assert not failure


def test_log_event_with_fallback_logs_when_disabled(caplog: pytest.LogCaptureFixture):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: "hash")

    logger = logging.getLogger("test.audit.disabled")
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="dave",
            action="demo.disabled",
            entity="resource",
            before={},
            after={},
            failure_message="should not trigger",
            disabled_message="audit disabled",
        )

    assert result.handled is False
    assert result.ip_hash is None
    assert result.hash_fallback is False
    assert result.hash_error is None
    assert result.log_error is None
    assert result.context is None
    assert result.context_evaluated is True
    assert result.fallback_logged is True
    assert result.fallback_extra == {
        "audit": {
            "actor": "dave",
            "action": "demo.disabled",
            "entity": "resource",
            "before": {},
            "after": {},
            "ip_hash": None,
            "ip_address": None,
            "hash_fallback": False,
        }
    }
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    assert not result
    record = next(record for record in caplog.records if record.message == "audit disabled")
    assert record.audit == {
        "actor": "dave",
        "action": "demo.disabled",
        "entity": "resource",
        "before": {},
        "after": {},
        "ip_address": None,
        "ip_hash": None,
        "hash_fallback": False,
    }


def test_log_event_with_fallback_context_factory_skips_when_unused():
    hooks = audit_hooks.AuditHooks(
        log=lambda **payload: None,
        hash_ip=lambda value: pytest.fail("hash_ip should not be called"),
    )
    logger = logging.getLogger("test.audit.context_factory.success")
    factory_calls: List[int] = []

    def context_factory() -> Mapping[str, str]:
        factory_calls.append(1)
        return {"source": "factory"}

    result = audit_hooks.log_event_with_fallback(
        hooks,
        logger,
        actor="erin",
        action="demo.factory.success",
        entity="resource",
        before={},
        after={},
        failure_message="should not trigger",
        context_factory=context_factory,
    )

    assert result.handled is True
    assert result.context is None
    assert result.context_evaluated is False
    assert factory_calls == []
    assert result.fallback_logged is False
    assert_no_fallback_extra(result)


def test_log_event_with_fallback_context_factory_failure_records_metadata(
    caplog: pytest.LogCaptureFixture,
):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: "hash")

    logger = logging.getLogger("test.audit.context_factory.failure")
    factory_calls: List[str] = []

    def failing_factory() -> Mapping[str, Any]:
        factory_calls.append("called")
        raise RuntimeError("context boom")

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="erin",
            action="demo.context.failure",
            entity="resource",
            before={},
            after={},
            failure_message="should not trigger",
            disabled_message="audit disabled",
            disabled_level=logging.INFO,
            context_factory=failing_factory,
        )

    assert factory_calls == ["called"]
    assert result.handled is False
    assert result.context is None
    assert isinstance(result.context_error, RuntimeError)
    assert str(result.context_error) == "context boom"
    assert result.context_evaluated is True
    assert result.fallback_logged is True
    assert result.fallback_extra is not None
    assert result.fallback_extra["audit"]["actor"] == "erin"
    assert result.fallback_extra.get("audit_context_error") == {
        "type": "RuntimeError",
        "message": "context boom",
    }
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True

    context_error_records = [
        record
        for record in caplog.records
        if record.message == "Audit fallback context factory raised; omitting context metadata."
    ]
    assert context_error_records
    error_record = context_error_records[0]
    assert getattr(error_record, "audit_context_error") == {
        "type": "RuntimeError",
        "message": "context boom",
    }

    disabled_record = next(record for record in caplog.records if record.message == "audit disabled")
    assert getattr(disabled_record, "audit_context_error") == {
        "type": "RuntimeError",
        "message": "context boom",
    }


def test_log_event_with_fallback_context_factory_used_for_fallback(
    caplog: pytest.LogCaptureFixture,
):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: value)
    logger = logging.getLogger("test.audit.context_factory.fallback")
    factory_calls: List[int] = []

    def context_factory() -> Mapping[str, str]:
        factory_calls.append(1)
        return {"audit": {"source": "factory"}}

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="frank",
            action="demo.factory.disabled",
            entity="resource",
            before={},
            after={},
            failure_message="should not trigger",
            disabled_message="audit disabled via context factory",
            context_factory=context_factory,
        )

    assert result.handled is False
    assert factory_calls == [1]
    assert result.context == {"audit": {"source": "factory"}}
    assert result.context_evaluated is True
    assert result.fallback_logged is True
    assert result.fallback_extra == {"audit": {"source": "factory"}}
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    record = next(
        entry
        for entry in caplog.records
        if entry.message == "audit disabled via context factory"
    )
    assert record.audit == {"source": "factory"}


def test_log_audit_event_with_fallback_event_context_factory_skips_when_unused():
    hooks = audit_hooks.AuditHooks(
        log=lambda **payload: None,
        hash_ip=lambda value: pytest.fail("hash_ip should not be called"),
    )
    logger = logging.getLogger("test.audit.event_factory.success")
    factory_calls: List[int] = []

    def context_factory() -> Mapping[str, str]:
        factory_calls.append(1)
        return {"audit": {"source": "event"}}

    event = audit_hooks.AuditEvent(
        actor="gail",
        action="demo.event.success",
        entity="resource",
        before={},
        after={},
        context_factory=context_factory,
    )

    result = audit_hooks.log_audit_event_with_fallback(
        hooks,
        logger,
        event,
        failure_message="should not trigger",
        disabled_message="should not log",
    )

    assert result.handled is True
    assert result.context is None
    assert result.context_evaluated is False
    assert factory_calls == []
    assert result.fallback_logged is False
    assert_no_fallback_extra(result)


def test_log_audit_event_with_fallback_event_context_factory_used_for_fallback(
    caplog: pytest.LogCaptureFixture,
):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: value)
    logger = logging.getLogger("test.audit.event_factory.fallback")
    factory_calls: List[int] = []

    def context_factory() -> Mapping[str, str]:
        factory_calls.append(1)
        return {"audit": {"source": "event"}}

    event = audit_hooks.AuditEvent(
        actor="hank",
        action="demo.event.disabled",
        entity="resource",
        before={},
        after={},
        context_factory=context_factory,
    )

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = audit_hooks.log_audit_event_with_fallback(
            hooks,
            logger,
            event,
            failure_message="should not trigger",
            disabled_message="audit disabled via event context factory",
        )

    assert result.handled is False
    assert result.context == {"audit": {"source": "event"}}
    assert factory_calls == [1]
    assert result.context_evaluated is True
    assert result.fallback_logged is True
    assert result.fallback_extra == {"audit": {"source": "event"}}
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    record = next(
        entry
        for entry in caplog.records
        if entry.message == "audit disabled via event context factory"
    )
    assert record.audit == {"source": "event"}


def test_log_audit_event_with_fallback_reuses_resolved_context(
    caplog: pytest.LogCaptureFixture,
):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: value)
    logger = logging.getLogger("test.audit.event_factory.resolved")
    calls: List[int] = []

    def context_factory() -> Mapping[str, str]:
        calls.append(1)
        return {"audit": {"source": f"call-{len(calls)}"}}

    event = audit_hooks.AuditEvent(
        actor="iona",
        action="demo.event.resolved",
        entity="resource",
        before={},
        after={},
        context_factory=context_factory,
    )

    updated_event, resolved_context = event.resolve_context_metadata()
    assert calls == [1]

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = audit_hooks.log_audit_event_with_fallback(
            hooks,
            logger,
            updated_event,
            failure_message="should not trigger",
            disabled_message="audit disabled via resolved context",
            resolved_context=resolved_context,
        )

    assert result.handled is False
    assert result.context == {"audit": {"source": "call-1"}}
    assert result.context_error is None
    assert result.context_evaluated is True
    assert calls == [1]
    assert result.fallback_logged is True
    assert result.fallback_extra == {"audit": {"source": "call-1"}}
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    record = next(
        entry
        for entry in caplog.records
        if entry.message == "audit disabled via resolved context"
    )
    assert record.audit == {"source": "call-1"}


def test_log_audit_event_with_fallback_resolved_context_error_logged_once(
    caplog: pytest.LogCaptureFixture,
):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: value)
    logger = logging.getLogger("test.audit.event_factory.resolved_error")
    calls: List[int] = []

    def failing_factory() -> Mapping[str, str]:
        calls.append(1)
        raise RuntimeError("context failure")

    event = audit_hooks.AuditEvent(
        actor="jules",
        action="demo.event.resolved_error",
        entity="resource",
        before={},
        after={},
        context_factory=failing_factory,
    )

    updated_event, resolved_context = event.resolve_context_metadata(drop_factory=True)
    assert calls == [1]
    assert isinstance(resolved_context.error, RuntimeError)

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = audit_hooks.log_audit_event_with_fallback(
            hooks,
            logger,
            updated_event,
            failure_message="should not trigger",
            disabled_message="audit disabled via resolved context error",
            resolved_context=resolved_context,
        )

    assert result.handled is False
    assert result.context is None
    assert result.context_error is resolved_context.error
    assert result.fallback_logged is True
    assert result.fallback_extra is not None
    assert result.fallback_extra.get("audit_context_error") == {
        "type": "RuntimeError",
        "message": "context failure",
    }
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    failure_records = [
        entry
        for entry in caplog.records
        if entry.message == "Audit fallback context factory raised; omitting context metadata."
    ]
    assert len(failure_records) == 1
    disabled_record = next(
        entry
        for entry in caplog.records
        if entry.message == "audit disabled via resolved context error"
    )
    assert getattr(disabled_record, "audit_context_error") == {
        "type": "RuntimeError",
        "message": "context failure",
    }


def test_log_event_with_fallback_handles_exceptions(caplog: pytest.LogCaptureFixture):
    def failing_log(**payload):
        raise RuntimeError("boom")

    hooks = audit_hooks.AuditHooks(log=failing_log, hash_ip=lambda value: "hash")
    logger = logging.getLogger("test.audit.errors")

    with caplog.at_level(logging.ERROR, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="erin",
            action="demo.error",
            entity="resource",
            before={},
            after={},
            failure_message="failed to record",
        )

    assert result.handled is False
    assert isinstance(result.log_error, RuntimeError)
    assert result.hash_fallback is False
    assert result.hash_error is None
    assert result.context is None
    assert result.fallback_logged is True
    assert result.fallback_extra == {
        "audit": {
            "actor": "erin",
            "action": "demo.error",
            "entity": "resource",
            "before": {},
            "after": {},
            "ip_hash": None,
            "ip_address": None,
            "hash_fallback": False,
        }
    }
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    error_record = next(
        record
        for record in caplog.records
        if record.levelno == logging.ERROR and "failed to record" in record.message
    )
    assert error_record.audit == {
        "actor": "erin",
        "action": "demo.error",
        "entity": "resource",
        "before": {},
        "after": {},
        "ip_address": None,
        "ip_hash": None,
        "hash_fallback": False,
    }


def test_log_event_with_fallback_uses_fallback_hash_on_error(caplog: pytest.LogCaptureFixture):
    calls = []

    def fake_log(**payload):
        calls.append(payload)

    def failing_hash(value: Optional[str]) -> Optional[str]:
        raise RuntimeError("hash failure")

    hooks = audit_hooks.AuditHooks(log=fake_log, hash_ip=failing_hash)
    logger = logging.getLogger("test.audit.hash")

    with caplog.at_level(logging.ERROR, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="ivy",
            action="demo.hash",
            entity="resource",
            before={"before": True},
            after={"after": True},
            ip_address="192.168.0.10",
            failure_message="should not trigger",
        )

    expected_hash = audit_hooks._hash_ip_fallback("192.168.0.10")
    assert result.handled is True
    assert result.hash_fallback is True
    assert isinstance(result.hash_error, RuntimeError)
    assert result.log_error is None
    assert result.context is None
    assert result.fallback_logged is True
    assert result.fallback_extra == {
        "audit": {
            "actor": "ivy",
            "action": "demo.hash",
            "entity": "resource",
            "before": {"before": True},
            "after": {"after": True},
            "ip_hash": expected_hash,
            "ip_address": "192.168.0.10",
            "hash_fallback": True,
            "hash_error": {"type": "RuntimeError", "message": "hash failure"},
        }
    }
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    assert calls == [
        {
            "actor": "ivy",
            "action": "demo.hash",
            "entity": "resource",
            "before": {"before": True},
            "after": {"after": True},
            "ip_hash": expected_hash,
        }
    ]

    error_record = next(
        record for record in caplog.records if record.message == "Audit hash_ip callable failed; using fallback hash."
    )
    assert error_record.audit == {
        "actor": "ivy",
        "action": "demo.hash",
        "entity": "resource",
        "before": {"before": True},
        "after": {"after": True},
        "ip_address": "192.168.0.10",
        "ip_hash": expected_hash,
        "hash_fallback": True,
        "hash_error": {"type": "RuntimeError", "message": "hash failure"},
    }


def test_log_event_with_fallback_reuses_resolved_hash(caplog: pytest.LogCaptureFixture):
    hash_calls = {"count": 0}

    def tracking_hash(value: Optional[str]) -> Optional[str]:
        hash_calls["count"] += 1
        return f"hash:{value}"

    captured = []

    def fake_log(**payload):
        captured.append(payload)

    hooks = audit_hooks.AuditHooks(log=fake_log, hash_ip=tracking_hash)
    logger = logging.getLogger("test.audit.single_hash")

    with caplog.at_level(logging.ERROR, logger="shared.audit_hooks"):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="kelly",
            action="demo.single",
            entity="resource",
            before={"before": True},
            after={"after": True},
            ip_address="203.0.113.5",
            failure_message="should not trigger",
        )

    assert result.handled is True
    assert result.ip_hash == "hash:203.0.113.5"
    assert hash_calls["count"] == 1
    assert result.context is None
    assert result.fallback_logged is False
    assert_no_fallback_extra(result)
    assert captured == [
        {
            "actor": "kelly",
            "action": "demo.single",
            "entity": "resource",
            "before": {"before": True},
            "after": {"after": True},
            "ip_hash": "hash:203.0.113.5",
        }
    ]
    assert not [record for record in caplog.records if record.name == "shared.audit_hooks"]


def test_log_event_with_fallback_accepts_pre_resolved_hash(caplog: pytest.LogCaptureFixture):
    hash_calls = {"count": 0}

    def tracking_hash(value: Optional[str]) -> Optional[str]:
        hash_calls["count"] += 1
        return f"hash:{value}"

    captured = []

    def fake_log(**payload):
        captured.append(payload)

    hooks = audit_hooks.AuditHooks(log=fake_log, hash_ip=tracking_hash)

    resolved = hooks.resolve_ip_hash(ip_address="198.51.100.10", ip_hash=None)

    assert hash_calls["count"] == 1

    logger = logging.getLogger("test.audit.pre_resolved")

    with caplog.at_level(logging.INFO, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="lisa",
            action="demo.pre_resolved",
            entity="resource",
            before={"before": True},
            after={"after": True},
            ip_address="198.51.100.10",
            failure_message="should not trigger",
            resolved_ip_hash=resolved,
        )

    assert result.handled is True
    assert result.ip_hash == "hash:198.51.100.10"
    assert hash_calls["count"] == 1
    assert result.context is None
    assert result.fallback_logged is False
    assert_no_fallback_extra(result)
    assert captured == [
        {
            "actor": "lisa",
            "action": "demo.pre_resolved",
            "entity": "resource",
            "before": {"before": True},
            "after": {"after": True},
            "ip_hash": "hash:198.51.100.10",
        }
    ]
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


def test_log_event_falls_back_when_hash_raises(caplog: pytest.LogCaptureFixture):
    captured = {}

    def fake_log(**payload):
        captured.update(payload)

    def failing_hash(value: Optional[str]) -> Optional[str]:
        raise RuntimeError("hash failure")

    hooks = audit_hooks.AuditHooks(log=fake_log, hash_ip=failing_hash)

    with caplog.at_level(logging.ERROR, logger="shared.audit_hooks"):
        result = hooks.log_event(
            actor="jane",
            action="demo.log",
            entity="resource",
            before={"state": "before"},
            after={"state": "after"},
            ip_address="172.16.0.2",
        )

    assert result.handled is True
    expected_hash = audit_hooks._hash_ip_fallback("172.16.0.2")
    assert captured["ip_hash"] == expected_hash
    assert result.ip_hash == expected_hash
    assert result.hash_fallback is True
    assert isinstance(result.hash_error, RuntimeError)
    module_record = next(
        record for record in caplog.records if record.message == "Audit hash_ip callable failed; using fallback hash."
    )
    assert module_record.levelno == logging.ERROR
    assert module_record.audit["hash_error"] == {"type": "RuntimeError", "message": "hash failure"}
    assert result.fallback_logged is True
    assert result.fallback_extra == {
        "audit": {
            "actor": "jane",
            "action": "demo.log",
            "entity": "resource",
            "before": {"state": "before"},
            "after": {"state": "after"},
            "ip_hash": expected_hash,
            "ip_address": "172.16.0.2",
            "hash_fallback": True,
            "hash_error": {"type": "RuntimeError", "message": "hash failure"},
        }
    }
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True


def test_log_event_with_fallback_allows_custom_context(caplog: pytest.LogCaptureFixture):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: "hashed")
    logger = logging.getLogger("test.audit.context")

    context = {
        "audit": {"actor": "frank", "action": "demo.context"},
        "extra_field": "value",
    }

    with caplog.at_level(logging.INFO, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="frank",
            action="demo.context",
            entity="resource",
            before={"before": True},
            after={"after": True},
            ip_address="10.1.2.3",
            failure_message="should not trigger",
            disabled_message="audit disabled",
            disabled_level=logging.INFO,
            context=context,
        )

    assert result.handled is False
    assert result.hash_fallback is False
    assert result.hash_error is None
    assert result.log_error is None
    record = next(record for record in caplog.records if record.message == "audit disabled")
    assert record.audit == context["audit"]
    assert record.extra_field == "value"
    assert result.fallback_logged is True
    assert result.fallback_extra == {
        "audit": {"actor": "frank", "action": "demo.context"},
        "extra_field": "value",
    }
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True


class _TrackingMapping(Mapping[str, object]):
    """Mapping that records whether it has been iterated."""

    def __init__(self) -> None:
        self.accessed = False

    def __getitem__(self, key: str) -> object:
        self.accessed = True
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        self.accessed = True
        return iter(())

    def __len__(self) -> int:
        self.accessed = True
        return 0


def test_log_event_with_fallback_defers_extra_until_needed(caplog: pytest.LogCaptureFixture):
    captured: list[Mapping[str, object]] = []

    def fake_log(**payload: object) -> None:
        captured.append(payload)

    hooks = audit_hooks.AuditHooks(log=fake_log, hash_ip=lambda value: value)
    logger = logging.getLogger("test.audit.defer")
    context = _TrackingMapping()

    with caplog.at_level(logging.INFO, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="casey",
            action="demo.defer",
            entity="resource",
            before={"before": True},
            after={"after": True},
            ip_address="203.0.113.5",
            failure_message="should not trigger",
            context=context,
        )

    assert result.handled is True
    assert result.ip_hash == "203.0.113.5"
    assert result.context is context
    assert captured == [
        {
            "actor": "casey",
            "action": "demo.defer",
            "entity": "resource",
            "before": {"before": True},
            "after": {"after": True},
            "ip_hash": "203.0.113.5",
        }
    ]
    assert context.accessed is False
    assert not caplog.records
    assert result.fallback_logged is False
    assert_no_fallback_extra(result)


def test_log_event_with_fallback_fallback_extra_factory_skips_when_unused():
    hooks = audit_hooks.AuditHooks(log=lambda **payload: None, hash_ip=lambda value: value)
    logger = logging.getLogger("test.audit.fallback_extra.success")
    factory_calls: List[int] = []

    def extra_factory() -> Mapping[str, str]:
        factory_calls.append(1)
        return {"request_id": "abc"}

    result = audit_hooks.log_event_with_fallback(
        hooks,
        logger,
        actor="nina",
        action="demo.extra.success",
        entity="resource",
        before={},
        after={},
        failure_message="should not trigger",
        fallback_extra_factory=extra_factory,
    )

    assert result.handled is True
    assert factory_calls == []
    assert_no_fallback_extra(result)


def test_log_event_with_fallback_fallback_extra_factory_used_for_fallback(
    caplog: pytest.LogCaptureFixture,
):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: value)
    logger = logging.getLogger("test.audit.fallback_extra.fallback")
    factory_calls: List[str] = []

    def extra_factory() -> Mapping[str, str]:
        factory_calls.append("called")
        return {"request_id": "req-789"}

    with caplog.at_level(logging.INFO, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="oliver",
            action="demo.extra.disabled",
            entity="resource",
            before={},
            after={},
            failure_message="should not trigger",
            disabled_message="audit disabled",
            disabled_level=logging.INFO,
            fallback_extra_factory=extra_factory,
        )

    assert factory_calls == ["called"]
    assert result.handled is False
    assert result.fallback_logged is True
    assert result.fallback_extra is not None
    assert result.fallback_extra["request_id"] == "req-789"
    assert result.fallback_extra["audit"]["actor"] == "oliver"
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    disabled_record = next(record for record in caplog.records if record.message == "audit disabled")
    assert disabled_record.request_id == "req-789"


def test_log_event_with_fallback_fallback_extra_factory_failure(
    caplog: pytest.LogCaptureFixture,
):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: value)
    logger = logging.getLogger("test.audit.fallback_extra.failure")
    factory_calls: List[str] = []

    def failing_factory() -> Mapping[str, str]:
        factory_calls.append("called")
        raise RuntimeError("fallback extra boom")

    with caplog.at_level(logging.ERROR, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="piper",
            action="demo.extra.failure",
            entity="resource",
            before={},
            after={},
            failure_message="should not trigger",
            disabled_message="audit disabled",
            fallback_extra_factory=failing_factory,
        )

    assert factory_calls == ["called"]
    assert result.handled is False
    assert result.fallback_logged is True
    assert result.fallback_extra is not None
    assert "audit_fallback_extra_error" in result.fallback_extra
    assert isinstance(result.fallback_extra_error, RuntimeError)
    assert str(result.fallback_extra_error) == "fallback extra boom"
    assert result.fallback_extra_evaluated is True
    failure_record = next(
        record
        for record in caplog.records
        if record.message == "Audit fallback extra factory raised; omitting custom metadata."
    )
    assert getattr(failure_record, "audit_fallback_extra_error") == {
        "type": "RuntimeError",
        "message": "fallback extra boom",
    }


def test_log_event_with_fallback_uses_resolved_fallback_extra(
    caplog: pytest.LogCaptureFixture,
):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: value)
    logger = logging.getLogger("test.audit.fallback_extra.resolved")
    factory_calls: list[str] = []

    def extra_factory() -> Mapping[str, str]:
        factory_calls.append("called")
        return {"request_id": "req-999"}

    event = audit_hooks.AuditEvent(
        actor="quincy",
        action="demo.extra.resolved",
        entity="resource",
        before={},
        after={},
        fallback_extra_factory=extra_factory,
    )

    _, resolved_extra = event.resolve_fallback_extra_metadata()
    assert factory_calls == ["called"]

    with caplog.at_level(logging.INFO, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="quincy",
            action="demo.extra.resolved",
            entity="resource",
            before={},
            after={},
            failure_message="should not trigger",
            disabled_message="audit disabled",
            disabled_level=logging.INFO,
            fallback_extra_factory=extra_factory,
            resolved_fallback_extra=resolved_extra,
        )

    assert factory_calls == ["called"]
    assert result.handled is False
    assert result.fallback_logged is True
    assert result.fallback_extra is not None
    assert result.fallback_extra["request_id"] == "req-999"
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    disabled_record = next(record for record in caplog.records if record.message == "audit disabled")
    assert disabled_record.request_id == "req-999"

def test_log_audit_event_with_fallback_logs_event_payload():
    captured: dict[str, object] = {}

    def fake_log(**payload: object) -> None:
        captured.update(payload)  # type: ignore[arg-type]

    hooks = audit_hooks.AuditHooks(log=fake_log, hash_ip=lambda value: f"hash:{value}")
    event = audit_hooks.AuditEvent(
        actor="grace",
        action="demo.action",
        entity="resource",
        before={"before": True},
        after={"after": True},
        ip_address="10.1.2.3",
    )

    result = audit_hooks.log_audit_event_with_fallback(
        hooks,
        logging.getLogger("tests.audit"),
        event,
        failure_message="should not trigger",
        disabled_message="should not log",
    )

    assert result.handled is True
    assert result.ip_hash == "hash:10.1.2.3"
    assert captured == {
        "actor": "grace",
        "action": "demo.action",
        "entity": "resource",
        "before": {"before": True},
        "after": {"after": True},
        "ip_hash": "hash:10.1.2.3",
    }
    assert result.fallback_logged is False
    assert_no_fallback_extra(result)


def test_log_audit_event_with_fallback_uses_event_context(caplog: pytest.LogCaptureFixture):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: value)
    event = audit_hooks.AuditEvent(
        actor="ivy",
        action="demo.context",
        entity="resource",
        before={},
        after={},
        context={"request_id": "req-123"},
    )
    logger = logging.getLogger("tests.audit.event_context")

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = audit_hooks.log_audit_event_with_fallback(
            hooks,
            logger,
            event,
            failure_message="should not raise",
            disabled_message="Audit logging disabled",
        )

    assert result.handled is False
    assert result.context == {"request_id": "req-123"}
    assert result.context is not event.context
    assert result.context_evaluated is True
    assert len(caplog.records) == 1
    assert result.fallback_logged is True
    assert result.fallback_extra == {"request_id": "req-123"}
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    record = caplog.records[0]
    assert record.message == "Audit logging disabled"
    assert getattr(record, "request_id", None) == "req-123"


def test_log_event_with_fallback_includes_custom_extra(caplog: pytest.LogCaptureFixture):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: value)
    logger = logging.getLogger("tests.audit.custom_extra")
    fallback_extra: Mapping[str, Any] = {
        "request_id": "req-456",
        "audit": {"flow": "demo"},
    }

    with caplog.at_level(logging.INFO, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="lea",
            action="demo.custom",
            entity="resource",
            before={"before": True},
            after={"after": True},
            failure_message="should not raise",
            disabled_message="Audit logging disabled",
            disabled_level=logging.INFO,
            fallback_extra=fallback_extra,
        )

    assert fallback_extra == {
        "request_id": "req-456",
        "audit": {"flow": "demo"},
    }
    assert result.fallback_logged is True
    assert result.fallback_extra is not None
    assert result.fallback_extra is not fallback_extra
    assert result.fallback_extra["request_id"] == "req-456"
    audit_payload = result.fallback_extra["audit"]
    assert audit_payload["actor"] == "lea"
    assert audit_payload["flow"] == "demo"
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    record = caplog.records[0]
    assert record.message == "Audit logging disabled"
    assert getattr(record, "request_id", None) == "req-456"
    assert getattr(record, "audit", None)["flow"] == "demo"


def test_log_event_with_fallback_merges_result_and_wrapper_extra(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    hooks = audit_hooks.AuditHooks(
        log=lambda **_: None,
        hash_ip=lambda value: f"hash:{value}" if value is not None else None,
    )
    logger = logging.getLogger("tests.audit.merge_extra")

    hook_extra: dict[str, Any] = {"hooked": True, "audit": {"source": "hook"}}

    def fake_log_event(
        self: audit_hooks.AuditHooks,
        *,
        actor: str,
        action: str,
        entity: str,
        before: Mapping[str, Any],
        after: Mapping[str, Any],
        ip_address: Optional[str] = None,
        ip_hash: Optional[str] = None,
        resolved_ip_hash: audit_hooks.ResolvedIpHash | None = None,
    ) -> audit_hooks.AuditLogResult:
        resolved = resolved_ip_hash or audit_hooks.ResolvedIpHash(
            value=ip_hash,
            fallback=False,
            error=None,
        )
        return audit_hooks.AuditLogResult(
            handled=False,
            ip_hash=resolved.value,
            hash_fallback=resolved.fallback,
            hash_error=resolved.error,
            fallback_logged=True,
            fallback_extra=hook_extra,
            fallback_extra_evaluated=True,
        )

    monkeypatch.setattr(
        audit_hooks.AuditHooks,
        "log_event",
        fake_log_event,
        raising=False,
    )

    with caplog.at_level(logging.INFO, logger=logger.name):
        result = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="jill",
            action="demo.merge",
            entity="resource",
            before={"before": True},
            after={"after": True},
            ip_address="10.2.3.4",
            failure_message="should not raise",
            disabled_message="Audit logging disabled",
            disabled_level=logging.INFO,
        )

    assert hook_extra == {"hooked": True, "audit": {"source": "hook"}}
    assert result.fallback_logged is True
    assert result.fallback_extra is not None
    assert result.fallback_extra is not hook_extra
    assert result.fallback_extra.get("hooked") is True
    assert result.fallback_extra["audit"]["source"] == "hook"
    assert result.fallback_extra["audit"]["actor"] == "jill"
    assert result.fallback_extra["audit"]["ip_hash"] == "hash:10.2.3.4"
    assert result.fallback_extra["audit"]["before"] == {"before": True}
    assert result.fallback_extra["audit"]["after"] == {"after": True}
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is True
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.message == "Audit logging disabled"
    assert getattr(record, "audit", None)["actor"] == "jill"


def test_audit_event_log_with_fallback_method_delegates(monkeypatch):
    hooks = audit_hooks.AuditHooks(log=lambda **_: None, hash_ip=lambda value: value)
    logger = logging.getLogger("tests.audit.delegation")
    event = audit_hooks.AuditEvent(
        actor="heidi",
        action="demo.action",
        entity="resource",
        before={},
        after={},
    )

    expected = audit_hooks.AuditLogResult(
        handled=True,
        ip_hash=None,
        hash_fallback=False,
        hash_error=None,
        fallback_logged=False,
    )
    captured: dict[str, object] = {}

    def fake_helper(*args: object, **kwargs: object) -> audit_hooks.AuditLogResult:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(audit_hooks, "log_audit_event_with_fallback", fake_helper)

    fallback_extra = {"request_id": "abc"}
    result = event.log_with_fallback(
        hooks,
        logger,
        failure_message="failure",
        disabled_message="disabled",
        disabled_level=logging.INFO,
        context={"request_id": "abc"},
        fallback_extra=fallback_extra,
    )

    assert result is expected
    assert captured["args"] == (hooks, logger, event)
    assert captured["kwargs"] == {
        "failure_message": "failure",
        "disabled_message": "disabled",
        "disabled_level": logging.INFO,
        "context": {"request_id": "abc"},
        "context_factory": None,
        "resolved_ip_hash": None,
        "resolved_context": None,
        "fallback_extra": fallback_extra,
        "fallback_extra_factory": None,
        "resolved_fallback_extra": None,
    }


def test_audit_event_log_with_fallback_infers_event_context(monkeypatch):
    hooks = audit_hooks.AuditHooks(log=lambda **_: None, hash_ip=lambda value: value)
    logger = logging.getLogger("tests.audit.event_default")
    event = audit_hooks.AuditEvent(
        actor="judy",
        action="demo.infer",
        entity="resource",
        before={},
        after={},
        context={"source": "controller"},
    )

    expected = audit_hooks.AuditLogResult(
        handled=True,
        ip_hash=None,
        hash_fallback=False,
        hash_error=None,
        fallback_logged=False,
    )
    captured: dict[str, object] = {}

    def fake_helper(*args: object, **kwargs: object) -> audit_hooks.AuditLogResult:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(audit_hooks, "log_audit_event_with_fallback", fake_helper)

    result = event.log_with_fallback(
        hooks,
        logger,
        failure_message="failure",
    )

    assert result is expected
    assert captured["kwargs"]["context"] == {"source": "controller"}
    assert captured["kwargs"]["context_factory"] is None
    assert captured["kwargs"]["fallback_extra_factory"] is None


def test_audit_event_log_with_fallback_forwards_context_factory(monkeypatch):
    hooks = audit_hooks.AuditHooks(
        log=lambda **_: None,
        hash_ip=lambda value: value,
    )
    logger = logging.getLogger("tests.audit.event_factory")
    event = audit_hooks.AuditEvent(
        actor="kay", action="demo.factory", entity="resource", before={}, after={}
    )
    expected = audit_hooks.AuditLogResult(
        handled=True,
        ip_hash=None,
        hash_fallback=False,
        hash_error=None,
        fallback_logged=False,
    )
    captured: dict[str, object] = {}

    def fake_helper(*args: object, **kwargs: object) -> audit_hooks.AuditLogResult:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(audit_hooks, "log_audit_event_with_fallback", fake_helper)

    factory_calls: List[int] = []

    def context_factory() -> Mapping[str, str]:
        factory_calls.append(1)
        return {"audit": {"origin": "factory"}}

    result = event.log_with_fallback(
        hooks,
        logger,
        failure_message="failure",
        context_factory=context_factory,
    )

    assert result is expected
    assert captured["kwargs"]["context"] is None
    assert captured["kwargs"]["context_factory"] is context_factory
    assert captured["kwargs"]["fallback_extra_factory"] is None
    assert factory_calls == []


def test_audit_event_log_with_fallback_uses_event_context_factory(monkeypatch):
    hooks = audit_hooks.AuditHooks(
        log=lambda **_: None,
        hash_ip=lambda value: value,
    )
    logger = logging.getLogger("tests.audit.event_default_factory")
    factory_calls: List[int] = []

    def context_factory() -> Mapping[str, str]:
        factory_calls.append(1)
        return {"audit": {"origin": "event"}}

    event = audit_hooks.AuditEvent(
        actor="lee",
        action="demo.event.factory",
        entity="resource",
        before={},
        after={},
        context_factory=context_factory,
    )

    expected = audit_hooks.AuditLogResult(
        handled=True,
        ip_hash=None,
        hash_fallback=False,
        hash_error=None,
        fallback_logged=False,
    )
    captured: dict[str, object] = {}

    def fake_helper(*args: object, **kwargs: object) -> audit_hooks.AuditLogResult:
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(audit_hooks, "log_audit_event_with_fallback", fake_helper)

    result = event.log_with_fallback(
        hooks,
        logger,
        failure_message="failure",
    )

    assert result is expected
    assert captured["kwargs"]["context"] is None
    assert captured["kwargs"]["context_factory"] is context_factory
    assert captured["kwargs"]["fallback_extra_factory"] is None
    assert factory_calls == []


def test_audit_event_log_with_fallback_uses_event_fallback_extra(monkeypatch):
    hooks = audit_hooks.AuditHooks(log=lambda **_: None, hash_ip=lambda value: value)
    logger = logging.getLogger("tests.audit.event_fallback_extra")
    event = audit_hooks.AuditEvent(
        actor="max",
        action="demo.event.extra",
        entity="resource",
        before={},
        after={},
        fallback_extra={"request_id": "evt-123"},
    )

    expected = audit_hooks.AuditLogResult(
        handled=True,
        ip_hash=None,
        hash_fallback=False,
        hash_error=None,
        fallback_logged=False,
    )
    captured: dict[str, object] = {}

    def fake_helper(*args: object, **kwargs: object) -> audit_hooks.AuditLogResult:
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(audit_hooks, "log_audit_event_with_fallback", fake_helper)

    result = event.log_with_fallback(
        hooks,
        logger,
        failure_message="failure",
    )

    assert result is expected
    assert captured["kwargs"]["fallback_extra"] == {"request_id": "evt-123"}
    assert captured["kwargs"]["fallback_extra_factory"] is None


def test_audit_event_log_with_fallback_uses_event_fallback_extra_factory(monkeypatch):
    hooks = audit_hooks.AuditHooks(log=lambda **_: None, hash_ip=lambda value: value)
    logger = logging.getLogger("tests.audit.event_fallback_factory")
    factory_calls: List[int] = []

    def fallback_extra_factory() -> Mapping[str, str]:
        factory_calls.append(1)
        return {"request_id": "evt-456"}

    event = audit_hooks.AuditEvent(
        actor="nora",
        action="demo.event.fallback_factory",
        entity="resource",
        before={},
        after={},
        fallback_extra_factory=fallback_extra_factory,
    )

    expected = audit_hooks.AuditLogResult(
        handled=True,
        ip_hash=None,
        hash_fallback=False,
        hash_error=None,
        fallback_logged=False,
    )
    captured: dict[str, object] = {}

    def fake_helper(*args: object, **kwargs: object) -> audit_hooks.AuditLogResult:
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(audit_hooks, "log_audit_event_with_fallback", fake_helper)

    result = event.log_with_fallback(
        hooks,
        logger,
        failure_message="failure",
    )

    assert result is expected
    assert captured["kwargs"]["fallback_extra"] is None
    assert captured["kwargs"]["fallback_extra_factory"] is fallback_extra_factory
    assert factory_calls == []


def test_audit_event_log_with_fallback_forwards_resolved_fallback_extra(monkeypatch):
    hooks = audit_hooks.AuditHooks(log=lambda **_: None, hash_ip=lambda value: value)
    logger = logging.getLogger("tests.audit.event_resolved_extra")
    factory_calls: list[str] = []

    def fallback_extra_factory() -> Mapping[str, str]:
        factory_calls.append("called")
        return {"request_id": "evt-789"}

    event = audit_hooks.AuditEvent(
        actor="opal",
        action="demo.event.resolved_extra",
        entity="resource",
        before={},
        after={},
        fallback_extra_factory=fallback_extra_factory,
    )

    updated_event, resolved_extra = event.resolve_fallback_extra_metadata()
    assert factory_calls == ["called"]

    trimmed_event = updated_event.with_fallback_extra(None, merge=False)

    expected = audit_hooks.AuditLogResult(
        handled=True,
        ip_hash=None,
        hash_fallback=False,
        hash_error=None,
        fallback_logged=False,
    )
    captured: dict[str, object] = {}

    def fake_helper(*args: object, **kwargs: object) -> audit_hooks.AuditLogResult:
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(audit_hooks, "log_audit_event_with_fallback", fake_helper)

    result = trimmed_event.log_with_fallback(
        hooks,
        logger,
        failure_message="failure",
        fallback_extra_factory=fallback_extra_factory,
        resolved_fallback_extra=resolved_extra,
    )

    assert result is expected
    assert captured["kwargs"]["resolved_fallback_extra"] is resolved_extra
    assert captured["kwargs"]["fallback_extra"] is None
    assert captured["kwargs"]["fallback_extra_factory"] is fallback_extra_factory
    assert factory_calls == ["called"]


def test_resolve_ip_hash_prefers_explicit_hash():
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: pytest.fail("hash_ip should not be invoked"))

    resolved = hooks.resolve_ip_hash(ip_address="198.51.100.4", ip_hash="provided-hash")

    assert isinstance(resolved, audit_hooks.ResolvedIpHash)
    assert resolved.value == "provided-hash"
    assert resolved.fallback is False
    assert resolved.error is None


def test_resolve_ip_hash_hashes_when_no_override():
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: f"hashed:{value}")

    resolved = hooks.resolve_ip_hash(ip_address="203.0.113.5", ip_hash=None)

    assert resolved.value == "hashed:203.0.113.5"
    assert resolved.fallback is False
    assert resolved.error is None


def test_resolve_ip_hash_records_fallback_on_error():
    def failing_hash(value: Optional[str]) -> Optional[str]:
        raise RuntimeError("hash failure")

    hooks = audit_hooks.AuditHooks(log=None, hash_ip=failing_hash)

    resolved = hooks.resolve_ip_hash(ip_address="192.0.2.1", ip_hash=None)

    assert resolved.value == audit_hooks._hash_ip_fallback("192.0.2.1")
    assert resolved.fallback is True
    assert isinstance(resolved.error, RuntimeError)


def test_temporary_audit_hooks_override_and_restore():
    audit_hooks.reset_audit_hooks_cache()
    sentinel = audit_hooks.AuditHooks(log=lambda **kwargs: None, hash_ip=lambda value: "sentinel")

    with audit_hooks.temporary_audit_hooks(sentinel):
        assert audit_hooks.load_audit_hooks() is sentinel
        # The override should be returned on repeated calls without re-entering the context.
        assert audit_hooks.load_audit_hooks() is sentinel

    # Once the context exits the cached dependency-backed hooks are restored.
    restored = audit_hooks.load_audit_hooks()
    assert restored is not sentinel


def test_temporary_audit_hooks_override_even_after_initial_load():
    audit_hooks.reset_audit_hooks_cache()
    original = audit_hooks.load_audit_hooks()
    override = audit_hooks.AuditHooks(log=lambda **kwargs: None, hash_ip=lambda value: "override")

    with audit_hooks.temporary_audit_hooks(override):
        assert audit_hooks.load_audit_hooks() is override

    assert audit_hooks.load_audit_hooks() is original


def test_temporary_audit_hooks_nest_cleanly():
    audit_hooks.reset_audit_hooks_cache()
    outer = audit_hooks.AuditHooks(log=lambda **kwargs: None, hash_ip=lambda value: "outer")
    inner = audit_hooks.AuditHooks(log=lambda **kwargs: None, hash_ip=lambda value: "inner")

    with audit_hooks.temporary_audit_hooks(outer):
        assert audit_hooks.load_audit_hooks() is outer
        with audit_hooks.temporary_audit_hooks(inner):
            assert audit_hooks.load_audit_hooks() is inner
        # After the inner context the outer override should still be active.
        assert audit_hooks.load_audit_hooks() is outer

    # After both contexts exit the dependency-backed hooks should be available again.
    reset = audit_hooks.load_audit_hooks()
    assert reset is not inner
    assert reset is not outer


def test_audit_event_with_context_merges_without_mutating_original():
    original_context = {"existing": True, "audit": {"hash_fallback": False}}
    event = audit_hooks.AuditEvent(
        actor="ivy",
        action="demo.merge",
        entity="resource",
        before={},
        after={},
        context=original_context,
    )

    merged = event.with_context({"extra": "value"})

    assert merged is not event
    assert merged.context is not original_context
    assert merged.context == {
        "existing": True,
        "audit": {"hash_fallback": False},
        "extra": "value",
    }
    # The mapping stored on the original event is not mutated by merging.
    assert event.context is original_context
    assert event.context == {"existing": True, "audit": {"hash_fallback": False}}


def test_audit_event_with_context_replaces_or_clears():
    event = audit_hooks.AuditEvent(
        actor="jane",
        action="demo.replace",
        entity="resource",
        before={},
        after={},
        context={"initial": True},
    )

    replaced = event.with_context({"replacement": True}, merge=False)
    assert replaced.context == {"replacement": True}

    cleared = replaced.with_context(None, merge=False)
    assert cleared.context is None

    unchanged = event.with_context(None)
    assert unchanged is event


def test_audit_event_with_context_factory_clears_context_by_default():
    def original_factory() -> Mapping[str, str]:
        return {"factory": "original"}

    def new_factory() -> Mapping[str, str]:
        return {"factory": "new"}

    event = audit_hooks.AuditEvent(
        actor="keira",
        action="demo.context_factory",
        entity="resource",
        before={},
        after={},
        context={"cached": True},
        context_factory=original_factory,
    )

    updated = event.with_context_factory(new_factory)

    assert updated is not event
    assert updated.context is None
    assert updated.context_factory is new_factory
    # The original context payload remains intact on the source event.
    assert event.context == {"cached": True}
    assert event.context_factory is original_factory


def test_audit_event_with_context_factory_optionally_preserves_context():
    def factory() -> Mapping[str, str]:
        return {"factory": "value"}

    event = audit_hooks.AuditEvent(
        actor="luke",
        action="demo.context_factory.preserve",
        entity="resource",
        before={},
        after={},
        context={"cached": True},
        context_factory=None,
    )

    preserved = event.with_context_factory(factory, preserve_context=True)

    assert preserved.context_factory is factory
    assert preserved.context == {"cached": True}

    # Updating with the same factory while preserving context returns the same instance.
    same = preserved.with_context_factory(factory, preserve_context=True)
    assert same is preserved

    # Calling without preservation clears the stored context when already set.
    cleared = preserved.with_context_factory(factory)
    assert cleared.context is None


def test_audit_event_with_fallback_extra_merges_without_mutating_original():
    original_extra = {"existing": True, "audit": {"hash_fallback": False}}
    event = audit_hooks.AuditEvent(
        actor="mina",
        action="demo.fallback.merge",
        entity="resource",
        before={},
        after={},
        fallback_extra=original_extra,
    )

    merged = event.with_fallback_extra({"extra": "value"})

    assert merged is not event
    assert merged.fallback_extra is not original_extra
    assert merged.fallback_extra == {
        "existing": True,
        "audit": {"hash_fallback": False},
        "extra": "value",
    }
    assert event.fallback_extra is original_extra
    assert event.fallback_extra == {"existing": True, "audit": {"hash_fallback": False}}


def test_audit_event_with_fallback_extra_replaces_or_clears():
    event = audit_hooks.AuditEvent(
        actor="nate",
        action="demo.fallback.replace",
        entity="resource",
        before={},
        after={},
        fallback_extra={"initial": True},
    )

    replaced = event.with_fallback_extra({"replacement": True}, merge=False)
    assert replaced.fallback_extra == {"replacement": True}

    cleared = replaced.with_fallback_extra(None, merge=False)
    assert cleared.fallback_extra is None

    unchanged = event.with_fallback_extra(None)
    assert unchanged is event


def test_audit_event_with_fallback_extra_factory_clears_extra_by_default():
    def original_factory() -> Mapping[str, str]:
        return {"factory": "original"}

    def new_factory() -> Mapping[str, str]:
        return {"factory": "new"}

    event = audit_hooks.AuditEvent(
        actor="olga",
        action="demo.fallback.factory",
        entity="resource",
        before={},
        after={},
        fallback_extra={"cached": True},
        fallback_extra_factory=original_factory,
    )

    updated = event.with_fallback_extra_factory(new_factory)

    assert updated is not event
    assert updated.fallback_extra is None
    assert updated.fallback_extra_factory is new_factory
    assert event.fallback_extra == {"cached": True}
    assert event.fallback_extra_factory is original_factory


def test_audit_event_with_fallback_extra_factory_optionally_preserves_extra():
    def factory() -> Mapping[str, str]:
        return {"factory": "value"}

    event = audit_hooks.AuditEvent(
        actor="pilar",
        action="demo.fallback.factory.preserve",
        entity="resource",
        before={},
        after={},
        fallback_extra={"cached": True},
        fallback_extra_factory=None,
    )

    preserved = event.with_fallback_extra_factory(factory, preserve_fallback_extra=True)

    assert preserved.fallback_extra_factory is factory
    assert preserved.fallback_extra == {"cached": True}

    same = preserved.with_fallback_extra_factory(factory, preserve_fallback_extra=True)
    assert same is preserved

    cleared = preserved.with_fallback_extra_factory(factory)
    assert cleared.fallback_extra is None


def test_audit_event_resolve_fallback_extra_reuses_cached_mapping():
    extra = {"cached": True}
    event = audit_hooks.AuditEvent(
        actor="quinn",
        action="demo.fallback.resolve.cached",
        entity="resource",
        before={},
        after={},
        fallback_extra=extra,
        fallback_extra_factory=lambda: pytest.fail("factory should not run"),
    )

    updated, resolved = event.resolve_fallback_extra()

    assert updated is event
    assert resolved is extra


def test_audit_event_resolve_fallback_extra_evaluates_factory_once():
    calls: list[int] = []

    def factory() -> Mapping[str, str]:
        calls.append(len(calls))
        return {"request_id": "abc"}

    event = audit_hooks.AuditEvent(
        actor="rory",
        action="demo.fallback.resolve.factory",
        entity="resource",
        before={},
        after={},
        fallback_extra_factory=factory,
    )

    updated, resolved = event.resolve_fallback_extra()

    assert calls == [0]
    assert resolved == {"request_id": "abc"}
    assert updated.fallback_extra == {"request_id": "abc"}

    updated_again, resolved_again = updated.resolve_fallback_extra()

    assert updated_again is updated
    assert resolved_again == {"request_id": "abc"}
    assert calls == [0]


def test_audit_event_resolve_fallback_extra_drop_extra_clears_mapping():
    extra = {"trace": "token"}
    event = audit_hooks.AuditEvent(
        actor="casey",
        action="demo.fallback.resolve.drop", 
        entity="resource",
        before={},
        after={},
        fallback_extra=extra,
    )

    updated, resolved = event.resolve_fallback_extra(drop_extra=True)

    assert resolved is extra
    assert updated is not event
    assert updated.fallback_extra is None
    assert updated.fallback_extra_factory is None

    updated_again, resolved_again = updated.resolve_fallback_extra()

    assert updated_again is updated
    assert resolved_again is None


def test_audit_event_resolve_fallback_extra_metadata_drop_extra_preserves_value():
    calls: list[int] = []

    def factory() -> Mapping[str, str]:
        calls.append(len(calls))
        return {"request_id": "xyz"}

    event = audit_hooks.AuditEvent(
        actor="drew",
        action="demo.fallback.resolve.drop-metadata",
        entity="resource",
        before={},
        after={},
        fallback_extra_factory=factory,
    )

    updated, resolved = event.resolve_fallback_extra_metadata(
        drop_extra=True
    )

    assert calls == [0]
    assert resolved.value == {"request_id": "xyz"}
    assert resolved.evaluated is True
    assert resolved.error is None
    assert updated.fallback_extra is None
    # Factory remains so subsequent resolutions can rebuild the payload unless dropped.
    assert updated.fallback_extra_factory is factory

def test_audit_event_resolve_fallback_extra_metadata_captures_error():
    def factory() -> Mapping[str, str]:
        raise RuntimeError("extra boom")

    event = audit_hooks.AuditEvent(
        actor="sasha",
        action="demo.fallback.resolve.error",
        entity="resource",
        before={},
        after={},
        fallback_extra_factory=factory,
    )

    updated, resolved = event.resolve_fallback_extra_metadata(drop_factory=True)

    assert updated.fallback_extra_factory is None
    assert resolved.value is None
    assert isinstance(resolved.error, RuntimeError)
    assert str(resolved.error) == "extra boom"
    assert resolved.evaluated is True


def test_audit_event_resolve_fallback_extra_metadata_respects_skipped_factory():
    event = audit_hooks.AuditEvent(
        actor="taylor",
        action="demo.fallback.resolve.skip",
        entity="resource",
        before={},
        after={},
        fallback_extra_factory=lambda: {"should": "not-run"},
    )

    updated, resolved = event.resolve_fallback_extra_metadata(use_factory=False)

    assert updated is event
    assert resolved.value is None
    assert resolved.error is None
    assert resolved.evaluated is True


def test_audit_event_resolve_context_reuses_stored_mapping():
    context = {"source": "stored"}
    event = audit_hooks.AuditEvent(
        actor="maya",
        action="demo.context.resolve",
        entity="resource",
        before={},
        after={},
        context=context,
        context_factory=lambda: pytest.fail("factory should not run"),
    )

    updated, resolved = event.resolve_context()

    assert updated is event
    assert resolved is context


def test_audit_event_resolve_context_evaluates_factory_once():
    calls = {"count": 0}

    def factory() -> Mapping[str, str]:
        calls["count"] += 1
        return {"factory": calls["count"]}

    event = audit_hooks.AuditEvent(
        actor="nina",
        action="demo.context.factory",
        entity="resource",
        before={},
        after={},
        context_factory=factory,
    )

    updated, resolved = event.resolve_context()

    assert calls["count"] == 1
    assert resolved == {"factory": 1}
    assert updated is not event
    assert updated.context == {"factory": 1}
    # The factory is retained by default so callers can refresh later.
    assert updated.context_factory is factory


def test_audit_event_resolve_context_can_drop_factory_after_resolution():
    def factory() -> Mapping[str, str]:
        return {"factory": "value"}

    event = audit_hooks.AuditEvent(
        actor="oliver",
        action="demo.context.drop",
        entity="resource",
        before={},
        after={},
        context_factory=factory,
    )

    updated, resolved = event.resolve_context(drop_factory=True)

    assert resolved == {"factory": "value"}
    assert updated.context == {"factory": "value"}
    assert updated.context_factory is None


def test_audit_event_resolve_context_refreshes_cached_mapping():
    calls = {"count": 0}

    def factory() -> Mapping[str, int]:
        calls["count"] += 1
        return {"refresh": calls["count"]}

    event = audit_hooks.AuditEvent(
        actor="piper",
        action="demo.context.refresh",
        entity="resource",
        before={},
        after={},
        context={"refresh": 0},
        context_factory=factory,
    )

    updated, resolved = event.resolve_context(refresh=True)

    assert calls["count"] == 1
    assert resolved == {"refresh": 1}
    assert updated.context == {"refresh": 1}
    assert updated.context_factory is factory


def test_audit_event_resolve_context_allows_clearing_context_without_factory():
    event = audit_hooks.AuditEvent(
        actor="quinn",
        action="demo.context.clear",
        entity="resource",
        before={},
        after={},
        context={"cached": True},
    )

    updated, resolved = event.resolve_context(use_factory=False, refresh=True)

    assert resolved is None
    assert updated.context is None
    assert updated is not event


def test_audit_event_resolve_context_drops_factory_without_invocation():
    event = audit_hooks.AuditEvent(
        actor="riley",
        action="demo.context.drop_only",
        entity="resource",
        before={},
        after={},
        context={"cached": True},
        context_factory=lambda: pytest.fail("factory should not run"),
    )

    updated, resolved = event.resolve_context(use_factory=False, drop_factory=True)

    assert resolved == {"cached": True}
    assert updated.context == {"cached": True}
    assert updated.context_factory is None


def test_audit_event_resolve_context_metadata_reuses_cached_context():
    context = {"cached": True}
    event = audit_hooks.AuditEvent(
        actor="sasha",
        action="demo.context.metadata.cached",
        entity="resource",
        before={},
        after={},
        context=context,
        context_factory=lambda: pytest.fail("factory should not run"),
    )

    updated, resolved = event.resolve_context_metadata()

    assert updated is event
    assert isinstance(resolved, audit_hooks.ResolvedContext)
    assert resolved.value is context
    assert resolved.error is None
    assert resolved.evaluated is True


def test_audit_event_resolve_context_metadata_evaluates_factory_once():
    calls = {"count": 0}

    def factory() -> Mapping[str, str]:
        calls["count"] += 1
        return {"factory": calls["count"]}

    event = audit_hooks.AuditEvent(
        actor="taylor",
        action="demo.context.metadata.factory",
        entity="resource",
        before={},
        after={},
        context_factory=factory,
    )

    updated, resolved = event.resolve_context_metadata()

    assert calls["count"] == 1
    assert resolved.value == {"factory": 1}
    assert resolved.error is None
    assert resolved.evaluated is True
    assert updated.context == {"factory": 1}
    assert updated.context_factory is factory


def test_audit_event_resolve_context_metadata_captures_factory_error():
    calls = {"count": 0}

    def failing_factory() -> Mapping[str, str]:
        calls["count"] += 1
        raise RuntimeError("context failure")

    event = audit_hooks.AuditEvent(
        actor="ursula",
        action="demo.context.metadata.error",
        entity="resource",
        before={},
        after={},
        context_factory=failing_factory,
    )

    updated, resolved = event.resolve_context_metadata(drop_factory=True)

    assert calls["count"] == 1
    assert resolved.value is None
    assert isinstance(resolved.error, RuntimeError)
    assert resolved.evaluated is True
    assert updated.context is None
    assert updated.context_factory is None


def test_audit_event_resolve_context_metadata_respects_skipped_factory():
    event = audit_hooks.AuditEvent(
        actor="val",
        action="demo.context.metadata.skip",
        entity="resource",
        before={},
        after={},
        context_factory=lambda: {"should": "not run"},
    )

    updated, resolved = event.resolve_context_metadata(use_factory=False)

    assert updated is event
    assert resolved.value is None
    assert resolved.error is None
    assert resolved.evaluated is True


def test_audit_event_with_actor_updates_when_changed():
    event = audit_hooks.AuditEvent(
        actor="logan",
        action="demo.actor",
        entity="resource",
        before={},
        after={},
    )

    updated = event.with_actor("morgan")

    assert updated is not event
    assert updated.actor == "morgan"
    assert event.actor == "logan"
    assert updated.with_actor("morgan") is updated


def test_audit_event_with_action_updates_when_changed():
    event = audit_hooks.AuditEvent(
        actor="logan",
        action="demo.original",
        entity="resource",
        before={},
        after={},
    )

    updated = event.with_action("demo.updated")

    assert updated.action == "demo.updated"
    assert event.action == "demo.original"
    assert updated.with_action("demo.updated") is updated


def test_audit_event_with_entity_updates_when_changed():
    event = audit_hooks.AuditEvent(
        actor="logan",
        action="demo.entity",
        entity="resource",
        before={},
        after={},
    )

    updated = event.with_entity("resource:child")

    assert updated.entity == "resource:child"
    assert event.entity == "resource"
    assert updated.with_entity("resource:child") is updated


def test_audit_event_with_before_replaces_or_merges():
    original_before = {"version": 1, "status": "pending"}
    event = audit_hooks.AuditEvent(
        actor="logan",
        action="demo.before",
        entity="resource",
        before=original_before,
        after={},
    )

    replaced = event.with_before({"version": 2})
    assert replaced.before == {"version": 2}
    assert event.with_before(original_before) is event

    merged = event.with_before({"status": "approved"}, merge=True)
    assert merged.before == {"version": 1, "status": "approved"}
    assert merged.before is not original_before
    assert merged.with_before({"status": "approved"}, merge=True) is merged


def test_audit_event_with_after_replaces_or_merges():
    original_after = {"status": "pending"}
    event = audit_hooks.AuditEvent(
        actor="logan",
        action="demo.after",
        entity="resource",
        before={},
        after=original_after,
    )

    replaced = event.with_after({"status": "approved"})
    assert replaced.after == {"status": "approved"}
    assert event.with_after(original_after) is event

    merged = event.with_after({"notes": "reviewed"}, merge=True)
    assert merged.after == {"status": "pending", "notes": "reviewed"}
    assert merged.after is not original_after
    assert merged.with_after({"notes": "reviewed"}, merge=True) is merged


def test_audit_event_with_ip_address_resets_hash_by_default():
    event = audit_hooks.AuditEvent(
        actor="karl",
        action="demo.ip",
        entity="resource",
        before={},
        after={},
        ip_address="127.0.0.1",
        ip_hash="hashed:127.0.0.1",
    )

    updated = event.with_ip_address("10.0.0.1")

    assert updated is not event
    assert updated.ip_address == "10.0.0.1"
    assert updated.ip_hash is None
    # The original event remains unchanged.
    assert event.ip_address == "127.0.0.1"
    assert event.ip_hash == "hashed:127.0.0.1"


def test_audit_event_with_ip_address_preserves_hash_when_requested():
    event = audit_hooks.AuditEvent(
        actor="lena",
        action="demo.ip.preserve",
        entity="resource",
        before={},
        after={},
        ip_address="127.0.0.1",
        ip_hash="hashed:127.0.0.1",
    )

    updated = event.with_ip_address("10.0.0.1", preserve_hash=True)

    assert updated.ip_address == "10.0.0.1"
    assert updated.ip_hash == "hashed:127.0.0.1"


def test_audit_event_with_ip_address_same_value_clears_hash_when_needed():
    event = audit_hooks.AuditEvent(
        actor="maya",
        action="demo.ip.same",
        entity="resource",
        before={},
        after={},
        ip_address="127.0.0.1",
        ip_hash="stale",
    )

    updated = event.with_ip_address("127.0.0.1")

    assert updated.ip_address == "127.0.0.1"
    assert updated.ip_hash is None


def test_audit_event_with_ip_hash_updates_immutably():
    event = audit_hooks.AuditEvent(
        actor="nina",
        action="demo.ip.hash",
        entity="resource",
        before={},
        after={},
    )

    updated = event.with_ip_hash("hash")
    assert updated is not event
    assert updated.ip_hash == "hash"

    unchanged = updated.with_ip_hash("hash")
    assert unchanged is updated


def test_audit_event_resolve_ip_hash_reuses_explicit_hash():
    hooks = audit_hooks.AuditHooks(
        log=None,
        hash_ip=lambda value: pytest.fail("hash_ip should not be invoked"),
    )

    event = audit_hooks.AuditEvent(
        actor="oliver",
        action="demo.event.resolve",
        entity="resource",
        before={},
        after={},
        ip_hash="provided-hash",
    )

    resolved = event.resolve_ip_hash(hooks)

    assert isinstance(resolved, audit_hooks.ResolvedIpHash)
    assert resolved.value == "provided-hash"
    assert resolved.fallback is False
    assert resolved.error is None


def test_audit_event_resolve_ip_hash_hashes_missing_value():
    calls: List[Optional[str]] = []

    def tracking_hash(value: Optional[str]) -> Optional[str]:
        calls.append(value)
        return f"hashed:{value}"

    hooks = audit_hooks.AuditHooks(log=None, hash_ip=tracking_hash)

    event = audit_hooks.AuditEvent(
        actor="piper",
        action="demo.event.hash",
        entity="resource",
        before={},
        after={},
        ip_address="203.0.113.42",
    )

    resolved = event.resolve_ip_hash(hooks)

    assert resolved.value == "hashed:203.0.113.42"
    assert resolved.fallback is False
    assert resolved.error is None
    assert calls == ["203.0.113.42"]


def test_audit_event_to_payload_uses_resolved_hash_and_copies_payloads():
    event = audit_hooks.AuditEvent(
        actor="pepper",
        action="demo.event.payload",
        entity="resource",
        before={"before": True},
        after={"after": True},
        ip_address="198.51.100.10",
    )
    resolved = audit_hooks.ResolvedIpHash(value="hashed-value", fallback=False, error=None)

    payload = event.to_payload(resolved_ip_hash=resolved)

    assert payload == {
        "actor": "pepper",
        "action": "demo.event.payload",
        "entity": "resource",
        "before": {"before": True},
        "after": {"after": True},
        "ip_hash": "hashed-value",
        "ip_address": "198.51.100.10",
    }
    assert payload["before"] is not event.before
    assert payload["after"] is not event.after


def test_audit_event_to_payload_can_skip_ip_address_and_include_context():
    event = audit_hooks.AuditEvent(
        actor="quinn",
        action="demo.event.context",
        entity="resource",
        before={},
        after={},
        context={"request_id": "abc"},
        ip_address="203.0.113.5",
    )

    payload = event.to_payload(include_ip_address=False, include_context=True)

    assert "ip_address" not in payload
    assert payload["context"] == {"request_id": "abc"}
    assert payload["context"] is not event.context
    assert payload["ip_hash"] is None


def test_audit_event_to_payload_can_use_context_factory_on_demand():
    calls: list[str] = []
    context_value: Mapping[str, Any] = {"request_id": "xyz"}

    def build_context() -> Mapping[str, Any]:
        calls.append("called")
        return context_value

    event = audit_hooks.AuditEvent(
        actor="riley",
        action="demo.event.context.factory",
        entity="resource",
        before={},
        after={},
        context_factory=build_context,
    )

    payload = event.to_payload(include_context=True, use_context_factory=True)

    assert payload["context"] == {"request_id": "xyz"}
    assert payload["context"] is not context_value
    assert calls == ["called"]


def test_audit_event_to_payload_includes_context_metadata_from_resolved():
    event = audit_hooks.AuditEvent(
        actor="river",
        action="demo.event.context.metadata",
        entity="resource",
        before={},
        after={},
    )
    resolved_context = audit_hooks.ResolvedContext(
        value={"request_id": "resolved"},
        error=RuntimeError("context failed"),
        evaluated=True,
    )

    payload = event.to_payload(
        include_context_metadata=True,
        resolved_context=resolved_context,
    )

    assert payload["context_evaluated"] is True
    assert payload["context_error"] == {
        "type": "RuntimeError",
        "message": "context failed",
    }
    assert "context" not in payload


def test_audit_event_to_payload_resolves_context_factory_for_metadata():
    calls: list[str] = []

    def build_context() -> Mapping[str, Any]:
        calls.append("called")
        return {"request_id": "factory"}

    event = audit_hooks.AuditEvent(
        actor="skyler",
        action="demo.event.context.metadata.factory",
        entity="resource",
        before={},
        after={},
        context_factory=build_context,
    )

    payload = event.to_payload(
        include_context_metadata=True,
        use_context_factory=True,
    )

    assert payload["context_evaluated"] is True
    assert "context_error" not in payload
    assert "context" not in payload
    assert calls == ["called"]


def test_audit_event_to_payload_records_context_factory_errors():
    calls: list[str] = []

    def build_context() -> Mapping[str, Any]:
        calls.append("called")
        raise ValueError("boom")

    event = audit_hooks.AuditEvent(
        actor="tatum",
        action="demo.event.context.metadata.error",
        entity="resource",
        before={},
        after={},
        context_factory=build_context,
    )

    payload = event.to_payload(
        include_context_metadata=True,
        use_context_factory=True,
    )

    assert payload["context_evaluated"] is True
    assert payload["context_error"] == {
        "type": "ValueError",
        "message": "boom",
    }
    assert "context" not in payload
    assert calls == ["called"]


def test_audit_event_to_payload_skips_context_factory_when_not_included():
    calls: list[str] = []

    def build_context() -> Mapping[str, Any]:
        calls.append("called")
        return {"request_id": "xyz"}

    event = audit_hooks.AuditEvent(
        actor="sam",
        action="demo.event.context.factory.skip",
        entity="resource",
        before={},
        after={},
        context_factory=build_context,
    )

    payload = event.to_payload(use_context_factory=True)

    assert "context" not in payload
    assert calls == []


def test_audit_event_to_payload_can_include_hash_metadata():
    event = audit_hooks.AuditEvent(
        actor="sasha",
        action="demo.event.hash.metadata",
        entity="resource",
        before={},
        after={},
    )
    error = ValueError("hashing failed")
    resolved = audit_hooks.ResolvedIpHash(value="fallback-hash", fallback=True, error=error)

    payload = event.to_payload(
        include_hash_metadata=True,
        resolved_ip_hash=resolved,
    )

    assert payload["ip_hash"] == "fallback-hash"
    assert payload["hash_fallback"] is True
    assert payload["hash_error"] == {
        "type": "ValueError",
        "message": "hashing failed",
    }


def test_audit_event_to_payload_defaults_hash_metadata_when_not_provided():
    event = audit_hooks.AuditEvent(
        actor="taylor",
        action="demo.event.hash.metadata.default",
        entity="resource",
        before={},
        after={},
        ip_hash="pre-resolved",
    )

    payload = event.to_payload(include_hash_metadata=True)

    assert payload["ip_hash"] == "pre-resolved"
    assert payload["hash_fallback"] is False
    assert "hash_error" not in payload


def test_audit_event_to_payload_can_include_fallback_extra_mapping():
    event = audit_hooks.AuditEvent(
        actor="uma",
        action="demo.event.fallback.extra",
        entity="resource",
        before={},
        after={},
        fallback_extra={"correlation_id": "abc"},
    )

    payload = event.to_payload(include_fallback_extra=True)

    assert payload["fallback_extra"] == {"correlation_id": "abc"}
    assert payload["fallback_extra"] is not event.fallback_extra


def test_audit_event_to_payload_can_use_fallback_extra_factory():
    calls: list[str] = []

    def build_extra() -> Mapping[str, Any]:
        calls.append("called")
        return {"correlation_id": "xyz"}

    event = audit_hooks.AuditEvent(
        actor="vale",
        action="demo.event.fallback.extra.factory",
        entity="resource",
        before={},
        after={},
        fallback_extra_factory=build_extra,
    )

    payload = event.to_payload(
        include_fallback_extra=True,
        use_fallback_extra_factory=True,
    )

    assert payload["fallback_extra"] == {"correlation_id": "xyz"}
    assert calls == ["called"]

    assert "fallback_extra" not in event.to_payload()
    assert calls == ["called"]


def test_audit_event_to_payload_includes_fallback_extra_metadata_from_resolved():
    event = audit_hooks.AuditEvent(
        actor="wren",
        action="demo.event.fallback.extra.metadata",
        entity="resource",
        before={},
        after={},
    )
    resolved = audit_hooks.ResolvedFallbackExtra(
        value=None,
        error=RuntimeError("factory failed"),
        evaluated=True,
    )

    payload = event.to_payload(
        include_fallback_extra_metadata=True,
        resolved_fallback_extra=resolved,
    )

    assert payload["fallback_extra_evaluated"] is True
    assert payload["fallback_extra_error"] == {
        "type": "RuntimeError",
        "message": "factory failed",
    }
    assert "fallback_extra" not in payload


def test_audit_event_to_payload_resolves_fallback_extra_factory_for_metadata():
    calls: list[str] = []

    def build_extra() -> Mapping[str, Any]:
        calls.append("called")
        return {"correlation_id": "xyz"}

    event = audit_hooks.AuditEvent(
        actor="xander",
        action="demo.event.fallback.extra.metadata.factory",
        entity="resource",
        before={},
        after={},
        fallback_extra_factory=build_extra,
    )

    payload = event.to_payload(
        include_fallback_extra_metadata=True,
        use_fallback_extra_factory=True,
    )

    assert payload["fallback_extra_evaluated"] is True
    assert "fallback_extra_error" not in payload
    assert "fallback_extra" not in payload
    assert calls == ["called"]


def test_audit_event_to_payload_records_fallback_extra_factory_errors():
    calls: list[str] = []

    def build_extra() -> Mapping[str, Any]:
        calls.append("called")
        raise ValueError("boom")

    event = audit_hooks.AuditEvent(
        actor="yasmin",
        action="demo.event.fallback.extra.metadata.error",
        entity="resource",
        before={},
        after={},
        fallback_extra_factory=build_extra,
    )

    payload = event.to_payload(
        include_fallback_extra_metadata=True,
        use_fallback_extra_factory=True,
    )

    assert payload["fallback_extra_evaluated"] is True
    assert payload["fallback_extra_error"] == {
        "type": "ValueError",
        "message": "boom",
    }
    assert "fallback_extra" not in payload
    assert calls == ["called"]


def test_audit_event_with_resolved_ip_hash_updates_hash_without_dropping_ip():
    event = audit_hooks.AuditEvent(
        actor="owen",
        action="demo.update.hash",
        entity="resource",
        before={},
        after={},
        ip_address="203.0.113.10",
    )
    resolved = audit_hooks.ResolvedIpHash(value="hashed-value", fallback=False, error=None)

    updated = event.with_resolved_ip_hash(resolved)

    assert updated is not event
    assert updated.ip_hash == "hashed-value"
    assert updated.ip_address == "203.0.113.10"


def test_audit_event_with_resolved_ip_hash_can_drop_ip_address():
    event = audit_hooks.AuditEvent(
        actor="pax",
        action="demo.drop.ip",
        entity="resource",
        before={},
        after={},
        ip_address="198.51.100.20",
    )
    resolved = audit_hooks.ResolvedIpHash(value="hashed-value", fallback=True, error=ValueError("boom"))

    updated = event.with_resolved_ip_hash(resolved, drop_ip_address=True)

    assert updated.ip_hash == "hashed-value"
    assert updated.ip_address is None
    assert event.ip_address == "198.51.100.20"
    assert event.ip_hash is None


def test_audit_event_with_resolved_ip_hash_returns_self_when_no_change():
    event = audit_hooks.AuditEvent(
        actor="quinn",
        action="demo.same.hash",
        entity="resource",
        before={},
        after={},
        ip_address="192.0.2.5",
        ip_hash="existing",
    )
    resolved = audit_hooks.ResolvedIpHash(value="existing", fallback=False, error=None)

    assert event.with_resolved_ip_hash(resolved) is event


def test_audit_event_with_resolved_ip_hash_drop_without_address_returns_self():
    event = audit_hooks.AuditEvent(
        actor="ria",
        action="demo.no.address",
        entity="resource",
        before={},
        after={},
        ip_address=None,
        ip_hash="cached",
    )
    resolved = audit_hooks.ResolvedIpHash(value="cached", fallback=False, error=None)

    assert event.with_resolved_ip_hash(resolved, drop_ip_address=True) is event


def test_audit_event_ensure_resolved_ip_hash_updates_event_and_returns_metadata():
    calls: List[Optional[str]] = []

    def tracking_hash(value: Optional[str]) -> Optional[str]:
        calls.append(value)
        return f"hashed:{value}"

    hooks = audit_hooks.AuditHooks(log=None, hash_ip=tracking_hash)

    event = audit_hooks.AuditEvent(
        actor="sara",
        action="demo.ensure.hash",
        entity="resource",
        before={},
        after={},
        ip_address="203.0.113.77",
    )

    updated, resolved = event.ensure_resolved_ip_hash(hooks)

    assert updated is not event
    assert updated.ip_hash == "hashed:203.0.113.77"
    assert updated.ip_address == "203.0.113.77"
    assert resolved.value == "hashed:203.0.113.77"
    assert resolved.fallback is False
    assert resolved.error is None
    assert calls == ["203.0.113.77"]


def test_audit_event_ensure_resolved_ip_hash_can_drop_ip_address():
    def failing_hash(value: Optional[str]) -> Optional[str]:
        raise RuntimeError("hash failure")

    hooks = audit_hooks.AuditHooks(log=None, hash_ip=failing_hash)

    event = audit_hooks.AuditEvent(
        actor="tess",
        action="demo.ensure.drop",
        entity="resource",
        before={},
        after={},
        ip_address="198.51.100.44",
    )

    updated, resolved = event.ensure_resolved_ip_hash(hooks, drop_ip_address=True)

    assert updated.ip_address is None
    assert updated.ip_hash == audit_hooks._hash_ip_fallback("198.51.100.44")
    assert resolved.value == audit_hooks._hash_ip_fallback("198.51.100.44")
    assert resolved.fallback is True
    assert isinstance(resolved.error, RuntimeError)


def test_audit_event_ensure_resolved_ip_hash_returns_self_when_no_change():
    hooks = audit_hooks.AuditHooks(
        log=None,
        hash_ip=lambda value: pytest.fail("hash_ip should not be invoked"),
    )

    event = audit_hooks.AuditEvent(
        actor="uma",
        action="demo.ensure.unchanged",
        entity="resource",
        before={},
        after={},
        ip_address=None,
        ip_hash="cached",
    )

    updated, resolved = event.ensure_resolved_ip_hash(hooks)

    assert updated is event
    assert resolved.value == "cached"
    assert resolved.fallback is False
    assert resolved.error is None


def test_audit_event_ensure_resolved_metadata_updates_event_and_returns_results():
    hash_calls: List[Optional[str]] = []
    context_calls = {"count": 0}

    def tracking_hash(value: Optional[str]) -> Optional[str]:
        hash_calls.append(value)
        return f"hashed:{value}"

    def context_factory() -> Mapping[str, int]:
        context_calls["count"] += 1
        return {"factory_calls": context_calls["count"]}

    hooks = audit_hooks.AuditHooks(log=None, hash_ip=tracking_hash)

    event = audit_hooks.AuditEvent(
        actor="vera",
        action="demo.ensure.metadata",
        entity="resource",
        before={},
        after={},
        ip_address="203.0.113.90",
        context_factory=context_factory,
    )

    updated, resolved_hash, resolved_context = event.ensure_resolved_metadata(hooks)

    assert updated is not event
    assert updated.ip_hash == "hashed:203.0.113.90"
    assert updated.ip_address == "203.0.113.90"
    assert updated.context == {"factory_calls": 1}
    assert updated.context_factory is context_factory
    assert resolved_hash.value == "hashed:203.0.113.90"
    assert resolved_hash.fallback is False
    assert resolved_hash.error is None
    assert resolved_context.value == {"factory_calls": 1}
    assert resolved_context.error is None
    assert resolved_context.evaluated is True
    assert hash_calls == ["203.0.113.90"]
    assert context_calls == {"count": 1}


def test_audit_event_ensure_resolved_metadata_can_drop_ip_and_factory_on_error():
    def failing_hash(value: Optional[str]) -> Optional[str]:
        raise RuntimeError("hash failure")

    context_calls = {"count": 0}

    def failing_context() -> Mapping[str, str]:
        context_calls["count"] += 1
        raise ValueError("context failure")

    hooks = audit_hooks.AuditHooks(log=None, hash_ip=failing_hash)

    event = audit_hooks.AuditEvent(
        actor="will",
        action="demo.ensure.metadata.failure",
        entity="resource",
        before={},
        after={},
        ip_address="198.51.100.55",
        context_factory=failing_context,
    )

    updated, resolved_hash, resolved_context = event.ensure_resolved_metadata(
        hooks,
        drop_ip_address=True,
        drop_context_factory=True,
    )

    fallback_hash = audit_hooks._hash_ip_fallback("198.51.100.55")
    assert updated.ip_address is None
    assert updated.ip_hash == fallback_hash
    assert updated.context is None
    assert updated.context_factory is None
    assert isinstance(resolved_hash.error, RuntimeError)
    assert resolved_hash.value == fallback_hash
    assert resolved_hash.fallback is True
    assert resolved_context.value is None
    assert isinstance(resolved_context.error, ValueError)
    assert resolved_context.evaluated is True
    assert context_calls == {"count": 1}


def test_audit_event_ensure_resolved_metadata_returns_self_when_cached():
    hooks = audit_hooks.AuditHooks(
        log=None,
        hash_ip=lambda value: pytest.fail("hash_ip should not run"),
    )

    context = {"cached": True}
    event = audit_hooks.AuditEvent(
        actor="ximena",
        action="demo.ensure.metadata.cached",
        entity="resource",
        before={},
        after={},
        ip_hash="cached-hash",
        context=context,
    )

    updated, resolved_hash, resolved_context = event.ensure_resolved_metadata(
        hooks,
        use_context_factory=False,
    )

    assert updated is event
    assert resolved_hash.value == "cached-hash"
    assert resolved_hash.fallback is False
    assert resolved_hash.error is None
    assert resolved_context.value is context
    assert resolved_context.error is None
    assert resolved_context.evaluated is True


def test_audit_event_ensure_resolved_all_metadata_updates_event_and_returns_results():
    hash_calls: List[Optional[str]] = []
    context_calls = {"count": 0}
    fallback_extra_calls = {"count": 0}

    def tracking_hash(value: Optional[str]) -> Optional[str]:
        hash_calls.append(value)
        return f"hashed:{value}"

    def context_factory() -> Mapping[str, int]:
        context_calls["count"] += 1
        return {"context_calls": context_calls["count"]}

    def fallback_extra_factory() -> Mapping[str, int]:
        fallback_extra_calls["count"] += 1
        return {"extra_calls": fallback_extra_calls["count"]}

    hooks = audit_hooks.AuditHooks(log=None, hash_ip=tracking_hash)

    event = audit_hooks.AuditEvent(
        actor="yara",
        action="demo.ensure.all-metadata",
        entity="resource",
        before={},
        after={},
        ip_address="203.0.113.14",
        context_factory=context_factory,
        fallback_extra_factory=fallback_extra_factory,
    )

    (
        updated,
        resolved_hash,
        resolved_context,
        resolved_fallback_extra,
    ) = event.ensure_resolved_all_metadata(hooks)

    assert updated is not event
    assert updated.ip_hash == "hashed:203.0.113.14"
    assert updated.ip_address == "203.0.113.14"
    assert updated.context == {"context_calls": 1}
    assert updated.context_factory is context_factory
    assert updated.fallback_extra == {"extra_calls": 1}
    assert updated.fallback_extra_factory is fallback_extra_factory
    assert resolved_hash.value == "hashed:203.0.113.14"
    assert resolved_hash.fallback is False
    assert resolved_hash.error is None
    assert resolved_context.value == {"context_calls": 1}
    assert resolved_context.error is None
    assert resolved_context.evaluated is True
    assert resolved_fallback_extra.value == {"extra_calls": 1}
    assert resolved_fallback_extra.error is None
    assert resolved_fallback_extra.evaluated is True
    assert hash_calls == ["203.0.113.14"]
    assert context_calls == {"count": 1}
    assert fallback_extra_calls == {"count": 1}


def test_audit_event_ensure_resolved_all_metadata_honours_drop_and_failure_flags():
    def failing_hash(value: Optional[str]) -> Optional[str]:
        raise RuntimeError("hash failure")

    context_calls = {"count": 0}

    def failing_context() -> Mapping[str, str]:
        context_calls["count"] += 1
        raise ValueError("context failure")

    fallback_extra_calls = {"count": 0}

    def failing_fallback_extra() -> Mapping[str, str]:
        fallback_extra_calls["count"] += 1
        raise KeyError("extra failure")

    hooks = audit_hooks.AuditHooks(log=None, hash_ip=failing_hash)

    event = audit_hooks.AuditEvent(
        actor="zane",
        action="demo.ensure.all-metadata.failure",
        entity="resource",
        before={},
        after={},
        ip_address="198.51.100.101",
        context_factory=failing_context,
        fallback_extra_factory=failing_fallback_extra,
    )

    (
        updated,
        resolved_hash,
        resolved_context,
        resolved_fallback_extra,
    ) = event.ensure_resolved_all_metadata(
        hooks,
        drop_ip_address=True,
        drop_context_factory=True,
        use_context_factory=True,
        use_fallback_extra_factory=True,
        drop_fallback_extra=True,
        drop_fallback_extra_factory=True,
    )

    fallback_hash = audit_hooks._hash_ip_fallback("198.51.100.101")
    assert updated.ip_address is None
    assert updated.ip_hash == fallback_hash
    assert updated.context is None
    assert updated.context_factory is None
    assert updated.fallback_extra is None
    assert updated.fallback_extra_factory is None
    assert isinstance(resolved_hash.error, RuntimeError)
    assert resolved_hash.value == fallback_hash
    assert resolved_hash.fallback is True
    assert resolved_context.value is None
    assert isinstance(resolved_context.error, ValueError)
    assert resolved_context.evaluated is True
    assert resolved_fallback_extra.value is None
    assert isinstance(resolved_fallback_extra.error, KeyError)
    assert resolved_fallback_extra.evaluated is True
    assert context_calls == {"count": 1}
    assert fallback_extra_calls == {"count": 1}


def test_audit_event_resolve_ip_hash_preserves_fallback_metadata():
    def failing_hash(value: Optional[str]) -> Optional[str]:
        raise RuntimeError("hash failure")

    hooks = audit_hooks.AuditHooks(log=None, hash_ip=failing_hash)

    event = audit_hooks.AuditEvent(
        actor="quinn",
        action="demo.event.fallback",
        entity="resource",
        before={},
        after={},
        ip_address="198.51.100.9",
    )

    resolved = event.resolve_ip_hash(hooks)

    assert resolved.value == audit_hooks._hash_ip_fallback("198.51.100.9")
    assert resolved.fallback is True
    assert isinstance(resolved.error, RuntimeError)


def assert_no_fallback_extra(result: audit_hooks.AuditLogResult) -> None:
    """Assert that no fallback extra metadata was produced."""

    assert result.fallback_extra is None
    assert result.fallback_extra_error is None
    assert result.fallback_extra_evaluated is False

