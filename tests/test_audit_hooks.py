import builtins
import hashlib
import logging

from typing import Iterator, List, Mapping, Optional

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


def test_audit_log_result_truthiness():
    success = audit_hooks.AuditLogResult(
        handled=True,
        ip_hash="hash",
        hash_fallback=False,
        hash_error=None,
    )
    failure = audit_hooks.AuditLogResult(
        handled=False,
        ip_hash=None,
        hash_fallback=False,
        hash_error=None,
        log_error=None,
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
    assert factory_calls == []


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
    assert factory_calls == []


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
    assert factory_calls == [1]
    record = next(
        entry
        for entry in caplog.records
        if entry.message == "audit disabled via event context factory"
    )
    assert record.audit == {"source": "event"}


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

    assert result.handled is True
    assert result.hash_fallback is True
    assert isinstance(result.hash_error, RuntimeError)
    assert result.log_error is None
    expected_hash = audit_hooks._hash_ip_fallback("192.168.0.10")
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
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.message == "Audit logging disabled"
    assert getattr(record, "request_id", None) == "req-123"


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
        disabled_message="disabled",
        disabled_level=logging.INFO,
        context={"request_id": "abc"},
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
    assert factory_calls == []


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
