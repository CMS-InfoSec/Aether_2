import builtins
import hashlib
import logging

from typing import Optional

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
