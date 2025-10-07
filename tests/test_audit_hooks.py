import builtins
import hashlib
import logging

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

    handled = hooks.log_event(
        actor="alice",
        action="test.action",
        entity="resource",
        before={},
        after={},
        ip_address="127.0.0.1",
    )

    assert handled is False


def test_log_event_records_with_ip_hash():
    calls = []

    def fake_log(**payload):
        calls.append(payload)

    hooks = audit_hooks.AuditHooks(log=fake_log, hash_ip=lambda value: f"hashed:{value}")

    handled = hooks.log_event(
        actor="bob",
        action="demo.action",
        entity="resource",
        before={"before": True},
        after={"after": True},
        ip_address="10.0.0.5",
    )

    assert handled is True
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

    handled = hooks.log_event(
        actor="carol",
        action="demo.override",
        entity="resource",
        before={},
        after={},
        ip_hash="explicit-hash",
    )

    assert handled is True
    assert captured["ip_hash"] == "explicit-hash"


def test_log_event_with_fallback_logs_when_disabled(caplog: pytest.LogCaptureFixture):
    hooks = audit_hooks.AuditHooks(log=None, hash_ip=lambda value: "hash")

    logger = logging.getLogger("test.audit.disabled")
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        handled = audit_hooks.log_event_with_fallback(
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

    assert handled is False
    assert any(record.message == "audit disabled" for record in caplog.records)


def test_log_event_with_fallback_handles_exceptions(caplog: pytest.LogCaptureFixture):
    def failing_log(**payload):
        raise RuntimeError("boom")

    hooks = audit_hooks.AuditHooks(log=failing_log, hash_ip=lambda value: "hash")
    logger = logging.getLogger("test.audit.errors")

    with caplog.at_level(logging.ERROR, logger=logger.name):
        handled = audit_hooks.log_event_with_fallback(
            hooks,
            logger,
            actor="erin",
            action="demo.error",
            entity="resource",
            before={},
            after={},
            failure_message="failed to record",
        )

    assert handled is False
    assert any(
        record.levelno == logging.ERROR and "failed to record" in record.message
        for record in caplog.records
    )


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
