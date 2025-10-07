import builtins
import hashlib

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
    audit_hooks.load_audit_hooks.cache_clear()
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

    audit_hooks.load_audit_hooks.cache_clear()


def test_load_audit_hooks_caches_optional_import(monkeypatch):
    audit_hooks.load_audit_hooks.cache_clear()
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

    audit_hooks.load_audit_hooks.cache_clear()


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
