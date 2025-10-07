import builtins
import hashlib

import pytest

from shared import audit_hooks


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "anonymous"),
        ("127.0.0.1", hashlib.sha256("127.0.0.1".encode("utf-8")).hexdigest()),
    ],
)
def test_hash_ip_fallback_produces_stable_hash(value, expected):
    assert audit_hooks._hash_ip_fallback(value) == expected


def test_load_audit_hooks_degrades_when_dependency_missing(monkeypatch):
    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "common.utils.audit_logger":
            raise ModuleNotFoundError("audit logger unavailable")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    hooks = audit_hooks.load_audit_hooks()

    assert hooks.log is None
    assert hooks.hash_ip(None) == "anonymous"
    assert hooks.hash_ip("127.0.0.1") == hashlib.sha256("127.0.0.1".encode("utf-8")).hexdigest()
