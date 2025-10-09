"""Regression tests for the kill switch notifications optional dependencies."""

from __future__ import annotations

import importlib
import sys

import pytest


@pytest.fixture
def kill_alerts_without_requests(monkeypatch):
    """Reload the :mod:`kill_alerts` module without the requests dependency."""

    module_name = "kill_alerts"
    original_module = sys.modules.pop(module_name, None)
    monkeypatch.setitem(sys.modules, "requests", None)
    module = importlib.import_module(module_name)
    yield module
    sys.modules.pop(module_name, None)
    if original_module is not None:
        sys.modules[module_name] = original_module
    else:
        importlib.import_module(module_name)


def test_kill_alerts_requires_requests(kill_alerts_without_requests):
    module = kill_alerts_without_requests
    with pytest.raises(module.MissingDependencyError, match="requests is required"):
        module._require_requests()


def test_notification_helpers_raise_without_requests(kill_alerts_without_requests, monkeypatch):
    module = kill_alerts_without_requests
    monkeypatch.setenv("KILL_ALERT_EMAIL_API_KEY", "token")
    monkeypatch.setenv("KILL_ALERT_EMAIL_FROM", "from@example.com")
    monkeypatch.setenv("KILL_ALERT_EMAIL_TO", "to@example.com")
    with pytest.raises(module.MissingDependencyError):
        module._send_email({"account_id": "acct", "reason_code": "RC", "triggered_at": "now"})
