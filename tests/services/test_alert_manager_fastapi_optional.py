from __future__ import annotations
import builtins
import importlib
import sys
from typing import Iterable

import pytest


def _purge_modules(prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name, None)


def test_setup_alerting_without_fastapi(monkeypatch: pytest.MonkeyPatch) -> None:
    """services.alert_manager should configure metrics when FastAPI is missing."""

    _purge_modules(["fastapi", "services.alert_manager"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    alert_manager = importlib.import_module("services.alert_manager")

    app = alert_manager.FastAPI()
    alert_manager.setup_alerting(app, alertmanager_url="http://alertmanager.local")

    manager = alert_manager.get_alert_manager_instance()

    assert manager is not None
    assert getattr(app.state, "alert_manager", None) is manager

    # Ensure repeated configuration resets the manager without raising errors.
    alert_manager.setup_alerting(app, alertmanager_url=None)
    assert getattr(app.state, "alert_manager", None) is alert_manager.get_alert_manager_instance()
