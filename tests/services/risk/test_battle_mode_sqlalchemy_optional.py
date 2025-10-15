from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from typing import Iterator

import pytest


def _clear_sqlalchemy_modules() -> None:
    for name in list(sys.modules):
        if name == "sqlalchemy" or name.startswith("sqlalchemy."):
            sys.modules.pop(name, None)


def _install_optional_helpers() -> None:
    from shared.common_bootstrap import ensure_common_helpers

    ensure_common_helpers()


@pytest.fixture(autouse=True)
def _reset_sqlalchemy() -> Iterator[None]:
    _clear_sqlalchemy_modules()
    _install_optional_helpers()
    yield
    _clear_sqlalchemy_modules()


def test_battle_mode_controller_uses_in_memory_fallback() -> None:
    battle_mode = importlib.import_module("battle_mode")
    battle_mode = importlib.reload(battle_mode)

    calls = {"count": 0}

    @contextmanager
    def _unused_session():
        calls["count"] += 1
        yield object()

    controller = battle_mode.BattleModeController(lambda: _unused_session(), threshold=0.75)

    assert getattr(controller, "_use_in_memory") is True

    initial = controller.status(" account-123 ")
    assert initial.account_id == "account-123"
    assert initial.engaged is False
    assert initial.reason is None

    engaged = controller.manual_toggle("account-123", engage=True, reason="manual override")
    assert engaged.engaged is True
    assert engaged.reason == "manual override"
    assert engaged.entered_at is not None

    controller.auto_update("account-123", volatility=2.5, metric="realised_vol")
    still_engaged = controller.status("account-123")
    assert still_engaged.engaged is True
    assert still_engaged.reason
    assert still_engaged.last_updated is not None

    controller.auto_update("account-123", volatility=0.1, metric="realised_vol", reason="volatility normalised")
    disengaged = controller.status("account-123")
    assert disengaged.engaged is False
    assert disengaged.exited_at is not None
    assert "exit" in (disengaged.reason or "")

    record = controller._load_last_record("account-123")
    assert record is not None
    assert record.exited_at is not None

    assert calls["count"] == 0

    battle_mode.create_battle_mode_tables(object())
